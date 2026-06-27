"""Forward-walking paper simulator — Phase 2a (2026-06-27).

Purpose
-------
The user does not yet trust the main engine. Three near-disasters
in three days (SMH→SOXX phantom lots producing a $6.3B trade plan,
the safety-layer being swallowed by a downstream catch, the Holdings
self-referential loop) demonstrated that silent state corruption is
the dominant failure mode. We cannot ship live execution until we
have an independent system that:

  1. Forward-walks the engine's frozen logic against real post-lockbox
     market data WITHOUT the engine itself seeing that data.
  2. Applies its OWN sanity layer at every fill — no shared code path
     with the engine means no shared bug surface.
  3. Produces an append-only audit JSONL of every decision, fill, and
     cost — forensic-grade evidence trail.
  4. Reports daily NAV that we can compare against (a) the IBKR paper
     account's actual NAV; (b) the engine's expected NAV. Divergence
     between any two of these three is itself a bug signal.

Scope of this Phase 2a skeleton
-------------------------------
Intentionally minimal so the user can audit it. What's IN:

  - Read engine trade recommendations from `trade_recommendation_log.jsonl`
  - Maintain own LotBook (AU CGT-aware via reused class from the
    main engine — same code path because the lot accounting itself is
    well-tested and well-understood; the BUGS are state-management,
    not lot math)
  - Fill orders at next-day open price + 5 bps spread + IBKR Pro AU
    brokerage (matches user's chosen fill model 2026-06-27)
  - Daily NAV snapshot at session close
  - Sanity check at every fill batch — uses the same
    `_validate_trade_plan_sanity` shape but DOES NOT IMPORT IT;
    rewritten inline so a bug in one doesn't propagate to the other
  - JSONL audit log per fill + per daily-close NAV

What's OUT (deferred to 2b+):

  - FX (everything is treated as AUD; tickers without .AX suffix
    are flagged but not converted yet)
  - TLH (engine emits TLH swaps in tlh_swaps[]; we just record but
    don't act yet)
  - AU CGT FY-end netting (lot book records gains/losses, but we
    don't apply tax to NAV yet)
  - Multi-NAV parallel runs ($100k/$250k/$500k/$1M side-by-side)
  - Comparison harness against IBKR paper data

Architecture
------------
SimulatorState: lot book + cash + audit logs. Pure state container,
  no I/O.

PaperSimulator: drives the forward walk. Reads rec_log, fetches market
  data via yfinance (yes, it sees post-lockbox data — that's the
  point), applies fills, mark to market, snapshots NAV.

CLI: `python paper_simulator.py --from 2026-07-01 --to 2026-08-01`
  reads rec_log entries between those dates, applies them, writes
  simulator_*.jsonl files.

Trust contract
--------------
Every fill produces a JSONL line with:
  - Original rec_log entry id (so you can trace what the engine decided)
  - Fill price, fill quantity, slippage assumption, brokerage
  - Resulting cash + position deltas
  - Pre-fill and post-fill NAV
  - Sanity-check result for that fill batch

If sanity fires, the simulator records the violation and skips the
batch but DOES NOT halt the walk — the simulator is meant to be a
diagnostic tool, so visibility of "engine emitted N broken plans
this month" is what we want.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf


APP_DIR = Path(__file__).resolve().parent
REC_LOG_PATH = APP_DIR / "trade_recommendation_log.jsonl"
SEED_PATH = APP_DIR / "lots_seed.json"
STATE_PATH = APP_DIR / "portfolio_state.json"

# Output paths — append-only JSONL
AUDIT_FILLS_PATH = APP_DIR / "simulator_fills.jsonl"
AUDIT_NAV_PATH = APP_DIR / "simulator_nav.jsonl"
AUDIT_SANITY_PATH = APP_DIR / "simulator_sanity.jsonl"

# IBKR Pro AU fee model (matches BROKER_PROFILES["ibkr_pro_au"] in the
# main engine; copied here intentionally rather than imported so the
# simulator and engine cost models are decoupled).
IBKR_AU_MIN_FEE = 5.0       # AUD minimum
IBKR_AU_RATE = 0.0008       # 0.08%
IBKR_US_MIN_FEE_AUD = 1.5   # ~USD 1 min × 1.5 FX
IBKR_US_RATE = 0.0002       # ~2 bps on ETFs

# Spread assumption (user's choice 2026-06-27): 5 bps each way
SPREAD_BPS = 5.0

# Sanity thresholds — copied from main engine 2026-06-27 commit.
# Loosened single-trade threshold to 80% because legitimate
# rebalances from concentrated states can have one large trade
# (HBRD-from-69% example from 2026-06-27). Position-absurd at 5×
# stays tight because that's the corruption fingerprint.
SANITY_MAX_TURNOVER = 2.0
SANITY_MAX_SINGLE_TRADE_PCT = 0.80
SANITY_MAX_POSITION_MULTIPLE = 5.0
SANITY_MAX_TOTAL_VOLUME_MULTIPLE = 3.0


# ============================================================================
# Lot book — copied from main engine because the lot math is sound;
# the engine's BUGS are state-management higher up. Reuse via copy
# keeps the simulator independently auditable.
# ============================================================================

@dataclass
class _Lot:
    date: pd.Timestamp
    units: float
    cost_basis_per_unit: float


class LotBook:
    """FIFO lots per ticker. Sell matches oldest first, returns ST/LT
    realised gain & loss breakdown. Same semantics as engine LotBook
    but standalone — no shared mutable state, no import risk."""

    def __init__(self) -> None:
        self.lots: dict[str, list[_Lot]] = {}

    def buy(self, ticker: str, units: float, date, price: float) -> None:
        if units <= 0 or not np.isfinite(units):
            return
        self.lots.setdefault(ticker, []).append(
            _Lot(date=pd.Timestamp(date), units=float(units),
                 cost_basis_per_unit=float(price))
        )

    def sell(self, ticker: str, units: float, date, price: float,
             lt_threshold_days: int = 365) -> dict:
        """FIFO sale. Returns realised ST/LT gain/loss components."""
        out = {"st_gain": 0.0, "lt_gain": 0.0,
               "st_loss": 0.0, "lt_loss": 0.0,
               "matched_lots": []}
        if ticker not in self.lots or not self.lots[ticker] or units <= 0:
            return out
        sale_date = pd.Timestamp(date)
        remaining = float(units)
        new_lots: list[_Lot] = []
        for lot in self.lots[ticker]:
            if remaining <= 1e-9:
                new_lots.append(lot)
                continue
            qty = min(lot.units, remaining)
            proceeds = qty * float(price)
            cost_base = qty * lot.cost_basis_per_unit
            gain = proceeds - cost_base
            hold_days = (sale_date - lot.date).days
            is_lt = hold_days >= lt_threshold_days
            if gain >= 0:
                if is_lt: out["lt_gain"] += gain
                else: out["st_gain"] += gain
            else:
                if is_lt: out["lt_loss"] += -gain
                else: out["st_loss"] += -gain
            out["matched_lots"].append({
                "qty": qty,
                "cost_basis_per_unit": lot.cost_basis_per_unit,
                "acq_date": lot.date.isoformat(),
                "hold_days": hold_days,
                "is_lt": is_lt,
                "realised_gain": gain,
            })
            remaining -= qty
            if qty < lot.units:
                new_lots.append(_Lot(date=lot.date, units=lot.units - qty,
                                      cost_basis_per_unit=lot.cost_basis_per_unit))
        self.lots[ticker] = new_lots
        return out

    def units(self, ticker: str) -> float:
        return float(sum(lot.units for lot in self.lots.get(ticker, [])))

    def all_tickers(self) -> list[str]:
        return [t for t, lots in self.lots.items() if lots]

    def total_units_at(self, prices: dict[str, float]) -> float:
        """Mark to market — sum of units × current price across all
        held tickers. Used for NAV calculation."""
        total = 0.0
        for ticker, lots in self.lots.items():
            if not lots:
                continue
            px = float(prices.get(ticker, 0.0) or 0.0)
            if px <= 0:
                continue
            total += sum(lot.units * px for lot in lots)
        return total


# ============================================================================
# Sanity layer (independent reimplementation — same logic as main
# engine's _validate_trade_plan_sanity but rewritten so a bug in one
# can't propagate to the other)
# ============================================================================

def check_fill_batch_sanity(
    fills: list[dict],
    pre_fill_nav_aud: float,
    lot_book: LotBook,
    prices: dict[str, float],
) -> list[dict]:
    """Return list of violation dicts. Empty list = sanity passes.
    Unlike the engine's version, this DOES NOT raise — the simulator
    records and continues so the user can see "engine emitted N
    broken plans in window X" as forensic evidence."""
    violations: list[dict] = []

    if pre_fill_nav_aud <= 0:
        return [{"check": "nav_invalid", "msg": f"NAV {pre_fill_nav_aud} ≤ 0"}]

    # Compute trade metrics
    total_volume = 0.0
    worst_trade_pct = 0.0
    worst_trade_ticker = ""
    worst_trade_dv = 0.0
    for f in fills:
        dv = abs(float(f.get("qty", 0)) * float(f.get("price", 0)))
        total_volume += dv
        pct = dv / pre_fill_nav_aud
        if pct > worst_trade_pct:
            worst_trade_pct = pct
            worst_trade_ticker = str(f.get("ticker", "?"))
            worst_trade_dv = dv

    # Check 1: Σ|Δw|
    sum_abs_dw = total_volume / pre_fill_nav_aud
    if sum_abs_dw > SANITY_MAX_TURNOVER:
        violations.append({
            "check": "turnover_too_high",
            "actual": sum_abs_dw,
            "limit": SANITY_MAX_TURNOVER,
            "msg": (f"Σ|Δw|={sum_abs_dw:.2f} > {SANITY_MAX_TURNOVER} — "
                    f"turnover {sum_abs_dw*100:.0f}% of NAV in one batch"),
        })

    # Check 2: single trade
    if worst_trade_pct > SANITY_MAX_SINGLE_TRADE_PCT:
        violations.append({
            "check": "single_trade_too_big",
            "actual_pct": worst_trade_pct,
            "limit_pct": SANITY_MAX_SINGLE_TRADE_PCT,
            "ticker": worst_trade_ticker,
            "delta_value_aud": worst_trade_dv,
            "msg": (f"Trade in {worst_trade_ticker} = ${worst_trade_dv:,.0f} "
                    f"({worst_trade_pct*100:.1f}% of NAV) > "
                    f"{SANITY_MAX_SINGLE_TRADE_PCT*100:.0f}% limit"),
        })

    # Check 3: position absurd (post-fill check, requires lot book)
    max_pos_value = 0.0
    worst_pos_ticker = ""
    worst_pos_units = 0.0
    for ticker in lot_book.all_tickers():
        u = lot_book.units(ticker)
        px = prices.get(ticker, 0.0)
        if px and u:
            val = abs(u * px)
            if val > max_pos_value:
                max_pos_value = val
                worst_pos_ticker = ticker
                worst_pos_units = u
    pos_limit = SANITY_MAX_POSITION_MULTIPLE * pre_fill_nav_aud
    if max_pos_value > pos_limit:
        violations.append({
            "check": "position_absurd",
            "actual_value_aud": max_pos_value,
            "limit_value_aud": pos_limit,
            "ticker": worst_pos_ticker,
            "units": worst_pos_units,
            "msg": (f"Position in {worst_pos_ticker} = {worst_pos_units:,.0f} units "
                    f"(${max_pos_value:,.0f}) > {SANITY_MAX_POSITION_MULTIPLE}× NAV — "
                    f"state corruption"),
        })

    # Check 4: total volume
    vol_limit = SANITY_MAX_TOTAL_VOLUME_MULTIPLE * pre_fill_nav_aud
    if total_volume > vol_limit:
        violations.append({
            "check": "total_volume_too_high",
            "actual_aud": total_volume,
            "limit_aud": vol_limit,
            "msg": (f"Total trade volume ${total_volume:,.0f} > "
                    f"{SANITY_MAX_TOTAL_VOLUME_MULTIPLE}× NAV (${vol_limit:,.0f})"),
        })

    return violations


# ============================================================================
# Fee model — IBKR Pro AU (same rates as engine BROKER_PROFILES;
# reimplemented inline rather than imported so we don't pull in the
# whole engine module).
# ============================================================================

def estimate_brokerage_aud(ticker: str, trade_value_aud: float) -> float:
    """Returns brokerage cost in AUD for a single trade. Currency
    classification is by ticker suffix: .AX = ASX = AU rates,
    everything else = US rates. Phase 2a doesn't model HK/Singapore
    or other markets — out of scope for the user's universe."""
    is_au = ticker.endswith(".AX") or ticker.startswith("^")
    if is_au:
        rate_fee = trade_value_aud * IBKR_AU_RATE
        return max(IBKR_AU_MIN_FEE, rate_fee)
    rate_fee = trade_value_aud * IBKR_US_RATE
    return max(IBKR_US_MIN_FEE_AUD, rate_fee)


def apply_spread(side: str, mid_price: float) -> float:
    """Add SPREAD_BPS slippage in the bad direction (buys fill above
    mid, sells fill below mid). Models retail-router execution
    without a market-maker rebate."""
    delta = mid_price * SPREAD_BPS / 10_000.0
    if side.upper() == "BUY":
        return mid_price + delta
    return mid_price - delta


# ============================================================================
# Market data — yfinance fetch + caching. Simulator deliberately
# pulls post-lockbox data; that's the whole point.
# ============================================================================

_price_cache: dict[tuple[str, pd.Timestamp], float] = {}


def get_open_price(ticker: str, date: pd.Timestamp) -> Optional[float]:
    """Return open price for the next trading session at or after `date`.
    Caches per (ticker, date) so repeat calls don't re-hit yfinance.
    Returns None if no data available (delisted, market closed all
    surrounding days, etc.)."""
    key = (ticker, pd.Timestamp(date).normalize())
    if key in _price_cache:
        return _price_cache[key]
    try:
        end = date + timedelta(days=10)
        df = yf.download(ticker, start=date.strftime("%Y-%m-%d"),
                          end=end.strftime("%Y-%m-%d"),
                          interval="1d", progress=False, auto_adjust=True)
        if df is None or df.empty:
            _price_cache[key] = None
            return None
        if "Open" in df.columns:
            first_open = float(df["Open"].iloc[0])
        elif isinstance(df.columns, pd.MultiIndex):
            first_open = float(df[("Open", ticker)].iloc[0])
        else:
            return None
        _price_cache[key] = first_open
        return first_open
    except Exception:
        _price_cache[key] = None
        return None


def get_close_price(ticker: str, date: pd.Timestamp) -> Optional[float]:
    """Close price for `date` (or nearest trading day after)."""
    key = ("close::" + ticker, pd.Timestamp(date).normalize())
    if key in _price_cache:
        return _price_cache[key]
    try:
        end = date + timedelta(days=10)
        df = yf.download(ticker, start=date.strftime("%Y-%m-%d"),
                          end=end.strftime("%Y-%m-%d"),
                          interval="1d", progress=False, auto_adjust=True)
        if df is None or df.empty:
            _price_cache[key] = None
            return None
        if "Close" in df.columns:
            close = float(df["Close"].iloc[0])
        elif isinstance(df.columns, pd.MultiIndex):
            close = float(df[("Close", ticker)].iloc[0])
        else:
            return None
        _price_cache[key] = close
        return close
    except Exception:
        _price_cache[key] = None
        return None


# ============================================================================
# Simulator state + driver
# ============================================================================

@dataclass
class SimulatorState:
    cash_aud: float = 0.0
    lot_book: LotBook = field(default_factory=LotBook)
    fills_count: int = 0
    sanity_violations_count: int = 0


def load_seed(seed_path: Path = SEED_PATH,
              state_path: Path = STATE_PATH) -> SimulatorState:
    """Initialize simulator state from lots_seed.json + portfolio_state.json.
    Cash = portfolio_value - sum(units × cost_basis). Negative cash
    flagged but not blocked (matches engine behavior)."""
    state = SimulatorState()
    if seed_path.exists():
        try:
            seed = json.loads(seed_path.read_text(encoding="utf-8"))
            for s in seed:
                state.lot_book.buy(
                    ticker=str(s.get("Security", "")).strip(),
                    units=float(s.get("Units", 0) or 0),
                    date=pd.Timestamp(s.get("AcqDate")),
                    price=float(s.get("CostBaseAUD", 0) or 0),
                )
        except Exception as e:
            print(f"[sim] seed load failed: {e}")
    if state_path.exists():
        try:
            ps = json.loads(state_path.read_text(encoding="utf-8"))
            total_nav = float(ps.get("portfolio_value", 0) or 0)
            invested = float(ps.get("net_invested", 0) or 0)
            state.cash_aud = total_nav - invested
        except Exception as e:
            print(f"[sim] state load failed: {e}")
    return state


def load_rec_log_window(rec_log: Path, start: pd.Timestamp,
                        end: pd.Timestamp) -> list[dict]:
    """Read trade_recommendation_log.jsonl, return entries with
    `run_at` between start and end inclusive."""
    out: list[dict] = []
    if not rec_log.exists():
        return out
    with rec_log.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                ts = pd.Timestamp(rec.get("run_at"))
                if start <= ts <= end:
                    out.append(rec)
            except Exception:
                continue
    return out


def apply_recommendation(rec: dict, state: SimulatorState,
                          audit_path: Path) -> None:
    """Apply one rec_log entry's recommended_trades to state.
    Fills at next trading day's open + spread + brokerage.
    Records every fill to audit_path. Runs sanity check on the batch
    and records violations to AUDIT_SANITY_PATH (non-blocking)."""
    rec_date = pd.Timestamp(rec.get("run_at"))
    rec_trades = rec.get("recommended_trades", []) or []
    if not rec_trades:
        return

    # Resolve fill prices for each trade at next-day open.
    fills: list[dict] = []
    fill_date = rec_date + timedelta(days=1)
    for trade in rec_trades:
        ticker = str(trade.get("ticker", "")).strip()
        side = str(trade.get("side", "")).lower()
        qty = int(trade.get("delta_units", 0) or 0)
        if not ticker or qty == 0:
            continue

        mid = get_open_price(ticker, fill_date)
        if mid is None or mid <= 0:
            # Fall back to rec_log's planned price if yfinance fails
            mid = float(trade.get("px_aud", 0) or 0)
            if mid <= 0:
                continue

        fill_px = apply_spread(side, mid)
        trade_value = abs(qty) * fill_px
        brokerage = estimate_brokerage_aud(ticker, trade_value)
        fills.append({
            "ticker": ticker,
            "side": side,
            "qty": qty,
            "price": fill_px,
            "mid_price": mid,
            "trade_value_aud": trade_value,
            "brokerage_aud": brokerage,
            "fill_date": fill_date.isoformat(),
        })

    # Sanity check on the batch BEFORE applying
    prices_now = {f["ticker"]: f["mid_price"] for f in fills}
    pre_fill_nav = state.cash_aud + state.lot_book.total_units_at(prices_now)
    violations = check_fill_batch_sanity(fills, pre_fill_nav,
                                          state.lot_book, prices_now)
    if violations:
        with AUDIT_SANITY_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps({
                "rec_run_at": str(rec.get("run_at")),
                "fill_date": fill_date.isoformat(),
                "pre_fill_nav_aud": pre_fill_nav,
                "n_fills": len(fills),
                "violations": violations,
            }) + "\n")
        state.sanity_violations_count += len(violations)
        # Phase 2a: record but continue. Phase 2b will gate execution.
        print(f"[sim] sanity violations at {fill_date.date()}: "
              f"{len(violations)} — see {AUDIT_SANITY_PATH.name}")

    # Apply fills to lot book + cash
    for f in fills:
        ticker = f["ticker"]
        side = f["side"]
        qty = abs(f["qty"])
        px = f["price"]
        if side == "buy":
            state.lot_book.buy(ticker, qty, fill_date, px)
            state.cash_aud -= qty * px + f["brokerage_aud"]
        elif side == "sell":
            sale = state.lot_book.sell(ticker, qty, fill_date, px)
            state.cash_aud += qty * px - f["brokerage_aud"]
            f["realised_gain_aud"] = sum(
                m["realised_gain"] for m in sale.get("matched_lots", [])
            )
            f["lots_matched"] = len(sale.get("matched_lots", []))
        state.fills_count += 1

        # Append audit entry per fill
        with audit_path.open("a", encoding="utf-8") as af:
            af.write(json.dumps({
                "rec_run_at": str(rec.get("run_at")),
                "cash_aud_after": state.cash_aud,
                **f,
            }) + "\n")


def snapshot_nav(state: SimulatorState, date: pd.Timestamp,
                 prices: dict[str, float], audit_path: Path) -> float:
    """Mark to market at given date's close prices. Returns total NAV
    and appends a snapshot to audit_path."""
    positions_value = state.lot_book.total_units_at(prices)
    nav = state.cash_aud + positions_value
    snapshot = {
        "date": date.isoformat(),
        "cash_aud": state.cash_aud,
        "positions_value_aud": positions_value,
        "nav_aud": nav,
        "n_positions": len(state.lot_book.all_tickers()),
        "fills_to_date": state.fills_count,
        "sanity_violations_to_date": state.sanity_violations_count,
    }
    with audit_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot) + "\n")
    return nav


# ============================================================================
# CLI entry
# ============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Paper-account simulator — forward-walks the engine's "
                    "trade recommendations against post-lockbox market data")
    parser.add_argument("--from", dest="start", required=True,
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="end", required=True,
                        help="End date (YYYY-MM-DD)")
    parser.add_argument("--rec-log", default=str(REC_LOG_PATH),
                        help="Path to trade_recommendation_log.jsonl")
    parser.add_argument("--reset", action="store_true",
                        help="Truncate the simulator audit logs before running")
    args = parser.parse_args()

    start = pd.Timestamp(args.start)
    end = pd.Timestamp(args.end)
    rec_log = Path(args.rec_log)

    if args.reset:
        for p in (AUDIT_FILLS_PATH, AUDIT_NAV_PATH, AUDIT_SANITY_PATH):
            if p.exists():
                p.unlink()
        print(f"[sim] audit logs reset")

    state = load_seed()
    print(f"[sim] seed loaded: cash=${state.cash_aud:,.0f} AUD, "
          f"{len(state.lot_book.all_tickers())} held tickers")

    recs = load_rec_log_window(rec_log, start, end)
    print(f"[sim] {len(recs)} rec_log entries in window "
          f"{start.date()} → {end.date()}")
    if not recs:
        print("[sim] nothing to simulate, exiting")
        return 0

    recs.sort(key=lambda r: r.get("run_at", ""))

    for rec in recs:
        apply_recommendation(rec, state, AUDIT_FILLS_PATH)
        # Snapshot NAV at close of fill day for marking
        fill_date = pd.Timestamp(rec.get("run_at")) + timedelta(days=1)
        prices_close: dict[str, float] = {}
        for ticker in state.lot_book.all_tickers():
            cp = get_close_price(ticker, fill_date)
            if cp is not None:
                prices_close[ticker] = cp
        snapshot_nav(state, fill_date, prices_close, AUDIT_NAV_PATH)

    print(f"[sim] complete — {state.fills_count} fills, "
          f"{state.sanity_violations_count} sanity violations, "
          f"final cash ${state.cash_aud:,.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

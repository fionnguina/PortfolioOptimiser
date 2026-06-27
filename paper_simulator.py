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

# Output paths — append-only JSONL. Multi-NAV mode suffixes label.
def audit_paths(label: str = "default") -> dict[str, Path]:
    """Return the trio of audit log paths for a given simulator label.
    Default label = "default" (no suffix). Multi-NAV mode passes
    label = "100k" / "250k" / etc. so each NAV's logs stay isolated."""
    suffix = "" if label == "default" else f"_{label}"
    return {
        "fills":  APP_DIR / f"simulator_fills{suffix}.jsonl",
        "nav":    APP_DIR / f"simulator_nav{suffix}.jsonl",
        "sanity": APP_DIR / f"simulator_sanity{suffix}.jsonl",
    }

# Live broker NAV history — written by the engine's drift tracker.
# Used by --compare mode for divergence diagnostics.
LIVE_NAV_PATH = APP_DIR / "live_nav_history.jsonl"

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
_fx_cache: dict[pd.Timestamp, float] = {}


def _is_aud_native(ticker: str) -> bool:
    """ASX tickers (.AX) and AU index (^AORD) are AUD-denominated; everything
    else (US ETFs, indices like ^GSPC) is USD."""
    return ticker.endswith(".AX") or ticker.startswith("^AORD")


def get_aud_per_usd(date: pd.Timestamp) -> float:
    """Returns AUD per USD on the given date. Caches per-day so a
    daily-rebalance walk doesn't hit yfinance more than once per
    trading session. Defaults to a conservative 1.50 if the fetch
    fails — same fallback the engine uses elsewhere."""
    key = pd.Timestamp(date).normalize()
    if key in _fx_cache:
        return _fx_cache[key]
    try:
        end = date + timedelta(days=10)
        df = yf.download("USDAUD=X", start=date.strftime("%Y-%m-%d"),
                          end=end.strftime("%Y-%m-%d"),
                          interval="1d", progress=False, auto_adjust=True)
        if df is None or df.empty:
            _fx_cache[key] = 1.50
            return 1.50
        if "Close" in df.columns:
            fx = float(df["Close"].iloc[0])
        elif isinstance(df.columns, pd.MultiIndex):
            fx = float(df[("Close", "USDAUD=X")].iloc[0])
        else:
            _fx_cache[key] = 1.50
            return 1.50
        if not np.isfinite(fx) or fx <= 0:
            fx = 1.50
        _fx_cache[key] = fx
        return fx
    except Exception:
        _fx_cache[key] = 1.50
        return 1.50


def _fetch_price_native(ticker: str, date: pd.Timestamp,
                         field: str = "Open") -> Optional[float]:
    """Internal: pull price in the ticker's native currency. AUD for
    .AX/^AORD, USD for everything else."""
    try:
        end = date + timedelta(days=10)
        df = yf.download(ticker, start=date.strftime("%Y-%m-%d"),
                          end=end.strftime("%Y-%m-%d"),
                          interval="1d", progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None
        if field in df.columns:
            return float(df[field].iloc[0])
        if isinstance(df.columns, pd.MultiIndex):
            return float(df[(field, ticker)].iloc[0])
        return None
    except Exception:
        return None


def get_open_price(ticker: str, date: pd.Timestamp) -> Optional[float]:
    """Return open price for the next trading session at or after `date`,
    converted to AUD for non-AUD tickers. Caches per (ticker, date)
    so a daily-rebalance walk doesn't re-hit yfinance."""
    key = (ticker, pd.Timestamp(date).normalize())
    if key in _price_cache:
        return _price_cache[key]
    native = _fetch_price_native(ticker, date, field="Open")
    if native is None:
        _price_cache[key] = None
        return None
    if _is_aud_native(ticker):
        aud_px = native
    else:
        fx = get_aud_per_usd(date)
        aud_px = native * fx
    _price_cache[key] = aud_px
    return aud_px


def get_close_price(ticker: str, date: pd.Timestamp) -> Optional[float]:
    """Close price for `date` in AUD."""
    key = ("close::" + ticker, pd.Timestamp(date).normalize())
    if key in _price_cache:
        return _price_cache[key]
    native = _fetch_price_native(ticker, date, field="Close")
    if native is None:
        _price_cache[key] = None
        return None
    if _is_aud_native(ticker):
        aud_px = native
    else:
        fx = get_aud_per_usd(date)
        aud_px = native * fx
    _price_cache[key] = aud_px
    return aud_px


# ============================================================================
# Simulator state + driver
# ============================================================================

@dataclass
class SimulatorState:
    cash_aud: float = 0.0
    lot_book: LotBook = field(default_factory=LotBook)
    fills_count: int = 0
    sanity_violations_count: int = 0
    batches_rejected: int = 0
    label: str = "default"   # used by multi-NAV mode to suffix audit files


def load_seed(seed_path: Path = SEED_PATH,
              state_path: Path = STATE_PATH,
              starting_cash_override: Optional[float] = None,
              label: str = "default") -> SimulatorState:
    """Initialize simulator state.

    Two modes:
      - Default: read positions from lots_seed.json + cash from
        portfolio_state.json (matches the user's real broker state).
      - Cash-only (starting_cash_override set): no positions, all
        capital in cash. Used for multi-NAV runs that want to compare
        "start with $X cash on lockbox-date+1, let engine recommend
        from scratch" across multiple NAVs."""
    state = SimulatorState(label=label)
    if starting_cash_override is not None:
        state.cash_aud = float(starting_cash_override)
        return state
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
                          paths: dict[str, Path]) -> None:
    """Apply one rec_log entry's recommended_trades to state.
    Fills at next trading day's open + spread + brokerage.
    Records every fill to paths['fills']. Sanity violations record
    to paths['sanity'] AND block the batch from being applied
    (Phase 2b)."""
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

    # Sanity check on the batch BEFORE applying. Phase 2b: violations
    # BLOCK the fill batch — the rec_log entry is recorded as rejected,
    # cash and lot book are untouched, the walk advances. This matches
    # what the engine's sanity layer does (halt before side effects),
    # adapted for a non-halting simulator (we want to see how many
    # batches WOULD have been blocked over a window).
    prices_now = {f["ticker"]: f["mid_price"] for f in fills}
    pre_fill_nav = state.cash_aud + state.lot_book.total_units_at(prices_now)
    violations = check_fill_batch_sanity(fills, pre_fill_nav,
                                          state.lot_book, prices_now)
    if violations:
        with paths["sanity"].open("a", encoding="utf-8") as f:
            f.write(json.dumps({
                "label": state.label,
                "rec_run_at": str(rec.get("run_at")),
                "fill_date": fill_date.isoformat(),
                "pre_fill_nav_aud": pre_fill_nav,
                "n_fills": len(fills),
                "batch_rejected": True,
                "violations": violations,
            }) + "\n")
        state.sanity_violations_count += len(violations)
        state.batches_rejected += 1
        print(f"[sim:{state.label}] BATCH REJECTED at {fill_date.date()}: "
              f"{len(violations)} sanity violations — see {paths['sanity'].name}")
        return  # do not apply any of this batch's fills

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
        with paths["fills"].open("a", encoding="utf-8") as af:
            af.write(json.dumps({
                "label": state.label,
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
        "label": state.label,
        "date": date.isoformat(),
        "cash_aud": state.cash_aud,
        "positions_value_aud": positions_value,
        "nav_aud": nav,
        "n_positions": len(state.lot_book.all_tickers()),
        "fills_to_date": state.fills_count,
        "sanity_violations_to_date": state.sanity_violations_count,
        "batches_rejected_to_date": state.batches_rejected,
    }
    with audit_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot) + "\n")
    return nav


# ============================================================================
# CLI entry
# ============================================================================

def run_single(start: pd.Timestamp, end: pd.Timestamp, rec_log: Path,
                state: SimulatorState, reset: bool) -> SimulatorState:
    """Run one simulator instance over a date window. Used by both
    single-NAV (default) and multi-NAV modes."""
    paths = audit_paths(state.label)
    if reset:
        for p in paths.values():
            if p.exists():
                p.unlink()

    recs = load_rec_log_window(rec_log, start, end)
    print(f"[sim:{state.label}] seed: cash=${state.cash_aud:,.0f} AUD, "
          f"{len(state.lot_book.all_tickers())} held tickers · "
          f"{len(recs)} rec_log entries in window "
          f"{start.date()} → {end.date()}")
    if not recs:
        return state
    recs.sort(key=lambda r: r.get("run_at", ""))

    for rec in recs:
        apply_recommendation(rec, state, paths)
        fill_date = pd.Timestamp(rec.get("run_at")) + timedelta(days=1)
        prices_close: dict[str, float] = {}
        for ticker in state.lot_book.all_tickers():
            cp = get_close_price(ticker, fill_date)
            if cp is not None:
                prices_close[ticker] = cp
        snapshot_nav(state, fill_date, prices_close, paths["nav"])

    print(f"[sim:{state.label}] done — {state.fills_count} fills, "
          f"{state.batches_rejected} batches rejected, "
          f"{state.sanity_violations_count} violations, "
          f"final cash ${state.cash_aud:,.0f}")
    return state


def compare_to_live_nav(simulator_label: str = "default") -> None:
    """Post-hoc comparison: align simulator NAV time series against
    live_nav_history.jsonl (engine drift tracker's actual broker
    NAV record). Prints divergence per common date + summary stats.
    Does NOT run a simulation — read-only analysis of existing logs."""
    sim_paths = audit_paths(simulator_label)
    sim_nav_path = sim_paths["nav"]
    if not sim_nav_path.exists():
        print(f"[compare] {sim_nav_path.name} missing — run a simulation first")
        return
    if not LIVE_NAV_PATH.exists():
        print(f"[compare] {LIVE_NAV_PATH.name} missing — engine hasn't logged "
              f"live NAV yet (needs LIVE_TRADING_START_DATE to be reached)")
        return

    def _read_jsonl(p: Path) -> pd.DataFrame:
        rows = []
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
        return pd.DataFrame(rows)

    sim_df = _read_jsonl(sim_nav_path)
    live_df = _read_jsonl(LIVE_NAV_PATH)
    if sim_df.empty or live_df.empty:
        print(f"[compare] one of the logs is empty: "
              f"sim={len(sim_df)} live={len(live_df)}")
        return

    # Normalise date columns. live_nav_history.jsonl writes a different
    # schema than simulator_nav.jsonl — be defensive.
    def _to_date_series(df: pd.DataFrame) -> pd.Series:
        for col in ("date", "timestamp", "as_of"):
            if col in df.columns:
                return pd.to_datetime(df[col]).dt.date
        raise ValueError(f"no date column in {df.columns.tolist()}")

    def _to_nav_series(df: pd.DataFrame) -> pd.Series:
        for col in ("nav_aud", "nav", "portfolio_value_aud", "portfolio_value"):
            if col in df.columns:
                return pd.to_numeric(df[col], errors="coerce")
        raise ValueError(f"no nav column in {df.columns.tolist()}")

    try:
        sim_df["_date"] = _to_date_series(sim_df)
        sim_df["_nav"]  = _to_nav_series(sim_df)
        live_df["_date"] = _to_date_series(live_df)
        live_df["_nav"]  = _to_nav_series(live_df)
    except Exception as e:
        print(f"[compare] schema problem: {e}")
        return

    # Per-day comparison (last sim NAV per day vs last live NAV per day).
    sim_daily = sim_df.groupby("_date")["_nav"].last().rename("sim")
    live_daily = live_df.groupby("_date")["_nav"].last().rename("live")
    joined = pd.concat([sim_daily, live_daily], axis=1, join="inner").dropna()

    if joined.empty:
        print("[compare] no overlapping dates between sim and live NAV logs")
        return

    joined["diff"] = joined["sim"] - joined["live"]
    joined["pct_diff"] = (joined["sim"] - joined["live"]) / joined["live"]
    print(f"[compare] {len(joined)} overlapping dates "
          f"{joined.index.min()} → {joined.index.max()}")
    print(f"  Mean abs diff:   ${joined['diff'].abs().mean():,.0f} AUD")
    print(f"  Max  abs diff:   ${joined['diff'].abs().max():,.0f} AUD")
    print(f"  Mean abs %:      {joined['pct_diff'].abs().mean()*100:.2f}%")
    print(f"  Max  abs %:      {joined['pct_diff'].abs().max()*100:.2f}%")
    print()
    print(joined.tail(10).to_string())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Paper-account simulator — forward-walks the engine's "
                    "trade recommendations against post-lockbox market data")
    parser.add_argument("--from", dest="start",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="end",
                        help="End date (YYYY-MM-DD)")
    parser.add_argument("--rec-log", default=str(REC_LOG_PATH),
                        help="Path to trade_recommendation_log.jsonl")
    parser.add_argument("--reset", action="store_true",
                        help="Truncate the simulator audit logs before running")
    parser.add_argument("--starting-cash", type=float, default=None,
                        help="Override seed: start with this much AUD cash "
                             "and no positions. Used for greenfield runs "
                             "from the lockbox date.")
    parser.add_argument("--multi-nav", action="store_true",
                        help="Run 4 simulations in parallel at $100k / $250k "
                             "/ $500k / $1M starting cash. Audit files get "
                             "NAV suffix. Same window for all four.")
    parser.add_argument("--multi-nav-amounts", default="100000,250000,500000,1000000",
                        help="Comma-separated NAVs for --multi-nav mode")
    parser.add_argument("--compare", action="store_true",
                        help="Post-hoc compare simulator_nav.jsonl vs "
                             "live_nav_history.jsonl. Does not simulate; "
                             "just analyses existing logs.")
    parser.add_argument("--compare-label", default="default",
                        help="Simulator label to compare against (for "
                             "multi-NAV runs you'd pass e.g. '1M')")
    args = parser.parse_args()

    if args.compare:
        compare_to_live_nav(args.compare_label)
        return 0

    if not args.start or not args.end:
        parser.error("--from and --to are required (unless --compare)")
        return 2

    start = pd.Timestamp(args.start)
    end = pd.Timestamp(args.end)
    rec_log = Path(args.rec_log)

    if args.multi_nav:
        try:
            navs = sorted({float(x.strip()) for x in args.multi_nav_amounts.split(",")
                            if x.strip()})
        except Exception as e:
            print(f"[sim] bad --multi-nav-amounts: {e}")
            return 2

        def _nav_label(nav: float) -> str:
            return (f"{int(nav/1_000_000)}M" if nav >= 1_000_000
                    else f"{int(nav/1000)}k")

        results: list[tuple[float, SimulatorState]] = []
        for nav in navs:
            label = _nav_label(nav)
            print(f"\n=== Multi-NAV run @ ${nav:,.0f} (label={label}) ===")
            state = load_seed(starting_cash_override=nav, label=label)
            state = run_single(start, end, rec_log, state, args.reset)
            results.append((nav, state))

        # Summary table
        print()
        print("=" * 88)
        print("Multi-NAV summary")
        print("=" * 88)
        print(f"{'NAV':>12}  {'Final Cash':>14}  {'Fills':>6}  {'Rejected':>9}  {'Violations':>11}")
        for nav, st in results:
            print(f"  ${nav:>10,.0f}  ${st.cash_aud:>13,.0f}  "
                  f"{st.fills_count:>6}  {st.batches_rejected:>9}  "
                  f"{st.sanity_violations_count:>11}")
        print("=" * 88)
        return 0

    # Single-NAV mode
    state = load_seed(starting_cash_override=args.starting_cash, label="default")
    run_single(start, end, rec_log, state, args.reset)
    return 0


if __name__ == "__main__":
    sys.exit(main())

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

# Sanity thresholds — two-tier (% of NAV AND absolute AUD) so large
# accounts aren't underprotected. The 2026-06-27 multi-NAV smoke test
# revealed that percentage-only thresholds let bigger absolute-dollar
# trades through at large NAVs — the same buggy rec_log window that
# blocked 100% of trades at $100k NAV blocked only 41% at $1M because
# $500k trades looked like 50% of $1M (within limit) vs 200% of $250k
# (rejected). Absolute caps backstop this.
SANITY_MAX_TURNOVER = 2.0
SANITY_MAX_SINGLE_TRADE_PCT = 0.80
SANITY_MAX_POSITION_MULTIPLE = 5.0
SANITY_MAX_TOTAL_VOLUME_MULTIPLE = 3.0
# Absolute AUD caps — calibrated so a $1M account legitimately
# rebalancing 5% to one ticker ($50k) passes, while the $542k HBRD
# trade that should have been blocked at $1M (54% of NAV, under the
# 80% pct limit) actually trips. Configurable per CLI flag.
SANITY_MAX_SINGLE_TRADE_AUD = 200_000.0
SANITY_MAX_TOTAL_VOLUME_AUD = 600_000.0
# AU CGT — long-term holding threshold for 50% discount eligibility
CGT_LT_THRESHOLD_DAYS = 365
CGT_LT_DISCOUNT = 0.50
CGT_MTR = 0.30  # 30% marginal tax rate (user's bracket)
# Australian financial year ends 30 June.
def _fy_end_for(date: pd.Timestamp) -> pd.Timestamp:
    """Return the 30 June that closes the FY containing `date`. e.g.
    2026-08-15 → 2027-06-30, 2026-05-15 → 2026-06-30."""
    d = pd.Timestamp(date)
    if d.month >= 7:
        return pd.Timestamp(year=d.year + 1, month=6, day=30)
    return pd.Timestamp(year=d.year, month=6, day=30)


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

    # Check 2: single trade (% of NAV)
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

    # Check 2b: single trade (absolute AUD) — backstop for large NAVs
    if worst_trade_dv > SANITY_MAX_SINGLE_TRADE_AUD:
        violations.append({
            "check": "single_trade_abs_too_big",
            "actual_aud": worst_trade_dv,
            "limit_aud": SANITY_MAX_SINGLE_TRADE_AUD,
            "ticker": worst_trade_ticker,
            "msg": (f"Trade in {worst_trade_ticker} = ${worst_trade_dv:,.0f} "
                    f"> ${SANITY_MAX_SINGLE_TRADE_AUD:,.0f} absolute cap"),
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

    # Check 4: total volume (% of NAV)
    vol_limit = SANITY_MAX_TOTAL_VOLUME_MULTIPLE * pre_fill_nav_aud
    if total_volume > vol_limit:
        violations.append({
            "check": "total_volume_too_high",
            "actual_aud": total_volume,
            "limit_aud": vol_limit,
            "msg": (f"Total trade volume ${total_volume:,.0f} > "
                    f"{SANITY_MAX_TOTAL_VOLUME_MULTIPLE}× NAV (${vol_limit:,.0f})"),
        })

    # Check 4b: total volume (absolute AUD) — backstop for large NAVs
    if total_volume > SANITY_MAX_TOTAL_VOLUME_AUD:
        violations.append({
            "check": "total_volume_abs_too_high",
            "actual_aud": total_volume,
            "limit_aud": SANITY_MAX_TOTAL_VOLUME_AUD,
            "msg": (f"Total trade volume ${total_volume:,.0f} > "
                    f"${SANITY_MAX_TOTAL_VOLUME_AUD:,.0f} absolute cap"),
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
    tlh_swaps_applied: int = 0
    tlh_loss_realised_aud: float = 0.0
    # AU CGT financial-year buckets — accumulate gains/losses since the
    # last FY-end settle. At each FY crossover we net st vs lt, apply
    # the LT 50% discount, charge tax to cash, and carry forward any
    # net loss to next FY (per AU CGT rules).
    fy_buckets: dict[str, float] = field(default_factory=lambda: {
        "st_gain": 0.0, "lt_gain": 0.0, "st_loss": 0.0, "lt_loss": 0.0,
    })
    carried_losses: dict[str, float] = field(default_factory=lambda: {
        "st_loss": 0.0, "lt_loss": 0.0,
    })
    current_fy_end: Optional[pd.Timestamp] = None
    cgt_tax_paid_aud: float = 0.0
    cgt_settles_count: int = 0


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


def settle_fy_if_crossed(state: SimulatorState, fill_date: pd.Timestamp,
                          paths: dict[str, Path]) -> None:
    """If `fill_date` is in a different financial year than the FY we've
    been accumulating buckets for, settle the prior FY's net taxable
    position now: net st gains vs st losses, net lt vs lt, apply LT
    discount, charge tax to cash, carry forward any net loss. Writes
    an audit entry to the sanity log (overloaded for any forensic
    event, not just sanity violations).

    Standard AU CGT rules per ATO TR 95/35 + s102-5 ITAA 1997:
      - Capital losses net first within ST and LT separately
      - Net ST loss can offset against ST gains in current FY
      - Net LT loss can offset against LT gains in current FY
      - Excess loss carries forward to subsequent FY (no expiry)
      - 50% LT discount applied AFTER net LT gain is calculated
      - Tax = (net_st_gain + net_lt_gain × (1 - LT_DISCOUNT)) × MTR
    """
    new_fy_end = _fy_end_for(fill_date)
    if state.current_fy_end is None:
        state.current_fy_end = new_fy_end
        return
    if new_fy_end <= state.current_fy_end:
        return

    # New FY crossed — settle the prior one.
    b = state.fy_buckets
    # Apply carried-forward losses BEFORE netting current-year buckets,
    # because carry-fwd losses can offset against this year's gains.
    st_loss_available = b["st_loss"] + state.carried_losses["st_loss"]
    lt_loss_available = b["lt_loss"] + state.carried_losses["lt_loss"]

    net_st_gain = b["st_gain"] - st_loss_available
    net_lt_gain = b["lt_gain"] - lt_loss_available

    # Carry-forward calculation
    new_carry_st = 0.0
    new_carry_lt = 0.0
    if net_st_gain < 0:
        new_carry_st = -net_st_gain
        net_st_gain = 0.0
    if net_lt_gain < 0:
        new_carry_lt = -net_lt_gain
        net_lt_gain = 0.0

    # LT discount applies to LT NET gain only
    discounted_lt_gain = net_lt_gain * (1.0 - CGT_LT_DISCOUNT)
    taxable_income = net_st_gain + discounted_lt_gain
    tax_due = max(0.0, taxable_income * CGT_MTR)

    state.cash_aud -= tax_due
    state.cgt_tax_paid_aud += tax_due
    state.cgt_settles_count += 1
    state.carried_losses = {"st_loss": new_carry_st, "lt_loss": new_carry_lt}
    state.fy_buckets = {"st_gain": 0.0, "lt_gain": 0.0,
                         "st_loss": 0.0, "lt_loss": 0.0}

    # Audit entry — FY settlement is the kind of thing you want a
    # forensic trail of even when nothing went wrong.
    settle_audit = {
        "label": state.label,
        "event": "fy_settle",
        "fy_end": state.current_fy_end.isoformat(),
        "net_st_gain_after_carry": net_st_gain,
        "net_lt_gain_after_carry": net_lt_gain,
        "discounted_lt_gain": discounted_lt_gain,
        "taxable_income": taxable_income,
        "tax_due_aud": tax_due,
        "cash_aud_after": state.cash_aud,
        "carry_fwd_st": new_carry_st,
        "carry_fwd_lt": new_carry_lt,
    }
    with paths["sanity"].open("a", encoding="utf-8") as f:
        f.write(json.dumps(settle_audit) + "\n")
    print(f"[sim:{state.label}] FY-end {state.current_fy_end.date()} settled: "
          f"tax=${tax_due:,.0f}, carry_fwd=${new_carry_st+new_carry_lt:,.0f}")

    state.current_fy_end = new_fy_end


def apply_recommendation(rec: dict, state: SimulatorState,
                          paths: dict[str, Path]) -> None:
    """Apply one rec_log entry's recommended_trades to state.
    Fills at next trading day's open + spread + brokerage.
    Records every fill to paths['fills']. Sanity violations record
    to paths['sanity'] AND block the batch from being applied
    (Phase 2b)."""
    rec_date = pd.Timestamp(rec.get("run_at"))
    rec_trades = rec.get("recommended_trades", []) or []
    tlh_swaps = rec.get("tlh_swaps", []) or []
    if not rec_trades and not tlh_swaps:
        return

    # FY-end check BEFORE any new fills — if the new fill date falls in
    # the next FY, we need to settle the prior FY first so this batch's
    # gains don't leak across the boundary.
    fill_date = rec_date + timedelta(days=1)
    settle_fy_if_crossed(state, fill_date, paths)

    # Resolve fill prices for each trade at next-day open.
    fills: list[dict] = []
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
            "kind": "rebalance",
        })

    # TLH swaps (engine emits these alongside rebalance trades — sell
    # ticker_sold, buy ticker_bought at same dollar value to harvest
    # the unrealised loss). We append them to the fill batch so the
    # sanity layer sees the COMBINED set. TLH-only swaps net to zero
    # in turnover, so they shouldn't trip Σ|Δw|.
    for swap in tlh_swaps:
        sold = str(swap.get("ticker_sold", "")).strip()
        bought = str(swap.get("ticker_bought", "")).strip()
        units_sold = int(swap.get("units_sold", 0) or 0)
        units_bought = int(swap.get("units_bought", 0) or 0)
        sale_price = float(swap.get("sale_price", 0) or 0)
        buy_price = float(swap.get("buy_price", 0) or 0)
        if not sold or not bought or units_sold <= 0 or units_bought <= 0:
            continue

        # Resolve actual fill prices via yfinance (engine's planned px
        # is from earlier in the day; market may have moved). Fall back
        # to engine's planned price.
        sale_mid = get_open_price(sold, fill_date)
        if sale_mid is None or sale_mid <= 0:
            sale_mid = sale_price
        buy_mid = get_open_price(bought, fill_date)
        if buy_mid is None or buy_mid <= 0:
            buy_mid = buy_price
        sale_fill = apply_spread("sell", sale_mid) if sale_mid > 0 else sale_price
        buy_fill = apply_spread("buy", buy_mid) if buy_mid > 0 else buy_price

        fills.append({
            "ticker": sold,
            "side": "sell",
            "qty": units_sold,
            "price": sale_fill,
            "mid_price": sale_mid,
            "trade_value_aud": units_sold * sale_fill,
            "brokerage_aud": estimate_brokerage_aud(sold, units_sold * sale_fill),
            "fill_date": fill_date.isoformat(),
            "kind": "tlh_sell",
            "tlh_pair": bought,
        })
        fills.append({
            "ticker": bought,
            "side": "buy",
            "qty": units_bought,
            "price": buy_fill,
            "mid_price": buy_mid,
            "trade_value_aud": units_bought * buy_fill,
            "brokerage_aud": estimate_brokerage_aud(bought, units_bought * buy_fill),
            "fill_date": fill_date.isoformat(),
            "kind": "tlh_buy",
            "tlh_pair": sold,
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

    # Apply fills to lot book + cash, and accumulate FY-bucket gains/losses
    for f in fills:
        ticker = f["ticker"]
        side = f["side"]
        qty = abs(f["qty"])
        px = f["price"]
        if side == "buy":
            state.lot_book.buy(ticker, qty, fill_date, px)
            state.cash_aud -= qty * px + f["brokerage_aud"]
        elif side == "sell":
            sale = state.lot_book.sell(ticker, qty, fill_date, px,
                                        lt_threshold_days=CGT_LT_THRESHOLD_DAYS)
            state.cash_aud += qty * px - f["brokerage_aud"]
            f["realised_gain_aud"] = sum(
                m["realised_gain"] for m in sale.get("matched_lots", [])
            )
            f["lots_matched"] = len(sale.get("matched_lots", []))
            # Accumulate gain/loss components into FY buckets so the
            # next FY-end settle can net + tax.
            state.fy_buckets["st_gain"] += sale.get("st_gain", 0.0)
            state.fy_buckets["lt_gain"] += sale.get("lt_gain", 0.0)
            state.fy_buckets["st_loss"] += sale.get("st_loss", 0.0)
            state.fy_buckets["lt_loss"] += sale.get("lt_loss", 0.0)
            # TLH-specific accounting: if this was a tlh_sell fill,
            # the loss it realised is engine-attributed TLH harvest.
            if f.get("kind") == "tlh_sell":
                _loss_aud = (sale.get("st_loss", 0.0)
                             + sale.get("lt_loss", 0.0))
                state.tlh_loss_realised_aud += _loss_aud
                state.tlh_swaps_applied += 1
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
          f"{state.tlh_swaps_applied} TLH swaps "
          f"(${state.tlh_loss_realised_aud:,.0f} loss realised), "
          f"{state.cgt_settles_count} FY settles "
          f"(${state.cgt_tax_paid_aud:,.0f} tax paid), "
          f"final cash ${state.cash_aud:,.0f}")
    return state


def produce_nav_chart(simulator_label: str = "default",
                       output_path: Optional[Path] = None) -> Optional[Path]:
    """Produce a PNG chart of the simulator's NAV time series with
    rejected-batch dates marked in red. Returns the output path, or
    None if there's not enough data to plot.

    Read-only: consumes simulator_nav{_label}.jsonl + simulator_sanity{_label}.jsonl,
    writes simulator_nav_chart{_label}.png. Doesn't modify simulator state.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")  # headless
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        print("[chart] matplotlib not available — skipping chart")
        return None

    paths = audit_paths(simulator_label)
    nav_path = paths["nav"]
    sanity_path = paths["sanity"]

    if not nav_path.exists():
        print(f"[chart] {nav_path.name} missing — run a simulation first")
        return None

    # Load NAV time series
    nav_records: list[dict] = []
    with nav_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                nav_records.append(json.loads(line))
            except Exception:
                continue
    if not nav_records:
        print(f"[chart] {nav_path.name} is empty")
        return None

    nav_df = pd.DataFrame(nav_records)
    nav_df["date"] = pd.to_datetime(nav_df["date"])
    nav_df = nav_df.sort_values("date")

    # Load sanity rejections (filter to batch_rejected:True)
    rejected_dates: list[pd.Timestamp] = []
    if sanity_path.exists():
        with sanity_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    if r.get("batch_rejected"):
                        d = r.get("fill_date") or r.get("rec_run_at")
                        if d:
                            rejected_dates.append(pd.Timestamp(d))
                except Exception:
                    continue

    if output_path is None:
        suffix = "" if simulator_label == "default" else f"_{simulator_label}"
        output_path = APP_DIR / f"simulator_nav_chart{suffix}.png"

    fig, ax = plt.subplots(figsize=(11.5, 5))
    ax.plot(nav_df["date"], nav_df["nav_aud"], linewidth=1.6,
            label=f"Simulator NAV ({simulator_label})", color="#1f4e8a")
    ax.fill_between(nav_df["date"], nav_df["nav_aud"], 0,
                     alpha=0.10, color="#1f4e8a")

    # Vertical lines at rejected batch dates
    if rejected_dates:
        for d in rejected_dates:
            ax.axvline(d, color="#c53030", linewidth=0.8, alpha=0.4)
        ax.axvline(rejected_dates[0], color="#c53030", linewidth=0.8,
                    alpha=0.4, label=f"Batch rejected ({len(rejected_dates)} total)")

    # Horizontal line at starting NAV for reference
    starting_nav = float(nav_df["nav_aud"].iloc[0])
    ax.axhline(starting_nav, color="#888888", linewidth=0.8,
                linestyle=":", alpha=0.7,
                label=f"Starting NAV: ${starting_nav:,.0f}")

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.yaxis.set_major_formatter(plt.FuncFormatter(
        lambda x, _p: f"${x/1000:,.0f}k"))
    ax.set_title(
        f"Simulator NAV — {simulator_label}    "
        f"({nav_df['date'].min().date()} → {nav_df['date'].max().date()}; "
        f"{len(nav_df)} snapshots, {len(rejected_dates)} batches rejected)",
        fontsize=10,
    )
    ax.set_ylabel("NAV (AUD)")
    ax.set_xlabel("Date")
    ax.legend(loc="upper left", frameon=False, fontsize=8)
    ax.grid(True, linestyle="--", alpha=0.4)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_path, dpi=140, bbox_inches="tight")
    plt.close(fig)
    print(f"[chart] wrote {output_path.name}")
    return output_path


def compare_to_metrics_history(simulator_label: str = "default",
                                 metrics_path: Optional[Path] = None) -> None:
    """Cross-check: align simulator NAV against the engine's
    metrics_history.jsonl expected NAV per snapshot date. Engine
    records `expected_brokerage_aud`, regime mix, 10Y annualised
    return, etc. — we derive an "expected NAV" by compounding the
    annualised return from snapshot to today and comparing.

    Note: this is approximate. Engine's metrics_history captures
    BACKTEST stats, not forward NAV. True forward divergence requires
    live broker NAV via --compare. This mode is a sanity-check that
    the engine's reported metrics are at least directionally
    consistent with the simulator's forward walk."""
    sim_paths = audit_paths(simulator_label)
    nav_path = sim_paths["nav"]
    if not nav_path.exists():
        print(f"[vs-metrics] {nav_path.name} missing — run a sim first")
        return
    if metrics_path is None:
        metrics_path = APP_DIR / "metrics_history.jsonl"
    if not metrics_path.exists():
        print(f"[vs-metrics] {metrics_path.name} missing — engine hasn't logged")
        return

    # Read simulator NAV per day (last snapshot per date)
    sim_recs = []
    with nav_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                sim_recs.append(json.loads(line.strip()))
            except Exception:
                continue
    if not sim_recs:
        print(f"[vs-metrics] sim NAV log empty")
        return
    sim_df = pd.DataFrame(sim_recs)
    sim_df["date"] = pd.to_datetime(sim_df["date"]).dt.date
    sim_daily = sim_df.groupby("date")["nav_aud"].last()

    # Read engine snapshots
    eng_recs = []
    with metrics_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                eng_recs.append(json.loads(line.strip()))
            except Exception:
                continue
    if not eng_recs:
        print(f"[vs-metrics] metrics_history log empty")
        return

    # For each engine snapshot, extract 10Y annualised return + timestamp
    # and project the implied NAV trajectory forward.
    eng_points: list[tuple] = []
    for r in eng_recs:
        try:
            ts = pd.Timestamp(r["timestamp"]).date()
            horizons = r.get("horizons", [])
            tenY = next((h for h in horizons
                          if h.get("horizon") == "10Y"), None)
            if tenY is None:
                continue
            ann_ret = tenY.get("strategy_ann_return")
            if ann_ret is None:
                continue
            eng_points.append((ts, float(ann_ret)))
        except Exception:
            continue

    if not eng_points:
        print(f"[vs-metrics] no 10Y Strategy points in metrics_history")
        return

    print(f"[vs-metrics] {len(eng_points)} engine snapshots, "
          f"{len(sim_daily)} simulator NAV days")
    print(f"  Sim NAV: ${sim_daily.iloc[0]:,.0f} (first) → "
          f"${sim_daily.iloc[-1]:,.0f} (last) over "
          f"{(sim_daily.index[-1] - sim_daily.index[0]).days} days")
    sim_days = (sim_daily.index[-1] - sim_daily.index[0]).days
    if sim_days > 0:
        sim_implied_ann = ((sim_daily.iloc[-1] / sim_daily.iloc[0])
                            ** (365.0 / sim_days) - 1.0)
        print(f"  Sim implied annualised: {sim_implied_ann*100:+.2f}%")
    print(f"  Engine 10Y Strategy ann return (latest snapshot): "
          f"{eng_points[-1][1]*100:+.2f}% @ {eng_points[-1][0]}")
    diff = (sim_implied_ann - eng_points[-1][1]
            if sim_days > 0 else 0.0)
    print(f"  Divergence: {diff*100:+.2f} pp  "
          f"({'OK — within 5 pp' if abs(diff) < 0.05 else 'INVESTIGATE'})")


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

    # Divergence alerts — if any day's |pct_diff| exceeds threshold,
    # write an append-only alert file so a nightly cron / health
    # check can detect quietly-growing divergence over time. Sim ≠
    # live broker NAV is itself a bug signal — one of three things
    # is wrong (engine logic, simulator logic, or the broker is doing
    # something the engine didn't expect like rejected orders).
    DIVERGENCE_THRESHOLD = 0.05  # 5%
    bad_days = joined[joined["pct_diff"].abs() > DIVERGENCE_THRESHOLD]
    if not bad_days.empty:
        alert_path = APP_DIR / "simulator_divergence_alert.jsonl"
        with alert_path.open("a", encoding="utf-8") as f:
            for date, row in bad_days.iterrows():
                f.write(json.dumps({
                    "alert_at": pd.Timestamp.now().isoformat(timespec="seconds"),
                    "simulator_label": simulator_label,
                    "date": str(date),
                    "sim_nav_aud": float(row["sim"]),
                    "live_nav_aud": float(row["live"]),
                    "diff_aud": float(row["diff"]),
                    "pct_diff": float(row["pct_diff"]),
                    "threshold_pct": DIVERGENCE_THRESHOLD,
                }) + "\n")
        print()
        print(f"[compare] DIVERGENCE ALERTS: {len(bad_days)} day(s) exceeded "
              f"±{DIVERGENCE_THRESHOLD*100:.0f}% — see {alert_path.name}")
    else:
        print()
        print(f"[compare] no day exceeded ±{DIVERGENCE_THRESHOLD*100:.0f}% threshold — clean")


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
    parser.add_argument("--vs-metrics", action="store_true",
                        help="Cross-check sim NAV against engine's "
                             "metrics_history.jsonl 10Y annualised return. "
                             "Read-only — does not simulate.")
    parser.add_argument("--chart", action="store_true",
                        help="After simulation (or alone with --compare), "
                             "render NAV time-series chart PNG with "
                             "rejected-batch dates marked.")
    parser.add_argument("--compare-label", default="default",
                        help="Simulator label to compare against (for "
                             "multi-NAV runs you'd pass e.g. '1M')")
    args = parser.parse_args()

    if args.compare:
        compare_to_live_nav(args.compare_label)
        if args.chart:
            produce_nav_chart(args.compare_label)
        return 0
    if args.vs_metrics:
        compare_to_metrics_history(args.compare_label)
        if args.chart:
            produce_nav_chart(args.compare_label)
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
            if args.chart:
                produce_nav_chart(label)
            results.append((nav, state))

        # Summary table
        print()
        print("=" * 110)
        print("Multi-NAV summary")
        print("=" * 110)
        print(f"{'NAV':>12}  {'Final Cash':>14}  {'Fills':>6}  {'Rej':>4}  "
              f"{'Viol':>5}  {'TLH':>4}  {'TLH Loss':>11}  {'FY':>3}  {'Tax':>10}")
        for nav, st in results:
            print(f"  ${nav:>10,.0f}  ${st.cash_aud:>13,.0f}  "
                  f"{st.fills_count:>6}  {st.batches_rejected:>4}  "
                  f"{st.sanity_violations_count:>5}  {st.tlh_swaps_applied:>4}  "
                  f"${st.tlh_loss_realised_aud:>10,.0f}  "
                  f"{st.cgt_settles_count:>3}  ${st.cgt_tax_paid_aud:>9,.0f}")
        print("=" * 110)
        return 0

    # Single-NAV mode
    state = load_seed(starting_cash_override=args.starting_cash, label="default")
    run_single(start, end, rec_log, state, args.reset)
    if args.chart:
        produce_nav_chart("default")
    return 0


if __name__ == "__main__":
    sys.exit(main())

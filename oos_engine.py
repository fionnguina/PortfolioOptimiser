"""OOS ensemble walk-forward backtest engine (module split #18, 2026-07-10).

The analytics core: solve 5 candidates/rebalance, softmax-blend by rolling Sortino,
apply Sigma-shrinkage + vol-target + crisis/crash hedges + TLH + FY CGT, hold 1mo.
Move-set is CLOSED (verified): run_oos + its 7 analytics helpers + _is_us_ticker
call only each other, imported module fns, and libs. The ~25 engine CONFIG values
are injected by the engine (_sync_oos_engine) once after config is defined; they do
not change mid-run. Validated by walk-forward-CV byte-diff (MaxDD/Sharpe/return).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from brokerage import BROKER_CONFIG
from cgt import CGT_CONFIG, LotBook, _effective_cgt_rate
from tlh import TLH_ENABLED, _run_tlh_pass
from solvers import _ledoit_wolf_cc, solve_candidate_portfolios
from factors import auto_recommend_factor_tilts
from ensemble import softmax_ensemble_weights

# Config injected by the engine's _sync_oos_engine() after config is defined.
COV_SHRINKAGE = None
CRASH_HEDGE_BASKET = None
CRASH_HEDGE_DD_RELEASE = None
CRASH_HEDGE_DD_TRIGGER = None
CRASH_HEDGE_LOOKBACK_DAYS = None
CRISIS_HEDGE_BAND_SD = None
CRISIS_HEDGE_MA_DAYS = None
CRISIS_HEDGE_TICKER = None
CRISIS_HEDGE_WEIGHT = None
EARLY_TRIGGER_DD_DEEPEN = None
EARLY_TRIGGER_MIN_DAYS = None
ENSEMBLE_SLOT_NAMES = ()
LT_DEFER_DD_CONDITIONAL = None
LT_DEFER_RELEASE_DD = None
LT_DEFER_WINDOW_DAYS = None
MU_SHRINKAGE_LAMBDA = None
PER_ASSET_WEIGHT_CAPS = None
RETURN_OUTLIER_THRESHOLD = None
SKIP_REBAL_DELTA = None
SKIP_REBAL_DELTA_CALM = None
STRETCH_FLOOR_CALM = None
STRETCH_FLOOR_PREDICTIVE = None
TLH_PAIRS = None
TREND_SLEEVE_WEIGHT = None
VOL_TARGET_ANNUAL = None


def _is_us_ticker(t) -> bool:
    """A US-listed security: no '.AX' suffix and not an index symbol."""
    s = str(t)
    return not s.endswith(".AX") and not s.startswith("^")


def estimate_rebalance_cost_fraction(
    w_old: pd.Series,
    w_new: pd.Series,
    portfolio_value_aud: float = 1_000_000.0,
    broker_cfg: dict | None = None,
) -> float:
    """Total cost of rebalancing from w_old to w_new, as a FRACTION of NAV.

    Returns e.g. 0.0023 for a 23-bps drag. Subtract this from the realised
    return on the rebalance day to model net-of-cost performance.

    Components:
      1. Fixed per-trade fees (AU $11 etc.) — scaled by NAV (small trades hurt
         small portfolios more, become negligible at scale)
      2. Bid/ask spread cost — bps × trade value (delta weight × NAV)
      3. FX one-way conversion cost — bps × US trade value
    """
    if broker_cfg is None:
        broker_cfg = BROKER_CONFIG

    tickers = sorted(set(w_old.index).union(w_new.index))
    delta = (w_new.reindex(tickers).fillna(0.0) -
             w_old.reindex(tickers).fillna(0.0)).abs()

    n_au_trades = int(sum(1 for t in tickers if delta[t] > 1e-6 and not _is_us_ticker(t)))
    n_us_trades = int(sum(1 for t in tickers if delta[t] > 1e-6 and     _is_us_ticker(t)))
    au_turnover = float(sum(delta[t] for t in tickers if not _is_us_ticker(t)))
    us_turnover = float(sum(delta[t] for t in tickers if     _is_us_ticker(t)))

    # 1. Fixed fees (AUD) as fraction of portfolio
    fixed_cost = (n_au_trades * float(broker_cfg["au_flat_fee_aud"]) +
                  n_us_trades * float(broker_cfg["us_flat_fee_aud"])
                  ) / max(float(portfolio_value_aud), 1.0)

    # 2. Spread costs (decimal)
    spread_cost = (au_turnover * float(broker_cfg["au_spread_bps"]) / 10_000.0 +
                   us_turnover * float(broker_cfg["us_spread_bps"]) / 10_000.0)

    # 3. FX cost (decimal, one-way per US trade)
    fx_cost = us_turnover * float(broker_cfg["fx_spread_bps"]) / 10_000.0

    return float(fixed_cost + spread_cost + fx_cost)


def _apply_mu_shrinkage(mu: "pd.Series") -> "pd.Series":
    """Shrink expected returns toward the cross-sectional median (see above)."""
    lam = float(globals().get("MU_SHRINKAGE_LAMBDA", 0.0) or 0.0)
    if lam <= 0.0 or mu is None or len(mu) == 0:
        return mu
    prior = float(pd.to_numeric(mu, errors="coerce").median())
    return (1.0 - lam) * mu + lam * prior


def _check_crash_trigger(spy_history, as_of, state: dict,
                          dd_trigger: float | None = None,
                          dd_release: float | None = None,
                          lookback_days: int | None = None) -> bool:
    """Asymmetric crash hedge trigger with hysteresis.

    Enters HEDGE-ON state when SPY peak-to-current drawdown crosses below
    dd_trigger (e.g. -15%). Stays ON until drawdown recovers above
    dd_release (e.g. -5%). The hysteresis prevents whipsaw oscillation
    around the threshold.

    `state` is a mutable dict carried across calls — initial state should
    be `{"active": False}`. The function mutates state["active"] in place
    and also returns the new active flag.

    spy_history: pd.Series of SPY prices, indexed by date.
    as_of: pd.Timestamp — current evaluation date.
    """
    dd_trigger = CRASH_HEDGE_DD_TRIGGER if dd_trigger is None else dd_trigger
    dd_release = CRASH_HEDGE_DD_RELEASE if dd_release is None else dd_release
    lookback_days = CRASH_HEDGE_LOOKBACK_DAYS if lookback_days is None else lookback_days

    if spy_history is None or spy_history.empty:
        return state.get("active", False)
    hist = spy_history.loc[:as_of]
    if hist.empty:
        return state.get("active", False)
    window = hist.tail(lookback_days)
    if window.empty:
        return state.get("active", False)
    rolling_peak = float(window.max())
    current = float(window.iloc[-1])
    if rolling_peak <= 0:
        return state.get("active", False)
    dd = (current - rolling_peak) / rolling_peak  # negative for drawdown

    active = bool(state.get("active", False))
    if not active and dd <= dd_trigger:
        active = True
    elif active and dd >= dd_release:
        active = False
    state["active"] = active
    state["last_dd"] = dd
    return active


def _apply_crash_hedge(weights: pd.Series, basket: dict[str, float] | None = None,
                       available_tickers: pd.Index | None = None) -> pd.Series:
    """Replace `weights` with the crash-hedge basket.

    If a basket ticker isn't in `available_tickers` (e.g. HBRD.AX before
    its 2017 listing), its weight is reallocated proportionally to the
    remaining tickers. Returns a normalised Series indexed like `weights`
    (zero-weight on non-basket tickers).
    """
    basket = CRASH_HEDGE_BASKET if basket is None else basket
    if not basket:
        return weights
    # Filter basket to tickers we actually have data for.
    if available_tickers is not None:
        usable = {k: v for k, v in basket.items() if k in available_tickers}
    else:
        usable = dict(basket)
    if not usable:
        # No basket tickers available — fall back to original weights
        # rather than zero portfolio (defensive against missing data).
        return weights
    total = sum(usable.values())
    if total <= 0:
        return weights
    usable = {k: v / total for k, v in usable.items()}
    # Build a zero series on the original ticker index, then set basket weights.
    out = pd.Series(0.0, index=weights.index)
    for tkr, w in usable.items():
        if tkr in out.index:
            out[tkr] = w
        else:
            # Ticker not in original weight index — extend index
            out[tkr] = w
    # Renormalise (safety against rounding)
    s = out.sum()
    if s > 0:
        out = out / s
    return out


def _compute_trend_sleeve(px, as_of, universe, caps,
                          lookback: int = 252, skip: int = 21,
                          vol_window: int = 63) -> "pd.Series":
    """Long-only inverse-vol time-series-momentum weights (trend sleeve).

    Per asset: signal = 12-1M return (price[t-skip]/price[t-lookback]-1);
    weight ∝ max(0, signal) / trailing vol; normalized over positive-trend
    assets; PER_ASSET_WEIGHT_CAPS applied (cap-0 excluded). Returns an EMPTY
    Series when nothing trends or history is insufficient (→ sleeve = cash).
    """
    hist = px.loc[:as_of]
    if len(hist) < lookback + 5:
        return pd.Series(dtype=float)
    p_skip = hist.iloc[-(skip + 1)]
    p_base = hist.iloc[-(lookback + 1)]
    mom = (p_skip / p_base) - 1.0
    vol = hist.pct_change().tail(vol_window).std()
    raw = {}
    for tk in universe:
        cap = float(caps.get(tk, 1.0)) if caps else 1.0
        if cap <= 0:
            continue
        m = float(mom.get(tk, float("nan")))
        v = float(vol.get(tk, float("nan")))
        if not np.isfinite(m) or not np.isfinite(v) or v <= 0 or m <= 0:
            continue
        raw[tk] = m / v
    if not raw:
        return pd.Series(dtype=float)
    w = pd.Series(raw)
    w = w / w.sum()
    if caps:
        for tk in list(w.index):
            w[tk] = min(float(w[tk]), float(caps.get(tk, 1.0)))
        if w.sum() > 0:
            w = w / w.sum()
    return w


def blend_ensemble_signals(
    backward_weights: pd.Series,
    forward_weights: pd.Series,
    backward_alpha: float = 0.7,
) -> pd.Series:
    """Linearly blend two probability distributions over the same slot index.

    backward_alpha controls how much weight goes on the EWMA-Sortino signal
    vs the forward regime signal (default 0.7 = 70% backward, 30% forward).
    The result is renormalised to sum to 1.

    Why blend distributions (not raw scores)? They're already on the same
    [0, 1] scale and sum to 1 — addition is well-defined and the result is
    still a probability distribution. Avoids the rescaling pitfalls of
    blending raw Sortinos (range ~[-3, +5]) with preferences (range [0, 1]).
    """
    if backward_weights is None or backward_weights.empty:
        return forward_weights.copy() if forward_weights is not None else pd.Series(dtype=float)
    if forward_weights is None or forward_weights.empty:
        return backward_weights.copy()

    # Align on the union of indices, fill missing with 0.
    idx = list(backward_weights.index.union(forward_weights.index))
    bw = backward_weights.reindex(idx).fillna(0.0)
    fw = forward_weights.reindex(idx).fillna(0.0)
    a = float(np.clip(backward_alpha, 0.0, 1.0))
    blended = a * bw + (1.0 - a) * fw
    s = float(blended.sum())
    if s <= 0:
        # Fall back to equal weights if both signals collapsed.
        return pd.Series(1.0 / max(len(idx), 1), index=idx)
    return blended / s


def compute_forward_regime_signal(
    benchmark_prices: pd.Series,
    as_of_date: pd.Timestamp,
    slot_names: "tuple[str, ...] | None" = None,
    dd_pct_floor: float = 0.20,
    gaussian_width: float = 0.40,
) -> pd.Series:
    """Forward-looking regime preference, independent of past candidate scores.

    Returns a probability distribution over slot_names. Aggressive end gets
    more weight in bullish conditions, lower-aggression end gets more weight
    in risk-off conditions.

    Inputs (both derivable from benchmark price history alone):
      1. Drawdown from 52-week high (deeper DD → favour low-aggression slot)
      2. 20-day SMA vs 50-day SMA cross (20d > 50d → bullish, else bearish)

    The 20d/50d cross replaced the prior 200-day MA test because the 200-day
    signal lags 4-6 months out of a crash — the engine was staying defensive
    well past the actual SPY recovery (visible in 2020 H2 and 2022 H2 of the
    regime strip). 20d/50d flips bullish within ~3-5 weeks of a true recovery
    while the drawdown component still provides the deep-crash protection.

    These are blended 50/50 to a [0, 1] regime intensity score, which is then
    mapped to slot preferences via a Gaussian centred on the matching aggression
    level. Wider gaussian_width spreads weight; narrower concentrates it.

    Warm-up: if benchmark has < 50 days of data before as_of_date, returns
    uniform weights.
    """
    if slot_names is None:
        slot_names = ENSEMBLE_SLOT_NAMES
    n = len(slot_names)
    eq = pd.Series(1.0 / n, index=list(slot_names))
    if benchmark_prices is None or len(benchmark_prices) == 0:
        return eq

    px = pd.to_numeric(pd.Series(benchmark_prices), errors="coerce").dropna()
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index()
    as_of_date = pd.Timestamp(as_of_date)
    px = px[px.index <= as_of_date]
    if len(px) < 50:
        return eq

    px_last = float(px.iloc[-1])
    # 52-week (252-day) trailing high — drawdown reference point.
    rolling_max_52w = float(px.tail(252).max()) if len(px) >= 252 else float(px.max())
    dd_pct = (px_last - rolling_max_52w) / rolling_max_52w if rolling_max_52w > 0 else 0.0
    # 20-day vs 50-day SMA cross — fast trend reference point (replaces 200d MA).
    ma_20 = float(px.tail(20).mean())
    ma_50 = float(px.tail(50).mean())
    above_ma = 1.0 if ma_20 > ma_50 else 0.0

    # Drawdown signal: 1.0 at peak, linearly decreasing to 0.0 at -dd_pct_floor.
    dd_signal = max(0.0, 1.0 + dd_pct / dd_pct_floor)  # dd_pct is <= 0
    regime_intensity = 0.5 * dd_signal + 0.5 * above_ma  # in [0, 1]

    # Map slots onto an aggression axis: Modest=0.0 .. Stretch=1.0
    aggressions = np.linspace(0.0, 1.0, n)
    # Gaussian preference peaked at regime_intensity.
    prefs = np.exp(-((aggressions - regime_intensity) ** 2) /
                   (2.0 * float(gaussian_width) ** 2))
    s = float(prefs.sum())
    if s <= 0 or not np.isfinite(s):
        return eq
    return pd.Series(prefs / s, index=list(slot_names))


def run_oos_ensemble_walk_forward(
    prices_aud: pd.DataFrame,
    train_window_months: int = 24,
    rebalance: str = "MS",
    benchmark_ticker: str = "SPY",
    score_lookback_days: int = 252,
    lambda_temp: float = 2.0,
    sortino_halflife_days: int = 60,
    forward_signal_alpha: float = 0.5,
    starting_nav_aud: float = 1_000_000.0,
    skip_rebal_delta: float | None = None,
    turnover_penalty: float = 0.0,
    crash_hedge: bool = False,
    crash_hedge_dd_trigger: float | None = None,
    crash_hedge_dd_release: float | None = None,
    slot_weights_override: dict | None = None,
    auto_factor_tilts: bool = False,
    ff_factors: pd.DataFrame | None = None,
    factor_betas: pd.DataFrame | None = None,
    factor_tilt_lookback_days: int | None = None,
    factor_tilt_band: float = 0.10,
) -> dict:
    """Ensemble walk-forward: solve 5 candidates per rebalance, softmax-blend
    by rolling 12M Sortino, hold the blended portfolio for 1 month.

    Returns a dict with:
        blended_returns       Series of daily blended strategy returns
        per_candidate_returns DataFrame of daily per-candidate returns
        softmax_history       DataFrame of softmax weights (rows=rebal dates)
        blended_weights       DataFrame of blended ticker weights per rebal date
        per_candidate_weights dict[slot_name -> DataFrame of weights per rebal date]
    """
    px = prices_aud.copy()
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    px = px.drop(columns=[c for c in ["PortfolioValue"] if c in px.columns], errors="ignore")

    oos_end = px.index.max()
    lead = pd.DateOffset(months=train_window_months)
    oos_start = px.index.min() + lead

    daily_rets_all = px.pct_change()
    daily_rets_all = daily_rets_all.where(daily_rets_all.abs() <= RETURN_OUTLIER_THRESHOLD)

    cal_dates = pd.date_range(start=oos_start, end=oos_end, freq=rebalance)
    scheduled_dates = []
    for d in cal_dates:
        loc = px.index.searchsorted(d, side="left")
        if loc < len(px.index):
            scheduled_dates.append(px.index[loc])
    scheduled_dates = sorted(set(scheduled_dates))

    # --- Conditional rebalancing: insert early-trigger dates between scheduled
    # ones whenever SPY drawdown deepens by more than EARLY_TRIGGER_DD_DEEPEN
    # since the prior scheduled rebal. Catches fast regime shifts at 6W cadence.
    augmented_dates = list(scheduled_dates)
    n_early_triggered = 0
    # Kept as a set so the LT-deferral shield can be released on exactly these
    # stress-inserted dates (LT_DEFER_DD_CONDITIONAL).
    _early_trigger_dates: set = set()
    if (benchmark_ticker in px.columns
            and EARLY_TRIGGER_DD_DEEPEN > 0
            and len(scheduled_dates) > 1):
        spy = px[benchmark_ticker].sort_index()
        for k in range(len(scheduled_dates) - 1):
            t0 = scheduled_dates[k]
            t1 = scheduled_dates[k + 1]
            window = spy.loc[t0:t1]
            if len(window) < 2:
                continue
            peak = window.cummax()
            dd = (window / peak) - 1.0
            dd_at_t0 = float(dd.iloc[0])
            trigger_mask = ((dd - dd_at_t0) <= -EARLY_TRIGGER_DD_DEEPEN)
            trigger_mask &= (window.index >= t0 + pd.Timedelta(days=EARLY_TRIGGER_MIN_DAYS))
            trigger_dates = window.index[trigger_mask]
            if len(trigger_dates) > 0:
                augmented_dates.append(trigger_dates[0])
                _early_trigger_dates.add(trigger_dates[0])
                n_early_triggered += 1

    # --- Crash-hedge forced rebals: pre-scan daily SPY drawdown to find
    # hysteresis state-change days and insert them as off-cycle rebals.
    # This fixes the COVID-2020 miss where the 6W cadence skipped past the
    # active hedge window. The scan uses the SAME hysteresis rule as the
    # main loop's _check_crash_trigger so the two state machines converge.
    _hedge_trigger_eff = (CRASH_HEDGE_DD_TRIGGER if crash_hedge_dd_trigger is None
                          else float(crash_hedge_dd_trigger))
    _hedge_release_eff = (CRASH_HEDGE_DD_RELEASE if crash_hedge_dd_release is None
                          else float(crash_hedge_dd_release))
    n_hedge_forced = 0
    if crash_hedge and benchmark_ticker in px.columns:
        spy = px[benchmark_ticker].sort_index()
        spy_window = spy.loc[:oos_end]
        if not spy_window.empty:
            rolling_peak = (spy_window.rolling(
                window=CRASH_HEDGE_LOOKBACK_DAYS, min_periods=1).max())
            dd_daily = (spy_window / rolling_peak - 1.0).dropna()
            # Restrict to OOS range — pre-OOS state changes don't need rebals.
            dd_oos = dd_daily.loc[oos_start:oos_end]
            sim_state = False
            for d, dd_val in dd_oos.items():
                if not sim_state and dd_val <= _hedge_trigger_eff:
                    sim_state = True
                    augmented_dates.append(d)
                    n_hedge_forced += 1
                elif sim_state and dd_val >= _hedge_release_eff:
                    sim_state = False
                    augmented_dates.append(d)
                    n_hedge_forced += 1

    rebal_dates = pd.DatetimeIndex(sorted(set(augmented_dates)))
    n_scheduled = len(scheduled_dates)
    if len(rebal_dates) == 0:
        return {"blended_returns": pd.Series(dtype=float),
                "per_candidate_returns": pd.DataFrame(),
                "softmax_history": pd.DataFrame(),
                "blended_weights": pd.DataFrame(),
                "per_candidate_weights": {n: pd.DataFrame() for n in ENSEMBLE_SLOT_NAMES}}

    per_candidate_weights: dict[str, dict[pd.Timestamp, pd.Series]] = {
        n: {} for n in ENSEMBLE_SLOT_NAMES
    }
    blended_weights: dict[pd.Timestamp, pd.Series] = {}
    softmax_rows: dict[pd.Timestamp, pd.Series] = {}
    per_candidate_segments: dict[str, list[pd.Series]] = {n: [] for n in ENSEMBLE_SLOT_NAMES}
    blended_segments: list[pd.Series] = []
    # NET-of-cost tracking: running NAV + previous-rebalance weights so we can
    # apply realistic transaction costs on each rebalance day.
    rebalance_costs: dict[pd.Timestamp, float] = {}
    rebalance_taxes: dict[pd.Timestamp, float] = {}
    _prev_blend_w = pd.Series(dtype=float)
    # LT-deferral diagnostics (0 when LT_DEFER_WINDOW_DAYS=0).
    _defer_events = 0
    _defer_value_total = 0.0
    _defer_released_rebals = 0
    # Trailing benchmark drawdown for the LT_DEFER_RELEASE_DD rule (same
    # 252d-rolling-peak measure as the crash hedge).
    _lt_defer_reldd = float(globals().get("LT_DEFER_RELEASE_DD", 0.0) or 0.0)
    _spy_dd_for_defer = None
    if _lt_defer_reldd < 0 and benchmark_ticker in px.columns:
        _spy_ser_defer = px[benchmark_ticker].sort_index()
        _spy_dd_for_defer = (_spy_ser_defer
                             / _spy_ser_defer.rolling(window=252, min_periods=1).max()
                             - 1.0)
    # Trailing benchmark drawdown for the calm-conditional skip threshold
    # (pre-registered asym-rebalance experiment, 2026-07-08).
    _skip_calm_delta = float(globals().get("SKIP_REBAL_DELTA_CALM", 0.0) or 0.0)
    # Insurance-premium experiment (2026-07-09): min top-slot (Stretch) weight
    # on CALM rebalances. See reference-insurance-premium-experiment memory.
    _stretch_floor = float(globals().get("STRETCH_FLOOR_CALM", 0.0) or 0.0)
    _stretch_predictive = bool(globals().get("STRETCH_FLOOR_PREDICTIVE", False))
    _spy_dd_for_calm = None
    _spy_ma200 = None       # 200d SMA for the predictive trend gate
    _spy_price_calm = None
    _n_calm_widened = 0
    _n_stretch_floored = 0
    # Trend sleeve (core-satellite). Universe = tradable ETFs, no benchmarks.
    _trend_sleeve_w = float(globals().get("TREND_SLEEVE_WEIGHT", 0.0) or 0.0)
    _trade_universe = [c for c in px.columns
                       if c not in (benchmark_ticker, "PortfolioValue")
                       and not str(c).startswith("^")]
    _caps_for_trend = globals().get("PER_ASSET_WEIGHT_CAPS", {}) or {}
    _n_trend_applied = 0
    _cov_shrinkage = bool(globals().get("COV_SHRINKAGE", False))
    _lw_delta_sum = 0.0
    _lw_delta_n = 0
    _vol_target = float(globals().get("VOL_TARGET_ANNUAL", 0.0) or 0.0)
    _n_vol_scaled = 0
    # Market-timed inverse-ETF crisis hedge: hold BEAR.AX when SPY < 200d SMA.
    _crisis_hedge_w = float(globals().get("CRISIS_HEDGE_WEIGHT", 0.0) or 0.0)
    _crisis_hedge_tkr = str(globals().get("CRISIS_HEDGE_TICKER", "BEAR.AX"))
    _crisis_ma_days = int(globals().get("CRISIS_HEDGE_MA_DAYS", 200))
    _crisis_band_sd = float(globals().get("CRISIS_HEDGE_BAND_SD", 0.0) or 0.0)
    _n_crisis_hedged = 0
    _spy_ma_crisis = None
    _spy_lb_crisis = None  # lower band = MA - band_sd·σ
    if (_crisis_hedge_w > 0 and benchmark_ticker in px.columns
            and _crisis_hedge_tkr in px.columns):
        _spy_ser_crisis = px[benchmark_ticker].sort_index()
        _mp_crisis = max(2, _crisis_ma_days // 2)
        _spy_ma_crisis = _spy_ser_crisis.rolling(
            window=_crisis_ma_days, min_periods=_mp_crisis).mean()
        _spy_sd_crisis = _spy_ser_crisis.rolling(
            window=_crisis_ma_days, min_periods=_mp_crisis).std()
        _spy_lb_crisis = _spy_ma_crisis - _crisis_band_sd * _spy_sd_crisis
    if (_skip_calm_delta > 0 or _stretch_floor > 0) and benchmark_ticker in px.columns:
        _spy_ser_calm = px[benchmark_ticker].sort_index()
        _spy_dd_for_calm = (_spy_ser_calm
                            / _spy_ser_calm.rolling(window=252, min_periods=1).max()
                            - 1.0)
        if _stretch_floor > 0 and _stretch_predictive:
            _spy_price_calm = _spy_ser_calm
            _spy_ma200 = _spy_ser_calm.rolling(window=200, min_periods=100).mean()
    _running_nav = float(starting_nav_aud)  # AUD; flat-fee impact scales with NAV
    # Conditional rebalancing diagnostics
    n_skipped = 0
    n_executed = 0
    # Lot book for CGT modelling — tracks acquisition dates + cost basis FIFO.
    _lot_book = LotBook()
    # FY accumulators: AU financial year runs 1 Jul – 30 Jun. Gains/losses
    # accumulate through the year; tax applied at FY-end with cross-offset +
    # loss carry-forward (the real AU rule, vastly more favourable than the
    # per-rebalance approximation).
    _fy_buckets = {"st_gain": 0.0, "lt_gain": 0.0, "st_loss": 0.0, "lt_loss": 0.0}
    _carried_losses = {"st_loss": 0.0, "lt_loss": 0.0}
    _current_fy_end: pd.Timestamp | None = None
    # TLH state — cooldown maps ticker → last TLH-sell date; events accumulate
    # for the engine return dict (consumed by Excel/PPT/log writers).
    _tlh_cooldown: dict[str, pd.Timestamp] = {}
    _tlh_events_all: list[dict] = []
    # Crash-hedge state: hysteresis flag (mutated by _check_crash_trigger).
    _hedge_state: dict = {"active": False, "last_dd": 0.0}
    _hedge_active_rebals = 0  # count of rebalances where hedge was active
    _hedge_n_triggers = 0     # count of OFF→ON transitions
    _hedge_events: list[dict] = []  # state-change log

    def _fy_end_for(date: pd.Timestamp) -> pd.Timestamp:
        d = pd.Timestamp(date)
        # AU FY ends 30 June. If date is Jul–Dec, FY-end is 30 Jun next year.
        if d.month >= 7:
            return pd.Timestamp(year=d.year + 1, month=6, day=30)
        return pd.Timestamp(year=d.year, month=6, day=30)

    def _apply_fy_tax(buckets: dict, carried: dict, nav: float) -> tuple[float, dict]:
        """Compute tax owed on prior FY with full netting + carry-forward.
        Returns (tax_fraction_of_nav, new_carried_losses)."""
        st_gain = buckets["st_gain"]
        lt_gain = buckets["lt_gain"]
        st_loss = buckets["st_loss"] + carried["st_loss"]
        lt_loss = buckets["lt_loss"] + carried["lt_loss"]
        # 1) Within-category netting
        st_net = st_gain - st_loss
        lt_net = lt_gain - lt_loss
        # 2) Cross-category offset (losses can reduce other-category gains)
        if st_net < 0 and lt_net > 0:
            offset = min(lt_net, -st_net)
            lt_net -= offset; st_net += offset
        if lt_net < 0 and st_net > 0:
            offset = min(st_net, -lt_net)
            st_net -= offset; lt_net += offset
        # 3) Tax on positive net gains; carry forward leftover losses
        tax_aud = 0.0
        if st_net > 0:
            tax_aud += st_net * _effective_cgt_rate(short_term=True)
        if lt_net > 0:
            tax_aud += lt_net * _effective_cgt_rate(short_term=False)
        new_carried = {
            "st_loss": max(0.0, -st_net),
            "lt_loss": max(0.0, -lt_net),
        }
        return tax_aud / max(nav, 1.0), new_carried

    for i, t in enumerate(rebal_dates):
        train_px = px.loc[t - lead : t]
        if len(train_px) < 60:
            continue
        train_rets = train_px.pct_change()
        train_rets = train_rets.where(train_rets.abs() <= RETURN_OUTLIER_THRESHOLD)
        coverage = train_rets.notna().sum() / max(len(train_rets), 1)
        good_cols = coverage[coverage >= 0.8].index.tolist()
        if len(good_cols) < 3:
            continue
        train_rets = train_rets[good_cols].dropna(how="any")
        if len(train_rets) < 60:
            continue

        log_ret = np.log1p(train_rets)
        mu = pd.Series(np.expm1(log_ret.mean() * 252.0), index=train_rets.columns)
        mu = _apply_mu_shrinkage(mu)
        if _cov_shrinkage:
            Sigma, _lw_delta = _ledoit_wolf_cc(train_rets)
            _lw_delta_sum += _lw_delta
            _lw_delta_n += 1
        else:
            Sigma = train_rets.cov()
        spy_mu = float(mu[benchmark_ticker]) if benchmark_ticker in mu.index else None

        # If auto factor tilts enabled, compute the trailing-N-day factor
        # recommendation up to THIS rebalance and pass to candidate solvers.
        # No look-ahead: we slice ff_factors at t before scoring.
        _live_tilt_targets = None
        _live_tilt_bands = None
        _live_use_mask = None
        if (auto_factor_tilts and ff_factors is not None
                and factor_betas is not None and not ff_factors.empty):
            try:
                ff_up_to_t = ff_factors.loc[:t]
                if not ff_up_to_t.empty:
                    _live_tilt_targets = auto_recommend_factor_tilts(
                        ff_up_to_t,
                        lookback_days=factor_tilt_lookback_days,
                    )
                    if _live_tilt_targets:
                        _live_tilt_bands = {f: factor_tilt_band
                                             for f in _live_tilt_targets}
                        _live_use_mask = {f: True for f in _live_tilt_targets}
            except Exception:
                _live_tilt_targets = None

        candidates = solve_candidate_portfolios(
            mu, Sigma, spy_mu,
            w_prev=_prev_blend_w if not _prev_blend_w.empty else None,
            turnover_penalty=float(turnover_penalty),
            tilt_targets=_live_tilt_targets,
            tilt_bands=_live_tilt_bands,
            B=factor_betas,
            use_mask=_live_use_mask,
            tilt_mode="soft",
        )
        # All candidates must be solvable to participate; otherwise skip rebalance.
        if all(w.empty for w in candidates.values()):
            continue

        # Score: rolling Sortino over prior per-candidate daily returns.
        prior_panel = pd.DataFrame()
        if all(per_candidate_segments[n] for n in ENSEMBLE_SLOT_NAMES):
            cand_series = {n: pd.concat(per_candidate_segments[n]).sort_index()
                           for n in ENSEMBLE_SLOT_NAMES}
            prior_panel = pd.DataFrame(cand_series)
            prior_panel = prior_panel[~prior_panel.index.duplicated(keep="last")]
        # Benchmark daily returns up to (but not including) t — for IR scoring.
        bench_rets_for_score = None
        if benchmark_ticker in px.columns:
            _bench_px = px[benchmark_ticker].loc[:t]
            bench_rets_for_score = _bench_px.pct_change().dropna()
        soft_w = softmax_ensemble_weights(prior_panel,
                                          lookback_days=score_lookback_days,
                                          lambda_temp=lambda_temp,
                                          halflife_days=sortino_halflife_days,
                                          benchmark_returns=bench_rets_for_score)
        # Blend with forward-looking regime signal (benchmark drawdown + 200d MA).
        # This reduces whipsaws by anchoring the ensemble to market conviction
        # rather than relying purely on past per-candidate performance.
        if benchmark_ticker in px.columns:
            fwd_w = compute_forward_regime_signal(
                benchmark_prices=px[benchmark_ticker],
                as_of_date=t,
            )
            soft_w = blend_ensemble_signals(
                backward_weights=soft_w,
                forward_weights=fwd_w,
                backward_alpha=forward_signal_alpha,
            )
        # Optional override: replace softmax with a fixed slot-weight map.
        # Used by --stretch-only-test and similar diagnostic modes to force
        # a specific allocation across slots without rewriting the engine.
        if slot_weights_override is not None:
            override = pd.Series(slot_weights_override).reindex(
                ENSEMBLE_SLOT_NAMES, fill_value=0.0).astype(float)
            override_sum = float(override.sum())
            if override_sum > 0:
                soft_w = override / override_sum
        # Insurance-premium floor: on CALM rebalances raise the top slot to
        # STRETCH_FLOOR_CALM, redistributing the deficit proportionally from the
        # defensive slots. Stress rebalances (early-trigger OR SPY 252d DD ≤ -5%)
        # are left untouched so the softmax still de-risks in the first leg down.
        if (_stretch_floor > 0 and slot_weights_override is None
                and _spy_dd_for_calm is not None and t not in _early_trigger_dates):
            try:
                _dd_now_floor = float(_spy_dd_for_calm.asof(t))
            except Exception:
                _dd_now_floor = -1.0
            # Predictive trend gate: also require SPY > 200d MA (releases the
            # floor through sustained bears before the reactive DD confirms).
            _trend_ok = True
            if _stretch_predictive and _spy_ma200 is not None:
                try:
                    _px_now = float(_spy_price_calm.asof(t))
                    _ma_now = float(_spy_ma200.asof(t))
                    _trend_ok = (np.isfinite(_px_now) and np.isfinite(_ma_now)
                                 and _px_now > _ma_now)
                except Exception:
                    _trend_ok = False
            if np.isfinite(_dd_now_floor) and _dd_now_floor > -0.05 and _trend_ok:
                _top = ENSEMBLE_SLOT_NAMES[-1]
                _cur_top = float(soft_w.get(_top, 0.0))
                if _cur_top < _stretch_floor:
                    _others = [n for n in ENSEMBLE_SLOT_NAMES if n != _top]
                    _osum = float(soft_w.reindex(_others).fillna(0.0).sum())
                    if _osum > 0:
                        soft_w = soft_w.copy()
                        _scale = (1.0 - _stretch_floor) / _osum
                        for _n in _others:
                            soft_w[_n] = float(soft_w.get(_n, 0.0)) * _scale
                        soft_w[_top] = _stretch_floor
                        _n_stretch_floored += 1
        softmax_rows[t] = soft_w

        # Save per-candidate weights at this rebal.
        for n in ENSEMBLE_SLOT_NAMES:
            if not candidates[n].empty:
                per_candidate_weights[n][t] = candidates[n]

        # Realised holding window
        seg_end = rebal_dates[i + 1] if i + 1 < len(rebal_dates) else oos_end + pd.Timedelta(days=1)

        # Per-candidate realised returns (for next iteration's scoring) using
        # only THIS candidate's weights — independent of softmax.
        for n in ENSEMBLE_SLOT_NAMES:
            w_cand = candidates[n]
            if w_cand.empty:
                continue
            held = daily_rets_all.loc[t:seg_end, w_cand.index].fillna(0.0)
            if len(held) > 0 and held.index[0] == t:
                held = held.iloc[1:]
            if held.empty:
                continue
            seg = (held * w_cand.reindex(held.columns).fillna(0.0)).sum(axis=1)
            per_candidate_segments[n].append(seg)

        # Blended portfolio weights = sum_i (soft_w_i * candidate_i_weights),
        # then renormalise (in case some candidates didn't cover all tickers).
        ticker_idx = sorted(set().union(*[set(c.index) for c in candidates.values() if not c.empty]))
        if not ticker_idx:
            continue
        w_blend = pd.Series(0.0, index=ticker_idx)
        for n in ENSEMBLE_SLOT_NAMES:
            if candidates[n].empty or soft_w.get(n, 0.0) <= 0:
                continue
            w_blend = w_blend.add(candidates[n].reindex(ticker_idx).fillna(0.0) * float(soft_w[n]),
                                  fill_value=0.0)
        w_blend = w_blend[w_blend > 1e-6]
        if w_blend.empty or w_blend.sum() <= 0:
            continue
        w_blend = w_blend / w_blend.sum()

        # Trend-following sleeve (core-satellite): blend a long-only inverse-vol
        # TSMOM sleeve with the ensemble. If nothing trends the sleeve is CASH,
        # so the blend de-risks by _trend_sleeve_w (w_blend then sums to <1).
        if _trend_sleeve_w > 0:
            _sleeve = _compute_trend_sleeve(px, t, _trade_universe, _caps_for_trend)
            _union_ts = sorted(set(w_blend.index) | set(_sleeve.index))
            w_blend = ((1.0 - _trend_sleeve_w) * w_blend.reindex(_union_ts).fillna(0.0)
                       + _trend_sleeve_w * _sleeve.reindex(_union_ts).fillna(0.0))
            w_blend = w_blend[w_blend > 1e-6]
            if w_blend.empty:
                continue
            if not _sleeve.empty:
                _n_trend_applied += 1

        # Volatility targeting (long-only): cap the ex-ante portfolio vol at
        # VOL_TARGET_ANNUAL by scaling the blend toward cash. σ_ex_ante =
        # sqrt(w'Σw) annualized from the SAME Σ the solve used (no look-ahead).
        # Long-only → scale capped at 1.0 (de-risk only, never lever up).
        if _vol_target > 0:
            _cv = [c for c in w_blend.index if c in Sigma.index]
            if len(_cv) >= 2:
                _wv = w_blend.reindex(_cv).fillna(0.0).values
                _Sig = Sigma.reindex(index=_cv, columns=_cv).fillna(0.0).values
                _var_d = float(_wv @ _Sig @ _wv)
                _sig_ann = float(np.sqrt(max(_var_d, 0.0) * 252.0))
                if _sig_ann > _vol_target > 0:
                    w_blend = w_blend * (_vol_target / _sig_ann)  # rest → cash
                    _n_vol_scaled += 1

        # Market-timed inverse-ETF crisis hedge (partial short): when SPY is
        # below its 200d SMA (trend down), scale the long book to (1-w) and
        # allocate w to BEAR.AX (-1x AU inverse). First lever with genuine
        # NEGATIVE beta. Applied AFTER vol-target (on the already-de-risked
        # book), BEFORE the crash-hedge. Uses only history up to t (no look-
        # ahead: the SMA is trailing). No effect unless _crisis_hedge_w > 0.
        if _crisis_hedge_w > 0 and _spy_lb_crisis is not None:
            _spy_now = _spy_ser_crisis.asof(t)
            _lb_now = _spy_lb_crisis.asof(t)
            if (pd.notna(_spy_now) and pd.notna(_lb_now)
                    and float(_spy_now) < float(_lb_now)):
                w_blend = w_blend * (1.0 - _crisis_hedge_w)
                w_blend.loc[_crisis_hedge_tkr] = (
                    w_blend.get(_crisis_hedge_tkr, 0.0) + _crisis_hedge_w)
                w_blend = w_blend[w_blend > 1e-6]
                _n_crisis_hedged += 1

        # Crash-hedge overlay: if SPY is in a deep enough drawdown (with
        # hysteresis), override the engine's blended target with the hedge
        # basket. Runs only if `crash_hedge=True` was passed to the engine.
        if crash_hedge and benchmark_ticker in px.columns:
            was_active = bool(_hedge_state.get("active", False))
            is_active = _check_crash_trigger(
                spy_history=px[benchmark_ticker],
                as_of=t,
                state=_hedge_state,
                dd_trigger=_hedge_trigger_eff,
                dd_release=_hedge_release_eff,
            )
            if is_active:
                _hedge_active_rebals += 1
                # Replace blended target with hedge basket. Extend ticker
                # index so hedge tickers are present even if engine wasn't
                # holding them.
                avail = px.columns
                hedge_w = _apply_crash_hedge(
                    weights=w_blend.reindex(w_blend.index.union(CRASH_HEDGE_BASKET.keys()))
                                .fillna(0.0),
                    basket=CRASH_HEDGE_BASKET,
                    available_tickers=avail,
                )
                hedge_w = hedge_w[hedge_w > 1e-6]
                if not hedge_w.empty and hedge_w.sum() > 0:
                    w_blend = hedge_w / hedge_w.sum()
            # Log state transitions (OFF→ON and ON→OFF) for diagnostics.
            if is_active != was_active:
                _hedge_events.append({
                    "date": t,
                    "transition": "ON" if is_active else "OFF",
                    "spy_dd": float(_hedge_state.get("last_dd", 0.0)),
                })
                if is_active:
                    _hedge_n_triggers += 1

        # --- Conditional skip: if target weight change is tiny, hold prior
        # weights — saves brokerage + CGT realisation on no-op re-trims.
        skip_rebal = False
        _skip_delta_eff = float(SKIP_REBAL_DELTA if skip_rebal_delta is None
                                 else skip_rebal_delta)
        # Calm-conditional widening: CALM = not an early-trigger insertion
        # AND benchmark trailing-252d DD > -5%. Any stress signal keeps the
        # tight production threshold (classifier FIXED per pre-registration).
        if (_skip_calm_delta > 0 and _spy_dd_for_calm is not None
                and t not in _early_trigger_dates):
            try:
                _dd_now_calm = float(_spy_dd_for_calm.asof(t))
            except Exception:
                _dd_now_calm = -1.0
            if np.isfinite(_dd_now_calm) and _dd_now_calm > -0.05:
                _skip_delta_eff = _skip_calm_delta
                _n_calm_widened += 1
        if not _prev_blend_w.empty and _skip_delta_eff > 0:
            union_idx = sorted(set(_prev_blend_w.index).union(w_blend.index))
            delta_sum = float(
                (w_blend.reindex(union_idx).fillna(0.0)
                 - _prev_blend_w.reindex(union_idx).fillna(0.0)).abs().sum()
            )
            if delta_sum < _skip_delta_eff:
                skip_rebal = True
                w_blend = _prev_blend_w.copy()

        if skip_rebal:
            n_skipped += 1
        else:
            n_executed += 1

        # Resolve price snapshot once — needed by LT-deferral below and by
        # the lot update + TLH pass further down.
        try:
            px_at_t = px.loc[:t].iloc[-1]
        except Exception:
            px_at_t = None

        # --- LT-discount-aware sell deferral -----------------------------
        # Shield gain lots within LT_DEFER_WINDOW_DAYS of 12mo eligibility:
        # keep their weight (delta added back to the seller), and fund that
        # by shrinking this rebalance's BUYS pro-rata — the cash for those
        # buys was never raised. Return stream + brokerage + lot book all
        # see the adjusted weights, so the drift cost is fully modeled.
        _lt_defer_days = int(globals().get("LT_DEFER_WINDOW_DAYS", 0) or 0)
        # DD-conditional release: the shield stands down entirely so de-risking
        # sells run unshielded. Two rules (release-DD replaces early-trigger):
        #   LT_DEFER_RELEASE_DD < 0  → release when benchmark trailing-252d DD
        #                              at t is at/below the threshold (bear state).
        #   LT_DEFER_DD_CONDITIONAL  → release at early-triggered rebal dates.
        if _lt_defer_reldd < 0 and _spy_dd_for_defer is not None:
            try:
                _dd_now = float(_spy_dd_for_defer.asof(t))
            except Exception:
                _dd_now = 0.0
            _lt_shield_released = bool(np.isfinite(_dd_now) and _dd_now <= _lt_defer_reldd)
        else:
            _lt_shield_released = (bool(globals().get("LT_DEFER_DD_CONDITIONAL", False))
                                   and t in _early_trigger_dates)
        if _lt_defer_days > 0 and _lt_shield_released:
            _defer_released_rebals += 1
        if (_lt_defer_days > 0 and not _lt_shield_released
                and not skip_rebal and px_at_t is not None
                and not _prev_blend_w.empty and _running_nav > 0):
            try:
                _union = sorted(set(_prev_blend_w.index).union(w_blend.index))
                _w_new_u = w_blend.reindex(_union).fillna(0.0)
                _w_old_u = _prev_blend_w.reindex(_union).fillna(0.0)
                _defer_extra: dict[str, float] = {}
                for tkr in _union:
                    p = float(px_at_t.get(tkr, np.nan))
                    if not np.isfinite(p) or p <= 0:
                        continue
                    cur_u = _lot_book.units(tkr)
                    tgt_u = (float(_w_new_u.get(tkr, 0.0)) * _running_nav) / p
                    sell_qty = cur_u - tgt_u
                    if sell_qty <= 1e-9:
                        continue
                    prot = _lot_book.near_lt_gain_units(tkr, p, t, _lt_defer_days)
                    if prot <= 0:
                        continue
                    free = max(0.0, cur_u - prot)
                    deferred_u = sell_qty - min(sell_qty, free)
                    if deferred_u > 1e-9:
                        _defer_extra[tkr] = deferred_u * p / _running_nav
                _D = float(sum(_defer_extra.values()))
                if 0.0 < _D < 0.5:
                    _delta_u = _w_new_u - _w_old_u
                    _buys = _delta_u[_delta_u > 1e-12]
                    _total_buys = float(_buys.sum())
                    if _total_buys > 1e-9:
                        w_eff = _w_new_u.copy()
                        for tkr, extra in _defer_extra.items():
                            w_eff[tkr] = float(w_eff.get(tkr, 0.0)) + extra
                        _reduce = min(_D, _total_buys)
                        for tkr, bamt in _buys.items():
                            w_eff[tkr] -= _reduce * (float(bamt) / _total_buys)
                        w_eff = w_eff.clip(lower=0.0)
                        _s = float(w_eff.sum())
                        if _s > 0:
                            w_blend = w_eff / _s
                        _defer_events += len(_defer_extra)
                        _defer_value_total += _D * _running_nav
            except Exception:
                pass

        blended_weights[t] = w_blend

        # Blended realised return segment (gross, before transaction costs)
        held_b = daily_rets_all.loc[t:seg_end, w_blend.index].fillna(0.0)
        if len(held_b) > 0 and held_b.index[0] == t:
            held_b = held_b.iloc[1:]
        if held_b.empty:
            continue
        seg_b = (held_b * w_blend.reindex(held_b.columns).fillna(0.0)).sum(axis=1)

        # NET-of-cost adjustment: charge the rebalance cost on the FIRST day
        # of the holding window. Skipped rebalances incur no cost.
        if skip_rebal:
            cost_frac = 0.0
        else:
            cost_frac = estimate_rebalance_cost_fraction(
                w_old=_prev_blend_w,
                w_new=w_blend,
                portfolio_value_aud=_running_nav,
            )
        rebalance_costs[t] = cost_frac

        # CGT: realise lot-level gains/losses at this rebalance and accumulate
        # them into the running FINANCIAL-YEAR buckets. Tax is NOT applied per
        # rebalance — that overestimates because it ignores intra-FY loss
        # offsetting. Tax is applied at FY-end (below) on net taxable.
        # Skipped rebalances do NOT update the lot book (no trades occurred).
        # (px_at_t resolved above, before the LT-deferral adjustment.)
        if not skip_rebal and px_at_t is not None:
            try:
                # Protect predicate for LT-deferral: skip near-LT gain lots
                # during FIFO allocation. Mirrors near_lt_gain_units exactly
                # so the weight adjustment above and the lot allocation here
                # shield the same lots.
                _protect_fn = None
                if _lt_defer_days > 0 and not _lt_shield_released:
                    _lt_thresh_days = int(CGT_CONFIG["lt_holding_days"])
                    def _make_protect(price_now):
                        def _protect(lot, hold_days):
                            return (price_now > float(lot["cost_basis_per_unit"])
                                    and (_lt_thresh_days - _lt_defer_days)
                                        <= hold_days < _lt_thresh_days)
                        return _protect
                    _protect_fn = _make_protect

                tickers_traded = sorted(set(_prev_blend_w.index).union(w_blend.index))
                for tkr in tickers_traded:
                    p = float(px_at_t.get(tkr, np.nan))
                    if not np.isfinite(p) or p <= 0:
                        continue
                    cur_units = _lot_book.units(tkr)
                    w_new_t = float(w_blend.get(tkr, 0.0))
                    target_units = (w_new_t * _running_nav) / p
                    delta_units = target_units - cur_units
                    if delta_units > 1e-9:
                        _lot_book.buy(tkr, delta_units, t, p)
                    elif delta_units < -1e-9:
                        out = _lot_book.sell(
                            tkr, -delta_units, t, p,
                            protect=(_protect_fn(p) if _protect_fn else None),
                        )
                        for k in _fy_buckets:
                            _fy_buckets[k] += out[k]
            except Exception:
                pass

        # TLH pass: scan current lots for unrealised losses, swap to substitute
        # (sells loss lot, buys substitute at same dollar value). Runs even on
        # skipped rebalances — a position may have drifted into harvestable
        # loss without the optimiser wanting to trim it. Losses fold straight
        # into the FY bucket so they offset gains at FY-end netting.
        if TLH_ENABLED and px_at_t is not None:
            try:
                tlh_out = _run_tlh_pass(
                    lot_book=_lot_book,
                    price_snapshot=px_at_t,
                    as_of=t,
                    cooldown_state=_tlh_cooldown,
                    pairs=TLH_PAIRS,
                    nav_aud=_running_nav,
                )
                for k in _fy_buckets:
                    _fy_buckets[k] += tlh_out["realised"][k]
                if tlh_out["n_events"]:
                    _tlh_events_all.extend(tlh_out["events"])
            except Exception as _e:
                # TLH is non-essential — never break the backtest on it.
                pass

        # FY-end tax check: if this rebalance falls in a NEW financial year
        # compared to the prior one, settle the prior FY's tax bill now.
        tax_frac = 0.0
        new_fy_end = _fy_end_for(t)
        if _current_fy_end is not None and new_fy_end > _current_fy_end:
            tax_frac, _carried_losses = _apply_fy_tax(_fy_buckets, _carried_losses, _running_nav)
            _fy_buckets = {"st_gain": 0.0, "lt_gain": 0.0, "st_loss": 0.0, "lt_loss": 0.0}
        _current_fy_end = new_fy_end
        rebalance_taxes[t] = tax_frac

        # Apply BOTH brokerage cost and (FY-end) tax to the first realised day
        # of the holding window. Brokerage hits every rebalance; tax hits only
        # at the first rebalance of a new FY.
        total_drag = cost_frac + tax_frac
        if len(seg_b) > 0 and total_drag > 0:
            seg_b.iloc[0] = float(seg_b.iloc[0]) - total_drag
        # Update running NAV for the next iteration (compound the net segment).
        _running_nav = float(_running_nav * float((1.0 + seg_b).prod()))
        _prev_blend_w = w_blend.copy()

        blended_segments.append(seg_b)

    if not blended_segments:
        return {"blended_returns": pd.Series(dtype=float),
                "per_candidate_returns": pd.DataFrame(),
                "softmax_history": pd.DataFrame(),
                "blended_weights": pd.DataFrame(),
                "per_candidate_weights": {n: pd.DataFrame() for n in ENSEMBLE_SLOT_NAMES}}

    blended_returns = pd.concat(blended_segments).sort_index()
    blended_returns = blended_returns[~blended_returns.index.duplicated(keep="last")]

    per_cand_rets_df = pd.DataFrame({
        n: (pd.concat(per_candidate_segments[n]).sort_index()
            if per_candidate_segments[n] else pd.Series(dtype=float))
        for n in ENSEMBLE_SLOT_NAMES
    })
    per_cand_rets_df = per_cand_rets_df[~per_cand_rets_df.index.duplicated(keep="last")]

    softmax_history = pd.DataFrame.from_dict(softmax_rows, orient="index").fillna(0.0)
    softmax_history = softmax_history.reindex(columns=ENSEMBLE_SLOT_NAMES, fill_value=0.0)
    blended_weights_df = pd.DataFrame.from_dict(blended_weights, orient="index").fillna(0.0)
    per_cand_weights_dfs = {n: pd.DataFrame.from_dict(per_candidate_weights[n], orient="index").fillna(0.0)
                            for n in ENSEMBLE_SLOT_NAMES}

    rebalance_costs_ser = pd.Series(rebalance_costs).sort_index() if rebalance_costs else pd.Series(dtype=float)
    rebalance_taxes_ser = pd.Series(rebalance_taxes).sort_index() if rebalance_taxes else pd.Series(dtype=float)
    if _skip_calm_delta > 0:
        print(f"[skip-calm] calm threshold {_skip_calm_delta*100:.0f}% applied at "
              f"{_n_calm_widened} rebal(s); skipped {n_skipped} of "
              f"{n_skipped + n_executed} total")
    if _stretch_floor > 0:
        print(f"[stretch-floor] top slot floored to {_stretch_floor*100:.0f}% "
              f"at {_n_stretch_floored} calm rebal(s)")
    if _trend_sleeve_w > 0:
        print(f"[trend-sleeve] {_trend_sleeve_w*100:.0f}% core-satellite blend; "
              f"sleeve invested (non-cash) at {_n_trend_applied} rebal(s)")
    if _cov_shrinkage and _lw_delta_n > 0:
        print(f"[cov-shrink] Ledoit-Wolf mean shrinkage δ={_lw_delta_sum/_lw_delta_n:.3f} "
              f"over {_lw_delta_n} rebal(s)")
    if _vol_target > 0:
        print(f"[vol-target] target {_vol_target*100:.0f}% ann; de-risked toward cash "
              f"at {_n_vol_scaled} rebal(s)")
    if _crisis_hedge_w > 0:
        _band_str = (f"{_crisis_ma_days}dMA" if _crisis_band_sd <= 0
                     else f"{_crisis_ma_days}dMA-{_crisis_band_sd:g}σ")
        print(f"[crisis-hedge] w={_crisis_hedge_w*100:.0f}% into {_crisis_hedge_tkr} "
              f"when SPY<{_band_str}; active at {_n_crisis_hedged} rebal(s)")
    if _defer_events > 0 or _defer_released_rebals > 0:
        print(f"[lt-defer] {_defer_events} lot-shield event(s) over window, "
              f"~${_defer_value_total:,.0f} of sells deferred past the 12mo "
              f"discount boundary (window={int(globals().get('LT_DEFER_WINDOW_DAYS', 0))}d); "
              f"shield released at {_defer_released_rebals} rebal(s)")
    return {
        "blended_returns": blended_returns,
        "per_candidate_returns": per_cand_rets_df,
        "softmax_history": softmax_history,
        "blended_weights": blended_weights_df,
        "per_candidate_weights": per_cand_weights_dfs,
        "rebalance_costs": rebalance_costs_ser,
        "rebalance_taxes": rebalance_taxes_ser,
        "n_scheduled": n_scheduled,
        "n_early_triggered": n_early_triggered,
        "n_skipped": n_skipped,
        "n_executed": n_executed,
        "tlh_events": _tlh_events_all,
        "hedge_events": _hedge_events,
        "hedge_active_rebals": _hedge_active_rebals,
        "hedge_n_triggers": _hedge_n_triggers,
        "hedge_forced_rebals": n_hedge_forced,
    }

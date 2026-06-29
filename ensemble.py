"""Ensemble regime-blending — the heart of the live trade plan.

Extracted from Portfolio_Optimiser.py for testability + module-split prep.
Pure functions: take per-candidate returns DataFrame + benchmark Series,
return softmax weights Series. No globals, no I/O.

Currently houses just `softmax_ensemble_weights`. Future scope: the
slot-construction helpers (Modest/Aggressive/Bold/Maximum/Stretch
solvers), the forward signal (20/50 SMA cross + 52W DD), and the
backward/forward blend will join here as the split progresses.

See ARCHITECTURE.md §3 "The regime ensemble in detail" for the design
rationale (why IR vs Sortino, why softmax not pick-one, why λ=3.0).
"""
from __future__ import annotations

import numpy as np
import pandas as pd


ANNUAL_TRADING_DAYS = 252


def softmax_ensemble_weights(
    per_candidate_returns: pd.DataFrame,
    lookback_days: int = 252,
    lambda_temp: float = 2.0,
    halflife_days: int = 60,
    benchmark_returns: pd.Series | None = None,
) -> pd.Series:
    """Softmax weight each candidate by its EWMA Information Ratio vs SPY.

    Replaces the prior EWMA Sortino with EWMA IR-vs-benchmark. The Sortino
    formula had a pathological failure mode: a Defensive slot with consistently
    small negative returns (e.g. SPY-25% target heavily allocating to inverse
    ETFs) has tiny downside semi-deviation, which inflates its Sortino ratio
    despite genuinely losing money. The softmax then over-weights it.

    Information Ratio penalises UNDERPERFORMANCE vs benchmark directly:
        IR = EWMA_mean(strat_ret - spy_ret) / EWMA_std(strat_ret - spy_ret)
    A candidate that systematically lags SPY gets a very negative IR no matter
    how low its absolute volatility is. Defensive slots only score competitively
    when they're actually beating SPY (i.e. during drawdowns) — which is when
    we want them activated.

    Falls back to absolute EWMA Sharpe (return / total vol) if no benchmark is
    provided — better than nothing but not the recommended path.

    Warm-up: equal weights until we have at least 60 daily observations.
    """
    candidates = list(per_candidate_returns.columns)
    n = len(candidates)
    if n == 0:
        return pd.Series(dtype=float)
    eq = pd.Series(1.0 / n, index=candidates)

    if per_candidate_returns.empty or len(per_candidate_returns) < 60:
        return eq

    win = per_candidate_returns.tail(lookback_days)
    if len(win) < 60:
        return eq

    # Align benchmark to the candidate window's index (use same dates only).
    bench_aligned = None
    if benchmark_returns is not None and not benchmark_returns.empty:
        bench_aligned = pd.to_numeric(benchmark_returns, errors="coerce")
        bench_aligned.index = pd.to_datetime(bench_aligned.index).tz_localize(None)
        bench_aligned = bench_aligned.sort_index().reindex(win.index)

    scores = {}
    for c in candidates:
        r = pd.to_numeric(win[c], errors="coerce").dropna()
        if len(r) < 60:
            scores[c] = np.nan
            continue
        if bench_aligned is not None:
            # Active return = strategy - benchmark, dropping any unaligned rows.
            pair = pd.concat([r, bench_aligned.reindex(r.index)], axis=1).dropna()
            if len(pair) < 60:
                scores[c] = np.nan
                continue
            active = pair.iloc[:, 0] - pair.iloc[:, 1]
            ewma_mean = float(active.ewm(halflife=halflife_days, adjust=False).mean().iloc[-1])
            # EWMA variance via EWMA of squared deviations
            active_demeaned = active - active.ewm(halflife=halflife_days, adjust=False).mean()
            ewma_var = float((active_demeaned ** 2).ewm(halflife=halflife_days, adjust=False).mean().iloc[-1])
            ewma_std = float(np.sqrt(ewma_var))
            if ewma_std > 0 and np.isfinite(ewma_std):
                # Information Ratio (annualised)
                scores[c] = (ewma_mean * ANNUAL_TRADING_DAYS /
                             (ewma_std * np.sqrt(ANNUAL_TRADING_DAYS)))
            else:
                scores[c] = np.nan
        else:
            # No benchmark → fall back to absolute Sharpe-style ratio
            ewma_mean = float(r.ewm(halflife=halflife_days, adjust=False).mean().iloc[-1])
            r_demeaned = r - r.ewm(halflife=halflife_days, adjust=False).mean()
            ewma_var = float((r_demeaned ** 2).ewm(halflife=halflife_days, adjust=False).mean().iloc[-1])
            ewma_std = float(np.sqrt(ewma_var))
            if ewma_std > 0 and np.isfinite(ewma_std):
                scores[c] = (ewma_mean * ANNUAL_TRADING_DAYS /
                             (ewma_std * np.sqrt(ANNUAL_TRADING_DAYS)))
            else:
                scores[c] = np.nan

    s = pd.Series(scores)
    if s.isna().all():
        return eq
    s_filled = s.fillna(s.min(skipna=True))
    z = float(lambda_temp) * s_filled.to_numpy(dtype=float)
    z = z - np.max(z)
    e = np.exp(z)
    w = e / e.sum() if e.sum() > 0 else np.full(n, 1.0 / n)
    return pd.Series(w, index=candidates)

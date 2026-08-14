"""Overfitting-aware validation statistics + universe-vintage filtering.

Two families, both aimed at weaknesses the 2026-08-13 audit established and
that no amount of additional backtesting can fix on its own:

  1. TRIAL-AWARE SHARPE INFERENCE (Bailey & Lopez de Prado).
     The engine's config was selected from ~150 logged backtest evaluations
     against a single 10-year path. A Sharpe reported without adjusting for
     that search is the maximum of a sample, not an estimate of the mean.
       - probabilistic_sharpe_ratio  P(true SR > threshold), skew/kurtosis aware
       - expected_max_sharpe         the SR you'd expect from N NULL trials
       - deflated_sharpe_ratio       PSR measured against that null
       - min_track_record_length     years of LIVE data needed to confirm it

  2. UNIVERSE VINTAGE.
     The 47-ticker universe was written in 2026 knowing which ETFs did well;
     16 of them did not exist at the backtest start. apply_universe_vintage
     restricts a price panel to instruments already trading as at a date, so
     "could this have been run in 2016?" becomes answerable.

All functions are pure: pandas/numpy in, floats/dicts out. No engine state.

Sharpe convention: every function here takes and returns a PER-PERIOD Sharpe
(daily, if fed daily returns). Annualise at the edges, not inside — mixing
the two is the classic way to get these formulas silently wrong.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

EULER_MASCHERONI = 0.5772156649015329


def _norm_cdf(x: float) -> float:
    from math import erf, sqrt
    return 0.5 * (1.0 + erf(float(x) / sqrt(2.0)))


def _norm_ppf(p: float) -> float:
    """Inverse standard-normal CDF (Acklam's rational approximation).

    Kept local so this module stays dependency-light — scipy is available in
    the venv but not guaranteed inside the frozen exe's trimmed imports.
    """
    if not (0.0 < p < 1.0):
        return float("nan")
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = np.sqrt(-2 * np.log(p))
        return float((((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) /
                     ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1))
    if p > phigh:
        q = np.sqrt(-2 * np.log(1 - p))
        return float(-(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) /
                     ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1))
    q = p - 0.5
    r = q * q
    return float((((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q /
                 (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1))


def sharpe_moments(returns: pd.Series) -> dict:
    """Per-period Sharpe plus the higher moments the PSR correction needs."""
    r = pd.to_numeric(returns, errors="coerce").dropna()
    n = len(r)
    if n < 3:
        return {"sr": np.nan, "n": n, "skew": np.nan, "kurt": np.nan}
    sd = float(r.std(ddof=1))
    if sd <= 0:
        return {"sr": np.nan, "n": n, "skew": np.nan, "kurt": np.nan}
    z = (r - r.mean()) / sd
    return {
        "sr": float(r.mean() / sd),
        "n": int(n),
        "skew": float((z ** 3).mean()),
        # RAW (not excess) kurtosis: the PSR formula uses (kurt-1)/4, which
        # is 1.0 for a normal distribution at kurt=3.
        "kurt": float((z ** 4).mean()),
    }


def probabilistic_sharpe_ratio(sr: float, n: int, skew: float, kurt: float,
                               sr_benchmark: float = 0.0) -> float:
    """P(true Sharpe > sr_benchmark), correcting for skew, kurtosis and n.

    Negative skew and fat tails INFLATE an observed Sharpe; this discounts it.
    All Sharpes are per-period.
    """
    if not np.isfinite([sr, skew, kurt]).all() or n < 3:
        return float("nan")
    denom = 1.0 - skew * sr + ((kurt - 1.0) / 4.0) * sr ** 2
    if denom <= 0:
        return float("nan")
    return _norm_cdf((sr - sr_benchmark) * np.sqrt(n - 1) / np.sqrt(denom))


def expected_max_sharpe(sr_variance: float, n_trials: int) -> float:
    """Expected MAXIMUM per-period Sharpe across n_trials of a null strategy.

    This is the bar a real edge has to clear. Search hard enough over random
    strategies and one will look good; this says how good, for free.
    """
    if n_trials < 2 or not np.isfinite(sr_variance) or sr_variance <= 0:
        return 0.0
    sd = float(np.sqrt(sr_variance))
    g = EULER_MASCHERONI
    return float(sd * ((1.0 - g) * _norm_ppf(1.0 - 1.0 / n_trials)
                       + g * _norm_ppf(1.0 - 1.0 / (n_trials * np.e))))


def deflated_sharpe_ratio(returns: pd.Series, trial_sharpes, n_trials=None) -> dict:
    """PSR measured against the expected max Sharpe of the search that found it.

    `trial_sharpes` is the spread of Sharpes across the variants actually
    evaluated — its VARIANCE is what sets the null. Pass n_trials to override
    the count (correlated variants are not independent trials, so the honest
    move is to report a RANGE, not one number).
    """
    m = sharpe_moments(returns)
    t = pd.to_numeric(pd.Series(list(trial_sharpes)), errors="coerce").dropna()
    if len(t) < 2 or not np.isfinite(m["sr"]):
        return {**m, "dsr": np.nan, "sr_null": np.nan, "n_trials": len(t)}
    n_tr = int(n_trials or len(t))
    # Trial Sharpes are typically stored ANNUALISED; the null must be built in
    # the same per-period units as m["sr"].
    ppy = _infer_ppy(returns)
    var_ann = float(t.var(ddof=1))
    sr_null = expected_max_sharpe(var_ann / ppy, n_tr)
    return {
        **m,
        "n_trials": n_tr,
        "sr_null": sr_null,
        "sr_null_ann": sr_null * np.sqrt(ppy),
        "dsr": probabilistic_sharpe_ratio(m["sr"], m["n"], m["skew"], m["kurt"],
                                          sr_benchmark=sr_null),
    }


def min_track_record_length(sr: float, n_unused: int, skew: float, kurt: float,
                            sr_benchmark: float = 0.0,
                            confidence: float = 0.95) -> float:
    """Observations of LIVE data needed to confirm sr > sr_benchmark.

    The honest answer to "how long before we know this works". Returns
    observations; divide by periods-per-year for years. inf when the observed
    Sharpe does not exceed the benchmark at all — no amount of data confirms
    an edge that isn't there.
    """
    if not np.isfinite([sr, skew, kurt]).all() or sr <= sr_benchmark:
        return float("inf")
    denom = 1.0 - skew * sr + ((kurt - 1.0) / 4.0) * sr ** 2
    if denom <= 0:
        return float("inf")
    z = _norm_ppf(confidence)
    return float(1.0 + denom * (z / (sr - sr_benchmark)) ** 2)


def _infer_ppy(returns: pd.Series) -> float:
    idx = getattr(returns, "index", None)
    if isinstance(idx, pd.DatetimeIndex) and len(returns) > 1:
        yrs = (idx[-1] - idx[0]).days / 365.25
        if yrs > 0:
            ppy = len(returns) / yrs
            if 200.0 <= ppy <= 400.0:
                return float(ppy)
    return 252.0


# ---------------------------------------------------------------------------
# Universe vintage
# ---------------------------------------------------------------------------

def first_trade_dates(panel: pd.DataFrame) -> dict:
    """First real observation per column. Call BEFORE any ffill/bfill."""
    return {c: panel[c].first_valid_index() for c in panel.columns}


def apply_universe_vintage(panel: pd.DataFrame, vintage, keep=()) -> tuple:
    """Drop columns not yet trading as at `vintage`.

    Answers "could this strategy have been run then?" — the coverage gate
    handles a ticker's TIMING, but the candidate list itself was written in
    2026 with the outcomes known. `keep` protects non-tradeable columns the
    engine needs regardless (benchmarks like ^AORD).

    Returns (filtered_panel, dropped_tickers).
    """
    if vintage is None or panel is None or panel.empty:
        return panel, []
    v = pd.Timestamp(vintage)
    fv = first_trade_dates(panel)
    drop = [c for c, f in fv.items()
            if c not in keep and (f is None or f > v)]
    if not drop:
        return panel, []
    return panel.drop(columns=drop), sorted(drop)


# ---------------------------------------------------------------------------
# Probability of Backtest Overfitting (CSCV)
# ---------------------------------------------------------------------------

def probability_of_backtest_overfitting(trial_matrix: pd.DataFrame,
                                        n_splits: int = 16) -> dict:
    """PBO via Combinatorially Symmetric Cross-Validation.

    Bailey, Borwein, Lopez de Prado & Zhu. Answers the question the Deflated
    Sharpe cannot: when you pick the best config in-sample, how often does it
    land BELOW the median out-of-sample? That is overfitting measured directly,
    rather than inferred from a trial count.

    `trial_matrix`: rows = time, columns = one config's return series, all on
    the same evaluation window (variant_store.load_trial_matrix guarantees
    this — comparing configs across different windows is meaningless here).

    Returns pbo plus the logit distribution. PBO near 0 => the in-sample
    winner generally stays a winner. PBO >= 0.5 => selection is noise.
    """
    from itertools import combinations

    M = trial_matrix.dropna(how="any")
    n_obs, n_trials = M.shape
    if n_trials < 2 or n_obs < n_splits * 2:
        return {"pbo": np.nan, "n_trials": n_trials, "n_obs": n_obs,
                "reason": "need >=2 trials and >=2 rows per split"}
    if n_splits % 2:
        n_splits += 1

    blocks = np.array_split(np.arange(n_obs), n_splits)
    half = n_splits // 2
    logits = []
    for combo in combinations(range(n_splits), half):
        is_rows = np.concatenate([blocks[i] for i in combo])
        oos_rows = np.concatenate([blocks[i] for i in range(n_splits)
                                   if i not in combo])
        is_m, oos_m = M.values[is_rows], M.values[oos_rows]

        def _sr(a):
            sd = a.std(axis=0, ddof=1)
            with np.errstate(divide="ignore", invalid="ignore"):
                return np.where(sd > 0, a.mean(axis=0) / sd, -np.inf)

        best = int(np.argmax(_sr(is_m)))
        oos_sr = _sr(oos_m)
        # Relative rank of the IS winner in the OOS ordering.
        rank = float((oos_sr < oos_sr[best]).sum() + 1) / (n_trials + 1)
        rank = min(max(rank, 1e-6), 1 - 1e-6)
        logits.append(np.log(rank / (1.0 - rank)))

    logits = np.asarray(logits, dtype=float)
    # With few trials the OOS rank can only take a handful of values, so the
    # statistic is an artifact of the arithmetic rather than a measurement —
    # 2 trials can only ever yield a rank of 1/3 or 2/3. Flag it loudly rather
    # than let a number that looks like a result get quoted as one.
    underpowered = n_trials < 10
    out = {
        "pbo": float((logits <= 0).mean()),
        "n_trials": int(n_trials),
        "n_obs": int(n_obs),
        "n_combinations": int(len(logits)),
        "logit_median": float(np.median(logits)),
        "underpowered": bool(underpowered),
    }
    if underpowered:
        out["reason"] = (f"only {n_trials} trials — PBO needs >=10 distinct "
                         f"configs on one window to mean anything; treat as N/A")
    return out

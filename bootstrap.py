"""Stationary block bootstrap for OOS return series (validation breadth).

Turns a single realised walk-forward path into a DISTRIBUTION of headline
metrics, so "Sharpe 0.94 / MaxDD -20%" becomes "Sharpe 0.94 [90% CI ...],
5th-pct MaxDD -x%, beats SPY in N% of paths". Quantifies how much of the
headline is signal vs the luck of one path.

Method: Politis & Romano (1994) stationary block bootstrap — resample blocks of
GEOMETRICALLY-distributed length (mean `mean_block`) with wrap-around. Random
block lengths keep the resampled series stationary while preserving the
short-horizon dependence (vol-clustering, momentum) that an iid bootstrap
destroys. A paired resample (SAME block indices for strategy and benchmark)
gives an honest alpha CI that respects their co-movement.

Pure + unit-tested; no engine state. Reuses metrics._series_metrics so the
bootstrapped numbers match the engine's own metric conventions exactly.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from metrics import _series_metrics


def stationary_block_bootstrap_indices(n: int, mean_block: float,
                                       rng: np.random.Generator) -> np.ndarray:
    """One resampled index path of length n (Politis-Romano stationary bootstrap).

    Each block starts at a uniform-random position and has Geometric(1/mean_block)
    length; indices wrap around (circular) so every observation is reachable and
    the series stays stationary."""
    if n <= 0:
        return np.empty(0, dtype=int)
    p = 1.0 / max(float(mean_block), 1.0)
    out = np.empty(n, dtype=int)
    i = 0
    while i < n:
        start = int(rng.integers(0, n))
        length = int(rng.geometric(p))
        for j in range(length):
            if i >= n:
                break
            out[i] = (start + j) % n
            i += 1
    return out


def block_bootstrap_metrics(returns, n_boot: int = 1000, mean_block: float = 20.0,
                            seed: int = 42, rf_annual: float = 0.0,
                            bench=None) -> pd.DataFrame:
    """Bootstrap `n_boot` synthetic paths and return one metrics row each.

    Columns: ann_return, sharpe, max_drawdown, ann_vol (+ alpha_vs_bench,
    sharpe_minus_bench when `bench` is given — computed on the SAME indices as
    the strategy path, so the paired advantage respects co-movement). `bench`
    must align 1:1 with `returns` (same length/order)."""
    r = pd.to_numeric(pd.Series(returns), errors="coerce").dropna()
    n = len(r)
    if n < 30:
        return pd.DataFrame()
    r_arr = r.to_numpy(dtype=float)
    b_arr = None
    if bench is not None:
        b = pd.to_numeric(pd.Series(bench), errors="coerce")
        b = b.reindex(r.index) if b.index.equals(r.index) or len(b) == n else b
        b_arr = pd.Series(b).to_numpy(dtype=float)[:n]
    rng = np.random.default_rng(seed)
    rows = []
    for _ in range(int(n_boot)):
        ix = stationary_block_bootstrap_indices(n, mean_block, rng)
        m = _series_metrics(pd.Series(r_arr[ix]), rf_annual)
        row = {
            "ann_return": m["Annualised Return"],
            "sharpe": m["Sharpe Ratio"],
            "max_drawdown": m["Max Drawdown"],
            "ann_vol": m["Annualised Volatility"],
        }
        if b_arr is not None and not np.isnan(b_arr).all():
            bm = _series_metrics(pd.Series(b_arr[ix]), rf_annual)
            row["alpha_vs_bench"] = m["Annualised Return"] - bm["Annualised Return"]
            row["sharpe_minus_bench"] = m["Sharpe Ratio"] - bm["Sharpe Ratio"]
        rows.append(row)
    return pd.DataFrame(rows)


def _worst_window(returns_arr: np.ndarray, win: int) -> tuple[int, int]:
    """[start, end) of the `win`-day window with the lowest compounded return."""
    n = len(returns_arr)
    if n < win or win <= 0:
        return 0, n
    csum = np.cumsum(np.log1p(returns_arr))
    best_i, best_val = 0, np.inf
    for i in range(0, n - win + 1):
        val = csum[i + win - 1] - (csum[i - 1] if i > 0 else 0.0)
        if val < best_val:
            best_val, best_i = val, i
    return best_i, best_i + win


def crisis_stressed_bootstrap_metrics(returns, n_sim: int = 1000,
                                      crisis_prob: float = 0.20,
                                      crisis_severity: float = 1.5,
                                      crisis_len: int = 60,
                                      mean_block: float = 20.0, seed: int = 42,
                                      rf_annual: float = 0.0) -> pd.DataFrame:
    """Monte-Carlo tail probe for UNSEEN regimes. A stationary block bootstrap
    (preserves vol-clustering) into which, with probability `crisis_prob` per
    path, we inject a crisis worse than any in-sample: the worst `crisis_len`-day
    window's daily returns amplified by `crisis_severity` (>1 = beyond history).

    Answers "what's the tail if a crisis 1.5x worse than 2016-2026's worst hits
    ~20% of decade-paths?" — the honest step past the bootstrap, which can never
    exceed the sample's worst block. Calibrated to real data + explicit stress
    assumptions (crisis_prob, crisis_severity), not a black-box generator."""
    r = pd.to_numeric(pd.Series(returns), errors="coerce").dropna()
    n = len(r)
    if n < 60:
        return pd.DataFrame()
    r_arr = r.to_numpy(dtype=float)
    cwin = min(int(crisis_len), n)
    c0, c1 = _worst_window(r_arr, cwin)
    crisis = r_arr[c0:c1] * float(crisis_severity)
    L = len(crisis)
    rng = np.random.default_rng(seed)
    rows = []
    for _ in range(int(n_sim)):
        ix = stationary_block_bootstrap_indices(n, mean_block, rng)
        path = r_arr[ix].copy()
        had_crisis = False
        if rng.random() < crisis_prob and L > 0 and n > L:
            start = int(rng.integers(0, n - L))
            path[start:start + L] = crisis
            had_crisis = True
        m = _series_metrics(pd.Series(path), rf_annual)
        rows.append({
            "ann_return": m["Annualised Return"],
            "sharpe": m["Sharpe Ratio"],
            "max_drawdown": m["Max Drawdown"],
            "ann_vol": m["Annualised Volatility"],
            "had_crisis": had_crisis,
        })
    return pd.DataFrame(rows)


def summarize_distribution(df: pd.DataFrame,
                           pcts=(5, 25, 50, 75, 95)) -> dict:
    """Percentile summary + robustness fractions for a bootstrap metrics frame."""
    if df is None or df.empty:
        return {}
    out = {"n_boot": int(len(df)), "percentiles": {}}
    for col in df.columns:
        # astype(float) so a bool column (e.g. had_crisis) doesn't blow up
        # np.percentile ("boolean subtract not supported").
        s = pd.to_numeric(df[col], errors="coerce").astype(float).dropna()
        if s.empty:
            continue
        out["percentiles"][col] = {f"p{p}": float(np.percentile(s, p)) for p in pcts}
        out["percentiles"][col]["mean"] = float(s.mean())
    # Robustness fractions (share of paths clearing a sensible bar)
    frac = {}
    if "sharpe" in df:
        frac["sharpe_gt_0"] = float((df["sharpe"] > 0).mean())
        frac["sharpe_gt_0p5"] = float((df["sharpe"] > 0.5).mean())
    if "alpha_vs_bench" in df:
        frac["beats_bench"] = float((df["alpha_vs_bench"] > 0).mean())
    if "sharpe_minus_bench" in df:
        frac["sharpe_beats_bench"] = float((df["sharpe_minus_bench"] > 0).mean())
    out["robustness_fractions"] = frac
    return out

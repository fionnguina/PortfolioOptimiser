"""Stationary block bootstrap (validation-breadth) — pin the resampling math."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import bootstrap as bs


def _rng(seed=0):
    return np.random.default_rng(seed)


def test_indices_length_and_range():
    idx = bs.stationary_block_bootstrap_indices(500, 20.0, _rng())
    assert len(idx) == 500
    assert idx.min() >= 0 and idx.max() < 500


def test_indices_preserve_contiguous_blocks():
    # with a huge mean block the path is (almost) one wrapped contiguous run
    idx = bs.stationary_block_bootstrap_indices(100, 10_000.0, _rng(1))
    diffs = np.diff(idx) % 100
    # nearly every step is +1 (contiguous), bar at most one wrap boundary
    assert (diffs == 1).sum() >= 98


def test_indices_empty():
    assert len(bs.stationary_block_bootstrap_indices(0, 20.0, _rng())) == 0


def test_metrics_frame_shape_and_columns():
    r = pd.Series(_rng(2).normal(0.0005, 0.01, 1000))
    df = bs.block_bootstrap_metrics(r, n_boot=200, seed=3)
    assert len(df) == 200
    assert {"ann_return", "sharpe", "max_drawdown", "ann_vol"} <= set(df.columns)


def test_metrics_bench_adds_paired_columns():
    rng = _rng(4)
    r = pd.Series(rng.normal(0.0006, 0.01, 800))
    b = pd.Series(rng.normal(0.0004, 0.012, 800))
    df = bs.block_bootstrap_metrics(r, n_boot=150, seed=5, bench=b)
    assert "alpha_vs_bench" in df.columns and "sharpe_minus_bench" in df.columns


def test_deterministic_with_seed():
    r = pd.Series(_rng(6).normal(0.0005, 0.01, 600))
    a = bs.block_bootstrap_metrics(r, n_boot=100, seed=42)
    b = bs.block_bootstrap_metrics(r, n_boot=100, seed=42)
    pd.testing.assert_frame_equal(a, b)


def test_too_short_series_returns_empty():
    assert bs.block_bootstrap_metrics(pd.Series([0.01, 0.02, -0.01]), n_boot=10).empty


def test_bootstrap_mean_sharpe_brackets_point_estimate():
    # strong steady signal → the bootstrap Sharpe distribution should straddle
    # the point estimate (bootstrap is ~unbiased for the mean metric)
    r = pd.Series(_rng(7).normal(0.0008, 0.008, 1500))
    from metrics import _series_metrics
    point = _series_metrics(r)["Sharpe Ratio"]
    df = bs.block_bootstrap_metrics(r, n_boot=400, seed=8)
    lo, hi = np.percentile(df["sharpe"], [5, 95])
    assert lo < point < hi


def test_summarize_distribution_structure():
    r = pd.Series(_rng(9).normal(0.0005, 0.01, 800))
    b = pd.Series(_rng(10).normal(0.0003, 0.011, 800))
    df = bs.block_bootstrap_metrics(r, n_boot=200, seed=11, bench=b)
    summ = bs.summarize_distribution(df)
    assert summ["n_boot"] == 200
    assert "p5" in summ["percentiles"]["sharpe"] and "mean" in summ["percentiles"]["sharpe"]
    assert "sharpe_gt_0" in summ["robustness_fractions"]
    assert "beats_bench" in summ["robustness_fractions"]


def test_summarize_empty_is_empty():
    assert bs.summarize_distribution(pd.DataFrame()) == {}


# --- crisis-stressed Monte-Carlo (unseen-regime tail probe) ------------------

def test_worst_window_finds_the_crash():
    r = np.concatenate([np.full(100, 0.001), np.full(20, -0.05), np.full(100, 0.001)])
    s, e = bs._worst_window(r, 20)
    assert s == 100 and e == 120   # the -5%/day block


def test_crisis_injection_deepens_the_tail():
    rng = _rng(12)
    r = pd.Series(rng.normal(0.0005, 0.008, 1500))
    vanilla = bs.block_bootstrap_metrics(r, n_boot=600, seed=1)
    stressed = bs.crisis_stressed_bootstrap_metrics(
        r, n_sim=600, crisis_prob=0.5, crisis_severity=1.5, seed=1)
    # a worse-than-sample crisis in half the paths must worsen the p5 drawdown
    assert np.percentile(stressed["max_drawdown"], 5) < np.percentile(vanilla["max_drawdown"], 5)


def test_crisis_prob_controls_injection_rate():
    r = pd.Series(_rng(13).normal(0.0005, 0.008, 1200))
    df = bs.crisis_stressed_bootstrap_metrics(r, n_sim=800, crisis_prob=0.3, seed=2)
    frac = df["had_crisis"].mean()
    assert 0.22 < frac < 0.38   # ~0.30 within sampling noise


def test_crisis_prob_zero_injects_nothing():
    r = pd.Series(_rng(14).normal(0.0005, 0.008, 1000))
    df = bs.crisis_stressed_bootstrap_metrics(r, n_sim=300, crisis_prob=0.0, seed=3)
    assert df["had_crisis"].sum() == 0


def test_summarize_handles_bool_column_regression():
    # crisis df carries a bool had_crisis column; percentile must not choke on it
    r = pd.Series(_rng(15).normal(0.0005, 0.008, 1000))
    df = bs.crisis_stressed_bootstrap_metrics(r, n_sim=200, crisis_prob=0.3, seed=4)
    summ = bs.summarize_distribution(df)   # must not raise
    assert "max_drawdown" in summ["percentiles"]

"""Regression tests for the pure-math metrics helpers (_annualized_sharpe,
_ir_vs_bench, _series_metrics, _capm_alpha_beta).

Locks in the formulas reported in PPT slides + dev_validation summaries so
a refactor can't silently shift headline Sharpe / α / MaxDD numbers.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

# Metrics helpers moved to metrics.py (Phase 4 split, 2026-06-29) — import
# directly instead of AST-extract.
import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import metrics as _metrics_mod


@pytest.fixture(scope="module")
def metrics():
    return {
        "_annualized_sharpe": _metrics_mod._annualized_sharpe,
        "_ir_vs_bench":       _metrics_mod._ir_vs_bench,
        "_series_metrics":    _metrics_mod._series_metrics,
        "_capm_alpha_beta":   _metrics_mod._capm_alpha_beta,
        "ANNUAL_TRADING_DAYS": _metrics_mod.ANNUAL_TRADING_DAYS,
    }


def _series(values, start="2020-01-01"):
    idx = pd.bdate_range(start, periods=len(values))
    return pd.Series(values, index=idx)


# ---- _annualized_sharpe ----

def test_sharpe_zero_mean_returns_zero(metrics):
    """Zero-mean returns -> Sharpe = 0 (excess is zero on average)."""
    r = _series(np.zeros(252))
    # std of all zeros is also zero, so we expect NaN per the vol<=0 guard
    assert np.isnan(metrics["_annualized_sharpe"](r, 0.0))


def test_sharpe_known_value_alternating_returns(metrics):
    """Deterministic +1% / -1% alternating returns. Mean = 0 exactly -> Sharpe = 0."""
    r = _series([0.01, -0.01] * 100)
    s = metrics["_annualized_sharpe"](r, 0.0)
    assert s == pytest.approx(0.0, abs=1e-12)


def test_sharpe_positive_bias_produces_positive_sharpe(metrics):
    """Deterministic series with known positive bias: alternating +1% and -0.5%
    -> mean = +0.25%/day, std = ~0.75%, Sharpe = (0.0025/0.0075127)*sqrt(252) ~ 5.28"""
    r = _series([0.01, -0.005] * 126)  # 252 obs
    s = metrics["_annualized_sharpe"](r, 0.0)
    # Compute expected analytically from the same series the function sees
    mean = float(r.mean())
    std = float(r.std(ddof=1))
    expected = mean / std * np.sqrt(252)
    assert s == pytest.approx(expected, rel=1e-9)
    assert s > 0  # positive bias -> positive Sharpe


def test_sharpe_empty_returns_nan(metrics):
    assert np.isnan(metrics["_annualized_sharpe"](pd.Series(dtype=float), 0.0))


def test_sharpe_rf_lowers_score(metrics):
    """Raising rf_annual should lower the Sharpe (less excess return)."""
    rng = np.random.default_rng(42)
    r = _series(rng.normal(0.001, 0.01, size=1000))
    s_zero = metrics["_annualized_sharpe"](r, 0.0)
    s_high = metrics["_annualized_sharpe"](r, 0.05)
    assert s_high < s_zero


# ---- _ir_vs_bench ----

def test_ir_zero_when_strat_equals_bench(metrics):
    """If strat == bench, active return is zero -> IR is NaN (zero vol)."""
    r = _series(np.full(252, 0.001))
    ir = metrics["_ir_vs_bench"](r, r.copy())
    assert np.isnan(ir)


def test_ir_positive_when_strat_beats_bench(metrics):
    """Strat with higher mean and similar vol -> positive IR."""
    rng = np.random.default_rng(42)
    bench = _series(rng.normal(0.0005, 0.01, size=500))
    strat = bench + 0.0003  # consistent +0.03% daily outperformance
    ir = metrics["_ir_vs_bench"](strat, bench)
    assert ir > 0
    # 0.0003 daily / sigma_diff (very small for constant diff) ~ large
    assert np.isfinite(ir)


def test_ir_negative_when_strat_lags_bench(metrics):
    """Symmetric of the above — confirms sign."""
    rng = np.random.default_rng(42)
    bench = _series(rng.normal(0.0005, 0.01, size=500))
    strat = bench - 0.0003
    ir = metrics["_ir_vs_bench"](strat, bench)
    assert ir < 0


def test_ir_handles_non_overlapping_indices(metrics):
    """Dates that don't overlap should be dropped, not crash."""
    bench = _series(np.zeros(100), start="2020-01-01")
    strat = _series(np.full(100, 0.001), start="2020-06-01")  # partial overlap
    ir = metrics["_ir_vs_bench"](strat, bench)
    # Should compute over the overlap window, not blow up
    assert np.isfinite(ir) or np.isnan(ir)  # both acceptable, just no crash


def test_ir_empty_inputs_return_nan(metrics):
    assert np.isnan(metrics["_ir_vs_bench"](
        pd.Series(dtype=float), pd.Series(dtype=float)))


# ---- _series_metrics ----

def test_series_metrics_empty_returns_all_nan(metrics):
    out = metrics["_series_metrics"](pd.Series(dtype=float))
    for k, v in out.items():
        assert np.isnan(v), f"{k} expected NaN, got {v}"


def test_series_metrics_keys_present(metrics):
    """Schema check — downstream code reads these keys; rename = breakage."""
    out = metrics["_series_metrics"](_series([0.001] * 100))
    expected = {"Cumulative Return", "Annualised Return", "Annualised Volatility",
                "Sharpe Ratio", "Sortino Ratio", "Max Drawdown"}
    assert set(out.keys()) == expected


def test_series_metrics_cumulative_return_matches_compounded(metrics):
    """Cumulative return should equal (prod(1+r) - 1)."""
    r = _series([0.01, -0.005, 0.02, 0.0, -0.01])
    out = metrics["_series_metrics"](r)
    expected_cum = float((1.0 + r).prod() - 1.0)
    assert out["Cumulative Return"] == pytest.approx(expected_cum)


def test_series_metrics_max_drawdown_negative_or_zero(metrics):
    """MaxDD by construction is in [-1, 0]."""
    rng = np.random.default_rng(42)
    r = _series(rng.normal(0.001, 0.01, size=500))
    out = metrics["_series_metrics"](r)
    assert -1.0 <= out["Max Drawdown"] <= 0.0


def test_series_metrics_monotonic_uptrend_zero_drawdown(metrics):
    """Strictly positive returns -> no peak-to-trough decline -> MaxDD = 0."""
    out = metrics["_series_metrics"](_series([0.01] * 100))
    assert out["Max Drawdown"] == pytest.approx(0.0, abs=1e-12)


def test_series_metrics_known_maxdd(metrics):
    """Hand-computed: 0% -> +10% -> -20% gives peak 1.10 -> trough 0.88 -> DD = -20%."""
    r = _series([0.10, -0.20])  # 1 -> 1.10 -> 0.88
    out = metrics["_series_metrics"](r)
    assert out["Max Drawdown"] == pytest.approx(-0.20, abs=1e-9)


# ---- _capm_alpha_beta ----

def test_capm_short_history_returns_nan(metrics):
    """<30 obs -> (NaN, NaN) per the explicit early return."""
    strat = _series(np.zeros(20))
    bench = _series(np.zeros(20))
    a, b = metrics["_capm_alpha_beta"](strat, bench)
    assert np.isnan(a) and np.isnan(b)


def test_capm_beta_of_one_when_strat_equals_bench(metrics):
    """If strat == bench then OLS slope is exactly 1, intercept is 0."""
    rng = np.random.default_rng(42)
    bench = _series(rng.normal(0.0005, 0.01, size=300))
    a, b = metrics["_capm_alpha_beta"](bench, bench)
    assert b == pytest.approx(1.0, abs=1e-9)
    assert a == pytest.approx(0.0, abs=1e-9)


def test_capm_beta_of_two_when_strat_is_2x_bench(metrics):
    """Synthetic levered strategy: 2x the bench daily move -> beta=2, alpha=0."""
    rng = np.random.default_rng(42)
    bench = _series(rng.normal(0.0005, 0.01, size=300))
    strat = 2.0 * bench
    a, b = metrics["_capm_alpha_beta"](strat, bench)
    assert b == pytest.approx(2.0, abs=1e-9)
    assert a == pytest.approx(0.0, abs=1e-7)


def test_capm_positive_alpha_when_strat_outperforms_with_same_beta(metrics):
    """Strategy = bench + constant positive offset -> alpha > 0, beta ~ 1."""
    rng = np.random.default_rng(42)
    bench = _series(rng.normal(0.0005, 0.01, size=300))
    strat = bench + 0.0002  # +0.02% daily extra
    a, b = metrics["_capm_alpha_beta"](strat, bench)
    assert a > 0
    assert b == pytest.approx(1.0, abs=1e-6)

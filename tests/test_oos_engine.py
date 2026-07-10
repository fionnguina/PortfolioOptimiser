"""Unit tests for the OOS backtest engine's analytics helpers (oos_engine.py).

The 797-line run_oos_ensemble_walk_forward is validated end-to-end by the
walk-forward-CV byte-diff; here we lock in the small, load-bearing helpers it
composes: crash-trigger hysteresis, crash-hedge basket replacement, ensemble
signal blending, mu-shrinkage, the forward regime signal, the trend sleeve, the
US-ticker test, and the rebalance-cost estimate.
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import oos_engine
import brokerage


# ============================================================================
# _is_us_ticker
# ============================================================================

@pytest.mark.parametrize("tkr,expected", [
    ("SMH", True), ("SPY", True), ("QQQ", True),
    ("IVV.AX", False), ("VAS.AX", False), ("GOLD.AX", False),
    ("^AORD", False), ("^GSPC", False),
])
def test_is_us_ticker(tkr, expected):
    assert oos_engine._is_us_ticker(tkr) is expected


# ============================================================================
# blend_ensemble_signals
# ============================================================================

def _dist(vals, names=("Modest", "Mid", "Stretch")):
    return pd.Series(vals, index=list(names))

def test_blend_alpha_one_is_all_backward():
    bw, fw = _dist([0.2, 0.3, 0.5]), _dist([0.9, 0.05, 0.05])
    out = oos_engine.blend_ensemble_signals(bw, fw, backward_alpha=1.0)
    pd.testing.assert_series_equal(out.sort_index(), bw.sort_index(), atol=1e-9)

def test_blend_alpha_zero_is_all_forward():
    bw, fw = _dist([0.2, 0.3, 0.5]), _dist([0.9, 0.05, 0.05])
    out = oos_engine.blend_ensemble_signals(bw, fw, backward_alpha=0.0)
    pd.testing.assert_series_equal(out.sort_index(), fw.sort_index(), atol=1e-9)

def test_blend_sums_to_one_and_is_convex():
    bw, fw = _dist([0.2, 0.3, 0.5]), _dist([0.8, 0.1, 0.1])
    out = oos_engine.blend_ensemble_signals(bw, fw, backward_alpha=0.7)
    assert out.sum() == pytest.approx(1.0)
    # 70/30 blend of Modest: 0.7*0.2 + 0.3*0.8 = 0.38
    assert out["Modest"] == pytest.approx(0.38, abs=1e-9)

def test_blend_empty_backward_returns_forward():
    fw = _dist([0.5, 0.3, 0.2])
    out = oos_engine.blend_ensemble_signals(pd.Series(dtype=float), fw, 0.7)
    pd.testing.assert_series_equal(out.sort_index(), fw.sort_index())


# ============================================================================
# _apply_mu_shrinkage
# ============================================================================

@pytest.fixture
def _mu_lambda():
    saved = oos_engine.MU_SHRINKAGE_LAMBDA
    yield lambda v: setattr(oos_engine, "MU_SHRINKAGE_LAMBDA", v)
    oos_engine.MU_SHRINKAGE_LAMBDA = saved

def test_mu_shrinkage_lambda_zero_is_identity(_mu_lambda):
    _mu_lambda(0.0)
    mu = pd.Series([0.20, 0.10, 0.02], index=["A", "B", "C"])
    pd.testing.assert_series_equal(oos_engine._apply_mu_shrinkage(mu), mu)

def test_mu_shrinkage_full_collapses_to_median(_mu_lambda):
    _mu_lambda(1.0)
    mu = pd.Series([0.20, 0.10, 0.02], index=["A", "B", "C"])  # median 0.10
    out = oos_engine._apply_mu_shrinkage(mu)
    assert np.allclose(out.values, 0.10)

def test_mu_shrinkage_partial_pulls_toward_median(_mu_lambda):
    _mu_lambda(0.5)
    mu = pd.Series([0.20, 0.10, 0.02], index=["A", "B", "C"])  # median 0.10
    out = oos_engine._apply_mu_shrinkage(mu)
    assert out["A"] == pytest.approx(0.15)  # 0.5*0.20 + 0.5*0.10
    assert out["C"] == pytest.approx(0.06)  # 0.5*0.02 + 0.5*0.10


# ============================================================================
# _check_crash_trigger (hysteresis)
# ============================================================================

def _spy_with_drawdown(peak=100.0, trough=80.0, n=300):
    """Ramp to peak then fall to trough — a -20% drawdown at the end."""
    up = np.linspace(90, peak, n // 2)
    down = np.linspace(peak, trough, n - n // 2)
    vals = np.concatenate([up, down])
    idx = pd.date_range("2020-01-01", periods=len(vals))
    return pd.Series(vals, index=idx)

def test_crash_trigger_fires_below_threshold():
    spy = _spy_with_drawdown(trough=80.0)  # -20% DD
    state = {"active": False}
    active = oos_engine._check_crash_trigger(
        spy, spy.index[-1], state, dd_trigger=-0.15, dd_release=-0.05, lookback_days=252)
    assert active is True
    assert state["active"] is True

def test_crash_trigger_stays_off_in_mild_dip():
    spy = _spy_with_drawdown(trough=95.0)  # only -5% DD, above -15% trigger
    state = {"active": False}
    active = oos_engine._check_crash_trigger(
        spy, spy.index[-1], state, dd_trigger=-0.15, dd_release=-0.05, lookback_days=252)
    assert active is False

def test_crash_trigger_hysteresis_holds_between_bands():
    """Once active, a recovery to -10% (between release -5% and trigger -15%)
    keeps the hedge ON — no whipsaw."""
    idx = pd.date_range("2020-01-01", periods=3)
    # peak 100, current 90 => -10% DD (between -15% trigger and -5% release)
    spy = pd.Series([100.0, 100.0, 90.0], index=idx)
    state = {"active": True}  # already hedged
    active = oos_engine._check_crash_trigger(
        spy, idx[-1], state, dd_trigger=-0.15, dd_release=-0.05, lookback_days=252)
    assert active is True  # stays on

def test_crash_trigger_releases_above_release_band():
    idx = pd.date_range("2020-01-01", periods=3)
    spy = pd.Series([100.0, 100.0, 98.0], index=idx)  # -2% DD, above -5% release
    state = {"active": True}
    active = oos_engine._check_crash_trigger(
        spy, idx[-1], state, dd_trigger=-0.15, dd_release=-0.05, lookback_days=252)
    assert active is False


# ============================================================================
# _apply_crash_hedge
# ============================================================================

def test_crash_hedge_replaces_with_normalised_basket():
    w = pd.Series({"SMH": 0.5, "QQQ": 0.3, "GOLD.AX": 0.1, "HBRD.AX": 0.1})
    basket = {"GOLD.AX": 0.6, "HBRD.AX": 0.4}
    out = oos_engine._apply_crash_hedge(w, basket=basket, available_tickers=w.index)
    assert out.sum() == pytest.approx(1.0)
    assert out["GOLD.AX"] == pytest.approx(0.6)
    assert out["HBRD.AX"] == pytest.approx(0.4)
    assert out["SMH"] == pytest.approx(0.0)

def test_crash_hedge_reallocates_unavailable_ticker():
    w = pd.Series({"SMH": 1.0, "GOLD.AX": 0.0})
    basket = {"GOLD.AX": 0.6, "HBRD.AX": 0.4}  # HBRD not available
    out = oos_engine._apply_crash_hedge(w, basket=basket, available_tickers=["SMH", "GOLD.AX"])
    # HBRD dropped -> GOLD takes all
    assert out["GOLD.AX"] == pytest.approx(1.0)

def test_crash_hedge_empty_basket_returns_original():
    w = pd.Series({"SMH": 0.6, "QQQ": 0.4})
    out = oos_engine._apply_crash_hedge(w, basket={}, available_tickers=w.index)
    pd.testing.assert_series_equal(out, w)


# ============================================================================
# compute_forward_regime_signal
# ============================================================================

def _trend_prices(direction="up", n=260):
    if direction == "up":
        vals = np.linspace(80, 120, n)
    else:
        vals = np.linspace(120, 80, n)
    return pd.Series(vals, index=pd.date_range("2020-01-01", periods=n))

def test_regime_signal_sums_to_one():
    spy = _trend_prices("up")
    slots = ("Modest", "Mid", "Stretch")
    out = oos_engine.compute_forward_regime_signal(spy, spy.index[-1], slot_names=slots)
    assert out.sum() == pytest.approx(1.0)
    assert list(out.index) == list(slots)

def test_regime_signal_warmup_is_uniform():
    """< 50 days of history -> uniform preference (can't judge regime)."""
    spy = _trend_prices("up", n=20)
    slots = ("Modest", "Mid", "Stretch")
    out = oos_engine.compute_forward_regime_signal(spy, spy.index[-1], slot_names=slots)
    assert np.allclose(out.values, 1.0 / 3)

def test_regime_signal_bull_favours_aggressive_end():
    """Strong uptrend should put more weight on Stretch than Modest."""
    spy = _trend_prices("up")
    slots = ("Modest", "Mid", "Stretch")
    out = oos_engine.compute_forward_regime_signal(spy, spy.index[-1], slot_names=slots)
    assert out["Stretch"] > out["Modest"]


# ============================================================================
# _compute_trend_sleeve
# ============================================================================

def test_trend_sleeve_holds_uptrenders_only():
    n = 300
    idx = pd.date_range("2020-01-01", periods=n)
    px = pd.DataFrame({
        "UP":   np.linspace(80, 140, n),   # strong uptrend -> positive 12-1M
        "DOWN": np.linspace(140, 80, n),   # downtrend -> excluded
    }, index=idx)
    w = oos_engine._compute_trend_sleeve(px, idx[-1], ["UP", "DOWN"], caps={})
    assert "DOWN" not in w.index or w.get("DOWN", 0.0) == 0.0
    assert w.get("UP", 0.0) > 0.0

def test_trend_sleeve_nothing_trends_returns_empty():
    n = 300
    idx = pd.date_range("2020-01-01", periods=n)
    px = pd.DataFrame({"FLAT": np.full(n, 100.0)}, index=idx)  # no trend
    w = oos_engine._compute_trend_sleeve(px, idx[-1], ["FLAT"], caps={})
    assert w.empty


# ============================================================================
# estimate_rebalance_cost_fraction
# ============================================================================

def test_rebalance_cost_zero_when_no_turnover():
    w = pd.Series({"IVV.AX": 0.5, "SMH": 0.5})
    cost = oos_engine.estimate_rebalance_cost_fraction(
        w, w.copy(), 250_000.0, brokerage.BROKER_CONFIG)
    assert cost == pytest.approx(0.0, abs=1e-9)

def test_rebalance_cost_positive_with_turnover():
    w_old = pd.Series({"IVV.AX": 1.0, "SMH": 0.0})
    w_new = pd.Series({"IVV.AX": 0.0, "SMH": 1.0})
    cost = oos_engine.estimate_rebalance_cost_fraction(
        w_old, w_new, 250_000.0, brokerage.BROKER_CONFIG)
    assert cost > 0.0

"""Unit tests for the MV optimisation core (solvers.py).

Locks in the numerical contracts of the optimiser the whole engine depends on:
long-only tangency (max_sharpe_long_only), the frontier point solve
(solve_frontier_point_cvxpy), the 5-slot candidate solve
(solve_candidate_portfolios), and Ledoit-Wolf covariance shrinkage
(_ledoit_wolf_cc). Also pins the config-injection contract — the engine syncs
PER_ASSET_WEIGHT_CAPS / SECTOR_GROUP_CAPS / ENSEMBLE_SLOTS into the module.
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import solvers


@pytest.fixture(autouse=True)
def _reset_solver_config():
    """Save/restore the injected config globals around each test so a test that
    sets caps/slots can't leak into another. Default = no caps, no groups."""
    saved = (solvers.PER_ASSET_WEIGHT_CAPS, solvers.SECTOR_GROUP_CAPS,
             solvers.ENSEMBLE_SLOTS, solvers.ENSEMBLE_SLOT_NAMES)
    solvers.PER_ASSET_WEIGHT_CAPS = {}
    solvers.SECTOR_GROUP_CAPS = {}
    solvers.ENSEMBLE_SLOTS = ()
    solvers.ENSEMBLE_SLOT_NAMES = ()
    yield
    (solvers.PER_ASSET_WEIGHT_CAPS, solvers.SECTOR_GROUP_CAPS,
     solvers.ENSEMBLE_SLOTS, solvers.ENSEMBLE_SLOT_NAMES) = saved


def _diag_cov(variances, tickers):
    return pd.DataFrame(np.diag(variances), index=tickers, columns=tickers)


# ============================================================================
# max_sharpe_long_only
# ============================================================================

def test_max_sharpe_weights_sum_to_one_and_nonneg():
    idx = ["A", "B", "C"]
    mu = pd.Series([0.10, 0.09, 0.08], index=idx)
    Sig = _diag_cov([0.04, 0.04, 0.04], idx)
    w = solvers.max_sharpe_long_only(mu, Sig)
    assert abs(w.sum() - 1.0) < 1e-6
    assert (w >= -1e-9).all()


def test_max_sharpe_tilts_to_best_asset():
    """Equal variance -> tangency overweights the highest-mu asset."""
    idx = ["A", "B", "C"]
    mu = pd.Series([0.15, 0.09, 0.08], index=idx)
    Sig = _diag_cov([0.04, 0.04, 0.04], idx)
    w = solvers.max_sharpe_long_only(mu, Sig)
    assert w["A"] == pytest.approx(max(w), rel=1e-6)
    assert w["A"] > w["B"] and w["A"] > w["C"]


def test_max_sharpe_cap_zero_excludes_asset():
    """A cap of 0.0 must force weight ~0 (the semis-exclusion mechanism)."""
    idx = ["A", "B", "C"]
    mu = pd.Series([0.15, 0.09, 0.08], index=idx)
    Sig = _diag_cov([0.04, 0.04, 0.04], idx)
    solvers.PER_ASSET_WEIGHT_CAPS = {"A": 0.0}
    w = solvers.max_sharpe_long_only(mu, Sig)
    assert w.get("A", 0.0) < 1e-6
    assert abs(w.sum() - 1.0) < 1e-6


def test_max_sharpe_cap_binds_upper_bound():
    idx = ["A", "B", "C"]
    mu = pd.Series([0.30, 0.05, 0.05], index=idx)  # A dominates
    Sig = _diag_cov([0.04, 0.04, 0.04], idx)
    solvers.PER_ASSET_WEIGHT_CAPS = {"A": 0.20}
    w = solvers.max_sharpe_long_only(mu, Sig)
    assert w["A"] <= 0.20 + 1e-6


def test_max_sharpe_no_positive_excess_falls_back_to_min_variance():
    """No positive excess return -> min-variance fallback (lowest-var asset heavy)."""
    idx = ["A", "B", "C"]
    mu = pd.Series([-0.05, -0.06, -0.07], index=idx)  # all negative
    Sig = _diag_cov([0.09, 0.04, 0.01], idx)  # C lowest variance
    w = solvers.max_sharpe_long_only(mu, Sig, rf=0.0)
    assert abs(w.sum() - 1.0) < 1e-6
    assert w["C"] == pytest.approx(max(w), rel=1e-6)


def test_max_sharpe_empty_inputs_return_empty():
    w = solvers.max_sharpe_long_only(pd.Series(dtype=float), pd.DataFrame())
    assert w.empty


# ============================================================================
# solve_frontier_point_cvxpy
# ============================================================================

def test_frontier_solve_ok_and_normalised():
    idx = ["A", "B", "C"]
    mu = pd.Series([0.12, 0.10, 0.08], index=idx)
    Sig = _diag_cov([0.04, 0.03, 0.02], idx)
    w_arr, ok, note = solvers.solve_frontier_point_cvxpy(mu, Sig, target_return=0.09)
    assert ok
    w = pd.Series(w_arr, index=Sig.index)
    assert abs(w.sum() - 1.0) < 1e-6
    assert (w >= -1e-6).all()


def test_frontier_meets_return_floor():
    """use_inequality=True => achieved mu'w >= target."""
    idx = ["A", "B", "C"]
    mu = pd.Series([0.12, 0.10, 0.08], index=idx)
    Sig = _diag_cov([0.04, 0.03, 0.02], idx)
    tgt = 0.10
    w_arr, ok, _ = solvers.solve_frontier_point_cvxpy(mu, Sig, target_return=tgt)
    assert ok
    achieved = float(mu.values @ np.asarray(w_arr))
    assert achieved >= tgt - 1e-4


def test_frontier_cap_zero_binds():
    idx = ["A", "B", "C"]
    mu = pd.Series([0.20, 0.08, 0.08], index=idx)
    Sig = _diag_cov([0.04, 0.04, 0.04], idx)
    solvers.PER_ASSET_WEIGHT_CAPS = {"A": 0.0}
    # target 0.07 achievable by B/C (0.08) with A excluded -> feasible, w_A ~0
    w_arr, ok, _ = solvers.solve_frontier_point_cvxpy(mu, Sig, target_return=0.07)
    assert ok
    w = pd.Series(w_arr, index=idx)
    assert w["A"] < 1e-6


def test_frontier_group_cap_binds():
    """SECTOR_GROUP_CAPS: summed weight of a cluster <= cap."""
    idx = ["A", "B", "C"]
    mu = pd.Series([0.20, 0.18, 0.05], index=idx)
    Sig = _diag_cov([0.04, 0.04, 0.04], idx)
    solvers.SECTOR_GROUP_CAPS = {"grp": {"tickers": ["A", "B"], "cap": 0.30}}
    w_arr, ok, _ = solvers.solve_frontier_point_cvxpy(mu, Sig, target_return=0.08)
    w = pd.Series(w_arr, index=idx)
    assert w["A"] + w["B"] <= 0.30 + 1e-6


# ============================================================================
# solve_candidate_portfolios
# ============================================================================

def test_candidates_returns_all_slots_and_none_premium_is_tangency():
    idx = ["A", "B", "C"]
    mu = pd.Series([0.12, 0.10, 0.08], index=idx)
    Sig = _diag_cov([0.04, 0.03, 0.02], idx)
    slots = (("Modest", None), ("Stretch", 0.05))
    solvers.ENSEMBLE_SLOTS = slots
    solvers.ENSEMBLE_SLOT_NAMES = tuple(n for n, _ in slots)
    out = solvers.solve_candidate_portfolios(mu, Sig, spy_mu=0.09)
    assert set(out) == {"Modest", "Stretch"}
    tangency = solvers.max_sharpe_long_only(mu, Sig)
    # Modest (premium None) == tangency
    pd.testing.assert_series_equal(
        out["Modest"].sort_index(), tangency.sort_index(),
        check_names=False, atol=1e-6)


def test_candidates_empty_when_unsolvable():
    slots = (("Modest", None), ("Stretch", 0.05))
    solvers.ENSEMBLE_SLOTS = slots
    solvers.ENSEMBLE_SLOT_NAMES = tuple(n for n, _ in slots)
    out = solvers.solve_candidate_portfolios(
        pd.Series(dtype=float), pd.DataFrame(), spy_mu=0.09)
    assert all(w.empty for w in out.values())


# ============================================================================
# _ledoit_wolf_cc
# ============================================================================

def _returns(n=300, k=4, seed=0):
    rng = np.random.default_rng(seed)
    cols = [f"T{i}" for i in range(k)]
    return pd.DataFrame(rng.normal(0, 0.01, size=(n, k)), columns=cols)


def test_ledoit_wolf_returns_delta_in_unit_interval():
    cov, delta = solvers._ledoit_wolf_cc(_returns())
    assert 0.0 <= delta <= 1.0
    assert cov.shape == (4, 4)


def test_ledoit_wolf_symmetric_and_psd():
    cov, _ = solvers._ledoit_wolf_cc(_returns())
    M = cov.values
    assert np.allclose(M, M.T, atol=1e-12)
    eig = np.linalg.eigvalsh(M)
    assert (eig >= -1e-10).all()  # positive semi-definite


def test_ledoit_wolf_diagonal_matches_sample_variance():
    """Shrinkage targets the correlation matrix, so diagonal variances are
    preserved (up to the /(T-1) unbiased rescale)."""
    df = _returns()
    cov, _ = solvers._ledoit_wolf_cc(df)
    sample_var = df.var(ddof=1)
    for c in df.columns:
        assert cov.loc[c, c] == pytest.approx(sample_var[c], rel=0.05)


def test_ledoit_wolf_tiny_sample_degenerate():
    """T<2 or N<2 -> falls back to pandas .cov(), delta 0."""
    df = pd.DataFrame({"A": [0.01], "B": [0.02]})
    cov, delta = solvers._ledoit_wolf_cc(df)
    assert delta == 0.0


# ============================================================================
# _qis_shrinkage + estimate_covariance dispatcher (cov-estimator experiment)
# ============================================================================

def test_qis_symmetric_psd_and_trace_preserving():
    df = _returns(n=300, k=8)
    cov, intensity = solvers._qis_shrinkage(df)
    M = cov.values
    assert np.allclose(M, M.T, atol=1e-10)
    assert (np.linalg.eigvalsh(M) > 0).all()          # strictly PD
    # QIS preserves the trace of the sample covariance.
    assert np.trace(M) == pytest.approx(np.trace(df.cov().values), rel=1e-6)
    assert intensity >= 0.0


def test_qis_better_conditioned_than_sample():
    """The whole point: QIS yields a lower condition number than sample cov."""
    df = _returns(n=90, k=20, seed=3)               # moderate concentration
    cov, _ = solvers._qis_shrinkage(df)
    assert np.linalg.cond(cov.values) < np.linalg.cond(df.cov().values)


def test_qis_degenerate_falls_back():
    df = pd.DataFrame({"A": [0.01, 0.02], "B": [0.0, 0.01]})   # T<12
    cov, _ = solvers._qis_shrinkage(df)
    assert cov.shape == (2, 2)                        # returned something usable


def test_estimate_covariance_dispatch():
    df = _returns(n=200, k=6)
    sample, i0 = solvers.estimate_covariance(df, method="sample")
    assert i0 == 0.0
    assert np.allclose(sample.values, df.cov().values)
    lw, _ = solvers.estimate_covariance(df, method="lw_cc")
    assert np.allclose(lw.values, solvers._ledoit_wolf_cc(df)[0].values)
    qis, _ = solvers.estimate_covariance(df, method="qis")
    assert np.allclose(qis.values, solvers._qis_shrinkage(df)[0].values)
    # Unknown method -> incumbent lw_cc (never breaks the solve).
    unk, _ = solvers.estimate_covariance(df, method="banana")
    assert np.allclose(unk.values, lw.values)

"""Tests for factors.py — the FF5/MOM factor layer.

Covers the pure, network-free logic: region resolution + regions.json validation,
the trailing-N-day factor-momentum scorer + tilt clipping, the FF5 beta OLS
(incl. the multi-region standardisation semantics), and the cache poison-guard.
Download paths (_download_*, get_*_daily hitting Ken French) are intentionally
NOT exercised here.
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import factors


# === region_for_ticker ========================================================

@pytest.fixture
def clean_user_overrides():
    """region_for_ticker reads a shared mutable dict — save/restore it."""
    saved = dict(factors.USER_REGION_OVERRIDES)
    factors.USER_REGION_OVERRIDES.clear()
    yield factors.USER_REGION_OVERRIDES
    factors.USER_REGION_OVERRIDES.clear()
    factors.USER_REGION_OVERRIDES.update(saved)


def test_region_heuristic_by_suffix(clean_user_overrides):
    assert factors.region_for_ticker("AAPL") == "US"          # US-listed default
    assert factors.region_for_ticker("BHP.AX") == "AP_EX_JAPAN"
    assert factors.region_for_ticker("7203.T") == "JAPAN"     # Tokyo
    assert factors.region_for_ticker("VOD.L") == "EUROPE"     # London
    assert factors.region_for_ticker("SAP.DE") == "EUROPE"    # XETRA


def test_region_special_cases(clean_user_overrides):
    assert factors.region_for_ticker("IJP.AX") == "JAPAN"     # Japan tracker, ASX-listed
    assert factors.region_for_ticker("IVV.AX") == "US"        # hardcoded override
    assert factors.region_for_ticker("^AORD") == "AP_EX_JAPAN"


def test_region_override_precedence(clean_user_overrides):
    """USER override > hardcoded TICKER override > heuristic."""
    clean_user_overrides["IVV.AX"] = "EUROPE"                 # user beats hardcoded US
    assert factors.region_for_ticker("ivv.ax") == "EUROPE"   # case-insensitive
    clean_user_overrides["BHP.AX"] = "US"                    # user beats heuristic
    assert factors.region_for_ticker("BHP.AX") == "US"


# === regions.json load/save ===================================================

@pytest.fixture
def regions_json_tmp(tmp_path):
    saved = factors.REGIONS_JSON_PATH
    factors.REGIONS_JSON_PATH = tmp_path / "regions.json"
    yield factors.REGIONS_JSON_PATH
    factors.REGIONS_JSON_PATH = saved


def test_regions_json_roundtrip_and_validation(regions_json_tmp):
    factors._save_regions_json({"XYZ.AX": "US", "ABC.AX": "MARS", "def.ax": "japan"})
    loaded = factors._load_regions_json()
    assert loaded["XYZ.AX"] == "US"
    assert "ABC.AX" not in loaded          # invalid region silently dropped
    assert loaded["DEF.AX"] == "JAPAN"     # key + value uppercased


def test_regions_json_missing_returns_empty(regions_json_tmp):
    assert factors._load_regions_json() == {}


# === factor-momentum scorer + tilt recommender ================================

def _mom_frame(values):
    idx = pd.date_range("2026-01-01", periods=len(values), freq="B")
    return pd.DataFrame({"MOM": values, "RF": 0.0}, index=idx)


def test_factor_stats_matches_manual_annualisation():
    vals = [0.01, -0.005, 0.02, 0.00, 0.015, -0.01, 0.008, 0.012]
    ff = _mom_frame(vals)
    stats = factors.compute_factor_recent_stats(ff, lookback_days=len(vals))
    s = pd.Series(vals)
    exp_ret = s.mean() * factors.ANNUAL_TRADING_DAYS
    exp_vol = s.std() * np.sqrt(factors.ANNUAL_TRADING_DAYS)
    assert stats.loc["MOM", "ann_return"] == pytest.approx(exp_ret)
    assert stats.loc["MOM", "ann_vol"] == pytest.approx(exp_vol)
    assert stats.loc["MOM", "sharpe"] == pytest.approx(exp_ret / exp_vol)
    assert "RF" not in stats.index          # RF excluded from scoring


def test_auto_tilts_scale_and_clip():
    # A strongly positive-Sharpe factor: tilt = clip(sharpe * 0.10, ±0.30).
    ff = _mom_frame([0.02, 0.021, 0.019, 0.02, 0.02, 0.0205, 0.0195, 0.02])
    tilts = factors.auto_recommend_factor_tilts(ff, lookback_days=8)
    sharpe = factors.compute_factor_recent_stats(ff, 8).loc["MOM", "sharpe"]
    expected = max(-0.30, min(0.30, sharpe * factors.FACTOR_TILT_SHARPE_TO_MAG))
    assert tilts["MOM"] == pytest.approx(round(expected, 4))
    assert abs(tilts["MOM"]) <= factors.FACTOR_TILT_MAX_MAGNITUDE


def test_auto_tilts_hard_clip_at_cap():
    # Near-zero vol + steady drift => enormous Sharpe => tilt pinned at the cap.
    ff = _mom_frame([0.01, 0.0100001, 0.0099999, 0.01, 0.01, 0.01, 0.0100001, 0.01])
    tilts = factors.auto_recommend_factor_tilts(ff, lookback_days=8)
    assert tilts["MOM"] == pytest.approx(factors.FACTOR_TILT_MAX_MAGNITUDE)


def test_auto_tilts_empty_input():
    assert factors.auto_recommend_factor_tilts(pd.DataFrame()) == {}


# === FF5 OLS betas ============================================================

def _factor_df(rng, n, vol=0.01):
    idx = pd.date_range("2024-01-01", periods=n, freq="B")
    cols = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM"]
    data = {c: rng.normal(0, 1, n) * vol for c in cols}
    data["RF"] = np.zeros(n)
    return pd.DataFrame(data, index=idx)


def test_ff5_betas_recover_known_loadings():
    rng = np.random.default_rng(42)
    ff = _factor_df(rng, 400)
    # Security with a KNOWN factor structure: 1.2*Mkt + 0.5*SMB + tiny noise.
    y = 1.2 * ff["Mkt-RF"] + 0.5 * ff["SMB"] + rng.normal(0, 1e-5, len(ff))
    wide = pd.DataFrame({"SEC": y}, index=ff.index)
    B, alpha, resid = factors.compute_ff5_betas(wide, ff, min_obs=100, n_lags=0)
    assert B.loc["SEC", "Mkt-RF"] == pytest.approx(1.2, abs=0.02)
    assert B.loc["SEC", "SMB"] == pytest.approx(0.5, abs=0.02)
    assert abs(B.loc["SEC", "HML"]) < 0.02
    assert float(alpha["SEC"]) == pytest.approx(0.0, abs=1e-4)


def test_ff5_betas_below_min_obs_skipped():
    rng = np.random.default_rng(1)
    ff = _factor_df(rng, 50)
    wide = pd.DataFrame({"SEC": ff["Mkt-RF"].values}, index=ff.index)
    B, alpha, resid = factors.compute_ff5_betas(wide, ff, min_obs=120, n_lags=0)
    assert pd.isna(B.loc["SEC", "Mkt-RF"])   # too few obs -> left NaN


def test_multi_region_standardisation_rescales_beta():
    """A security regressed against a HIGHER-vol region gets its beta scaled up
    when standardise_factors rescales that region to the reference (US) vol.
    beta_standardised / beta_raw should equal (region_vol / ref_vol)."""
    rng = np.random.default_rng(7)
    us = _factor_df(rng, 400, vol=0.01)
    ap = _factor_df(rng, 400, vol=0.02)          # ~2x the US market vol
    ap_sec = pd.DataFrame({"AAA.AX": 1.0 * ap["Mkt-RF"].values}, index=ap.index)
    regional = {"US": us, "AP_EX_JAPAN": ap}
    region_map = lambda t: "AP_EX_JAPAN"

    B_raw, _, _ = factors.compute_ff5_betas_multi_region(
        ap_sec, regional, region_map, min_obs=100, n_lags=0, standardise_factors=False)
    B_std, _, _ = factors.compute_ff5_betas_multi_region(
        ap_sec, regional, region_map, min_obs=100, n_lags=0, standardise_factors=True)

    assert B_raw.loc["AAA.AX", "Mkt-RF"] == pytest.approx(1.0, abs=0.02)
    expected_ratio = float(ap["Mkt-RF"].std()) / float(us["Mkt-RF"].std())
    assert B_std.loc["AAA.AX", "Mkt-RF"] == pytest.approx(expected_ratio, abs=0.05)


# === cache poison-guard =======================================================

def test_cached_read_rejects_empty_and_serves_hit(tmp_path, monkeypatch):
    monkeypatch.setattr(factors, "_CACHE_DIR", tmp_path)
    url = "http://example.test/factor.zip"

    # Empty build result must raise (never cache a poisoned empty frame).
    with pytest.raises(ValueError):
        factors._cached_read(url, lambda: pd.DataFrame())

    # A real frame is cached; a second call whose builder would EXPLODE still
    # returns the cached data -> proves the hit path skips the builder.
    good = pd.DataFrame({"MOM": [0.01, 0.02]},
                        index=pd.to_datetime(["2026-01-01", "2026-01-02"]))
    factors._cached_read(url, lambda: good)

    def _boom():
        raise AssertionError("builder should not run on a cache hit")

    hit = factors._cached_read(url, _boom)
    assert list(hit["MOM"]) == [0.01, 0.02]

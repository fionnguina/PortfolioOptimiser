"""Guards for the 2026-08-13 headline-slide review.

Two families:
  1. Metrics annualisation — the table used to disagree with its own chart
     because n_years was len(returns)/252 on a UNION AU+US calendar (~258
     rows/yr), reading a 10.0y window as 10.23y and understating every
     annualised figure.
  2. Lockbox boundary — nothing in the suite pinned the date, so a wrong
     edit failed silently. Refresh #2 moved it to 2026-07-30 and added a
     separate REPORTING lockbox for the published backtest.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import metrics as _m

_SRC = (Path(__file__).resolve().parent.parent / "Portfolio_Optimiser.py").read_text(
    encoding="utf-8")


# ---------------------------------------------------------------- annualisation

def _union_calendar(n_years: float = 10.0, per_year: int = 258) -> pd.DatetimeIndex:
    """~258 obs/yr — what the union of the AU and US trading calendars gives."""
    n = int(round(n_years * per_year))
    return pd.to_datetime(
        pd.Timestamp("2016-08-12")
        + pd.to_timedelta(np.linspace(0, n_years * 365.25, n), unit="D")
    )


def test_periods_per_year_derives_calendar_not_252():
    r = pd.Series(0.0004, index=_union_calendar())
    ppy = _m._periods_per_year(r)
    assert 250.0 < ppy < 266.0, ppy
    # The whole point: it must NOT silently return the 252 constant here.
    assert abs(ppy - 258.0) < 3.0, ppy


def test_periods_per_year_falls_back_without_datetime_index():
    r = pd.Series([0.001] * 100)  # RangeIndex
    assert _m._periods_per_year(r) == float(_m.ANNUAL_TRADING_DAYS)


def test_periods_per_year_falls_back_on_degenerate_span():
    idx = pd.to_datetime(["2026-01-02", "2026-01-02"])
    assert _m._periods_per_year(pd.Series([0.01, 0.01], index=idx)) == 252.0


def test_annualised_return_reconciles_with_cumulative():
    """The regression that put the table at odds with the chart.

    Compounding the reported Annualised Return over the series' true elapsed
    years must reproduce the reported Cumulative Return. Under the old
    len/252 convention this was off by ~10pp on a 10-year window.
    """
    idx = _union_calendar()
    rng = np.random.default_rng(7)
    r = pd.Series(rng.normal(0.0005, 0.009, size=len(idx)), index=idx)
    out = _m._series_metrics(r)

    elapsed = (idx[-1] - idx[0]).days / 365.25
    implied = (1.0 + out["Annualised Return"]) ** elapsed - 1.0
    assert implied == pytest.approx(out["Cumulative Return"], rel=2e-3)


def test_annualised_vol_uses_same_calendar_as_return():
    idx = _union_calendar()
    rng = np.random.default_rng(11)
    r = pd.Series(rng.normal(0.0005, 0.009, size=len(idx)), index=idx)
    out = _m._series_metrics(r)
    ppy = _m._periods_per_year(r)
    assert out["Annualised Volatility"] == pytest.approx(
        float(r.std(ddof=1) * np.sqrt(ppy)), rel=1e-9)


def test_rf_lowers_sharpe_and_sortino_in_series_metrics():
    """The slide reported rf=0. Passing a real rate must move both ratios."""
    idx = _union_calendar(3.0)
    rng = np.random.default_rng(3)
    r = pd.Series(rng.normal(0.0006, 0.009, size=len(idx)), index=idx)
    zero = _m._series_metrics(r, 0.0)
    real = _m._series_metrics(r, 0.04)
    assert real["Sharpe Ratio"] < zero["Sharpe Ratio"]
    assert real["Sortino Ratio"] < zero["Sortino Ratio"]
    # Return and drawdown are rf-independent.
    assert real["Annualised Return"] == pytest.approx(zero["Annualised Return"])
    assert real["Max Drawdown"] == pytest.approx(zero["Max Drawdown"])


def test_annualized_sharpe_keeps_252_default_for_legacy_callers():
    idx = pd.bdate_range("2020-01-01", periods=504)
    rng = np.random.default_rng(5)
    r = pd.Series(rng.normal(0.0005, 0.01, size=len(idx)), index=idx)
    expected = float(r.mean() / r.std(ddof=1) * np.sqrt(252))
    assert _m._annualized_sharpe(r, 0.0) == pytest.approx(expected, rel=1e-9)


# -------------------------------------------------------------------- lockbox

def test_lockbox_boundary_is_refresh_2_date():
    m = re.search(r'^LOCKBOX_BOUNDARY\s*=\s*"(\d{4}-\d{2}-\d{2})"', _SRC, re.M)
    assert m, "LOCKBOX_BOUNDARY constant missing"
    assert m.group(1) == "2026-07-30", m.group(1)


def test_no_stale_hardcoded_lockbox_dates():
    """Refresh #1's 2026-06-30 must not survive anywhere in the engine."""
    assert 'pd.Timestamp("2026-06-30")' not in _SRC


def test_reporting_lockbox_exists_and_is_separate():
    assert "REPORT_LOCKBOX_DATE" in _SRC
    assert "PORTOPT_REPORT_LOCKBOX" in _SRC
    # The published backtest reads its own truncated frame...
    assert "oos_prices_report" in _SRC
    # ...while the live regime/crash-hedge reads stay on the full panel.
    assert 'benchmark_prices=oos_prices_aud_long["SPY"]' in _SRC


def test_oos_cache_fingerprint_hashes_lockbox_state():
    """Two lockbox settings must never collide on one cache key."""
    assert 'h.update(f"lockbox:' in _SRC
    assert 'h.update(f"rlockbox:' in _SRC


def test_au_benchmark_is_total_return_not_price_index():
    """^AORD excludes dividends (~4.2%/yr understated). Reporting must not use it."""
    m = re.search(r'^AU_BENCH_TICKER\s*=\s*"([^"]+)"', _SRC, re.M)
    assert m, "AU_BENCH_TICKER missing"
    assert m.group(1) != "^AORD"
    # ^AORD stays in the universe for region betas, but only as the fallback.
    assert re.search(r'^AU_BENCH_FALLBACK\s*=\s*"\^AORD"', _SRC, re.M)
    assert 'EXCLUDE_FROM_OPT = {"^AORD"}' in _SRC


def test_benchmarks_are_not_zero_filled_onto_strategy_calendar():
    """The phantom-flat-day bug: deflated benchmark vol/Sharpe ~1-1.4%."""
    assert "spy_returns.reindex(s.index).fillna(0.0)" not in _SRC
    assert "aord_returns.reindex(s.index).fillna(0.0)" not in _SRC


# ------------------------------------------------ pre-inception back-fill

def test_backfill_is_off_by_default_in_engine():
    """LEGACY_BACKFILL must default False — bfill before pct_change is look-ahead."""
    m = re.search(r'^LEGACY_BACKFILL = bool\(os\.environ\.get\(\s*"PORTOPT_LEGACY_BACKFILL"',
                  _SRC, re.M)
    assert m, "LEGACY_BACKFILL env-gated constant missing"
    # No engine price panel may unconditionally back-fill any more.
    for pat in (
        "prices = prices.reindex(idx).ffill().bfill()",
        "    px = px.ffill().bfill()",
        "    prices_aud = prices_aud.ffill().bfill()",
        "    px = px.sort_index().ffill().bfill()",
        "        _oos_long_px = _oos_long_px.ffill().bfill()",
        "        oos_prices_aud_long = oos_prices_aud_long.ffill().bfill()",
    ):
        assert pat not in _SRC, f"unconditional back-fill still present: {pat!r}"


def test_oos_engine_and_research_modes_have_no_unconditional_backfill():
    root = Path(__file__).resolve().parent.parent
    oos = (root / "oos_engine.py").read_text(encoding="utf-8")
    assert "px = px.sort_index().ffill().bfill()" not in oos
    assert "LEGACY_BACKFILL" in oos

    rm = (root / "research_modes.py").read_text(encoding="utf-8")
    # Every research mode builds its OWN panel; --dev-validation would still
    # measure the buggy engine if these were missed.
    assert "px = px.sort_index().ffill().bfill()" not in rm
    assert 'px_aud = px_aud.ffill().bfill().dropna(how="all")' not in rm
    assert "def _fill_px(" in rm


def test_backfill_state_is_in_the_cache_fingerprint():
    """A fixed run and a legacy run must never collide on one cache key."""
    assert 'h.update(f"legacy_bfill:' in _SRC


def test_coverage_gate_excludes_a_pre_inception_ticker():
    """The behavioural guard: ffill-only must make the gate actually fire.

    Synthesises a ticker that starts halfway through the window. Under
    ffill+bfill it scores full coverage (the bug); under ffill only it must
    fall below the 0.8 threshold oos_engine uses.
    """
    idx = pd.bdate_range("2016-01-01", periods=500)
    full = pd.Series(np.linspace(100, 150, 500), index=idx)
    late = full.copy()
    late.iloc[:400] = np.nan          # lists only for the last 100 days
    px = pd.DataFrame({"OLD": full, "LATE": late})

    cov_legacy = px.ffill().bfill().pct_change().notna().sum() / len(px)
    cov_fixed = px.ffill().pct_change().notna().sum() / len(px)

    assert cov_legacy["LATE"] >= 0.8, "sanity: the bug should show full coverage"
    assert cov_fixed["LATE"] < 0.8, "fixed panel must fail the coverage gate"
    assert cov_fixed["OLD"] >= 0.8, "a genuinely-present ticker must still pass"


# ------------------------------------------------ contemporaneous risk-free rate

def _rf_series_flat_then_high():
    """Cheap stand-in for the RBA series: 0.10% for 5y, then 4.35%."""
    idx = pd.date_range("2016-01-31", "2026-07-31", freq="ME")
    vals = np.where(idx < pd.Timestamp("2022-05-01"), 0.001, 0.0435)
    return pd.Series(vals, index=idx)


def test_rf_daily_accepts_a_dated_series_and_forward_fills():
    idx = pd.bdate_range("2020-01-01", periods=400)
    rf = _rf_series_flat_then_high()
    out = _m._rf_daily(idx, rf, 258.0)
    assert isinstance(out, pd.Series) and len(out) == len(idx)
    assert out.notna().all(), "no NaN may reach the excess-return calculation"
    # 2020 sat at the 0.10% policy rate, so the daily rf must be ~0.
    assert out.max() < (1.0 + 0.002) ** (1 / 258.0) - 1.0


def test_rf_daily_scalar_path_unchanged():
    idx = pd.bdate_range("2020-01-01", periods=50)
    got = _m._rf_daily(idx, 0.0435, 252.0)
    assert got == pytest.approx((1.0 + 0.0435) ** (1 / 252.0) - 1.0)


def test_dated_rf_lands_between_zero_and_flat_current_rate():
    """The whole point: charging today's rate across a decade is too harsh."""
    idx = pd.bdate_range("2016-08-16", "2026-07-30")
    rng = np.random.default_rng(4)
    r = pd.Series(rng.normal(0.00055, 0.0088, len(idx)), index=idx)
    s_zero = _m._series_metrics(r, 0.0)["Sharpe Ratio"]
    s_flat = _m._series_metrics(r, 0.0435)["Sharpe Ratio"]
    s_ser = _m._series_metrics(r, _rf_series_flat_then_high())["Sharpe Ratio"]
    assert s_flat < s_ser < s_zero, (s_flat, s_ser, s_zero)


def test_empty_rf_series_degrades_to_zero_not_crash():
    idx = pd.bdate_range("2020-01-01", periods=60)
    assert _m._rf_daily(idx, pd.Series(dtype=float), 252.0) == 0.0


def test_engine_prefers_the_series_and_falls_back_to_the_scalar():
    assert "rf_series = get_rba_cash_rate_series()" in _SRC
    # The metrics call site must pick the Series when present...
    assert '_rfs = globals().get("rf_series")' in _SRC
    # ...and fall back to the scalar rather than a silent 0.
    assert 'else float(globals().get("rf_annual", 0.0) or 0.0))' in _SRC


# ------------------------------------------------------- validation statistics

def test_psr_rises_with_sample_length():
    """More data on the same Sharpe => more confidence it is real."""
    import validation as _v
    a = _v.probabilistic_sharpe_ratio(0.05, 250, 0.0, 3.0)
    b = _v.probabilistic_sharpe_ratio(0.05, 2500, 0.0, 3.0)
    assert b > a


def test_psr_penalises_negative_skew_and_fat_tails():
    import validation as _v
    clean = _v.probabilistic_sharpe_ratio(0.05, 2500, 0.0, 3.0)
    ugly = _v.probabilistic_sharpe_ratio(0.05, 2500, -0.8, 8.0)
    assert ugly < clean, "negative skew + fat tails must discount a Sharpe"


def test_expected_max_sharpe_grows_with_trials():
    """Search harder over a null and the best result looks better for free."""
    import validation as _v
    assert (_v.expected_max_sharpe(0.04 ** 2, 200)
            > _v.expected_max_sharpe(0.04 ** 2, 10) > 0)
    assert _v.expected_max_sharpe(0.0, 100) == 0.0


def test_deflated_sharpe_is_below_undeflated_psr():
    import validation as _v
    idx = pd.bdate_range("2016-08-16", periods=2500)
    rng = np.random.default_rng(9)
    r = pd.Series(rng.normal(0.0006, 0.009, len(idx)), index=idx)
    m = _v.sharpe_moments(r)
    psr = _v.probabilistic_sharpe_ratio(m["sr"], m["n"], m["skew"], m["kurt"])
    dsr = _v.deflated_sharpe_ratio(r, np.random.default_rng(2).normal(0.9, 0.04, 47))
    assert dsr["dsr"] < psr, "deflation must raise the bar, never lower it"


def test_min_track_record_length_is_infinite_without_an_edge():
    import validation as _v
    assert _v.min_track_record_length(0.0, 1000, 0.0, 3.0) == float("inf")
    finite = _v.min_track_record_length(0.05, 1000, 0.0, 3.0)
    assert np.isfinite(finite) and finite > 1


def test_universe_vintage_drops_only_late_listers_and_keeps_protected():
    import validation as _v
    idx = pd.bdate_range("2014-01-01", periods=800)
    old = pd.Series(np.linspace(10, 20, 800), index=idx)
    late = old.copy(); late.iloc[:600] = np.nan
    panel = pd.DataFrame({"OLD": old, "LATE": late, "^AORD": old})
    out, dropped = _v.apply_universe_vintage(panel, idx[500], keep=("^AORD",))
    assert dropped == ["LATE"]
    assert list(out.columns) == ["OLD", "^AORD"]
    # No vintage => untouched.
    same, none_dropped = _v.apply_universe_vintage(panel, None)
    assert none_dropped == [] and same.shape == panel.shape


def test_vintage_state_is_in_the_cache_fingerprint():
    assert 'h.update(f"vintage:' in _SRC


# --------------------------------------------------------- variant persistence

def _fake_returns(n=300, seed=0, start="2020-01-01"):
    rng = np.random.default_rng(seed)
    return pd.Series(rng.normal(0.0005, 0.009, n),
                     index=pd.bdate_range(start, periods=n))


def test_variant_store_roundtrip(tmp_path):
    import variant_store as vs
    r = _fake_returns()
    key = vs.persist_variant(r, {"VOL_TARGET_ANNUAL": 0.16}, {"mode": "research"},
                             app_dir=tmp_path)
    assert key
    back = vs.load_series(key, app_dir=tmp_path)
    pd.testing.assert_series_equal(back, r)
    idx = vs.load_index(app_dir=tmp_path)
    assert len(idx) == 1 and idx.iloc[0]["config_key"] and idx.iloc[0]["sharpe_ann"]


def test_variant_store_dedups_identical_config_and_window(tmp_path):
    import variant_store as vs
    r = _fake_returns()
    cfg = {"VOL_TARGET_ANNUAL": 0.16}
    k1 = vs.persist_variant(r, cfg, app_dir=tmp_path)
    k2 = vs.persist_variant(r, cfg, app_dir=tmp_path)
    assert k1 == k2
    assert len(vs.load_index(app_dir=tmp_path)) == 1, "re-run is the same trial"


def test_variant_store_separates_config_from_data(tmp_path):
    """PBO compares configs on the SAME data — the keys must not collapse."""
    import variant_store as vs
    r = _fake_returns()
    r_later = _fake_returns(start="2021-01-01")
    a = vs.persist_variant(r, {"VOL_TARGET_ANNUAL": 0.16}, app_dir=tmp_path)
    b = vs.persist_variant(r, {"VOL_TARGET_ANNUAL": 0.20}, app_dir=tmp_path)
    c = vs.persist_variant(r_later, {"VOL_TARGET_ANNUAL": 0.16}, app_dir=tmp_path)
    assert a != b, "different config must be a different variant"
    assert a != c, "different window must be a different evaluation"
    idx = vs.load_index(app_dir=tmp_path)
    assert idx["config_key"].nunique() == 2 and idx["data_key"].nunique() == 2


def test_trial_matrix_holds_data_fixed(tmp_path):
    import variant_store as vs
    r = _fake_returns()
    for vt in (0.12, 0.16, 0.20):
        vs.persist_variant(r * (1 + vt), {"VOL_TARGET_ANNUAL": vt}, app_dir=tmp_path)
    vs.persist_variant(_fake_returns(start="2022-01-01"), {"VOL_TARGET_ANNUAL": 0.16},
                       app_dir=tmp_path)
    m = vs.load_trial_matrix(app_dir=tmp_path)
    # Picks the window with the most configs — 3, not the lone later one.
    assert m.shape[1] == 3, m.shape


def test_variant_store_never_raises_on_bad_input(tmp_path):
    import variant_store as vs
    assert vs.persist_variant(None, {}, app_dir=tmp_path) is None
    assert vs.persist_variant(pd.Series(dtype=float), {}, app_dir=tmp_path) is None
    assert vs.load_series("nope", app_dir=tmp_path) is None


def test_engine_wires_the_sink_into_oos_engine():
    assert "_oos_engine.VARIANT_SINK = _variant_sink" in _SRC
    assert "def _variant_sink(" in _SRC
    assert "PORTOPT_VARIANT_STORE" in _SRC
    oos = (Path(__file__).resolve().parent.parent / "oos_engine.py").read_text(encoding="utf-8")
    assert "VARIANT_SINK(blended_returns)" in oos


# ------------------------------------------------------------------- PBO/CSCV

def _pbo_matrix(seed=0, T=1200, N=20):
    rng = np.random.default_rng(seed)
    return pd.DataFrame(rng.normal(0, 0.01, (T, N)),
                        index=pd.bdate_range("2020-01-01", periods=T))


def test_pbo_detects_a_genuine_persistent_edge():
    import validation as _v
    m = _pbo_matrix()
    m[0] = m[0] + 0.0012          # one column really is better
    assert _v.probability_of_backtest_overfitting(m)["pbo"] < 0.10


def test_pbo_flags_selection_that_cannot_persist():
    """Column-demeaning forces an in-sample winner to be an out-of-sample
    loser by construction — the pathological case PBO exists to catch."""
    import validation as _v
    m = _pbo_matrix()
    assert _v.probability_of_backtest_overfitting(m - m.mean())["pbo"] > 0.90


def test_pbo_orders_correctly_across_the_three_regimes():
    """The robust assertion: real edge < raw noise < anti-persistent.

    Pinning absolute values would be wrong — raw iid columns retain genuine
    finite-sample persistence, so a proper null sits below 0.5, not at it.
    """
    import validation as _v
    raw = _pbo_matrix()
    edge = raw.copy(); edge[0] = edge[0] + 0.0012
    p_edge = _v.probability_of_backtest_overfitting(edge)["pbo"]
    p_raw = _v.probability_of_backtest_overfitting(raw)["pbo"]
    p_anti = _v.probability_of_backtest_overfitting(raw - raw.mean())["pbo"]
    assert p_edge < p_raw < p_anti


def test_pbo_refuses_underpowered_input():
    import validation as _v
    one = _pbo_matrix(N=1)
    assert np.isnan(_v.probability_of_backtest_overfitting(one)["pbo"])
    tiny = _pbo_matrix(T=10, N=5)
    assert np.isnan(_v.probability_of_backtest_overfitting(tiny)["pbo"])

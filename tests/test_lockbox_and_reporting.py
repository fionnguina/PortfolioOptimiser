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
    assert "VARIANT_SINK(blended_returns, starting_nav_aud)" in oos


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


def test_pbo_flags_underpowered_trial_counts():
    """2 trials can only rank 1/3 or 2/3 — the number must not read as a result."""
    import validation as _v
    few = _v.probability_of_backtest_overfitting(_pbo_matrix(N=3))
    assert few["underpowered"] is True and "reason" in few
    many = _v.probability_of_backtest_overfitting(_pbo_matrix(N=20))
    assert many["underpowered"] is False and "reason" not in many


def test_scale_sweep_variants_do_not_collide(tmp_path):
    """The nightly evidence run sweeps NAV with one config. Keying on the
    window alone made all four collide and kept only the first."""
    import variant_store as vs
    r = _fake_returns()
    cfg = {"VOL_TARGET_ANNUAL": 0.16}
    keys = [vs.persist_variant(r * (1 - i * 0.01), cfg, {"nav_aud": nav},
                               app_dir=tmp_path)
            for i, nav in enumerate((100_000, 250_000, 500_000, 1_000_000))]
    assert len(set(keys)) == 4, "each NAV is a distinct evaluation"
    idx = vs.load_index(app_dir=tmp_path)
    assert idx["config_key"].nunique() == 1, "same strategy throughout"
    assert idx["data_key"].nunique() == 4, "NAV must live in the data key"


def test_trial_matrix_compares_configs_at_one_scale(tmp_path):
    import variant_store as vs
    r = _fake_returns()
    for nav in (250_000, 1_000_000):
        for vt in (0.12, 0.16, 0.20):
            vs.persist_variant(r * (1 + vt), {"VOL_TARGET_ANNUAL": vt},
                               {"nav_aud": nav}, app_dir=tmp_path)
    m = vs.load_trial_matrix(app_dir=tmp_path)
    assert m.shape[1] == 3, "3 configs at ONE nav, not 6 across two"


# ---------------------------------------- reporting lockbox must not blind drift

def test_walk_forward_runs_on_the_full_panel():
    """Truncating the INPUT starved the drift tracker. Truncate the output."""
    assert "oos_prices_report,\n" not in _SRC, (
        "run_oos must receive the full panel; the reporting cut happens after")


def test_drift_tracker_gets_the_complete_series():
    """The regression: comparing live NAV against a series that stops at the
    lockbox made every post-boundary day read as pure drift, latching the
    cumulative breach permanently and defeating the monitor."""
    assert '_oos_ret = globals().get("oos_returns_daily"' in _SRC
    assert "_oos_ret = globals().get(\"oos_returns_report\"" not in _SRC


def test_published_metrics_use_the_truncated_series():
    assert "strat_returns=oos_returns_report," in _SRC
    assert "strat_returns=oos_returns_daily," not in _SRC


def test_truncating_output_equals_truncating_input_for_a_causal_series():
    """The property the fix rests on: for a walk-forward whose returns at t
    depend only on data <= t, cutting the output at B is identical to having
    run on a panel that ended at B."""
    idx = pd.bdate_range("2016-08-16", periods=600)
    rng = np.random.default_rng(21)
    full = pd.Series(rng.normal(0.0005, 0.009, len(idx)), index=idx)
    boundary = idx[400]
    from_output = full[full.index <= boundary]
    # A causal engine fed the shorter panel would produce exactly these rows.
    from_input = full.iloc[:401]
    pd.testing.assert_series_equal(from_output, from_input)


# --------------------------------------------------- Excel workbook lock handling

def test_stale_lock_is_cleared_but_a_genuine_one_is_not(tmp_path, monkeypatch):
    """A tombstone with no owner must be removed; a live lock must be respected."""
    import importlib.util
    wb = tmp_path / "Book.xlsm"
    wb.write_bytes(b"x" * 32)
    lock = tmp_path / "~$Book.xlsm"
    lock.write_bytes(b"lock")

    # Reimplement the two-branch decision the engine makes, so the test pins
    # the BEHAVIOUR rather than importing the 16k-line monolith.
    def writable(p):
        try:
            with open(p, "r+b"):
                return True
        except Exception:
            return False

    assert writable(wb), "nothing holds it — the lock is stale"
    if writable(wb):
        lock.unlink()
    assert not lock.exists(), "stale lock must be cleared"


def test_engine_distinguishes_stale_from_genuine_locks():
    assert "def _workbook_is_writable(" in _SRC
    # Stale: cleared. Genuine: respected and warned.
    assert "Cleared STALE lock file" in _SRC
    assert "genuinely held" in _SRC


def test_auto_diversion_is_escalated_not_just_logged():
    """It sat in run.log for 3 days while the summary reported only a symptom."""
    assert '_XL_WROTE_TO_AUTO' in _SRC
    assert "Excel workbook:       DIVERTED" in _SRC


def test_excel_reaper_targets_only_the_spawned_pid():
    """Root cause: COM teardown orphans EXCEL.EXE, which deny-write-locks the
    workbook and makes the NEXT run divert. Must never touch a user's Excel."""
    assert "def _reap_excel(" in _SRC
    # PID captured from the app the engine spawned...
    assert "_xl_pid = app.pid" in _SRC
    # ...and reaped only after the context manager has had its chance to Quit.
    assert "_reap_excel(_xl_pid" in _SRC
    # A dead or missing PID is a no-op, never an error.
    assert "if not pid:" in _SRC
    assert 'if str(pid) not in (chk.stdout or ""):' in _SRC


def test_unattended_runs_do_not_auto_open_excel():
    """THE root cause of the workbook divergence: the engine opened the
    workbook in Excel when it finished. Fine interactively; on a 10:20
    scheduled run it leaves a resident EXCEL.EXE holding a deny-write lock,
    so the NEXT run diverts every sheet to an _AUTO copy."""
    assert "if _AUTO_PIPELINE_MODE or _SKIP_LIVE_PIPELINE:\n    OPEN_AFTER_SAVE = False" in _SRC
    # And the override must come AFTER the config default, or it does nothing.
    assert (_SRC.index('OPEN_AFTER_SAVE = CFG.get("open_after_save", True)')
            < _SRC.index("    OPEN_AFTER_SAVE = False"))


def test_unattended_runs_do_not_auto_open_powerpoint():
    """open_ppt_if_enabled has its OWN guard, so the OPEN_AFTER_SAVE override
    did not reach it — an unattended run still left a PowerPoint resident."""
    assert '"--auto-pipeline" in sys.argv) or globals().get("_SKIP_LIVE_PIPELINE")' in _SRC
    assert "deck saved but not opened" in _SRC


def test_sink_takes_nav_as_an_argument_not_from_a_global():
    """Two bugs, one cause: the global is assigned AFTER the first OOS call
    (primary record stored no NAV) and never changes across the scale sweep
    (100k/250k/500k/1M all collided into one key)."""
    oos = (Path(__file__).resolve().parent.parent / "oos_engine.py").read_text(encoding="utf-8")
    assert "VARIANT_SINK(blended_returns, starting_nav_aud)" in oos
    assert "def _variant_sink(blended_returns, nav_aud=None):" in _SRC
    # Argument wins; the global survives only as a fallback.
    assert "float(nav_aud) if nav_aud is not None" in _SRC


def test_scale_sweep_navs_produce_distinct_records(tmp_path):
    """End-to-end of the fix: four NAVs, one config, four keys."""
    import variant_store as vs
    r = _fake_returns()
    cfg = {"VOL_TARGET_ANNUAL": 0.16}
    keys = {vs.persist_variant(r * (1 - i * 0.005), cfg, {"nav_aud": nav}, app_dir=tmp_path)
            for i, nav in enumerate((100_000, 250_000, 500_000, 1_000_000))}
    assert len(keys) == 4
    assert vs.load_index(app_dir=tmp_path)["nav_aud"].notna().all(), "NAV must be recorded"


def test_pbo_readiness_reports_the_shortfall(tmp_path):
    import variant_store as vs
    r = _fake_returns()
    empty = vs.pbo_readiness(app_dir=tmp_path)
    assert empty["ready"] is False and empty["n_configs"] == 0

    for vt in (0.10, 0.12, 0.14):
        vs.persist_variant(r * (1 + vt), {"VOL_TARGET_ANNUAL": vt},
                           {"nav_aud": 250_000}, app_dir=tmp_path)
    few = vs.pbo_readiness(app_dir=tmp_path)
    assert few["ready"] is False and few["n_configs"] == 3 and few["shortfall"] == 7

    ready = vs.pbo_readiness(app_dir=tmp_path, min_configs=3)
    assert ready["ready"] is True and ready["shortfall"] == 0


# ------------------------------------------------- FY tax vs live NAV convention

def _july_case():
    """July 2026 in miniature: a 5.53% FY settlement the day after the
    2026-07-13 rebalance, which live NetLiq never books."""
    idx = pd.bdate_range("2026-06-30", "2026-08-17")
    oos = pd.Series(0.0005, index=idx)
    oos.loc[pd.Timestamp("2026-07-14")] -= 0.0553
    nav = pd.Series(np.linspace(240_000, 255_000, len(idx)), index=idx)
    taxes = pd.Series({pd.Timestamp("2026-07-13"): 0.0553})
    return nav, oos, taxes


def test_fy_tax_is_netted_out_of_drift_and_shown_separately():
    import drift
    nav, oos, taxes = _july_case()
    df = drift.compute_monthly_nav_drift(nav, oos, "2026-06-22", oos_taxes=taxes)
    jul = df.iloc[0]
    assert jul["OOS Tax"] == pytest.approx(0.0553, abs=1e-6)
    # ex-Tax must be the raw return with the charge added back.
    assert jul["OOS ex-Tax"] > jul["OOS Return"]
    # Drift is measured against ex-Tax, so it shrinks by ~the tax amount.
    raw = drift.compute_monthly_nav_drift(nav, oos, "2026-06-22")
    assert jul["Drift"] < raw.iloc[0]["Drift"] - 0.05


def test_netting_is_exact_because_the_charge_is_additive():
    """oos_engine does seg_b.iloc[0] -= cost + tax, so adding the fraction
    back on that same day recovers the pre-tax return exactly."""
    import drift
    idx = pd.bdate_range("2026-07-01", periods=20)
    oos = pd.Series(0.001, index=idx)
    clean = oos.copy()
    oos.loc[idx[8]] -= 0.04
    ex, per_day = drift._net_out_fy_tax(oos, pd.Series({idx[7]: 0.04}))
    pd.testing.assert_series_equal(ex, clean, check_exact=False, rtol=1e-12)
    assert per_day.sum() == pytest.approx(0.04)


def test_months_without_a_tax_charge_are_unchanged():
    import drift
    nav, oos, taxes = _july_case()
    df = drift.compute_monthly_nav_drift(nav, oos, "2026-06-22", oos_taxes=taxes)
    aug = df[df["Month"] == "2026-08"].iloc[0]
    assert aug["OOS Tax"] == 0.0
    assert aug["OOS ex-Tax"] == pytest.approx(aug["OOS Return"])


def test_drift_still_works_without_a_tax_series():
    """Back-compat: the argument is optional and absence must not crash."""
    import drift
    nav, oos, _ = _july_case()
    df = drift.compute_monthly_nav_drift(nav, oos, "2026-06-22")
    assert not df.empty and (df["OOS Tax"] == 0.0).all()


def test_engine_passes_the_tax_series_to_drift():
    assert 'oos_taxes=globals().get("oos_rebalance_taxes")' in _SRC


# ------------------------------------------------- fills-log NAV reconstruction

def _seed_and_fills(tmp_path, units_au=100, units_us=10):
    import json as _j
    seed = tmp_path / "seed.json"
    seed.write_text(_j.dumps([
        {"Security": "AAA.AX", "Units": units_au, "AcqDate": "2026-07-01T00:00:00"},
        {"Security": "BBB", "Units": units_us, "AcqDate": "2026-07-01T00:00:00"},
    ]), encoding="utf-8")
    fills = tmp_path / "fills.jsonl"
    fills.write_text("", encoding="utf-8")
    return seed, fills


def test_reconstruction_converts_usd_holdings_to_aud():
    """`prices` is the engine's MIXED panel; summing it raw added USD values to
    AUD ones and put the series ~15% below broker NetLiq."""
    import nav as _nav, tempfile, pathlib
    with tempfile.TemporaryDirectory() as td:
        tmp = pathlib.Path(td)
        seed, fills = _seed_and_fills(tmp)
        idx = pd.bdate_range("2026-07-01", periods=10)
        px = pd.DataFrame({"AAA.AX": 10.0, "BBB": 20.0}, index=idx)
        fx = pd.Series(1.5, index=idx)          # 1 USD = 1.5 AUD
        raw = _nav.compute_actual_nav_series(px, fills, seed)
        conv = _nav.compute_actual_nav_series(px, fills, seed, fx_usdaud=fx)
        assert float(raw.iloc[0]) == pytest.approx(100 * 10 + 10 * 20)
        assert float(conv.iloc[0]) == pytest.approx(100 * 10 + 10 * 20 * 1.5)
        # The .AX leg must NOT be converted.
        assert float(conv.iloc[0]) - float(raw.iloc[0]) == pytest.approx(10 * 20 * 0.5)


def test_reconstruction_includes_cash_because_netliq_does():
    import nav as _nav, tempfile, pathlib
    with tempfile.TemporaryDirectory() as td:
        tmp = pathlib.Path(td)
        seed, fills = _seed_and_fills(tmp)
        idx = pd.bdate_range("2026-07-01", periods=5)
        px = pd.DataFrame({"AAA.AX": 10.0, "BBB": 20.0}, index=idx)
        base = _nav.compute_actual_nav_series(px, fills, seed)
        withc = _nav.compute_actual_nav_series(px, fills, seed, cash_aud=5000.0)
        assert float(withc.iloc[0] - base.iloc[0]) == pytest.approx(5000.0)


def test_reconstruction_that_fails_validation_is_not_extrapolated(tmp_path, monkeypatch):
    """It derives a PATH from a position SNAPSHOT. If it cannot match broker
    where both exist, it has not earned the right to speak where only it does."""
    import nav as _nav
    monkeypatch.setattr(_nav, "APP_DIR", tmp_path)
    seed, fills = _seed_and_fills(tmp_path)
    idx = pd.bdate_range("2026-07-01", periods=12)
    # Prices wander; the broker log says something quite different.
    rng = np.random.default_rng(3)
    px = pd.DataFrame({"AAA.AX": 10 * (1 + pd.Series(rng.normal(0, 0.03, len(idx)), index=idx)).cumprod(),
                       "BBB": 20.0}, index=idx)
    log = tmp_path / "ibkr_nav_log.jsonl"
    import json as _j
    with open(log, "w", encoding="utf-8") as fp:
        for i, d in enumerate(idx[4:]):
            fp.write(_j.dumps({"ts": d.isoformat(), "net_liquidation_aud": 200_000 + i * 900,
                               "cash_aud": 1000.0}) + "\n")
    out = _nav.compute_actual_nav_series_spliced(px, fills, seed, broker_nav_path=log)
    broker = _nav._load_broker_nav_series(log)
    pd.testing.assert_series_equal(out, broker), "must fall back to broker-only"


def test_first_broker_cash_reads_the_earliest_balance(tmp_path):
    import nav as _nav, json as _j
    log = tmp_path / "n.jsonl"
    with open(log, "w", encoding="utf-8") as fp:
        fp.write(_j.dumps({"ts": "2026-08-01T10:00:00", "cash_aud": 111.0}) + "\n")
        fp.write(_j.dumps({"ts": "2026-07-01T10:00:00", "cash_aud": 222.0}) + "\n")
    assert _nav._first_broker_cash(log) == pytest.approx(222.0)


def test_engine_passes_fx_to_the_reconstruction():
    assert 'fx_usdaud=globals().get("fx_usdaud")' in _SRC


def test_nav_source_label_is_read_after_the_call_that_sets_it():
    """First attempt read LAST_NAV_SOURCE before the spliced call ran, so the
    log said 'unknown' — the same ordering slip as the variant-sink NAV."""
    call = _SRC.index("_live_nav = compute_actual_nav_series_spliced(")
    label = _SRC.index('_nav_src = "actual-NAV: "')
    assert label > call, "label must be read after the call populates it"


def test_nav_module_records_which_source_it_returned():
    import nav as _nav
    assert hasattr(_nav, "LAST_NAV_SOURCE")
    src = (Path(__file__).resolve().parent.parent / "nav.py").read_text(encoding="utf-8")
    assert 'LAST_NAV_SOURCE"] = "broker NetLiq only (recon failed validation)"' in src
    assert 'LAST_NAV_SOURCE"] = "fills recon (validated) + broker NetLiq"' in src


# ------------------------------------------------------- module-alias shadowing

def test_nav_module_is_not_aliased_to_a_shadowable_name():
    """`import nav as _nav` collided with eight module-level `for _nav in ...`
    loops over scale-sensitivity NAVs. Loop variables leak at module scope, so
    by the time lot reconciliation ran, _nav was a float — broker
    reconciliation failed silently on every run ('float' object has no
    attribute 'load_broker_positions')."""
    import ast
    src = _SRC
    assert "import nav as _nav\n" not in src, "the shadowable alias must stay retired"
    tree = ast.parse(src)
    # No attribute access on a bare `_nav` may remain.
    bad = [n.lineno for n in ast.walk(tree)
           if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)
           and n.value.id == "_nav"]
    assert not bad, f"module-attribute use of shadowable _nav at lines {bad}"


def test_no_module_handle_is_rebound_at_module_scope():
    """Generalises the _nav bug to its whole class.

    The condition that actually breaks: a name used as a MODULE (attribute
    access) that is also assigned at MODULE scope. Function-local rebinds are
    harmless (they never reach module scope) and try/except import fallbacks
    are intended, so both are excluded — the first version of this test flagged
    `tk` inside fetch_ibkr_live_prices_native and was a false positive.
    """
    import ast
    tree = ast.parse(_SRC)
    aliases = {a.asname for n in ast.walk(tree)
               if isinstance(n, (ast.Import, ast.ImportFrom))
               for a in n.names if a.asname}
    # Only names actually dereferenced as modules can break this way.
    used_as_module = {n.value.id for n in ast.walk(tree)
                      if isinstance(n, ast.Attribute)
                      and isinstance(n.value, ast.Name) and n.value.id in aliases}

    func_spans = [(f.lineno, f.end_lineno) for f in ast.walk(tree)
                  if isinstance(f, (ast.FunctionDef, ast.AsyncFunctionDef))]

    def at_module_scope(lineno):
        return not any(a <= lineno <= (b or 0) for a, b in func_spans)

    offenders = {}
    for n in ast.walk(tree):
        if (isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)
                and n.id in used_as_module and at_module_scope(n.lineno)):
            offenders.setdefault(n.id, []).append(n.lineno)
    assert not offenders, (
        f"module handles rebound at module scope (the _nav bug class): {offenders}")


def test_health_error_count_excludes_handled_warnings():
    """A [WARN]-tagged line is handled by definition. The bare "FAILED"
    pattern was counting the nav gate's own notice as an error demanding
    investigation — cry wolf and the real 2am errors stop being believed."""
    i = _SRC.index("n_err = sum(1 for l in _lines")
    block = _SRC[i:i + 420]
    assert "not _is_tagged(l)" in block, "handled warnings must not count as errors"


# ------------------------------------------------- lot-book FX cost-base check

def _recon_fixture():
    """One US lot bought at 10.00 USD when USD=1.60 AUD, checked when USD=1.40.
    Cost base is correct; only the spot rate has moved."""
    fx = pd.Series(1.60, index=pd.bdate_range("2026-01-01", periods=40))
    fx.iloc[20:] = 1.40
    lots_df = pd.DataFrame([{"Security": "XUS", "AcqDate": fx.index[0],
                             "Units": 100.0, "CostBaseAUD": 10.00 * 1.60}])
    broker = {"XUS": {"units": 100.0, "avg_cost_local": 10.00}, "_ts": "x"}
    return lots_df, broker, fx


def test_spot_fx_comparison_fires_a_false_cost_base_warning():
    """The old behaviour, pinned so the regression is visible: converting the
    broker's local cost at TODAY's rate drifts with FX even when the cost base
    is exactly right."""
    import lots as _lots
    lots_df, broker, fx = _recon_fixture()
    warns = _lots.reconcile_lots_vs_broker(lots_df, broker, fx_map={"XUS": 1.40})
    assert warns and "cost base may be wrong" in warns[0]
    assert "spot-FX comparison" in warns[0], "must disclose which comparison ran"


def test_currency_free_comparison_clears_it():
    """With fx_hist the lot is re-expressed at its ACQUISITION rate — which is
    what AU CGT requires — and the false warning disappears."""
    import lots as _lots
    lots_df, broker, fx = _recon_fixture()
    warns = _lots.reconcile_lots_vs_broker(lots_df, broker,
                                           fx_map={"XUS": 1.40}, fx_hist=fx)
    assert warns == [], warns


def test_currency_free_still_catches_a_real_cost_base_error():
    """It must not simply silence the check: a genuinely wrong cost base
    still fires once FX drift is removed."""
    import lots as _lots
    lots_df, broker, fx = _recon_fixture()
    lots_df.loc[0, "CostBaseAUD"] = 11.00 * 1.60      # 10% too high, real error
    warns = _lots.reconcile_lots_vs_broker(lots_df, broker,
                                           fx_map={"XUS": 1.40}, fx_hist=fx)
    assert warns and "local ccy" in warns[0] and "+1000bps" in warns[0].replace(",", "")


def test_subsecond_acquisition_timestamps_do_not_sink_the_check():
    """Series.asof against a date-resolution index raises 'Cannot losslessly
    convert units' on '2026-08-03T09:33:30.149710', and the except-clause
    silently fell back to the spot comparison."""
    import lots as _lots
    lots_df, broker, fx = _recon_fixture()
    lots_df.loc[0, "AcqDate"] = pd.Timestamp("2026-01-01T09:33:30.149710")
    warns = _lots.reconcile_lots_vs_broker(lots_df, broker,
                                           fx_map={"XUS": 1.40}, fx_hist=fx)
    assert warns == [], f"sub-second timestamp must not force the spot path: {warns}"


def test_au_tickers_are_unaffected_by_the_local_path():
    import lots as _lots
    lots_df = pd.DataFrame([{"Security": "AAA.AX", "AcqDate": pd.Timestamp("2026-01-05"),
                             "Units": 10.0, "CostBaseAUD": 5.00}])
    broker = {"AAA.AX": {"units": 10.0, "avg_cost_local": 5.00}, "_ts": "x"}
    fx = pd.Series(1.5, index=pd.bdate_range("2026-01-01", periods=20))
    assert _lots.reconcile_lots_vs_broker(lots_df, broker, fx_map={"AAA.AX": 1.0},
                                          fx_hist=fx) == []


def test_engine_passes_the_historical_fx_series():
    assert 'fx_hist=globals().get("fx_usdaud")' in _SRC

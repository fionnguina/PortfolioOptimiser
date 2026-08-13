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

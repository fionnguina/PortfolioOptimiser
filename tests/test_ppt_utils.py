"""Tests for ppt_utils.py — the report layer's pure primitives.

Focus on the date-window return math (which carried a real regression: a bad
_nearest_on_or_before fell back to idx[0] so 3M/6M/12M/3Y all showed the same
number) and the perf-value formatter. Two builders (_add_perf_table,
_add_change_run) are exercised against a real python-pptx slide.
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import ppt_utils


# === _nearest_on_or_before ====================================================

def test_nearest_before_start_returns_none():
    idx = pd.to_datetime(["2026-01-10", "2026-01-20"])
    assert ppt_utils._nearest_on_or_before(idx, "2026-01-01") is None


def test_nearest_exact_and_between():
    idx = pd.to_datetime(["2026-01-10", "2026-01-20", "2026-01-30"])
    assert ppt_utils._nearest_on_or_before(idx, "2026-01-20") == pd.Timestamp("2026-01-20")
    # between 20th and 30th -> the largest <= dt = the 20th
    assert ppt_utils._nearest_on_or_before(idx, "2026-01-25") == pd.Timestamp("2026-01-20")
    # after the end -> clamps to last
    assert ppt_utils._nearest_on_or_before(idx, "2026-02-15") == pd.Timestamp("2026-01-30")


def test_nearest_empty_index():
    assert ppt_utils._nearest_on_or_before(pd.to_datetime([]), "2026-01-01") is None


# === _period_total_return =====================================================

def test_period_total_return_known_window():
    idx = pd.date_range("2025-01-01", periods=400, freq="D")
    px = pd.Series(np.linspace(100.0, 200.0, 400), index=idx)
    # 12M ending at the last date: value doubled overall, so ~1yr slice < 100%.
    r = ppt_utils._period_total_return(px, idx[-1], years=1)
    end_v = px.iloc[-1]
    start_dt = ppt_utils._nearest_on_or_before(idx, idx[-1] - pd.DateOffset(years=1))
    start_v = px.loc[start_dt]
    assert r == pytest.approx(end_v / start_v - 1.0)


def test_period_total_return_window_before_series_is_nan():
    idx = pd.date_range("2026-01-01", periods=10, freq="D")
    px = pd.Series(np.arange(100, 110), index=idx)
    # 3Y lookback on a 10-day series -> start falls before series -> NaN (the
    # regression that this guard fixes: it must NOT fall back to idx[0]).
    assert np.isnan(ppt_utils._period_total_return(px, idx[-1], years=3))


def test_period_total_return_empty():
    assert np.isnan(ppt_utils._period_total_return(pd.Series(dtype=float), "2026-01-01", months=3))


# === _window_compound_total ===================================================

def test_window_compound_total_compounds_returns():
    idx = pd.date_range("2026-01-01", periods=40, freq="D")   # spans the 1M window
    rng = np.random.default_rng(3)
    r = pd.Series(rng.normal(0, 0.01, 40), index=idx)
    out = ppt_utils._window_compound_total(r, idx[-1], months=1)
    # Replicate the function's own window selection to derive the expectation.
    start = ppt_utils._nearest_on_or_before(idx, idx[-1] - pd.DateOffset(months=1))
    end = ppt_utils._nearest_on_or_before(idx, idx[-1])
    expected = float((1.0 + r.loc[start:end]).prod() - 1.0)
    assert out == pytest.approx(expected)


def test_window_compound_total_degenerate_window_is_nan():
    idx = pd.date_range("2026-01-01", periods=3, freq="D")
    r = pd.Series([0.01, 0.02, 0.03], index=idx)
    # end anchored to first date -> start >= end -> NaN
    assert np.isnan(ppt_utils._window_compound_total(r, idx[0], months=1))


# === _format_perf_value =======================================================

@pytest.mark.parametrize("v,fmt,expected", [
    (0.1234, "pct2", "12.34%"),
    (-0.05, "pct2", "-5.00%"),
    (1.23456, "dec3", "1.235"),
    (42.0, "raw", "42.0"),
    (float("nan"), "pct2", ""),
    (float("inf"), "pct2", ""),
    (None, "pct2", ""),
    ("not a number", "pct2", ""),
])
def test_format_perf_value(v, fmt, expected):
    assert ppt_utils._format_perf_value(v, fmt=fmt) == expected


# === slide builders (real python-pptx) ========================================

def _blank_slide():
    from pptx import Presentation
    prs = Presentation()
    return prs.slides.add_slide(prs.slide_layouts[6])  # blank layout


def test_add_perf_table_fills_cells():
    from pptx.util import Cm
    slide = _blank_slide()
    df = pd.DataFrame({"SPY": [0.10, 0.15], "Engine": [0.12, np.nan]},
                      index=["Return", "Sharpe"])
    tbl = ppt_utils._add_perf_table(slide, df, Cm(1), Cm(1), Cm(10), Cm(4),
                                    title="Metric", value_fmt="pct2")
    assert tbl.cell(0, 0).text == "Metric"
    assert tbl.cell(0, 1).text == "SPY"
    assert tbl.cell(1, 0).text == "Return"
    assert tbl.cell(1, 1).text == "10.00%"
    assert tbl.cell(2, 2).text == ""          # NaN cell renders blank


def test_add_change_run_signs_and_zero():
    slide = _blank_slide()
    tb = slide.shapes.add_textbox(0, 0, 100, 100)
    p = tb.text_frame.paragraphs[0]
    ppt_utils._add_change_run(p, 12.5)
    ppt_utils._add_change_run(p, -3.0)
    ppt_utils._add_change_run(p, 0)           # zero adds an empty run
    texts = [r.text for r in p.runs]
    assert " (+12.50)" in texts
    assert " (-3.00)" in texts
    assert "" in texts

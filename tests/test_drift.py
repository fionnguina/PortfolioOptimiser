"""Smoke tests for the drift tracker (fills, NAV, MaxDD)."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

from conftest import extract_funcs

# JSONL log writers/readers moved to jsonl_logs.py (Phase 4 split, 2026-06-29).
# Imported directly here; the remaining drift compute helpers still live in
# Portfolio_Optimiser.py and need the AST-extract trick.
import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
from jsonl_logs import (
    _load_recommendation_log as _ll_load_recs,
    append_live_nav_history as _ll_append_nav,
    _load_live_nav_series as _ll_load_nav,
)


@pytest.fixture(scope="module")
def drift():
    ns = extract_funcs(
        "compute_fill_drift",
        "_match_fill_to_recommendation",
        "compute_live_max_drawdown",
        "compute_monthly_nav_drift",
        extra_consts=("TARGET_PORTFOLIO_VALUE_AUD",),
    )
    # Inject the moved-out functions so the existing test bodies keep using
    # drift["name"](...) lookup unchanged.
    ns["_load_recommendation_log"] = _ll_load_recs
    ns["append_live_nav_history"] = _ll_append_nav
    ns["_load_live_nav_series"] = _ll_load_nav
    # compute_fill_drift internally calls _load_recommendation_log via its
    # enclosing namespace ns — the extract_funcs exec_module puts it there.
    return ns


# === Fill matching ============================================================

def _write_rec_log(path: Path, *entries: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


def test_fill_slippage_buy_side(tmp_path, drift):
    """Buy fill at higher price than recommended → positive slippage bps."""
    rec_log = tmp_path / "rec.jsonl"
    _write_rec_log(rec_log, {
        "run_at": "2026-08-01T10:00:00",
        "recommended_trades": [
            {"ticker": "SMH", "side": "buy", "delta_units": 10,
             "px_aud": 100.00, "delta_value_aud": 1000.0, "brokerage_aud": 5.0},
        ],
    })
    fills = pd.DataFrame({
        "Fill Date": pd.to_datetime(["2026-08-02"]),
        "Ticker": ["SMH"], "Units": [10],
        "Px AUD": [100.50], "Fees AUD": [5.0], "Notes": [""],
    })
    out = drift["compute_fill_drift"](fills, rec_log)
    row = out.iloc[0]
    # Paid 100.50 vs recommended 100 → +50 bps slippage (worse for buyer).
    assert row["Slippage (bps)"] == pytest.approx(50.0, abs=0.5)
    assert row["Side Actual"] == "buy"
    assert bool(row["Recommended"]) is True


def test_fill_slippage_sell_side(tmp_path, drift):
    """Sell fill at lower price than recommended → positive slippage bps
    (positive sign convention = worse-than-expected for the actor)."""
    rec_log = tmp_path / "rec.jsonl"
    _write_rec_log(rec_log, {
        "run_at": "2026-08-01T10:00:00",
        "recommended_trades": [
            {"ticker": "VLUE.AX", "side": "sell", "delta_units": -50,
             "px_aud": 40.00, "delta_value_aud": -2000.0, "brokerage_aud": 5.0},
        ],
    })
    fills = pd.DataFrame({
        "Fill Date": pd.to_datetime(["2026-08-02"]),
        "Ticker": ["VLUE.AX"], "Units": [-50],
        "Px AUD": [39.80], "Fees AUD": [5.0], "Notes": [""],
    })
    out = drift["compute_fill_drift"](fills, rec_log)
    row = out.iloc[0]
    # Got 39.80 vs recommended 40 → got LESS by 50 bps = +50 bps slippage.
    assert row["Slippage (bps)"] == pytest.approx(50.0, abs=0.5)
    assert row["Side Actual"] == "sell"


def test_fill_with_no_matching_rec(tmp_path, drift):
    """Fill with no prior recommendation → Recommended=False."""
    rec_log = tmp_path / "rec.jsonl"
    _write_rec_log(rec_log, {
        "run_at": "2026-08-01T10:00:00",
        "recommended_trades": [
            {"ticker": "SMH", "side": "buy", "delta_units": 10,
             "px_aud": 100.0, "delta_value_aud": 1000.0, "brokerage_aud": 5.0},
        ],
    })
    fills = pd.DataFrame({
        "Fill Date": pd.to_datetime(["2026-08-02"]),
        "Ticker": ["MYSTERY"], "Units": [100],
        "Px AUD": [10.0], "Fees AUD": [2.5], "Notes": ["unsolicited"],
    })
    out = drift["compute_fill_drift"](fills, rec_log)
    assert bool(out.iloc[0]["Recommended"]) is False
    assert pd.isna(out.iloc[0]["Slippage (bps)"])


def test_matches_most_recent_rec_for_ticker(tmp_path, drift):
    """If a ticker has multiple recommendations, use the most recent ≤ fill date."""
    rec_log = tmp_path / "rec.jsonl"
    _write_rec_log(rec_log,
        {
            "run_at": "2026-08-01T10:00:00",
            "recommended_trades": [
                {"ticker": "SMH", "side": "buy", "delta_units": 10,
                 "px_aud": 100.0, "delta_value_aud": 1000.0, "brokerage_aud": 5.0},
            ],
        },
        {
            "run_at": "2026-08-15T10:00:00",
            "recommended_trades": [
                {"ticker": "SMH", "side": "buy", "delta_units": 10,
                 "px_aud": 110.0, "delta_value_aud": 1100.0, "brokerage_aud": 5.0},
            ],
        },
    )
    fills = pd.DataFrame({
        "Fill Date": pd.to_datetime(["2026-08-20"]),
        "Ticker": ["SMH"], "Units": [10],
        "Px AUD": [110.0], "Fees AUD": [5.0], "Notes": [""],
    })
    out = drift["compute_fill_drift"](fills, rec_log)
    row = out.iloc[0]
    # Should match the 08-15 rec at $110, not 08-01 @ $100 → ~0 bps slippage.
    assert row["Px Recommended (AUD)"] == pytest.approx(110.0)
    assert row["Slippage (bps)"] == pytest.approx(0.0, abs=0.5)


# === NAV history + DD =========================================================

def test_nav_append_idempotent_within_day(tmp_path, drift):
    """Same-date entries replace prior — re-running same day doesn't dup-count."""
    p = tmp_path / "nav.jsonl"
    drift["append_live_nav_history"](p, 1_000_000.0, as_of_date="2026-08-01")
    drift["append_live_nav_history"](p, 1_005_000.0, as_of_date="2026-08-01")
    s = drift["_load_live_nav_series"](p)
    assert len(s) == 1
    assert float(s.iloc[0]) == 1_005_000.0


def test_nav_max_drawdown(tmp_path, drift):
    """Current DD measured from running peak."""
    p = tmp_path / "nav.jsonl"
    for date, nav in [
        ("2026-08-31", 1_000_000),
        ("2026-09-30", 1_100_000),  # new peak
        ("2026-10-31",   950_000),  # -13.64% DD
    ]:
        drift["append_live_nav_history"](p, nav, as_of_date=date)
    s = drift["_load_live_nav_series"](p)
    dd = drift["compute_live_max_drawdown"](s)
    assert dd == pytest.approx(-0.1364, abs=0.01)


def test_nav_max_drawdown_empty_series(drift):
    """Empty series → DD of 0 (not a crash)."""
    s = pd.Series(dtype=float)
    dd = drift["compute_live_max_drawdown"](s)
    assert dd == 0.0


# === Monthly NAV drift ========================================================

def test_monthly_nav_drift_skips_baseline_month(tmp_path, drift):
    """First NAV is the baseline; the month containing it should be skipped."""
    p = tmp_path / "nav.jsonl"
    for date, nav in [
        ("2026-08-31", 1_000_000),
        ("2026-09-30", 1_050_000),
        ("2026-10-31",   980_000),
    ]:
        drift["append_live_nav_history"](p, nav, as_of_date=date)
    nav_series = drift["_load_live_nav_series"](p)
    # No OOS comparison (empty series).
    df = drift["compute_monthly_nav_drift"](
        nav_series, pd.Series(dtype=float),
        live_start_date="2026-08-01",
    )
    # First baseline month (Aug) skipped → Sep onwards. 1.05/1.0 - 1 = +5%.
    assert df.iloc[0]["Month"] == "2026-09"
    assert df.iloc[0]["Live Return"] == pytest.approx(0.05, abs=0.001)
    # Oct: 0.98 / 1.05 - 1 = -6.67%
    assert df.iloc[1]["Live Return"] == pytest.approx(-0.0667, abs=0.001)


def test_monthly_nav_drift_inactive_when_start_none(tmp_path, drift):
    """LIVE_TRADING_START_DATE=None → empty DataFrame (drift inactive)."""
    p = tmp_path / "nav.jsonl"
    drift["append_live_nav_history"](p, 1_000_000.0)
    s = drift["_load_live_nav_series"](p)
    df = drift["compute_monthly_nav_drift"](s, pd.Series(dtype=float),
                                            live_start_date=None)
    assert df.empty

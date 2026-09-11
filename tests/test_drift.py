"""Smoke tests for the drift tracker (fills, NAV, MaxDD)."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

from conftest import extract_funcs  # noqa: F401

# Phase 4 split (2026-06-29): drift tracker now lives in drift.py + jsonl_logs.py.
# Tests import directly — no AST-extract needed.
import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
from jsonl_logs import (
    _load_recommendation_log,
    append_live_nav_history,
    _load_live_nav_series,
)
from drift import (
    _match_fill_to_recommendation,
    compute_fill_drift,
    compute_live_max_drawdown,
    compute_monthly_nav_drift,
)


@pytest.fixture(scope="module")
def drift():
    return {
        "_match_fill_to_recommendation": _match_fill_to_recommendation,
        "compute_fill_drift":            compute_fill_drift,
        "compute_live_max_drawdown":     compute_live_max_drawdown,
        "compute_monthly_nav_drift":     compute_monthly_nav_drift,
        "_load_recommendation_log":      _load_recommendation_log,
        "append_live_nav_history":       append_live_nav_history,
        "_load_live_nav_series":         _load_live_nav_series,
    }


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


def test_fill_missing_px_and_fees_leaves_metrics_blank(tmp_path, drift):
    """Old ledger with no Px AUD / Fees AUD columns → slippage + fee-delta are
    None (not a fake -100% slippage or a spurious -brokerage fee delta), but the
    fill still matches its rec and reports Fee Expected."""
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
        "Ticker": ["SMH"], "Units": [10],  # no Px AUD / Fees AUD columns
    })
    out = drift["compute_fill_drift"](fills, rec_log)
    row = out.iloc[0]
    assert bool(row["Recommended"]) is True
    assert pd.isna(row["Slippage (bps)"])
    assert pd.isna(row["Fee Delta (AUD)"])
    assert row["Fee Expected (AUD)"] == pytest.approx(5.0)


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


# --------------------------------------------------------------------------
# A partial first month is a stub, not a month (2026-08-20)
# --------------------------------------------------------------------------

def _drift_env(start_nav="2026-06-24"):
    idx = pd.bdate_range(start_nav, "2026-08-20")
    nav = pd.Series(250000.0 * (1 + pd.Series(range(len(idx)), index=idx) * 0.0002))
    # Big enough that even the five-day June stub breaches the 2% gate —
    # otherwise the "never warned on" test passes for the wrong reason.
    oos = pd.Series(0.006, index=idx)
    return nav, oos


def test_a_five_day_first_month_is_marked_partial():
    """2026-06 was five trading days (24th-30th) during which the book was
    about a third built — 39 of 60 trades executed after the 30th. Comparing
    that against a fully invested OOS expectation is not tracking error, but it
    read as -2.10% and breached the gate the moment the NAV reconstruction was
    extended back far enough to include it."""
    import drift as D

    nav, oos = _drift_env()
    df = D.compute_monthly_nav_drift(nav, oos, "2026-06-22")
    assert df.iloc[0]["Month"] == "2026-06"
    assert df.iloc[0]["Partial"] is True or bool(df.iloc[0]["Partial"])
    assert not any(bool(r["Partial"]) for _, r in df.iloc[1:].iterrows()), \
        "only the first row can be partial — every later baseline is a month-end"


def test_a_month_started_on_the_first_is_not_partial():
    """Coverage, not a month-end test on the baseline. An earlier version
    asked 'is the baseline a month-end' and called a series starting 1 July
    partial, which would have suppressed a real warning for a whole month."""
    import drift as D

    nav, oos = _drift_env()
    df = D.compute_monthly_nav_drift(nav, oos, "2026-07-01")
    assert df.iloc[0]["Month"] == "2026-07"
    assert not bool(df.iloc[0]["Partial"])


def test_a_partial_month_is_reported_but_never_warned_on(capsys):
    """It stays in the table — the number is real and worth seeing — but it
    cannot raise a [drift][WARN]."""
    import drift as D

    nav, oos = _drift_env()
    df = D.compute_monthly_nav_drift(nav, oos, "2026-06-22")
    assert abs(float(df.iloc[0]["Drift"])) > D.DRIFT_MONTHLY_THRESH, \
        "fixture must breach the threshold, or this proves nothing"
    D._print_drift_warnings(pd.DataFrame(), df, -0.01)
    out = capsys.readouterr().out
    assert "2026-06" not in out, out
    assert "2026-07" in out, "full months must still warn"


def test_full_months_still_warn_normally():
    """The guard must not become a licence to miss real drift."""
    import drift as D

    nav, oos = _drift_env(start_nav="2026-07-01")
    df = D.compute_monthly_nav_drift(nav, oos, "2026-07-01")
    breaches = [r for _, r in df.iterrows()
                if abs(float(r["Drift"])) > D.DRIFT_MONTHLY_THRESH
                and not bool(r["Partial"])]
    assert breaches, "a fully-covered month over threshold must still be warnable"


# === slippage from the broker statement (2026-09-11) ==========================
# Fills used to come from the Actual_Fills sheet, which mirrors submit-time
# state: qty_filled=0 and no fill price on this account. Slippage was therefore
# permanently None. The statement is the only source with a real fill price.

def _rec_log(tmp_path, run_at="2026-07-06T09:31:00", px=100.0, units=10):
    p = tmp_path / "rec.jsonl"
    p.write_text(json.dumps({
        "run_at": run_at,
        "recommended_trades": [
            {"ticker": "SMH", "side": "buy", "delta_units": units,
             "px_aud": px, "brokerage_aud": 5.0},
        ],
    }) + "\n", encoding="utf-8")
    return p


def _stmt_trades():
    import pandas as pd
    return pd.DataFrame([
        {"Security": "SMH", "DateTime": pd.Timestamp("2026-07-06 23:30:00"),
         "Units": 10.0, "Currency": "USD", "PriceLocal": 70.0, "CommLocal": -1.0},
        {"Security": "VLUE.AX", "DateTime": pd.Timestamp("2026-07-06 10:00:00"),
         "Units": 100.0, "Currency": "AUD", "PriceLocal": 37.0, "CommLocal": -5.0},
    ])


def test_fills_aud_builds_the_drift_schema_in_aud():
    import ibkr_statement as S
    import drift as D
    fx = pd.Series({pd.Timestamp("2026-07-06"): 1.50})
    out = S.fills_aud(_stmt_trades(), fx_usdaud=fx)
    assert set(out.columns) >= {"Fill Date", "Ticker", "Units", "Px AUD",
                                "Fees AUD", "Qty Confirmed"}
    smh = out[out["Ticker"] == "SMH"].iloc[0]
    assert float(smh["Px AUD"]) == pytest.approx(70.0 * 1.50)   # trade-date rate
    assert float(smh["Fees AUD"]) == pytest.approx(1.0 * 1.50)  # positive cost
    assert bool(smh["Qty Confirmed"]) is True
    aud = out[out["Ticker"] == "VLUE.AX"].iloc[0]
    assert float(aud["Px AUD"]) == pytest.approx(37.0)          # AUD untouched


def test_fills_aud_drops_zero_price_transfer_rows():
    """The 2026-06-23 account restructure booked 9 sells at PriceLocal 0.0.

    A zero price computes as -100% slippage, so these must drop rather than
    be priced as if they were fills.
    """
    import ibkr_statement as S
    import drift as D
    tr = _stmt_trades()
    tr.loc[len(tr)] = {"Security": "GOLD.AX",
                       "DateTime": pd.Timestamp("2026-06-23 14:00:00"),
                       "Units": -3259.0, "Currency": "AUD",
                       "PriceLocal": 0.0, "CommLocal": 0.0}
    out = S.fills_aud(tr, fx_usdaud=pd.Series({pd.Timestamp("2026-07-06"): 1.5}))
    assert "GOLD.AX" not in set(out["Ticker"])
    assert set(out["Ticker"]) == {"SMH", "VLUE.AX"}   # only the 0.0 row dropped


def test_slippage_is_computed_from_the_statement_price(tmp_path):
    import ibkr_statement as S
    import drift as D
    fx = pd.Series({pd.Timestamp("2026-07-06"): 1.50})
    fills = S.fills_aud(_stmt_trades(), fx_usdaud=fx)
    d = D.compute_fill_drift(fills, _rec_log(tmp_path, px=100.0))
    smh = d[d["Ticker"] == "SMH"].iloc[0]
    # Recommended at 100 AUD, filled at 105 AUD -> +500 bps, paid more on a buy.
    assert float(smh["Slippage (bps)"]) == pytest.approx(500.0)
    assert smh["Recommended"] is True or bool(smh["Recommended"])


def test_fills_predating_the_rec_log_are_excluded(tmp_path, capsys):
    """Adherence has no opinion on trades made before it existed."""
    import ibkr_statement as S
    import drift as D
    fills = S.fills_aud(_stmt_trades(), fx_usdaud=pd.Series({pd.Timestamp("2026-07-06"): 1.5}))
    # Rec log starts AFTER both fills.
    d = D.compute_fill_drift(fills, _rec_log(tmp_path, run_at="2026-08-01T09:31:00"))
    assert d.empty
    assert "predate the recommendation log" in capsys.readouterr().out


def test_historical_fills_are_summarised_not_warned(capsys):
    """51 real fills would otherwise print a [drift][WARN] each, every run."""
    import drift as D
    old = pd.Timestamp.now() - pd.Timedelta(days=60)
    d = pd.DataFrame([
        {"Fill Date": old, "Ticker": "SMH", "Slippage (bps)": 780.0,
         "Fees Actual (AUD)": 1.0, "Fee Expected (AUD)": 5.0,
         "Fee Delta (AUD)": -4.0, "Recommended": True},
        {"Fill Date": old, "Ticker": "VLUE.AX", "Slippage (bps)": 2.3,
         "Fees Actual (AUD)": 5.0, "Fee Expected (AUD)": 5.0,
         "Fee Delta (AUD)": 0.0, "Recommended": True},
    ])
    n = D._print_drift_warnings(d, pd.DataFrame(), -0.01)
    out = capsys.readouterr().out
    assert n == 0, "historical fills must not warn"
    assert "historical slippage" in out
    assert "[drift][WARN]" not in out
    assert "ASX" in out and "US" in out          # venues reported apart


def test_recent_fills_still_warn(capsys):
    import drift as D
    recent = pd.Timestamp.now() - pd.Timedelta(days=1)
    d = pd.DataFrame([
        {"Fill Date": recent, "Ticker": "SMH", "Slippage (bps)": 780.0,
         "Fees Actual (AUD)": 1.0, "Fee Expected (AUD)": 5.0,
         "Fee Delta (AUD)": -4.0, "Recommended": True},
    ])
    n = D._print_drift_warnings(d, pd.DataFrame(), -0.01)
    assert n == 1
    assert "slippage +780.0 bps" in capsys.readouterr().out


def test_non_adherence_is_judged_over_all_history(capsys):
    """A fill that never had a recommendation keeps mattering after 7 days."""
    import drift as D
    import drift as D
    old = pd.Timestamp.now() - pd.Timedelta(days=60)
    d = pd.DataFrame([
        {"Fill Date": old, "Ticker": "MYSTERY.AX", "Slippage (bps)": None,
         "Fees Actual (AUD)": None, "Fee Expected (AUD)": None,
         "Fee Delta (AUD)": None, "Recommended": False},
    ])
    n = D._print_drift_warnings(d, pd.DataFrame(), -0.01)
    out = capsys.readouterr().out
    assert n == 1
    assert "NO matching recommendation" in out and "MYSTERY.AX" in out

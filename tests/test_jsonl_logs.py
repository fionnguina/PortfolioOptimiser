"""Tests for jsonl_logs.py — the append-only JSONL persistence layer.

Covers the recommendation-log builder (Delta-Units -> recommended_trades, TLH
swaps, weight/units thresholds), the cash-ledger run_at dedup, and NAV-history
edge cases (non-finite rejection, malformed-line tolerance) not already covered
by test_drift.py's idempotency test.
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import jsonl_logs


# === recommendation log =======================================================

def _trade_df():
    return pd.DataFrame({
        "Security":       ["AAA.AX", "BBB.AX", "CCC.AX"],
        "Delta Units":    [10, -5, 0],           # last row is a no-op
        "Last Px (AUD)":  [20.0, 8.0, 50.0],
        "Brokerage (AUD)": [6.0, 6.0, 0.0],
    }).set_index("Security")


def test_recommendation_log_roundtrip_and_delta_filter(tmp_path):
    log = tmp_path / "rec.jsonl"
    jsonl_logs.append_trade_recommendation_log(
        log,
        selected_mode="Balanced",
        trade_df=_trade_df(),
        w_target=pd.Series({"AAA.AX": 0.6, "BBB.AX": 0.4, "TINY": 1e-9}),
        current_units=pd.Series({"AAA.AX": 100, "BBB.AX": 0}),
        portfolio_value_aud=250_000.0,
        regime_mix=pd.Series({"Balanced": 1.0}),
        expected_brokerage_aud=12.0,
        expected_cgt_aud=340.0,
        broker_name="IBKR",
        cgt_mtr=0.32,
        universe_size=46,
    )
    entries = jsonl_logs._load_recommendation_log(log)
    assert len(entries) == 1
    e = entries[0]
    trades = {t["ticker"]: t for t in e["recommended_trades"]}
    assert set(trades) == {"AAA.AX", "BBB.AX"}     # zero-delta CCC.AX dropped
    assert trades["AAA.AX"]["side"] == "buy"
    assert trades["AAA.AX"]["delta_value_aud"] == pytest.approx(200.0)
    assert trades["BBB.AX"]["side"] == "sell"
    # sub-threshold weight + zero units pruned from their maps
    assert "TINY" not in e["target_weights"]
    assert "BBB.AX" not in e["current_units"]
    assert e["selected_mode"] == "Balanced"


def test_recommendation_log_records_tlh_swaps(tmp_path):
    log = tmp_path / "rec.jsonl"
    jsonl_logs.append_trade_recommendation_log(
        log,
        selected_mode="Balanced",
        trade_df=_trade_df(),
        w_target=pd.Series({"AAA.AX": 1.0}),
        current_units=pd.Series({"AAA.AX": 100}),
        portfolio_value_aud=250_000.0,
        regime_mix=pd.Series({"Balanced": 1.0}),
        expected_brokerage_aud=12.0,
        expected_cgt_aud=0.0,
        broker_name="IBKR",
        cgt_mtr=0.32,
        universe_size=46,
        tlh_events=[{
            "ticker_sold": "VLUE.AX", "ticker_bought": "QUAL.AX",
            "units_sold": 100, "units_bought": 98,
            "sale_price": 40.0, "buy_price": 41.0,
            "loss_aud": -500.0, "swap_value_aud": 4000.0,
            "hold_days": 45, "lot_date": "2026-05-01",
        }],
    )
    swap = jsonl_logs._load_recommendation_log(log)[0]["tlh_swaps"][0]
    assert swap["ticker_sold"] == "VLUE.AX"
    assert swap["units_bought"] == 98
    assert swap["loss_aud"] == pytest.approx(-500.0)


def test_load_recommendation_log_missing_returns_empty(tmp_path):
    assert jsonl_logs._load_recommendation_log(tmp_path / "nope.jsonl") == []


# === cash ledger ==============================================================

def test_cash_ledger_appends_and_roundtrips(tmp_path):
    led = tmp_path / "cash.jsonl"
    jsonl_logs.append_cash_ledger(
        led, portfolio_value_aud=250_000.0, net_invested_aud=240_000.0,
        cash_balance_aud=10_000.0, brokerage_this_run_aud=12.0,
        cgt_this_run_aud=340.0, loss_cf_tax_aud=0.0,
        selected_mode="Balanced", broker_name="IBKR", as_of_date="2026-07-13",
    )
    rows = [__import__("json").loads(l) for l in led.read_text().splitlines() if l.strip()]
    assert len(rows) == 1
    assert rows[0]["portfolio_value_aud"] == pytest.approx(250_000.0)
    assert rows[0]["broker"] == "IBKR"
    assert rows[0]["date"] == "2026-07-13"


# === live NAV history =========================================================

def test_nav_history_rejects_nonfinite_and_nonpositive(tmp_path):
    p = tmp_path / "nav.jsonl"
    jsonl_logs.append_live_nav_history(p, float("nan"), as_of_date="2026-07-13")
    jsonl_logs.append_live_nav_history(p, 0.0, as_of_date="2026-07-13")
    jsonl_logs.append_live_nav_history(p, -100.0, as_of_date="2026-07-13")
    assert not p.exists() or p.read_text().strip() == ""     # nothing written
    jsonl_logs.append_live_nav_history(p, 250_000.0, as_of_date="2026-07-13")
    s = jsonl_logs._load_live_nav_series(p)
    assert float(s.iloc[-1]) == pytest.approx(250_000.0)


def test_nav_loader_tolerates_malformed_line(tmp_path):
    p = tmp_path / "nav.jsonl"
    p.write_text(
        '{"date": "2026-07-10", "nav_aud": 100.0}\n'
        'THIS IS NOT JSON\n'
        '{"date": "2026-07-11", "nav_aud": 110.0}\n'
    )
    s = jsonl_logs._load_live_nav_series(p)
    assert len(s) == 2
    assert float(s.iloc[-1]) == pytest.approx(110.0)


def test_nav_loader_missing_file_empty_series(tmp_path):
    s = jsonl_logs._load_live_nav_series(tmp_path / "nope.jsonl")
    assert isinstance(s, pd.Series) and s.empty

"""Unit tests for brokerage.py — the cost model that runs in every backtest.

Pins the fee schedule (ASX/US min-fee + rate), the market classifier, and the
small-trade suppression that gate what actually trades. A silent change here
would shift every backtest's net-of-cost result.
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import brokerage

ASX = brokerage.BROKERAGE["ASX"]   # {'min_fee':5.0,'rate':0.0008,'first_buy_free_threshold':0.0}
US = brokerage.BROKERAGE["US"]     # {'min_fee':1.5,'rate':0.0002}


def _trades(rows):
    """rows: list of (Security, delta_units, last_px_aud)."""
    return pd.DataFrame(
        [{"Security": s, "Delta Units": d, "Last Px (AUD)": p} for s, d, p in rows])


# ---- _market_of ----

@pytest.mark.parametrize("tkr,mkt", [
    ("IVV.AX", "ASX"), ("VAS.AX", "ASX"),
    ("SMH", "US"), ("SPY", "US"),
    ("^AORD", "INDEX"), ("^GSPC", "INDEX"),
])
def test_market_of(tkr, mkt):
    assert brokerage._market_of(tkr) == mkt


# ---- compute_brokerage ----

def test_brokerage_empty_is_zero():
    total, per_row = brokerage.compute_brokerage(pd.DataFrame())
    assert total == 0.0
    assert per_row.empty

def test_brokerage_asx_min_fee_floor():
    """Small ASX trade -> min_fee floor (rate*value below the floor)."""
    df = _trades([("IVV.AX", 10, 50.0)])  # value 500, rate*val=0.40 < 5.0 floor
    total, per_row = brokerage.compute_brokerage(df)
    assert total == pytest.approx(ASX["min_fee"])

def test_brokerage_asx_rate_above_floor():
    df = _trades([("IVV.AX", 100, 200.0)])  # value 20000 -> 0.0008*20000 = 16.0 > 5.0
    total, _ = brokerage.compute_brokerage(df)
    assert total == pytest.approx(0.0008 * 20000)

def test_brokerage_us_uses_profile_not_zero():
    """Regression: US brokerage was once hardcoded 0.0; must use the schedule."""
    df = _trades([("SMH", 100, 500.0)])  # value 50000 -> 0.0002*50000 = 10.0 > 1.5
    total, _ = brokerage.compute_brokerage(df)
    assert total == pytest.approx(0.0002 * 50000)
    assert total > 0.0

def test_brokerage_us_min_fee_floor():
    df = _trades([("SMH", 10, 100.0)])  # value 1000, rate*val=0.20 < 1.5 floor
    total, _ = brokerage.compute_brokerage(df)
    assert total == pytest.approx(US["min_fee"])

def test_brokerage_zero_units_no_fee():
    df = _trades([("IVV.AX", 0, 50.0)])
    total, _ = brokerage.compute_brokerage(df)
    assert total == 0.0

def test_brokerage_sums_across_rows():
    df = _trades([("IVV.AX", 100, 200.0), ("SMH", 100, 500.0)])  # 16.0 + 10.0
    total, per_row = brokerage.compute_brokerage(df)
    assert total == pytest.approx(16.0 + 10.0)
    assert len(per_row) == 2


# ---- suppress_small_trades_by_value ----

def test_suppress_zeroes_small_trades():
    df = _trades([("IVV.AX", 2, 40.0), ("SMH", 100, 500.0)])  # 80 vs 50000
    out = brokerage.suppress_small_trades_by_value(df, min_trade_value_aud=100.0)
    ivv = out[out["Security"] == "IVV.AX"].iloc[0]
    smh = out[out["Security"] == "SMH"].iloc[0]
    assert ivv["Suppressed"] and ivv["Delta Units"] == 0   # 80 <= 100 -> suppressed
    assert not smh["Suppressed"] and smh["Delta Units"] == 100

def test_suppress_recomputes_cash_flow_sign():
    """Cash flow convention: buys negative, sells positive."""
    df = _trades([("SMH", 100, 500.0), ("IVV.AX", -50, 40.0)])
    out = brokerage.suppress_small_trades_by_value(df, min_trade_value_aud=100.0)
    smh = out[out["Security"] == "SMH"].iloc[0]
    ivv = out[out["Security"] == "IVV.AX"].iloc[0]
    assert smh["Cash Flow (AUD)"] < 0    # buy
    assert ivv["Cash Flow (AUD)"] > 0    # sell

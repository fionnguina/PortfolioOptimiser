"""Unit tests for the data-layer modules: fx.py, lots.py, nav.py.

- fx: USD/AUD conversion + the loud-fallback contract.
- lots: CGT-naive lot construction from current holdings.
- nav: broker-truth NAV parsing + reconstruction/splice.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import fx
import lots
import nav


# ============================================================================
# fx.py
# ============================================================================

def test_fx_to_aud_maps_by_suffix():
    out = fx.fx_to_aud_for_tickers(["IVV.AX", "SMH", "^AORD", "QQQ"], usd_aud_rate=1.5)
    assert out["IVV.AX"] == 1.0   # AU-listed
    assert out["^AORD"] == 1.0    # index
    assert out["SMH"] == 1.5      # US
    assert out["QQQ"] == 1.5

def test_get_usd_aud_fx_reads_last_valid():
    saved = fx.fx_usdaud
    try:
        idx = pd.date_range("2026-07-01", periods=3)
        fx.fx_usdaud = pd.Series([1.48, 1.49, 1.505], index=idx)
        assert fx.get_usd_aud_fx() == pytest.approx(1.505)
    finally:
        fx.fx_usdaud = saved

def test_get_usd_aud_fx_falls_back_loudly(capsys):
    saved = fx.fx_usdaud
    try:
        fx.fx_usdaud = None
        rate = fx.get_usd_aud_fx(default=1.42)
        assert rate == 1.42
        assert "CRITICAL" in capsys.readouterr().out  # must log loudly, never silent
    finally:
        fx.fx_usdaud = saved


# ============================================================================
# lots.py — _build_lots_from_holdings
# ============================================================================

def test_build_lots_from_holdings_one_lot_per_ticker():
    units = pd.Series({"IVV.AX": 100.0, "SMH": 50.0})
    px = pd.Series({"IVV.AX": 40.0, "SMH": 200.0})
    today = pd.Timestamp("2026-07-10")
    df = lots._build_lots_from_holdings(units, px, today=today)
    assert set(df["Security"]) == {"IVV.AX", "SMH"}
    ivv = df[df["Security"] == "IVV.AX"].iloc[0]
    assert ivv["Units"] == pytest.approx(100.0)
    assert ivv["CostBaseAUD"] == pytest.approx(40.0)  # PER-UNIT cost basis (AUD)
    assert pd.Timestamp(ivv["AcqDate"]).normalize() == today

def test_build_lots_from_holdings_skips_zero_units():
    units = pd.Series({"IVV.AX": 0.0, "SMH": 10.0})
    px = pd.Series({"IVV.AX": 40.0, "SMH": 200.0})
    df = lots._build_lots_from_holdings(units, px)
    assert "IVV.AX" not in set(df["Security"])
    assert "SMH" in set(df["Security"])


def test_expand_with_lots_matches_sells_to_parcels():
    """Regression: lots.py must import _trade_delta_col/_security_from_row from
    cgt (the extraction dropped them, breaking the CSV-export fallback)."""
    trades = pd.DataFrame([{"Security": "IVV.AX", "Delta Units": -60,
                            "Last Px (AUD)": 50.0}])
    lotsdf = pd.DataFrame([{"Security": "IVV.AX", "AcqDate": "2024-01-01",
                            "Units": 100, "CostBaseAUD": 40.0}])
    out = lots.expand_with_lots(trades, lotsdf, pd.Timestamp("2026-07-13"), method="FIFO")
    assert not out.empty
    assert int(out.iloc[0]["UnitsSold"]) == 60  # sold 60 of the 100-unit lot

def test_expand_with_lots_empty_trades():
    out = lots.expand_with_lots(pd.DataFrame(), pd.DataFrame(),
                                pd.Timestamp("2026-07-13"))
    assert out.empty


# ============================================================================
# nav.py — broker NAV parsing + reconstruction/splice
# ============================================================================

def test_load_broker_nav_series_last_per_day(tmp_path):
    p = tmp_path / "ibkr_nav_log.jsonl"
    p.write_text(
        json.dumps({"ts": "2026-07-08T09:30:00", "net_liquidation_aud": 247000.0}) + "\n"
        + json.dumps({"ts": "2026-07-08T15:00:00", "net_liquidation_aud": 248500.0}) + "\n"
        + json.dumps({"ts": "2026-07-09T09:30:00", "net_liquidation_aud": 250000.0}) + "\n",
        encoding="utf-8")
    s = nav._load_broker_nav_series(p)
    assert len(s) == 2  # two distinct days
    # last snapshot per day wins
    assert s.loc[pd.Timestamp("2026-07-08")] == pytest.approx(248500.0)
    assert s.loc[pd.Timestamp("2026-07-09")] == pytest.approx(250000.0)

def test_load_broker_nav_series_missing_file_returns_empty(tmp_path):
    s = nav._load_broker_nav_series(tmp_path / "nope.jsonl")
    assert s.empty

def test_actual_nav_reconstruction_from_seed(tmp_path):
    """Seed one lot of 100 units; NAV = units * price over the window."""
    seed = tmp_path / "lots_seed.json"
    seed.write_text(json.dumps([
        {"Security": "IVV.AX", "AcqDate": "2026-07-01", "Units": 100},
    ]), encoding="utf-8")
    fills = tmp_path / "fills.jsonl"  # no fills
    fills.write_text("", encoding="utf-8")
    idx = pd.date_range("2026-07-01", periods=5)
    prices = pd.DataFrame({"IVV.AX": [40, 41, 42, 41, 43]}, index=idx)
    s = nav.compute_actual_nav_series(prices, fills, seed)
    assert not s.empty
    assert s.iloc[0] == pytest.approx(100 * 40)
    assert s.iloc[-1] == pytest.approx(100 * 43)

def test_spliced_nav_returns_recon_when_broker_sparse(tmp_path):
    """< 2 broker snapshots -> splice falls back to the reconstruction path."""
    seed = tmp_path / "lots_seed.json"
    seed.write_text(json.dumps([{"Security": "IVV.AX", "AcqDate": "2026-07-01", "Units": 100}]),
                    encoding="utf-8")
    fills = tmp_path / "fills.jsonl"; fills.write_text("", encoding="utf-8")
    broker = tmp_path / "nav.jsonl"  # empty broker log
    broker.write_text("", encoding="utf-8")
    idx = pd.date_range("2026-07-01", periods=4)
    prices = pd.DataFrame({"IVV.AX": [40, 41, 42, 43]}, index=idx)
    recon = nav.compute_actual_nav_series(prices, fills, seed)
    spliced = nav.compute_actual_nav_series_spliced(prices, fills, seed, broker_nav_path=broker)
    pd.testing.assert_series_equal(spliced, recon)

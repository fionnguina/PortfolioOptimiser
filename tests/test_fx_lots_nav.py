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


# ============================================================================
# fx.price_local_to_aud — the live-TLH currency conversion (extracted from the
# monolith closure so it's testable). Was the AUD-cost-vs-LOCAL-price bug.
# ============================================================================

def test_price_local_to_aud_us_ticker_converts():
    # SMH quoted in USD → multiply by the USD→AUD rate.
    assert fx.price_local_to_aud("SMH", 606.073604, {"SMH": 1.4312}) == pytest.approx(
        606.073604 * 1.4312)


def test_price_local_to_aud_ax_is_identity():
    # .AX already AUD → rate 1.0 even if fx_map has no entry.
    assert fx.price_local_to_aud("VLUE.AX", 36.63, {}) == pytest.approx(36.63)


def test_price_local_to_aud_index_caret_is_identity():
    assert fx.price_local_to_aud("^AORD", 8000.0, {}) == pytest.approx(8000.0)


def test_price_local_to_aud_unknown_us_rate_returns_none():
    # The load-bearing guard: unknown rate for a NON-AUD ticker → None
    # (exclude from TLH) rather than mis-pricing and fabricating a loss.
    assert fx.price_local_to_aud("SMH", 606.0, {}) is None
    assert fx.price_local_to_aud("SMH", 606.0, {"SMH": 0.0}) is None      # non-positive
    assert fx.price_local_to_aud("SMH", 606.0, {"SMH": float("nan")}) is None
    assert fx.price_local_to_aud("SMH", 606.0, {"SMH": float("inf")}) is None


def test_price_local_to_aud_bad_price_returns_none():
    assert fx.price_local_to_aud("SMH", None, {"SMH": 1.43}) is None


# ============================================================================
# nav.load_broker_positions + lots.reconcile_lots_vs_broker
#
# Regression cover for the 2026-07-08 SOXL/VEA miss: ibkr_paper_exec.py exited
# while both orders were PreSubmitted, so the fills log froze them at
# qty_filled=0 and the lot book kept SOXL / never acquired VEA.
# ============================================================================

def _nav_row(ts, positions, netliq=244439.66):
    return json.dumps({"ts": ts, "net_liquidation_aud": netliq,
                       "positions": positions}) + "\n"


def test_load_broker_positions_latest_row_wins(tmp_path):
    p = tmp_path / "ibkr_nav_log.jsonl"
    p.write_text(
        _nav_row("2026-07-08T11:09:11", [{"ticker": "SOXL", "units": 12.0,
                                          "avg_cost_local": 197.0, "currency": "USD"}])
        + _nav_row("2026-07-17T09:32:54", [{"ticker": "VEA", "units": 22.0,
                                            "avg_cost_local": 69.885459, "currency": "USD"}]),
        encoding="utf-8")
    out = nav.load_broker_positions(p)
    # Most recent snapshot wins outright (point-in-time, not a per-day series).
    assert "SOXL" not in out
    assert out["VEA"]["units"] == pytest.approx(22.0)
    assert out["VEA"]["avg_cost_local"] == pytest.approx(69.885459)
    assert out["_ts"] == "2026-07-17T09:32:54"


def test_load_broker_positions_missing_avg_cost_is_none(tmp_path):
    """Rows written before 2026-07-17 have no avg_cost_local field."""
    p = tmp_path / "ibkr_nav_log.jsonl"
    p.write_text(_nav_row("2026-07-08T11:09:11",
                          [{"ticker": "SMH", "units": 50.0}]), encoding="utf-8")
    out = nav.load_broker_positions(p)
    assert out["SMH"]["avg_cost_local"] is None
    assert out["SMH"]["units"] == pytest.approx(50.0)


def test_load_broker_positions_missing_file_returns_empty(tmp_path):
    assert nav.load_broker_positions(tmp_path / "nope.jsonl") == {}


def test_load_broker_positions_ignores_rows_without_positions(tmp_path):
    p = tmp_path / "ibkr_nav_log.jsonl"
    p.write_text(
        _nav_row("2026-07-08T11:09:11", [{"ticker": "VEA", "units": 22.0,
                                          "avg_cost_local": 69.88, "currency": "USD"}])
        + json.dumps({"ts": "2026-07-09T09:30:00", "net_liquidation_aud": 250000.0}) + "\n",
        encoding="utf-8")
    out = nav.load_broker_positions(p)
    assert out["VEA"]["units"] == pytest.approx(22.0)  # NAV-only row didn't clobber


def _book(rows):
    return pd.DataFrame(rows, columns=["Security", "AcqDate", "Units", "CostBaseAUD"])


def test_reconcile_clean_book_no_warnings():
    book = _book([["VLUE.AX", pd.Timestamp("2026-06-01"), 2657, 37.182692]])
    broker = {"VLUE.AX": {"units": 2657.0, "avg_cost_local": 37.182692,
                          "currency": "AUD"}, "_ts": "2026-07-17T09:32:54"}
    assert lots.reconcile_lots_vs_broker(book, broker, fx_map={}) == []


def test_reconcile_catches_the_soxl_vea_miss():
    """THE regression case: a qty_filled=0 SELL leaves SOXL in the book, and a
    qty_filled=0 BUY means VEA never enters it."""
    book = _book([["SOXL", pd.Timestamp("2026-06-01"), 12, 197.0]])
    broker = {"VEA": {"units": 22.0, "avg_cost_local": 69.885459, "currency": "USD"},
              "_ts": "2026-07-17T09:32:54"}
    warns = lots.reconcile_lots_vs_broker(book, broker, fx_map={"VEA": 1.55})
    joined = " | ".join(warns)
    assert len(warns) == 2
    assert "SOXL" in joined and "12" in joined   # book holds what broker sold
    assert "VEA" in joined and "22" in joined    # broker holds what book missed


def test_reconcile_converts_usd_avg_cost_with_same_fx_map():
    """A US ticker must NOT warn just because the broker quotes USD and the
    lot book stores AUD."""
    fx = 1.55
    book = _book([["SMH", pd.Timestamp("2026-06-01"), 50, 606.073604 * fx]])
    broker = {"SMH": {"units": 50.0, "avg_cost_local": 606.073604, "currency": "USD"}}
    assert lots.reconcile_lots_vs_broker(book, broker, fx_map={"SMH": fx}) == []


def test_reconcile_flags_cost_basis_divergence():
    fx = 1.55
    book = _book([["SMH", pd.Timestamp("2026-06-01"), 50, 606.073604 * fx * 1.02]])
    broker = {"SMH": {"units": 50.0, "avg_cost_local": 606.073604, "currency": "USD"}}
    warns = lots.reconcile_lots_vs_broker(book, broker, fx_map={"SMH": fx})
    assert len(warns) == 1
    assert "avg cost" in warns[0] and "+200bps" in warns[0]


def test_reconcile_units_weighted_average_across_lots():
    """Two lots of the same ticker average by units, not naively."""
    book = _book([["IVV.AX", pd.Timestamp("2026-05-01"), 100, 40.0],
                  ["IVV.AX", pd.Timestamp("2026-06-01"), 300, 60.0]])
    # units-weighted = (100*40 + 300*60) / 400 = 55.0; naive mean would be 50.0
    broker = {"IVV.AX": {"units": 400.0, "avg_cost_local": 55.0, "currency": "AUD"}}
    assert lots.reconcile_lots_vs_broker(book, broker, fx_map={}) == []


def test_reconcile_skips_cost_when_avg_cost_absent():
    """Pre-2026-07-17 snapshots have no avg_cost_local — units still checked,
    cost silently skipped rather than warned on."""
    book = _book([["VLUE.AX", pd.Timestamp("2026-06-01"), 2657, 99.0]])
    broker = {"VLUE.AX": {"units": 2657.0, "avg_cost_local": None, "currency": "AUD"}}
    assert lots.reconcile_lots_vs_broker(book, broker, fx_map={}) == []


def test_reconcile_empty_broker_is_noop():
    book = _book([["VLUE.AX", pd.Timestamp("2026-06-01"), 2657, 37.18]])
    assert lots.reconcile_lots_vs_broker(book, {}, fx_map={}) == []
    assert lots.reconcile_lots_vs_broker(book, None, fx_map={}) == []


# ============================================================================
# Seed watershed (SeedAsOf) — stops a back-filled historical fill from being
# replayed on top of a seed that already reflects it.
# ============================================================================

def _seed_file(tmp_path, lots_list):
    p = tmp_path / "lots_seed.json"
    p.write_text(json.dumps(lots_list), encoding="utf-8")
    return p


def _fill_row(ticker, side, qty, ts, px=10.0):
    return json.dumps({"ticker": ticker, "side": side, "qty_filled": qty,
                       "exec_timestamp": ts, "avg_fill_price_local": px,
                       "rec_px_aud": px}) + "\n"


def test_watershed_skips_fill_at_or_before_seed(tmp_path):
    """THE double-count guard: a 2026-07-08 BUY back-filled into the log must
    NOT be replayed onto a seed taken 2026-07-17 that already includes it."""
    seed = _seed_file(tmp_path, [{"Security": "VEA", "AcqDate": "2026-07-08T00:00:00",
                                  "Units": 22, "CostBaseAUD": 100.0,
                                  "SeedAsOf": "2026-07-17T12:13:50"}])
    fills = tmp_path / "fills.jsonl"
    fills.write_text(_fill_row("VEA", "BUY", 22, "2026-07-08T11:09:11", px=69.88),
                     encoding="utf-8")
    out = lots._build_lots_from_fills_log(fills, fx_map={"VEA": 1.4312},
                                          lot_match_method="FIFO", seed_path=seed)
    # Seed lot only -- the pre-seed fill was skipped, not stacked on top.
    assert out["Units"].sum() == 22
    assert len(out) == 1
    assert out.attrs["pre_seed_fills_skipped"] == 1


def test_watershed_applies_fill_after_seed(tmp_path):
    """A genuinely new fill after the watershed must still be applied."""
    seed = _seed_file(tmp_path, [{"Security": "VEA", "AcqDate": "2026-07-08T00:00:00",
                                  "Units": 22, "CostBaseAUD": 100.0,
                                  "SeedAsOf": "2026-07-17T12:13:50"}])
    fills = tmp_path / "fills.jsonl"
    fills.write_text(_fill_row("VEA", "BUY", 10, "2026-07-20T09:35:00", px=70.0),
                     encoding="utf-8")
    out = lots._build_lots_from_fills_log(fills, fx_map={"VEA": 1.4312},
                                          lot_match_method="FIFO", seed_path=seed)
    assert out["Units"].sum() == 32  # 22 seed + 10 new
    assert out.attrs["pre_seed_fills_skipped"] == 0


def test_watershed_absent_preserves_legacy_behaviour(tmp_path):
    """Legacy seeds have no SeedAsOf -> no filtering, exactly as before."""
    seed = _seed_file(tmp_path, [{"Security": "VEA", "AcqDate": "2026-07-08T00:00:00",
                                  "Units": 22, "CostBaseAUD": 100.0}])
    fills = tmp_path / "fills.jsonl"
    fills.write_text(_fill_row("VEA", "BUY", 22, "2026-07-08T11:09:11", px=69.88),
                     encoding="utf-8")
    out = lots._build_lots_from_fills_log(fills, fx_map={"VEA": 1.4312},
                                          lot_match_method="FIFO", seed_path=seed)
    assert out["Units"].sum() == 44  # double-counted -- the old behaviour
    assert out.attrs["seed_as_of"] is None


def test_watershed_skips_undated_fill(tmp_path):
    """A fill with no timestamp can't be placed either side of the watershed;
    with a seed present it must be dropped, not blindly applied."""
    seed = _seed_file(tmp_path, [{"Security": "VEA", "AcqDate": "2026-07-08T00:00:00",
                                  "Units": 22, "CostBaseAUD": 100.0,
                                  "SeedAsOf": "2026-07-17T12:13:50"}])
    fills = tmp_path / "fills.jsonl"
    fills.write_text(json.dumps({"ticker": "VEA", "side": "BUY", "qty_filled": 5,
                                 "avg_fill_price_local": 70.0}) + "\n", encoding="utf-8")
    out = lots._build_lots_from_fills_log(fills, fx_map={"VEA": 1.4312},
                                          lot_match_method="FIFO", seed_path=seed)
    assert out["Units"].sum() == 22
    assert out.attrs["pre_seed_fills_skipped"] == 1


def test_watershed_sell_after_seed_decrements(tmp_path):
    """A post-watershed SELL must still decrement the seed lot."""
    seed = _seed_file(tmp_path, [{"Security": "SOXL", "AcqDate": "2026-07-08T00:00:00",
                                  "Units": 12, "CostBaseAUD": 197.0,
                                  "SeedAsOf": "2026-07-17T12:13:50"}])
    fills = tmp_path / "fills.jsonl"
    fills.write_text(_fill_row("SOXL", "SELL", 12, "2026-07-21T10:00:00"),
                     encoding="utf-8")
    out = lots._build_lots_from_fills_log(fills, fx_map={"SOXL": 1.4312},
                                          lot_match_method="FIFO", seed_path=seed)
    assert out.empty or out["Units"].sum() == 0


def test_watershed_uses_max_across_seed_lots(tmp_path):
    seed = _seed_file(tmp_path, [
        {"Security": "VEA", "AcqDate": "2026-07-08T00:00:00", "Units": 22,
         "CostBaseAUD": 100.0, "SeedAsOf": "2026-07-15T00:00:00"},
        {"Security": "SMH", "AcqDate": "2026-07-08T00:00:00", "Units": 50,
         "CostBaseAUD": 867.0, "SeedAsOf": "2026-07-17T12:13:50"},
    ])
    fills = tmp_path / "fills.jsonl"
    fills.write_text(_fill_row("VEA", "BUY", 5, "2026-07-16T09:00:00", px=70.0),
                     encoding="utf-8")
    out = lots._build_lots_from_fills_log(fills, fx_map={"VEA": 1.4312},
                                          lot_match_method="FIFO", seed_path=seed)
    # 07-16 is before the LATEST watershed (07-17) -> skipped.
    assert out.attrs["seed_as_of"] == pd.Timestamp("2026-07-17T12:13:50")
    assert out["Units"].sum() == 72  # 22 + 50, no new lot


def test_watershed_tz_aware_seed_does_not_raise(tmp_path):
    """SeedAsOf written with a tz offset must not blow up against naive
    exec_timestamps."""
    seed = _seed_file(tmp_path, [{"Security": "VEA", "AcqDate": "2026-07-08T00:00:00",
                                  "Units": 22, "CostBaseAUD": 100.0,
                                  "SeedAsOf": "2026-07-17T12:13:50+10:00"}])
    fills = tmp_path / "fills.jsonl"
    fills.write_text(_fill_row("VEA", "BUY", 22, "2026-07-08T11:09:11", px=69.88),
                     encoding="utf-8")
    out = lots._build_lots_from_fills_log(fills, fx_map={"VEA": 1.4312},
                                          lot_match_method="FIFO", seed_path=seed)
    assert out["Units"].sum() == 22
    assert out.attrs["seed_as_of"].tzinfo is None


def test_reconcile_units_mismatch_suppresses_cost_warning():
    """One break per ticker: if units disagree, the cost comparison is
    meaningless and must not double-report."""
    book = _book([["SMH", pd.Timestamp("2026-06-01"), 10, 1.0]])
    broker = {"SMH": {"units": 50.0, "avg_cost_local": 606.07, "currency": "USD"}}
    warns = lots.reconcile_lots_vs_broker(book, broker, fx_map={"SMH": 1.55})
    assert len(warns) == 1
    assert "units" in warns[0]

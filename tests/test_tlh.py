"""Unit tests for tlh.py — the tax-loss-harvesting pass (shipped ~+1%/yr feature).

Pins the swap logic: a loss lot past threshold with a valid, non-cooled
substitute gets sold + swapped; gains, missing pairs, and cooldown violations
(wash-swap guard) do not. Also the lot-book builder + cooldown-state persistence.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import tlh
from cgt import LotBook, CGT_CONFIG


def _lotbook(rows):
    """rows: (ticker, units, acq_date, cost_basis_per_unit)."""
    df = pd.DataFrame(
        [{"Security": t, "Units": u, "AcqDate": d, "CostBaseAUD": c} for t, u, d, c in rows])
    return tlh._build_lot_book_from_df(df)


# ---- _build_lot_book_from_df ----

def test_build_lot_book_holds_valid_rows():
    lb = _lotbook([("IVV.AX", 100, "2024-01-01", 40.0),
                   ("SMH", 50, "2024-06-01", 200.0)])
    assert lb.units("IVV.AX") == pytest.approx(100)
    assert lb.units("SMH") == pytest.approx(50)

def test_build_lot_book_skips_invalid():
    lb = _lotbook([("IVV.AX", 0, "2024-01-01", 40.0),      # zero units
                   ("", 100, "2024-01-01", 40.0),           # blank ticker
                   ("SMH", 50, "2024-06-01", 0.0)])         # zero cost basis
    assert lb.units("IVV.AX") == pytest.approx(0)
    assert lb.units("SMH") == pytest.approx(0)


# ---- cooldown state persistence ----

def test_cooldown_state_roundtrip(tmp_path):
    p = tmp_path / "cooldown.json"
    state = {"IVV.AX": "2026-07-01T00:00:00"}
    tlh._save_tlh_cooldown_state(p, state)
    loaded = tlh._load_tlh_cooldown_state(p)
    # loader parses ISO strings back into Timestamps for direct date math
    assert pd.Timestamp(loaded.get("IVV.AX")) == pd.Timestamp("2026-07-01")

def test_cooldown_state_missing_file_is_empty(tmp_path):
    assert tlh._load_tlh_cooldown_state(tmp_path / "nope.json") == {}


# ---- _run_tlh_pass ----

def _loss_setup():
    """IVV.AX bought a year ago at 50; now 40 (-20%, -1000 AUD loss)."""
    as_of = pd.Timestamp("2026-07-10")
    lb = _lotbook([("IVV.AX", 100, "2025-06-01", 50.0)])
    prices = {"IVV.AX": 40.0, "VAS.AX": 40.0}
    pairs = {"IVV.AX": "VAS.AX"}
    return lb, prices, pairs, as_of

def test_tlh_harvests_loss_and_swaps():
    lb, prices, pairs, as_of = _loss_setup()
    cooldown = {}
    res = tlh._run_tlh_pass(lb, prices, as_of, cooldown, pairs,
                            min_loss_pct=-0.05, min_loss_aud=100.0,
                            cooldown_days=31, cfg=CGT_CONFIG)
    assert res["n_events"] == 1
    ev = res["events"][0]
    assert ev["ticker_sold"] == "IVV.AX" and ev["ticker_bought"] == "VAS.AX"
    assert res["total_loss_aud"] > 0            # positive magnitude of loss
    # book swapped: IVV.AX gone, VAS.AX now held (~100 units @ 40)
    assert lb.units("IVV.AX") == pytest.approx(0)
    assert lb.units("VAS.AX") == pytest.approx(100, rel=1e-6)
    # ticker sold recorded in cooldown for the wash-swap guard
    assert "IVV.AX" in cooldown

def test_tlh_skips_gains():
    """A lot in gain is never harvested."""
    as_of = pd.Timestamp("2026-07-10")
    lb = _lotbook([("IVV.AX", 100, "2025-06-01", 40.0)])  # cost 40, now 50 = gain
    res = tlh._run_tlh_pass(lb, {"IVV.AX": 50.0, "VAS.AX": 50.0}, as_of, {},
                            {"IVV.AX": "VAS.AX"}, min_loss_pct=-0.05,
                            min_loss_aud=100.0, cooldown_days=31, cfg=CGT_CONFIG)
    assert res["n_events"] == 0

def test_tlh_skips_when_no_substitute():
    lb, prices, _, as_of = _loss_setup()
    res = tlh._run_tlh_pass(lb, prices, as_of, {}, pairs={},  # no pairs
                            min_loss_pct=-0.05, min_loss_aud=100.0,
                            cooldown_days=31, cfg=CGT_CONFIG)
    assert res["n_events"] == 0

def test_tlh_respects_substitute_cooldown():
    """If the substitute was recently TLH-sold, buying it back is a wash-swap -> skip."""
    lb, prices, pairs, as_of = _loss_setup()
    cooldown = {"VAS.AX": (as_of - pd.Timedelta(days=5)).isoformat()}  # sold 5d ago < 31
    res = tlh._run_tlh_pass(lb, prices, as_of, cooldown, pairs,
                            min_loss_pct=-0.05, min_loss_aud=100.0,
                            cooldown_days=31, cfg=CGT_CONFIG)
    assert res["n_events"] == 0
    assert lb.units("IVV.AX") == pytest.approx(100)  # untouched

def test_tlh_skips_loss_below_min_aud():
    """Loss beyond the pct threshold but below the absolute $ floor -> skip."""
    as_of = pd.Timestamp("2026-07-10")
    lb = _lotbook([("IVV.AX", 1, "2025-06-01", 50.0)])  # 1 unit: -10 AUD loss
    res = tlh._run_tlh_pass(lb, {"IVV.AX": 40.0, "VAS.AX": 40.0}, as_of, {},
                            {"IVV.AX": "VAS.AX"}, min_loss_pct=-0.05,
                            min_loss_aud=100.0, cooldown_days=31, cfg=CGT_CONFIG)
    assert res["n_events"] == 0

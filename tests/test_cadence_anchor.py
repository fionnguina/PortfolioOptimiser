"""Live rebalance-cadence anchor (nav.last_position_change_date).

The 6-week cadence gate must anchor off when the broker book ACTUALLY moved, not
off fills-log qty_filled (permanently 0 — TWS serves no execution history). Units
change only on a trade, so a unit change between consecutive NAV snapshots is the
execution timing. Critically, a price/mark move with unchanged units must NOT be
read as a rebalance (no false anchor), or the 6W gate would never hold.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path as _Path

import pandas as pd

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import nav


def _write(tmp_path, snaps):
    p = tmp_path / "ibkr_nav_log.jsonl"
    p.write_text("\n".join(json.dumps(s) for s in snaps), encoding="utf-8")
    return p


def _snap(ts, units: dict, mark=100.0):
    return {"ts": ts, "positions": [
        {"ticker": t, "units": u, "mark_local": mark} for t, u in units.items()]}


def test_missing_file_returns_none(tmp_path):
    assert nav.last_position_change_date(tmp_path / "nope.jsonl") is None

def test_single_snapshot_returns_none(tmp_path):
    p = _write(tmp_path, [_snap("2026-07-28T09:33:00+10:00", {"SMH": 50})])
    assert nav.last_position_change_date(p) is None

def test_unchanged_units_returns_none(tmp_path):
    # two snapshots, identical units → no rebalance
    p = _write(tmp_path, [
        _snap("2026-07-28T09:33:00+10:00", {"SMH": 50, "VLUE.AX": 3374}),
        _snap("2026-07-29T09:33:00+10:00", {"SMH": 50, "VLUE.AX": 3374}),
    ])
    assert nav.last_position_change_date(p) is None

def test_price_move_same_units_is_not_a_change(tmp_path):
    # mark_local moves but units are identical → must NOT anchor (the key property)
    p = _write(tmp_path, [
        _snap("2026-07-28T09:33:00+10:00", {"SMH": 50}, mark=560.0),
        _snap("2026-07-29T09:33:00+10:00", {"SMH": 50}, mark=610.0),
    ])
    assert nav.last_position_change_date(p) is None

def test_units_change_returns_that_date(tmp_path):
    p = _write(tmp_path, [
        _snap("2026-07-28T09:33:00+10:00", {"SMH": 50}),
        _snap("2026-07-30T09:33:00+10:00", {"SMH": 90}),   # bought 40
    ])
    assert nav.last_position_change_date(p) == pd.Timestamp("2026-07-30")

def test_returns_most_recent_change_not_latest_snapshot(tmp_path):
    # change on 07-29, then stable 07-30/07-31 → anchor is 07-29, not 07-31
    p = _write(tmp_path, [
        _snap("2026-07-28T09:33:00+10:00", {"SMH": 50}),
        _snap("2026-07-29T09:33:00+10:00", {"SMH": 90}),   # changed
        _snap("2026-07-30T09:33:00+10:00", {"SMH": 90}),   # stable
        _snap("2026-07-31T09:33:00+10:00", {"SMH": 90}),   # stable
    ])
    assert nav.last_position_change_date(p) == pd.Timestamp("2026-07-29")

def test_closed_position_counts_as_change(tmp_path):
    # SOXX fully sold (drops out of the map) is a real book move
    p = _write(tmp_path, [
        _snap("2026-07-27T09:33:00+10:00", {"SMH": 50, "SOXX": 53}),
        _snap("2026-07-28T09:33:00+10:00", {"SMH": 50}),   # SOXX gone
    ])
    assert nav.last_position_change_date(p) == pd.Timestamp("2026-07-28")

def test_out_of_order_lines_are_sorted(tmp_path):
    # append order shouldn't matter — sorted by ts
    p = _write(tmp_path, [
        _snap("2026-07-30T09:33:00+10:00", {"SMH": 90}),
        _snap("2026-07-28T09:33:00+10:00", {"SMH": 50}),
    ])
    assert nav.last_position_change_date(p) == pd.Timestamp("2026-07-30")

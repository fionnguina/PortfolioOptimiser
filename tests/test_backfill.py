"""Tests for the --check-fills --write back-fill path (ibkr_paper_exec.py) and
its end-to-end effect on the lot book.

Regression cover for two bugs found reviewing today's back-fill feature:
  A. batch collapse — a back-fill row's exec_timestamp (real fill time) becoming
     the max and dropping every other outstanding order from the next check.
  B. duplicate append — a second --check-fills --write re-detecting the original
     qty_filled=0 row and appending a SECOND correction (double-counting the lot).
Both are handled by _plan_check_fills_batch (excludes back-fill rows from batch
selection; returns the already-backfilled identity set).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path as _Path

import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import ibkr_paper_exec as ipe
import lots


def _submit(ticker, side, qty, submit_ts):
    """An original submission row as _write_fills_log would write it: the whole
    batch shares one exec_timestamp, qty_filled=0 while PreSubmitted."""
    return {"ticker": ticker, "side": side, "qty_requested": qty, "qty_filled": 0,
            "status_final": "PreSubmitted", "order_id": 0,
            "exec_timestamp": submit_ts}


def _backfill(ticker, side, qty, fill_ts, px):
    """A correction row as _build_backfill_row produces: real fill time in
    exec_timestamp, qty_filled>0, stamped backfill_source."""
    return {"ticker": ticker, "side": side, "qty_requested": qty, "qty_filled": qty,
            "status_final": "Filled", "order_id": 0,
            "exec_timestamp": fill_ts, "avg_fill_price_local": px,
            "backfill_source": "check-fills"}


# === _plan_check_fills_batch =================================================

def test_batch_excludes_backfill_rows_no_collapse():
    """A back-fill row whose fill time POST-DATES the submission must not become
    the batch — the other outstanding orders stay in the check."""
    rows = [
        _submit("VEA", "BUY", 22, "2026-07-07T22:00:00"),
        _submit("SOXL", "SELL", 12, "2026-07-07T22:00:00"),
        _submit("SMH", "SELL", 85, "2026-07-07T22:00:00"),
        # correction for VEA, filled overnight — LATER exec_timestamp
        _backfill("VEA", "BUY", 22, "2026-07-08T11:09:11", 69.88),
    ]
    latest_ts, batch, already = ipe._plan_check_fills_batch(rows)
    assert latest_ts == "2026-07-07T22:00:00"           # submission time, not the fill
    assert len(batch) == 3                               # all three, not collapsed to 1
    assert {r["ticker"] for r in batch} == {"VEA", "SOXL", "SMH"}


def test_already_backfilled_identity_set():
    rows = [
        _submit("VEA", "BUY", 22, "2026-07-07T22:00:00"),
        _backfill("VEA", "BUY", 22, "2026-07-08T11:09:11", 69.88),
    ]
    _, _, already = ipe._plan_check_fills_batch(rows)
    assert ("VEA", "BUY", 22) in already                 # so a 2nd --write skips it


def test_batch_only_backfill_rows_returns_empty():
    rows = [_backfill("VEA", "BUY", 22, "2026-07-08T11:09:11", 69.88)]
    latest_ts, batch, already = ipe._plan_check_fills_batch(rows)
    assert batch == [] and latest_ts is None


def test_batch_picks_most_recent_submission():
    rows = [
        _submit("OLD", "BUY", 1, "2026-07-01T09:00:00"),
        _submit("NEW", "BUY", 2, "2026-07-08T09:00:00"),
    ]
    latest_ts, batch, _ = ipe._plan_check_fills_batch(rows)
    assert latest_ts == "2026-07-08T09:00:00"
    assert {r["ticker"] for r in batch} == {"NEW"}


# === end-to-end: the lot book counts a back-filled fill exactly once =========

def _write_log(tmp_path, rows):
    p = tmp_path / "ibkr_fills_log.jsonl"
    p.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    return p


def test_lot_book_counts_backfilled_fill_once(tmp_path):
    """Original qty_filled=0 row + one correction → the fill is counted ONCE
    (the stale row contributes nothing; builder skips qty_filled<=0)."""
    log = _write_log(tmp_path, [
        _submit("VEA", "BUY", 22, "2026-07-07T22:00:00"),
        _backfill("VEA", "BUY", 22, "2026-07-08T11:09:11", 69.88),
    ])
    book = lots._build_lots_from_fills_log(log, fx_map={"VEA": 1.4312},
                                           lot_match_method="FIFO", seed_path=None)
    vea = book[book["Security"] == "VEA"]
    assert vea["Units"].sum() == 22                       # once, not 0, not 44
    # AcqDate is the real fill time, not the submission time
    assert str(vea["AcqDate"].iloc[0]).startswith("2026-07-08")


def test_lot_book_no_double_count_if_two_corrections_slip_in(tmp_path):
    """Belt-and-braces: even if two identical corrections somehow reach the log,
    the seed-watershed / idempotency is the guard — here with no seed the two
    WOULD stack, proving why the _already_backfilled guard in the writer matters
    (this test documents the failure mode the guard prevents)."""
    log = _write_log(tmp_path, [
        _submit("VEA", "BUY", 22, "2026-07-07T22:00:00"),
        _backfill("VEA", "BUY", 22, "2026-07-08T11:09:11", 69.88),
        _backfill("VEA", "BUY", 22, "2026-07-08T11:09:11", 69.88),
    ])
    book = lots._build_lots_from_fills_log(log, fx_map={"VEA": 1.4312},
                                           lot_match_method="FIFO", seed_path=None)
    # Two corrections DO double-count — which is exactly why the writer must not
    # append a second one (see test_already_backfilled_identity_set).
    assert book[book["Security"] == "VEA"]["Units"].sum() == 44

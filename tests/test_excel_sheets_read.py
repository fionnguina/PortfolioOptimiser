"""Test _read_actual_fills — the Actual_Fills ledger reader for fill-adherence.

Regression: the sheet has a summary banner above the ledger table, and the
ledger header is on ~row 7 (Exec TS/Ticker/Side/Qty Filled/...). The old reader
assumed row 1 was the header and errored on the shape mismatch, so fill-adherence
never got any fills. Now it scans for the 'Ticker' header row and maps the ledger
schema to what drift's join expects (Fill Date/Ticker/Units, units signed by side).
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import excel_sheets


class _Sheet:
    name = "Actual_Fills"
    def __init__(self, value): self._value = value
    @property
    def used_range(self):
        return type("R", (), {"value": self._value})()

class _Sheets(list):
    def __getitem__(self, k):
        if isinstance(k, str):
            return next(s for s in self if s.name == k)
        return list.__getitem__(self, k)

class _WB:
    def __init__(self, value):
        self.sheets = _Sheets([_Sheet(value)])


def _sheet_like(width=6):
    def pad(row): return list(row) + [None] * (width - len(row))
    return [
        pad(["IBKR Actual Fills — Phase 3 log"]),
        pad(["Source: ibkr_fills_log.jsonl"]),
        pad(["Most recent batch: 2026-07-08"]),
        pad(["  Submitted: 6 · Filled: 6"]),
        pad(["Note: for broker truth use --check-fills"]),
        pad([None]),  # blank separator
        ["Exec TS", "Rec Run TS", "Ticker", "Side", "Qty Req", "Qty Filled"],
        ["2026-07-08T11:00:00", "2026-07-08", "SMH", "BUY", 42, 42],
        ["2026-07-08T11:01:00", "2026-07-08", "VLUE.AX", "SELL", 100, 100],
    ]


def test_read_actual_fills_finds_ledger_below_banner():
    df = excel_sheets._read_actual_fills(_WB(_sheet_like()))
    assert list(df.columns) == ["Fill Date", "Ticker", "Units"]
    assert len(df) == 2
    assert set(df["Ticker"]) == {"SMH", "VLUE.AX"}

def test_read_actual_fills_signs_units_by_side():
    df = excel_sheets._read_actual_fills(_WB(_sheet_like()))
    assert float(df[df["Ticker"] == "SMH"]["Units"].iloc[0]) == 42.0     # BUY -> +
    assert float(df[df["Ticker"] == "VLUE.AX"]["Units"].iloc[0]) == -100.0  # SELL -> -

def test_read_actual_fills_no_ticker_header_returns_empty():
    """Banner-only sheet (no ledger yet) -> clean empty, no error."""
    banner = [["IBKR Actual Fills — log empty"], ["Source: ..."]]
    assert excel_sheets._read_actual_fills(_WB(banner)).empty

def test_read_actual_fills_missing_sheet_returns_empty():
    class _EmptyWB:
        sheets = _Sheets([])
    assert excel_sheets._read_actual_fills(_EmptyWB()).empty

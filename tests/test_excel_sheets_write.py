"""Tests for excel_sheets.py writers — the sheet-building logic behind the
xlwings COM calls.

xlwings/Excel isn't available in CI, so we drive the writers with a capturing
fake workbook (records range assignments) + a MagicMock `.api` that absorbs the
COM-only calls (FillDown, NumberFormat, Validation). This exercises the real
DataFrame-shaping logic (column selection, the Holdings include-default-True
guard, the cash-ledger rename map) without an Excel process. Column widths in
_autofit_table_width are pure math and tested directly.
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import excel_sheets
from pptx.util import Cm


# === capturing fake workbook ==================================================

class _FakeRange:
    def __init__(self, sheet, addr):
        self._sheet, self._addr = sheet, addr
    @property
    def value(self):
        return self._sheet.vals.get(self._addr)
    @value.setter
    def value(self, v):
        self._sheet.vals[self._addr] = v
    @property
    def formula(self):
        return self._sheet.formulas.get(self._addr)
    @formula.setter
    def formula(self, v):
        self._sheet.formulas[self._addr] = v
    def options(self, *a, **k):
        return self
    def clear_contents(self):
        self._sheet.vals.clear()
    @property
    def api(self):
        return MagicMock()

class _FakeSheet:
    def __init__(self, name):
        self.name = name
        self.vals: dict = {}
        self.formulas: dict = {}
    def range(self, addr):
        return _FakeRange(self, addr)
    @property
    def used_range(self):
        return _FakeRange(self, "__used__")
    def autofit(self, *a, **k):
        pass

class _FakeSheets:
    def __init__(self, names):
        self._list = [_FakeSheet(n) for n in names]
    def __getitem__(self, key):
        if isinstance(key, str):
            for s in self._list:
                if s.name == key:
                    return s
            raise KeyError(key)
        return self._list[key]
    def add(self, name, after=None):
        s = _FakeSheet(name)
        self._list.append(s)
        return s

class _FakeWB:
    def __init__(self, names=()):
        self.sheets = _FakeSheets(list(names))


# === get_or_clear_sheet =======================================================

def test_get_or_clear_sheet_reuses_existing():
    wb = _FakeWB(["Holdings"])
    s = excel_sheets.get_or_clear_sheet(wb, "Holdings")
    assert s.name == "Holdings"
    assert len(wb.sheets._list) == 1        # reused, not added

def test_get_or_clear_sheet_adds_when_missing():
    wb = _FakeWB(["Holdings"])
    s = excel_sheets.get_or_clear_sheet(wb, "Lots")
    assert s.name == "Lots"
    assert len(wb.sheets._list) == 2


# === _write_holdings_sheet — include-default-True guard ========================

def test_holdings_sheet_defaults_missing_include_to_true():
    wb = _FakeWB(["Holdings"])
    prices = pd.DataFrame(
        {"AAA.AX": [10.0, 11.0], "BBB.AX": [20.0, 22.0], "PortfolioValue": [1, 1]},
        index=pd.date_range("2026-07-01", periods=2),
    )
    excel_sheets._write_holdings_sheet(
        wb, prices, units={"AAA.AX": 100},
        include_flags={"AAA.AX": False},          # BBB.AX intentionally absent
        fx_to_aud_map={"AAA.AX": 1.0, "BBB.AX": 1.5},
    )
    df = wb.sheets["Holdings"].vals["A2"]
    by_tkr = {r["Security"]: r for r in df.to_dict("records")}
    assert set(by_tkr) == {"AAA.AX", "BBB.AX"}     # PortfolioValue excluded
    assert by_tkr["AAA.AX"]["Include?"] is False   # explicit False respected
    assert by_tkr["BBB.AX"]["Include?"] is True    # missing -> default True (the guard)
    assert by_tkr["AAA.AX"]["Units"] == 100.0
    assert by_tkr["BBB.AX"]["Units"] == 0.0        # not held -> 0
    assert by_tkr["BBB.AX"]["FX to AUD"] == 1.5


# === _write_cash_ledger_sheet — summary + rename ==============================

def test_cash_ledger_sheet_summary_and_rename(monkeypatch):
    monkeypatch.setattr(excel_sheets, "TARGET_PORTFOLIO_VALUE_AUD", 250_000.0)
    wb = _FakeWB(["Cash_Ledger"])
    ledger = pd.DataFrame([
        {"date": "2026-07-10", "selected_mode": "Balanced",
         "portfolio_value_aud": 249_000.0, "cash_balance_aud": 9_000.0,
         "cum_brokerage_aud": 12.0, "cum_cgt_aud": 100.0,
         "drift_vs_start_aud": -500.0, "drift_vs_target_aud": -1_000.0},
        {"date": "2026-07-13", "selected_mode": "Balanced",
         "portfolio_value_aud": 250_500.0, "cash_balance_aud": 8_000.0,
         "cum_brokerage_aud": 24.0, "cum_cgt_aud": 100.0,
         "drift_vs_start_aud": 500.0, "drift_vs_target_aud": 500.0},
    ])
    excel_sheets._write_cash_ledger_sheet(wb, ledger)
    s = wb.sheets["Cash_Ledger"]
    assert s.vals["B1"] == 250_000.0                       # target
    assert s.vals["B2"] == 250_500.0                       # latest portfolio (last row)
    assert s.vals["B7"] == pytest.approx(24.0 + 100.0)     # total cost = cum broke + cum cgt
    out = s.vals["A10"]
    assert "Portfolio (AUD)" in out.columns                # renamed from portfolio_value_aud
    assert "date" not in out.columns                       # raw name gone
    assert "Mode" in out.columns

def test_cash_ledger_sheet_empty_stubs():
    wb = _FakeWB(["Cash_Ledger"])
    excel_sheets._write_cash_ledger_sheet(wb, pd.DataFrame())
    assert "empty" in str(wb.sheets["Cash_Ledger"].vals["A1"]).lower()


# === _write_tilts_sheet =======================================================

def test_write_tilts_sheet_reshapes():
    wb = _FakeWB(["Tilts"])
    tilts = pd.DataFrame(
        {"Target": [1.0, 0.0], "Band": [0.05, 0.05], "Use?": [True, False]},
        index=["Mkt-RF", "SMB"],
    )
    excel_sheets._write_tilts_sheet(wb, tilts)
    out = wb.sheets["Tilts"].vals["A2"]
    assert list(out.columns) == ["Factor", "Target", "Band", "Use?"]
    assert list(out["Factor"]) == ["Mkt-RF", "SMB"]


# === _autofit_table_width (pure) ==============================================

class _FakeCol:
    def __init__(self): self.width = None
class _FakeTable:
    def __init__(self, n): self.columns = [_FakeCol() for _ in range(n)]

def test_autofit_width_scales_to_total_and_ranks_by_content():
    df = pd.DataFrame({
        "Security": ["VERYLONGTICKER.AX", "X.AX"],
        "Target": ["1.0", "0.0"],
    })
    tbl = _FakeTable(2)
    excel_sheets._autofit_table_width(tbl, df, total_width_cm=12.02)
    widths = [c.width for c in tbl.columns]
    assert all(w is not None for w in widths)
    assert sum(widths) == pytest.approx(Cm(12.02), rel=1e-6)   # normalised to total
    assert widths[0] > widths[1]                               # Security wider than Target

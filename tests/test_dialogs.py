"""Tests for dialogs.py — the non-Tk logic: value coercion + file-based seed
readers. The Tk/CustomTkinter dialog builders (_edit_holdings_dialog_*) need a
display + user interaction and are not unit-tested; the seed readers below are
the COM-free data path they sit on top of.
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import dialogs


# === coercion helpers =========================================================

@pytest.mark.parametrize("value,expected", [
    (True, True), (False, False),
    (np.bool_(True), True),
    ("TRUE", True), ("yes", True), ("Y", True), ("1", True), ("t", True),
    ("FALSE", False), ("no", False), ("", False), ("garbage", False),
    (np.nan, False),
])
def test_to_bool_flag(value, expected):
    assert dialogs._to_bool_flag(value) == expected

def test_to_bool_flag_default_on_nan():
    assert dialogs._to_bool_flag(np.nan, default=True) is True

@pytest.mark.parametrize("value,expected", [
    ("3.5", 3.5), (7, 7.0), ("  2.0 ", 2.0),
    (None, 0.0), ("", 0.0), ("not-a-number", 0.0),
])
def test_to_float(value, expected):
    assert dialogs._to_float(value) == expected

def test_to_float_custom_default():
    assert dialogs._to_float("bad", default=1.5) == 1.5


# === holdings seed reader =====================================================

def _write_xlsx(path, sheets: dict):
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        for name, df in sheets.items():
            df.to_excel(xw, sheet_name=name, index=False)

def test_holdings_seed_sums_duplicates_and_reads_include(tmp_path):
    p = tmp_path / "wb.xlsx"
    _write_xlsx(p, {"Holdings": pd.DataFrame({
        "Security": ["AAA.AX", "AAA.AX", "BBB.AX", " "],   # dup + blank row
        "Units": [100, 50, 200, 999],
        "Include?": ["TRUE", "TRUE", "FALSE", "TRUE"],
    })})
    units, include = dialogs._read_holdings_seed_from_path(p)
    assert units["AAA.AX"] == 150.0        # duplicates summed
    assert units["BBB.AX"] == 200.0
    assert " " not in units.index          # blank security dropped
    assert include["AAA.AX"] is True
    assert include["BBB.AX"] is False

def test_holdings_seed_units_alias_fallback(tmp_path):
    p = tmp_path / "wb.xlsx"
    _write_xlsx(p, {"Holdings": pd.DataFrame({
        "Security": ["AAA.AX"], "Curr Units": [42],       # no "Units" column
    })})
    units, include = dialogs._read_holdings_seed_from_path(p)
    assert units["AAA.AX"] == 42.0
    assert include["AAA.AX"] is True       # no Include? column -> default True

def test_holdings_seed_missing_security_col_empty(tmp_path):
    p = tmp_path / "wb.xlsx"
    _write_xlsx(p, {"Holdings": pd.DataFrame({"Ticker": ["AAA.AX"], "Units": [1]})})
    units, include = dialogs._read_holdings_seed_from_path(p)
    assert units.empty and include == {}

def test_holdings_seed_missing_file_empty(tmp_path):
    units, include = dialogs._read_holdings_seed_from_path(tmp_path / "nope.xlsx")
    assert units.empty and include == {}


# === tilts seed reader ========================================================

def test_tilts_seed_reads_and_reindexes(tmp_path):
    p = tmp_path / "wb.xlsx"
    _write_xlsx(p, {"Tilts": pd.DataFrame({
        "Factor": ["Mkt-RF", "SMB"],
        "Target": [0.8, 0.2], "Band": [0.10, 0.05], "Use?": ["TRUE", "TRUE"],
    })})
    out = dialogs._read_tilts_seed_from_path(p)
    assert list(out.index) == list(dialogs.TILT_FACTORS)   # reindexed to full factor set
    assert out.loc["Mkt-RF", "Target"] == pytest.approx(0.8)
    assert out.loc["SMB", "Use?"] == True
    # A factor absent from the sheet falls back to the default row.
    assert out.loc["MOM", "Use?"] == False

def test_tilts_seed_malformed_returns_defaults(tmp_path):
    p = tmp_path / "wb.xlsx"
    _write_xlsx(p, {"Tilts": pd.DataFrame({"Factor": ["Mkt-RF"], "Target": [1.0]})})
    out = dialogs._read_tilts_seed_from_path(p)          # missing Band/Use? -> defaults
    assert list(out.index) == list(dialogs.TILT_FACTORS)
    assert out.loc["Mkt-RF", "Target"] == pytest.approx(1.0)
    assert bool(out.loc["Mkt-RF", "Use?"]) is True

def test_tilts_seed_missing_file_defaults(tmp_path):
    out = dialogs._read_tilts_seed_from_path(tmp_path / "nope.xlsx")
    assert list(out.index) == list(dialogs.TILT_FACTORS)
    assert out.loc["Mkt-RF", "Target"] == pytest.approx(1.0)

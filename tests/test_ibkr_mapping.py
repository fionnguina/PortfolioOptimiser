"""Smoke tests for IBKR contract mapping + price sanity.

The ticker-to-Contract mapping (.AX → ASX/AUD, else SMART/USD) and the
non-positive-price filter are both safety-critical: a wrong mapping orders
the wrong instrument; a -1 sentinel slipping into last_px_hold would torch
cash-flow maths.
"""
from __future__ import annotations

import math
import pandas as pd
import pytest

from conftest import extract_funcs


@pytest.fixture(scope="module")
def ibkr():
    return extract_funcs(
        "_ibkr_pick_price",
        "apply_ibkr_price_override",
        extra_consts=("IBKR_DIVERGENCE_WARN_BPS",),
    )


# === _ibkr_pick_price =========================================================

class _FakeTicker:
    """Minimal stand-in for ib_insync.Ticker for unit testing."""
    def __init__(self, last=None, close=None, bid=None, ask=None):
        self.last = last
        self.close = close
        self.bid = bid
        self.ask = ask


def test_pick_price_prefers_last(ibkr):
    tk = _FakeTicker(last=100.0, close=99.0, bid=99.5, ask=100.5)
    assert ibkr["_ibkr_pick_price"](tk) == 100.0


def test_pick_price_falls_back_to_close(ibkr):
    tk = _FakeTicker(last=float("nan"), close=99.0, bid=98.0, ask=99.5)
    assert ibkr["_ibkr_pick_price"](tk) == 99.0


def test_pick_price_falls_back_to_midpoint(ibkr):
    tk = _FakeTicker(last=float("nan"), close=float("nan"), bid=99.5, ask=100.5)
    assert ibkr["_ibkr_pick_price"](tk) == 100.0


def test_pick_price_rejects_negative_sentinel(ibkr):
    """IBKR uses -1.0 as 'no data'. Must NOT be accepted as a valid price."""
    tk = _FakeTicker(last=-1.0)
    assert ibkr["_ibkr_pick_price"](tk) is None


def test_pick_price_rejects_zero(ibkr):
    """A zero price would torch downstream maths just as badly as -1."""
    tk = _FakeTicker(last=0.0)
    assert ibkr["_ibkr_pick_price"](tk) is None


def test_pick_price_rejects_inf_nan(ibkr):
    tk = _FakeTicker(last=float("inf"))
    assert ibkr["_ibkr_pick_price"](tk) is None
    tk = _FakeTicker(last=float("nan"))
    assert ibkr["_ibkr_pick_price"](tk) is None


def test_pick_price_no_data_anywhere(ibkr):
    tk = _FakeTicker()
    assert ibkr["_ibkr_pick_price"](tk) is None


# === apply_ibkr_price_override ================================================

def test_override_replaces_when_positive(ibkr):
    last_px = pd.Series({"VLUE.AX": 43.00, "SMH": 600.0, "BEAR.AX": 7.20})
    ibkr_px = {"VLUE.AX": 43.05, "SMH": 615.0, "BEAR.AX": 7.21}
    updated, diag = ibkr["apply_ibkr_price_override"](last_px, ibkr_px)
    assert updated["VLUE.AX"] == 43.05
    assert updated["SMH"] == 615.0
    assert updated["BEAR.AX"] == 7.21
    assert diag["n_overridden"] == 3


def test_override_skips_non_positive_sentinel(ibkr):
    """Defensive: even if _ibkr_pick_price let one through, the override
    layer must reject -1 / 0 / NaN / inf."""
    last_px = pd.Series({"VLUE.AX": 43.00, "BAD.AX": 50.00})
    ibkr_px = {"VLUE.AX": 43.05, "BAD.AX": -1.0}
    updated, diag = ibkr["apply_ibkr_price_override"](last_px, ibkr_px)
    assert updated["VLUE.AX"] == 43.05
    # BAD.AX retains yfinance price, not the -1 sentinel.
    assert updated["BAD.AX"] == 50.00
    assert diag["n_overridden"] == 1


def test_override_warns_on_large_divergence(ibkr, capsys):
    """Divergence >100 bps triggers a printed [WARN] line."""
    last_px = pd.Series({"SMH": 600.0})
    ibkr_px = {"SMH": 615.0}  # +250 bps
    _, diag = ibkr["apply_ibkr_price_override"](last_px, ibkr_px)
    assert diag["n_warn"] == 1
    captured = capsys.readouterr()
    assert "[ibkr-price][WARN]" in captured.out
    assert "SMH" in captured.out


def test_override_no_warn_on_small_divergence(ibkr, capsys):
    """Divergence <100 bps stays quiet."""
    last_px = pd.Series({"VLUE.AX": 43.00})
    ibkr_px = {"VLUE.AX": 43.05}  # ~12 bps
    _, diag = ibkr["apply_ibkr_price_override"](last_px, ibkr_px)
    assert diag["n_warn"] == 0


def test_override_ignores_unknown_tickers(ibkr):
    """An IBKR price for a ticker not in last_px_hold is silently dropped."""
    last_px = pd.Series({"VLUE.AX": 43.00})
    ibkr_px = {"VLUE.AX": 43.05, "GHOST": 999.0}
    updated, diag = ibkr["apply_ibkr_price_override"](last_px, ibkr_px)
    assert "GHOST" not in updated.index
    assert diag["n_overridden"] == 1


def test_override_empty_dict_returns_unchanged(ibkr):
    """No IBKR prices → unchanged series + zero diagnostics."""
    last_px = pd.Series({"VLUE.AX": 43.00})
    updated, diag = ibkr["apply_ibkr_price_override"](last_px, {})
    assert updated.equals(last_px)
    assert diag["n_overridden"] == 0
    assert diag["n_warn"] == 0


def test_override_max_bps_tracked(ibkr):
    """The largest divergence (in absolute bps) is reported via diag."""
    last_px = pd.Series({"A": 100.0, "B": 100.0, "C": 100.0})
    ibkr_px = {"A": 100.50, "B": 102.0, "C": 99.5}  # 50, 200, -50 bps
    _, diag = ibkr["apply_ibkr_price_override"](last_px, ibkr_px)
    assert diag["max_bps"] == pytest.approx(200.0, abs=0.5)
    assert diag["max_bps_ticker"] == "B"

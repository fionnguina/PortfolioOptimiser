"""Tests for cash-fit sizing in make_trade_plan.

Pins the guarantee that a live plan's net buys never exceed the cash on hand:
when available_cash_aud is supplied, the target book is sized to
(holdings + cash - reserve), so the plan is always fundable. Falls back to
NAV sizing when cash is unknown (backward compatible).
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
from conftest import extract_funcs
import brokerage


@pytest.fixture(scope="module")
def make_trade_plan():
    ns = extract_funcs("make_trade_plan",
                       extra_consts=("CASH_RESERVE_PCT", "CASH_RESERVE_MIN_AUD"))
    ns["ASX_MIN_MARKETABLE_PARCEL_AUD"] = brokerage.ASX_MIN_MARKETABLE_PARCEL_AUD
    return ns["make_trade_plan"]


def _scenario():
    """Holdings ~$180.5k, fully-invested target, $67,929 cash (the real slide)."""
    px = {"BEAR.AX": 7.39, "GOLD.AX": 54.16, "HBRD.AX": 10.07, "PDBC": 23.61,
          "SMH": 873.85, "VEA": 101.70, "VLUE.AX": 36.92}
    cur = {"BEAR.AX": 1814, "GOLD.AX": 374, "HBRD.AX": 280, "PDBC": 0,
           "SMH": 50, "VEA": 22, "VLUE.AX": 2657}
    tgt = {"BEAR.AX": 2196, "GOLD.AX": 404, "HBRD.AX": 629, "PDBC": 82,
           "SMH": 92, "VEA": 47, "VLUE.AX": 3219}
    lp = pd.Series(px)
    fx = pd.Series(1.0, index=lp.index)
    units = pd.Series(cur)
    tgt_val = pd.Series({t: tgt[t] * px[t] for t in px})
    w = tgt_val / tgt_val.sum()  # fully-invested target weights
    inc = {t: True for t in px}
    holdings = float(sum(cur[t] * px[t] for t in px))
    return units, lp, fx, w, inc, holdings


def test_cash_fit_plan_fits_available_cash(make_trade_plan):
    units, lp, fx, w, inc, holdings = _scenario()
    cash = 67_929.21
    _, resid = make_trade_plan(units, lp, fx, w, inc,
                               portfolio_value_override=250_559.0,
                               available_cash_aud=cash)
    net_buys = -resid  # residual is negative (net buying)
    assert net_buys <= cash, "net buys must not exceed available cash"

def test_cash_fit_leaves_reserve(make_trade_plan):
    units, lp, fx, w, inc, holdings = _scenario()
    cash = 67_929.21
    _, resid = make_trade_plan(units, lp, fx, w, inc,
                               available_cash_aud=cash)
    net_buys = -resid
    reserve = max(300.0, 0.005 * (holdings + cash))
    # net buys ~ cash - reserve (within a unit of rounding on the priciest name)
    assert net_buys <= cash - reserve + 900  # SMH ~$874/unit rounding slack
    assert net_buys >= cash - reserve - 900

def test_nav_sizing_overshoots_without_cash_fit(make_trade_plan):
    """Regression: NAV sizing (no cash-fit) is what caused the overshoot."""
    units, lp, fx, w, inc, holdings = _scenario()
    _, resid = make_trade_plan(units, lp, fx, w, inc,
                               portfolio_value_override=250_559.0)
    net_buys = -resid
    assert net_buys > 67_929.21  # exceeds real cash -> the original bug

def test_cash_fit_none_falls_back_to_nav(make_trade_plan):
    """available_cash_aud=None -> behaves exactly like NAV sizing."""
    units, lp, fx, w, inc, _ = _scenario()
    _, r_none = make_trade_plan(units, lp, fx, w, inc,
                                portfolio_value_override=250_559.0,
                                available_cash_aud=None)
    _, r_nav = make_trade_plan(units, lp, fx, w, inc,
                               portfolio_value_override=250_559.0)
    assert r_none == pytest.approx(r_nav)

def test_cash_fit_reserve_floor_applies(make_trade_plan):
    """Tiny investable -> reserve floors at CASH_RESERVE_MIN_AUD ($300)."""
    px = {"AAA": 10.0}
    units = pd.Series({"AAA": 100})  # holdings $1000
    lp = pd.Series(px); fx = pd.Series(1.0, index=lp.index)
    w = pd.Series({"AAA": 1.0})
    _, resid = make_trade_plan(units, lp, fx, w, {"AAA": True},
                               available_cash_aud=500.0)
    net_buys = -resid
    # investable 1500; 0.5% = 7.50 < 300 floor -> reserve 300 -> buys ~ 500-300=200
    assert net_buys <= 500.0
    assert net_buys == pytest.approx(200.0, abs=10.0)  # 20 units * $10

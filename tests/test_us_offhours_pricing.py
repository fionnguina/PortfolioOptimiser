"""US-name off-hours pricing fallback (ibkr_paper_exec).

The autonomous run fires at ~09:3x AEST — US market shut, so US names have no
live/delayed top-of-book quote. Without a subscription-independent fallback the
US legs (e.g. the SMH buy) are classed `unpriceable` and silently DROPPED every
run, so the book never converges. These tests pin the fallback: a US order must
price off the last daily close (HMDS farm) when no live quote exists, and only be
dropped when BOTH sources fail. .AX behaviour (px_aud fallback) must be unchanged.
"""
from __future__ import annotations

import ibkr_paper_exec as ex


class _FakeTicker:
    def __init__(self, mp=float("nan"), last=None, close=None):
        self._mp = mp
        self.last = last
        self.close = close

    def marketPrice(self):
        return self._mp


class _FakeBar:
    def __init__(self, close):
        self.close = close


class _FakeIB:
    """reqTickers → live/delayed quote; reqHistoricalData → daily bars."""
    def __init__(self, tickers=None, bars=None):
        self._tickers = tickers or []
        self._bars = bars or []

    def reqTickers(self, *contracts):
        return self._tickers

    def reqHistoricalData(self, *a, **k):
        return self._bars


class _FakeOrder:
    def __init__(self, action):
        self.action = action
        self.lmtPrice = None


def _plan(ticker, action, px_aud=0.0):
    rec = {"ticker": ticker, "px_aud": px_aud}
    return [(rec, object(), _FakeOrder(action))]


# ---- _ref_local_price: live → hist-close fallback ----

def test_ref_price_uses_live_when_available():
    ib = _FakeIB(tickers=[_FakeTicker(mp=580.0)], bars=[_FakeBar(999.0)])
    assert ex._ref_local_price(ib, object()) == 580.0  # live wins, hist untouched

def test_ref_price_falls_back_to_hist_close():
    ib = _FakeIB(tickers=[], bars=[_FakeBar(575.0)])   # no live quote (market shut)
    assert ex._ref_local_price(ib, object()) == 575.0

def test_ref_price_none_when_both_fail():
    ib = _FakeIB(tickers=[], bars=[])
    assert ex._ref_local_price(ib, object()) is None


# ---- _price_orders_as_limits: US name off-hours ----

def test_us_buy_priced_from_hist_close_offhours():
    ib = _FakeIB(tickers=[], bars=[_FakeBar(575.0)])   # US market shut, hist close 575
    priced, unpriceable = ex._price_orders_as_limits(ib, _plan("SMH", "BUY"), 1.0)
    assert len(priced) == 1 and not unpriceable
    # BUY marketable limit = 575 * (1 + 1%) = 580.75
    assert priced[0][2].lmtPrice == 580.75

def test_us_order_dropped_only_when_no_live_and_no_hist():
    ib = _FakeIB(tickers=[], bars=[])                  # both sources dead
    priced, unpriceable = ex._price_orders_as_limits(ib, _plan("SMH", "BUY"), 1.0)
    assert not priced and len(unpriceable) == 1

def test_us_order_prefers_live_quote():
    ib = _FakeIB(tickers=[_FakeTicker(mp=580.0)], bars=[_FakeBar(999.0)])
    priced, _ = ex._price_orders_as_limits(ib, _plan("SMH", "BUY"), 1.0)
    assert priced[0][2].lmtPrice == round(580.0 * 1.01, 2)  # 585.80, not from hist


# ---- .AX behaviour must be unchanged ----

def test_asx_name_still_uses_px_aud_fallback():
    ib = _FakeIB(tickers=[], bars=[])                  # no live quote, no hist
    priced, unpriceable = ex._price_orders_as_limits(
        ib, _plan("IVV.AX", "SELL", px_aud=100.0), 1.0)
    assert len(priced) == 1 and not unpriceable
    # SELL marketable limit = 100 * (1 - 1%) = 99.0 (from px_aud, not hist)
    assert priced[0][2].lmtPrice == 99.0

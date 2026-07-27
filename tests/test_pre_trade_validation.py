"""Broker-truth pre-trade validation gate — the guardrail for safe autonomy.

Every check is pinned, including the exact 2026-07-23 failure (stale sheet →
naked SOXX short) that the engine's sheet-side sanity check couldn't catch.
"""
from __future__ import annotations

import ibkr_paper_exec as ex


def _t(ticker, du, dv):
    return {"ticker": ticker, "delta_units": du, "delta_value_aud": dv}


def test_clean_reconciled_plan_passes():
    trades = [_t("VLUE.AX", 100, 3600.0), _t("GOLD.AX", -20, -1040.0)]
    assumed = {"VLUE.AX": 2000, "GOLD.AX": 354}
    broker = {"VLUE.AX": 2000, "GOLD.AX": 354}
    ok, fails = ex.validate_pre_trade(trades, assumed, broker,
                                      available_cash_aud=50_000, nav_aud=246_000)
    assert ok and fails == []


def test_reconciliation_mismatch_aborts():
    # sheet says 280 HBRD, broker holds 777 → plan is stale
    trades = [_t("HBRD.AX", 500, 5000.0)]
    ok, fails = ex.validate_pre_trade(trades, {"HBRD.AX": 280}, {"HBRD.AX": 777},
                                      available_cash_aud=50_000, nav_aud=246_000)
    assert not ok
    assert any("RECONCILE" in f and "HBRD" in f for f in fails)


def test_todays_naked_soxx_short_is_caught():
    # the real incident: sheet SOXX=0, broker=-53, plan ignores SOXX
    trades = [_t("SMH", 38, 31_832.0)]                 # plan buys SMH, no SOXX
    assumed = {"SMH": 50, "SOXX": 0}
    broker = {"SMH": 50, "SOXX": -53}
    ok, fails = ex.validate_pre_trade(trades, assumed, broker,
                                      available_cash_aud=93_000, nav_aud=246_000)
    assert not ok
    # both the reconcile gap AND the uncovered short must fire
    assert any("RECONCILE" in f and "SOXX" in f for f in fails)
    assert any("SHORT" in f and "SOXX" in f for f in fails)


def test_resulting_short_from_oversell_aborts():
    trades = [_t("VEA", -20, -2000.0)]                 # sell 20, only hold 13
    ok, fails = ex.validate_pre_trade(trades, {"VEA": 13}, {"VEA": 13},
                                      available_cash_aud=50_000, nav_aud=246_000)
    assert not ok
    assert any("SHORT" in f and "VEA" in f for f in fails)


def test_turnover_over_bound_aborts():
    trades = [_t("VLUE.AX", 15_000, 600_000.0)]        # 600k/246k = 2.44x NAV > 2.0
    ok, fails = ex.validate_pre_trade(trades, {"VLUE.AX": 0}, {"VLUE.AX": 0},
                                      available_cash_aud=1_000_000, nav_aud=246_000,
                                      max_turnover=2.0)
    assert not ok
    assert any("TURNOVER" in f for f in fails)


def test_net_buys_over_cash_aborts():
    trades = [_t("VLUE.AX", 100, 100_000.0)]           # $100k buy, $50k cash
    ok, fails = ex.validate_pre_trade(trades, {"VLUE.AX": 0}, {"VLUE.AX": 0},
                                      available_cash_aud=50_000, nav_aud=246_000)
    assert not ok
    assert any("CASH" in f for f in fails)


def test_covering_an_existing_short_passes():
    # plan BUYS 53 SOXX to flatten the -53 short → allowed (and reconciled)
    trades = [_t("SOXX", 53, 29_000.0)]
    ok, fails = ex.validate_pre_trade(trades, {"SOXX": -53}, {"SOXX": -53},
                                      available_cash_aud=50_000, nav_aud=246_000)
    assert ok, fails


# --- --flatten mode: to-zero order builder for cap-0.0 stuck positions --------

def test_flatten_targets_covers_a_short():
    # broker holds a -53 SOXX short the engine can't touch (cap 0.0) → BUY 53
    out = ex._flatten_targets({"SOXX": -53, "SMH": 50}, {"SOXX"})
    assert out == [("SOXX", -53.0, "BUY", 53, 53)]


def test_flatten_targets_closes_a_long():
    # a cap-0.0 long residual is equally stuck → SELL to zero (negative delta)
    out = ex._flatten_targets({"PMGOLD.AX": 120}, {"PMGOLD"})
    assert out == [("PMGOLD.AX", 120.0, "SELL", 120, -120)]


def test_flatten_targets_skips_flat_and_unrequested():
    # already-flat (within tol) and non-requested names are dropped
    out = ex._flatten_targets({"SOXX": 0.4, "SMH": 50}, {"SOXX", "SMH"})
    assert [t[0] for t in out] == ["SMH"]
    assert ex._flatten_targets({"SMH": 50}, {"SOXX"}) == []


def test_flatten_built_order_clears_the_gate():
    # the order _flatten_targets builds for the SOXX short must PASS the same
    # broker-truth gate that blocks the normal exec path (assumed == broker truth)
    broker = {"SOXX": -53, "SMH": 50}
    tgts = ex._flatten_targets(broker, {"SOXX"})
    trades = [_t(tk, signed, 0.0) for tk, _held, _side, _qty, signed in tgts]
    ok, fails = ex.validate_pre_trade(trades, dict(broker), dict(broker),
                                      available_cash_aud=50_000, nav_aud=246_000)
    assert ok, fails


def test_missing_cash_or_nav_does_not_crash():
    trades = [_t("VLUE.AX", 10, 360.0)]
    ok, fails = ex.validate_pre_trade(trades, {"VLUE.AX": 100}, {"VLUE.AX": 100},
                                      available_cash_aud=None, nav_aud=None)
    assert ok and fails == []


# --- open-order guard (anti-stacking; the 2026-07-24 daily-churn fix) --------

def test_working_order_on_plan_ticker_aborts():
    # a prior run's HBRD buy is still working; re-submitting would STACK
    trades = [_t("HBRD.AX", 497, 5004.79), _t("VLUE.AX", 100, 3600.0)]
    assumed = {"HBRD.AX": 280, "VLUE.AX": 2000}
    broker = {"HBRD.AX": 280, "VLUE.AX": 2000}
    ok, fails = ex.validate_pre_trade(trades, assumed, broker,
                                      available_cash_aud=50_000, nav_aud=246_000,
                                      open_orders={"HBRD.AX": 497})
    assert not ok
    assert any("OPEN-ORDER" in f and "HBRD" in f for f in fails)


def test_working_order_on_unrelated_ticker_also_aborts():
    # book must be quiescent: even a working order the plan doesn't touch blocks
    trades = [_t("VLUE.AX", 100, 3600.0)]
    ok, fails = ex.validate_pre_trade(trades, {"VLUE.AX": 2000}, {"VLUE.AX": 2000},
                                      available_cash_aud=50_000, nav_aud=246_000,
                                      open_orders={"PDBC": -304})
    assert not ok
    assert any("OPEN-ORDER" in f and "PDBC" in f for f in fails)


def test_no_open_orders_passes():
    trades = [_t("VLUE.AX", 100, 3600.0)]
    for oo in (None, {}, {"VLUE.AX": 0.0}):
        ok, fails = ex.validate_pre_trade(trades, {"VLUE.AX": 2000}, {"VLUE.AX": 2000},
                                          available_cash_aud=50_000, nav_aud=246_000,
                                          open_orders=oo)
        assert ok, (oo, fails)


# --- data-farm liveness gate (the 2026-07-24 dead-feed fix) ------------------

def test_broken_data_farm_aborts():
    # feed down → market orders would sit unfilled → refuse
    trades = [_t("VLUE.AX", 100, 3600.0)]
    ok, fails = ex.validate_pre_trade(trades, {"VLUE.AX": 2000}, {"VLUE.AX": 2000},
                                      available_cash_aud=50_000, nav_aud=246_000,
                                      data_farm_broken=True,
                                      data_farm_reason="market-data farm connection is broken (usfarm.nj)")
    assert not ok
    assert any("MKT-DATA" in f and "unfilled" in f for f in fails)


def test_data_farm_ok_passes():
    trades = [_t("VLUE.AX", 100, 3600.0)]
    ok, fails = ex.validate_pre_trade(trades, {"VLUE.AX": 2000}, {"VLUE.AX": 2000},
                                      available_cash_aud=50_000, nav_aud=246_000,
                                      data_farm_broken=False)
    assert ok and fails == []


# --- _DataFarmMonitor: farm-status tracking from errorEvent codes ------------

class _FakeEvent:
    def __iadd__(self, fn): return self
    def __isub__(self, fn): return self


class _FakeIB:
    def __init__(self): self.errorEvent = _FakeEvent()
    def sleep(self, _s): pass


def test_farm_monitor_broken_then_recovers():
    mon = ex._DataFarmMonitor(_FakeIB())
    mon._on(-1, 2103, "Market data farm connection is broken:usfarm.nj", None)
    ok, why = mon.mktdata_ok(settle=0.0)
    assert not ok and "broken" in why.lower()
    mon._on(-1, 2104, "Market data farm connection is OK:usfarm.nj", None)
    ok, why = mon.mktdata_ok(settle=0.0)
    assert ok


def test_farm_monitor_silence_fails_open():
    ok, why = ex._DataFarmMonitor(_FakeIB()).mktdata_ok(settle=0.0)
    assert ok  # no status message → assume ok (a down feed reports BROKEN, not silence)


def test_farm_monitor_hmds_or_secdef_broken_does_not_block():
    # HMDS (historical) + sec-def don't gate live fills — only the market-data farm does
    mon = ex._DataFarmMonitor(_FakeIB())
    mon._on(-1, 2105, "HMDS data farm connection is broken:apachmds", None)
    mon._on(-1, 2157, "Sec-def data farm connection is broken:secdefsg", None)
    ok, _ = mon.mktdata_ok(settle=0.0)
    assert ok


# --- marketable LIMIT pricing (the off-hours gap-fill fix) -------------------

def test_marketable_limit_buy_caps_above():
    # BUY willing to pay UP TO +1% — the ceiling on a bad fill
    assert ex._marketable_limit_price(100.0, "BUY", 1.0) == 101.0
    assert ex._marketable_limit_price(7.33, "buy", 1.0) == 7.40  # rounds to 2dp


def test_marketable_limit_sell_floors_below():
    # SELL willing to accept DOWN TO -1% — the floor on a bad fill
    assert ex._marketable_limit_price(100.0, "SELL", 1.0) == 99.0
    assert ex._marketable_limit_price(582.5, "SELL", 1.0) == 576.67


def test_marketable_limit_collar_scales():
    assert ex._marketable_limit_price(100.0, "BUY", 0.5) == 100.5
    assert ex._marketable_limit_price(100.0, "BUY", 2.0) == 102.0


# --- shadow mode report body ------------------------------------------------

def test_shadow_body_execute_lists_orders():
    rec = {"run_at": "2026-07-23T09:32:09"}
    trades = [_t("SMH", 38, 31_832.0), _t("VEA", -16, -1610.0), _t("X", 0, 0.0)]
    body = ex._shadow_report_body(rec, trades, ok=True, fails=[])
    assert "WOULD EXECUTE" in body
    assert "BUY " in body and "SMH" in body
    assert "SELL" in body and "VEA" in body
    assert "X" not in body.split("no orders")[0].split("VEA")[-1]  # zero-delta skipped
    assert "no orders were placed" in body.lower()


def test_shadow_body_abort_lists_failures():
    rec = {"run_at": "2026-07-23T09:32:09"}
    trades = [_t("SMH", 38, 31_832.0)]
    body = ex._shadow_report_body(rec, trades, ok=False,
                                  fails=["RECONCILE: SOXX plan-assumed 0u != broker -53u"])
    assert "WOULD ABORT" in body
    assert "RECONCILE" in body and "SOXX" in body
    assert "would place" not in body.lower()

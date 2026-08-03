"""Lot-seed reconciliation against broker truth (lots.reconcile_seed_to_broker).

The fills log freezes rows at qty_filled=0 whenever an order fills after the
placing session ends, and the broker serves no historical executions to repair
it from — so the lot book is realigned to POSITIONS instead. This is CGT-
critical: it decides cost bases and, via AcqDate, 12-month LT-discount
eligibility. These tests pin the behaviour that must not drift, especially the
things a naive re-seed would get wrong.
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from lots import reconcile_seed_to_broker, derive_fx_from_snapshot

AS_OF = datetime(2026, 8, 3, 9, 33, 30)
USDAUD = 1.41973          # the rate the 2026-08-03 snapshot implies
FX = {"AUD": 1.0, "USD": USDAUD}


def _lot(sec, units, cost, acq="2026-07-08T00:00:00"):
    return {"Security": sec, "AcqDate": acq, "Units": units,
            "CostBaseAUD": cost, "SeedAsOf": "2026-07-28T09:33:26"}


def _pos(ticker, units, avg_local, mark_local, ccy="AUD"):
    # mkt_value_base is LOCAL currency — units * mark_local, never converted.
    # Mirroring that here is the point: a fixture that "helpfully" multiplied
    # by FX would hide exactly the bug this models.
    return {"ticker": ticker, "units": units, "avg_cost_local": avg_local,
            "mark_local": mark_local, "mkt_value_base": units * mark_local,
            "currency": ccy}


def _by_tkr(actions):
    return {a["ticker"]: a for a in actions}


# --- units agree -----------------------------------------------------------

def test_matching_units_left_untouched():
    seed = [_lot("GOLD.AX", 369, 54.62)]
    broker = {"GOLD.AX": _pos("GOLD.AX", 369, 54.62, 53.09)}
    out, actions = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    assert _by_tkr(actions)["GOLD.AX"]["action"] == "ok"
    assert len(out) == 1
    assert out[0]["Units"] == 369
    assert out[0]["CostBaseAUD"] == 54.62


def test_existing_acqdate_is_preserved():
    """A blunt re-seed would restamp this with today and reset the LT clock."""
    seed = [_lot("GOLD.AX", 369, 54.62, acq="2026-07-08T00:00:00")]
    broker = {"GOLD.AX": _pos("GOLD.AX", 369, 54.62, 53.09)}
    out, _ = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    assert out[0]["AcqDate"] == "2026-07-08T00:00:00"


def test_sub_tolerance_difference_is_not_a_change():
    seed = [_lot("GOLD.AX", 369.0, 54.62)]
    broker = {"GOLD.AX": _pos("GOLD.AX", 369.4, 54.62, 53.09)}
    _, actions = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    assert _by_tkr(actions)["GOLD.AX"]["action"] == "ok"


# --- broker holds MORE (a missed buy) --------------------------------------

def test_missed_buy_adds_a_lot_dated_at_the_snapshot():
    # book 347, broker 369 -> 22 units bought and never logged
    seed = [_lot("GOLD.AX", 347, 54.68)]
    broker = {"GOLD.AX": _pos("GOLD.AX", 369, 54.62, 53.09)}
    out, actions = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    assert _by_tkr(actions)["GOLD.AX"]["action"] == "added"
    assert len(out) == 2
    new = [L for L in out if L["Units"] == 22][0]
    assert new["AcqDate"] == AS_OF.isoformat()


def test_added_units_make_total_cost_base_match_broker_exactly():
    seed = [_lot("GOLD.AX", 347, 54.68)]
    broker = {"GOLD.AX": _pos("GOLD.AX", 369, 54.62, 53.09)}
    out, _ = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    book_cost = sum(L["Units"] * L["CostBaseAUD"] for L in out)
    assert abs(book_cost - 369 * 54.62) < 0.01


def test_nonsensical_implied_cost_falls_back_to_broker_average():
    # book cost so inflated that the implied cost on the delta goes negative
    seed = [_lot("GOLD.AX", 347, 5_000.0)]
    broker = {"GOLD.AX": _pos("GOLD.AX", 369, 54.62, 53.09)}
    out, actions = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    new = [L for L in out if L["Units"] == 22][0]
    assert new["CostBaseAUD"] == round(54.62, 7)
    assert "rejected as nonsensical" in _by_tkr(actions)["GOLD.AX"]["detail"]


def test_ticker_absent_from_seed_is_opened_at_broker_average():
    broker = {"VEA": _pos("VEA", 32, 70.57, 70.54, ccy="USD")}
    out, actions = reconcile_seed_to_broker([], broker, as_of=AS_OF, fx_map=FX)
    assert _by_tkr(actions)["VEA"]["action"] == "opened"
    assert len(out) == 1
    assert abs(out[0]["CostBaseAUD"] - 70.57 * USDAUD) < 0.01


def test_us_cost_converted_to_aud_not_left_local():
    """USD avg cost must land in AUD. Leaving it local books the cost base ~30%
    light and silently inflates every future realised gain."""
    broker = {"SMH": _pos("SMH", 50, 606.07, 538.45, ccy="USD")}
    out, _ = reconcile_seed_to_broker([], broker, as_of=AS_OF, fx_map=FX)
    assert abs(out[0]["CostBaseAUD"] - 606.07 * USDAUD) < 0.05
    assert out[0]["CostBaseAUD"] > 606.07 * 1.2      # definitively not local


def test_missing_fx_for_foreign_ccy_is_unpriceable_not_guessed():
    """Refusing beats defaulting to 1.0 — a silent 1.0 is the whole bug class."""
    broker = {"SMH": _pos("SMH", 50, 606.07, 538.45, ccy="USD")}
    out, actions = reconcile_seed_to_broker([], broker, as_of=AS_OF,
                                            fx_map={"AUD": 1.0})
    assert _by_tkr(actions)["SMH"]["action"] == "unpriceable"
    assert out == []


# --- broker holds FEWER (a missed sell) ------------------------------------

def test_missed_sell_reduces_fifo_oldest_first():
    seed = [_lot("HBRD.AX", 1000, 10.00, acq="2026-07-08T00:00:00"),
            _lot("HBRD.AX", 634, 10.20, acq="2026-07-20T00:00:00")]
    broker = {"HBRD.AX": _pos("HBRD.AX", 1508, 10.11, 10.05)}
    out, actions = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    assert _by_tkr(actions)["HBRD.AX"]["action"] == "reduced"
    assert sum(L["Units"] for L in out) == 1508
    # 126 released from the OLDEST lot, newest untouched
    oldest = [L for L in out if L["AcqDate"].startswith("2026-07-08")][0]
    newest = [L for L in out if L["AcqDate"].startswith("2026-07-20")][0]
    assert oldest["Units"] == 874
    assert newest["Units"] == 634


def test_missed_sell_consuming_a_whole_lot_drops_it():
    seed = [_lot("HBRD.AX", 100, 10.00, acq="2026-07-08T00:00:00"),
            _lot("HBRD.AX", 900, 10.20, acq="2026-07-20T00:00:00")]
    broker = {"HBRD.AX": _pos("HBRD.AX", 850, 10.20, 10.05)}
    out, _ = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    assert len(out) == 1
    assert out[0]["AcqDate"].startswith("2026-07-20")
    assert out[0]["Units"] == 850


def test_missed_sell_flags_unrecoverable_cgt():
    seed = [_lot("HBRD.AX", 1667, 10.11)]
    broker = {"HBRD.AX": _pos("HBRD.AX", 1508, 10.11, 10.05)}
    _, actions = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    assert "NOT recoverable" in _by_tkr(actions)["HBRD.AX"]["detail"]


def test_position_gone_from_broker_is_closed():
    seed = [_lot("SOXX", 53, 300.0)]
    broker = {"GOLD.AX": _pos("GOLD.AX", 369, 54.62, 53.09)}
    out, actions = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    assert _by_tkr(actions)["SOXX"]["action"] == "closed"
    assert not [L for L in out if L["Security"] == "SOXX"]


# --- watershed + safety ----------------------------------------------------

def test_every_lot_gets_the_new_seedasof_watershed():
    """Mixed stamps would let pre-seed fills replay and double-count."""
    seed = [_lot("GOLD.AX", 347, 54.68)]
    broker = {"GOLD.AX": _pos("GOLD.AX", 369, 54.62, 53.09)}
    out, _ = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    assert {L["SeedAsOf"] for L in out} == {AS_OF.isoformat()}


def test_unpriceable_position_is_left_alone_not_guessed():
    broker = {"XYZ": {"ticker": "XYZ", "units": 10, "avg_cost_local": None,
                      "mark_local": None, "mkt_value_base": None,
                      "currency": "USD"}}
    out, actions = reconcile_seed_to_broker([], broker, as_of=AS_OF)
    assert _by_tkr(actions)["XYZ"]["action"] == "unpriceable"
    assert out == []


def test_empty_broker_snapshot_changes_nothing_destructively():
    seed = [_lot("GOLD.AX", 369, 54.62)]
    out, actions = reconcile_seed_to_broker(seed, {}, as_of=AS_OF)
    # no broker truth at all -> nothing is asserted about the book
    assert out == []
    assert _by_tkr(actions)["GOLD.AX"]["action"] == "closed"


def test_reconcile_is_idempotent():
    seed = [_lot("GOLD.AX", 347, 54.68)]
    broker = {"GOLD.AX": _pos("GOLD.AX", 369, 54.62, 53.09)}
    once, _ = reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    twice, actions = reconcile_seed_to_broker(once, broker, as_of=AS_OF)
    assert _by_tkr(actions)["GOLD.AX"]["action"] == "ok"
    assert sum(L["Units"] for L in twice) == 369


def test_input_seed_is_not_mutated():
    seed = [_lot("HBRD.AX", 1667, 10.11)]
    broker = {"HBRD.AX": _pos("HBRD.AX", 1508, 10.11, 10.05)}
    reconcile_seed_to_broker(seed, broker, as_of=AS_OF)
    assert seed[0]["Units"] == 1667


# --- fx derivation ---------------------------------------------------------
# Built from the REAL 2026-08-03 snapshot: AUD block 157,731.98 local + USD
# block 36,571.84 local, reported as gross_positions_aud 209,654.30.

REAL_SNAPSHOT = {
    "gross_positions_aud": 209654.30,
    "positions": [
        _pos("BEAR.AX", 1644, 7.4037, 7.2769561),
        _pos("GOLD.AX", 369, 54.6222, 53.0900001),
        _pos("HBRD.AX", 1508, 10.1081, 10.0523996),
        _pos("VLUE.AX", 2974, 37.0406, 37.3300018),
        _pos("PDBC", 420, 17.3771, 17.6000004, ccy="USD"),
        _pos("SMH", 50, 606.0736, 538.4500122, ccy="USD"),
        _pos("VEA", 32, 70.5725, 70.5419540, ccy="USD"),
    ],
}


def test_fx_derived_from_real_snapshot_identity():
    fx = derive_fx_from_snapshot(REAL_SNAPSHOT)
    assert abs(fx["USD"] - USDAUD) < 1e-4
    assert fx["AUD"] == 1.0


def test_fx_is_not_one_for_usd():
    """The regression guard: dividing mkt_value_base by units*mark gives 1.0."""
    assert derive_fx_from_snapshot(REAL_SNAPSHOT)["USD"] > 1.3


def test_fx_omits_currency_when_two_foreign_blocks_are_ambiguous():
    snap = dict(REAL_SNAPSHOT)
    snap["positions"] = REAL_SNAPSHOT["positions"] + [
        _pos("IEU.L", 10, 50.0, 50.0, ccy="GBP")]
    fx = derive_fx_from_snapshot(snap)
    assert "USD" not in fx and "GBP" not in fx


def test_fx_rejects_implausible_rate():
    snap = {"gross_positions_aud": 1e9,
            "positions": [_pos("SMH", 50, 606.07, 538.45, ccy="USD")]}
    assert "USD" not in derive_fx_from_snapshot(snap)


def test_fx_handles_aud_only_and_empty_snapshots():
    assert derive_fx_from_snapshot(
        {"gross_positions_aud": 100.0,
         "positions": [_pos("GOLD.AX", 1, 50.0, 100.0)]}) == {"AUD": 1.0}
    assert derive_fx_from_snapshot({}) == {"AUD": 1.0}
    assert derive_fx_from_snapshot(None) == {"AUD": 1.0}


def test_end_to_end_usd_cost_base_from_real_snapshot():
    """The bug in full: PDBC went 304 -> 420 units. The 116 new units must be
    priced in AUD (~A$24.67), not left at the USD 17.38 the broker reports."""
    seed = [_lot("PDBC", 304, 24.7383, acq="2026-07-21T00:00:00")]
    broker = {p["ticker"]: p for p in REAL_SNAPSHOT["positions"]}
    fx = derive_fx_from_snapshot(REAL_SNAPSHOT)
    out, actions = reconcile_seed_to_broker(seed, broker, as_of=AS_OF, fx_map=fx)
    new = [L for L in out if L["Security"] == "PDBC" and L["Units"] == 116][0]
    assert new["CostBaseAUD"] > 20.0
    assert abs(sum(L["Units"] * L["CostBaseAUD"]
                   for L in out if L["Security"] == "PDBC")
               - 420 * 17.3771 * fx["USD"]) < 0.05

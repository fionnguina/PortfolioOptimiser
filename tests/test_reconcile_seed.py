"""Lot-seed reconciliation against broker truth (lots.reconcile_seed_to_broker).

The fills log freezes rows at qty_filled=0 whenever an order fills after the
placing session ends, and the broker serves no historical executions to repair
it from — so the lot book is realigned to POSITIONS instead. This is CGT-
critical: it decides cost bases and, via AcqDate, 12-month LT-discount
eligibility. These tests pin the behaviour that must not drift, especially the
things a naive re-seed would get wrong.
"""
from __future__ import annotations

import json
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


# --- pending-order watch (2026-08-03) --------------------------------------
# An order filling after the placing session ends is never confirmed: the row
# stays qty_filled=0 and --check-fills cannot see it. The reconcile resolves it
# either way, so it must SAY which — a non-fill used to be indistinguishable
# from silence, and it is the outcome that needs a human.

import ibkr_paper_exec as ex

MON_1300 = datetime(2026, 8, 3, 13, 0, 0)     # SMH submitted, US shut


def test_us_order_not_yet_decidable_before_its_close():
    # placed Mon 13:00 AEST; US close maps to Tue 08:00 AEST
    assert not ex._session_has_closed("SMH", MON_1300.isoformat(),
                                      now=datetime(2026, 8, 4, 7, 0))


def test_us_order_decidable_after_its_close():
    assert ex._session_has_closed("SMH", MON_1300.isoformat(),
                                  now=datetime(2026, 8, 4, 10, 20))


def test_asx_order_decidable_same_day_after_1600():
    placed = datetime(2026, 8, 3, 10, 25).isoformat()
    assert not ex._session_has_closed("VLUE.AX", placed,
                                      now=datetime(2026, 8, 3, 15, 0))
    assert ex._session_has_closed("VLUE.AX", placed,
                                  now=datetime(2026, 8, 3, 16, 30))


def test_wrapper_slot_can_always_decide_yesterdays_orders():
    """The 10:20 daily run must never be stuck on 'no verdict yet'."""
    run = datetime(2026, 8, 4, 10, 20)
    for tkr, placed in (("SMH", datetime(2026, 8, 3, 10, 25)),
                        ("VLUE.AX", datetime(2026, 8, 3, 10, 25))):
        assert ex._session_has_closed(tkr, placed.isoformat(), now=run)


def test_unparseable_timestamp_still_gets_a_verdict():
    assert ex._session_has_closed("SMH", "not-a-timestamp")
    assert ex._session_has_closed("SMH", None)


def _watch(tmp_path, rows, actions, now_row=None, resolved=None):
    p = tmp_path / "fills.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    return ex._pending_order_watch(p, actions, resolved=resolved)


def test_watch_reports_fill_when_units_moved(tmp_path):
    rows = [{"exec_timestamp": MON_1300.isoformat(), "ticker": "SMH",
             "side": "BUY", "qty_requested": 31, "qty_filled": 0,
             "is_done": False, "order_id": 97}]
    got = _watch(tmp_path, rows,
                 [{"ticker": "SMH", "action": "added", "units": 31,
                   "detail": ""}])
    assert got[0]["verdict"].startswith("FILLED")


def test_watch_reports_non_fill_once_session_closed(tmp_path):
    old = datetime(2026, 7, 20, 9, 35).isoformat()
    rows = [{"exec_timestamp": old, "ticker": "SMH", "side": "BUY",
             "qty_requested": 31, "qty_filled": 0, "is_done": False,
             "order_id": 97}]
    got = _watch(tmp_path, rows,
                 [{"ticker": "SMH", "action": "ok", "units": 0, "detail": ""}])
    assert got[0]["verdict"].startswith("DID NOT FILL")


def test_watch_will_not_call_a_live_order_dead(tmp_path):
    """THE double-buy guard: unchanged units before the session closes is not
    evidence of anything. Calling it a non-fill invites re-placing on top of a
    working order."""
    rows = [{"exec_timestamp": datetime.now().isoformat(), "ticker": "SMH",
             "side": "BUY", "qty_requested": 31, "qty_filled": 0,
             "is_done": False, "order_id": 97}]
    got = _watch(tmp_path, rows,
                 [{"ticker": "SMH", "action": "ok", "units": 0, "detail": ""}])
    assert got[0]["verdict"].startswith("STILL WORKING")
    assert "DID NOT FILL" not in got[0]["verdict"]


def test_watch_ignores_rows_that_already_confirmed_a_fill(tmp_path):
    rows = [{"exec_timestamp": MON_1300.isoformat(), "ticker": "SMH",
             "side": "BUY", "qty_requested": 31, "qty_filled": 31,
             "is_done": True, "order_id": 97}]
    assert _watch(tmp_path, rows, []) == []


def test_watch_empty_when_no_fills_log(tmp_path):
    assert ex._pending_order_watch(tmp_path / "nope.jsonl", []) == []


# --- perm-id resolution marker ------------------------------------------
# The fills-log row for a post-session fill stays qty_filled=0 forever, so it
# stays the "latest batch" and gets re-resolved every run. The units comparison
# is only meaningful BEFORE the fill is absorbed into the seed; afterwards
# "unchanged" means "already accounted for". Without a marker the verdict flips
# FILLED -> DID NOT FILL and tells the operator to re-buy what they own.

def _smh_row(ts, perm_id=634394398, order_id=97):
    return {"exec_timestamp": ts, "ticker": "SMH", "side": "BUY",
            "qty_requested": 31, "qty_filled": 0, "is_done": False,
            "order_id": order_id, "ibkr_perm_id": perm_id}


def test_watch_key_is_the_perm_id_not_the_session_order_id():
    """order_id restarts with each client connection and would collide across
    days; perm_id is the broker's permanent identity."""
    a = _smh_row(MON_1300.isoformat(), order_id=97)
    b = _smh_row(MON_1300.isoformat(), order_id=4)
    assert ex._watch_key(a) == ex._watch_key(b) == "perm:634394398"


def test_watch_key_falls_back_when_perm_id_missing_or_zero():
    base = {"exec_timestamp": MON_1300.isoformat(), "ticker": "smh",
            "side": "buy", "qty_requested": 31}
    k = ex._watch_key(base)
    assert k.startswith("sub:") and "SMH" in k and "BUY" in k
    assert ex._watch_key({**base, "ibkr_perm_id": 0}) == k
    assert ex._watch_key({**base, "ibkr_perm_id": None}) == k
    # A different order in the same batch must not collide.
    assert ex._watch_key({**base, "ticker": "VEA"}) != k


def test_resolved_order_is_not_re_reported(tmp_path):
    old = datetime(2026, 7, 20, 9, 35).isoformat()
    rows = [_smh_row(old)]
    actions = [{"ticker": "SMH", "action": "ok", "units": 0, "detail": ""}]
    assert _watch(tmp_path, rows, actions)[0]["verdict"].startswith("DID NOT")
    assert _watch(tmp_path, rows, actions,
                  resolved={"perm:634394398": {"verdict": "x"}}) == []


def test_absorbed_fill_never_flips_to_did_not_fill(tmp_path):
    """THE regression (SMH order 97, 2026-08-03). Day 1 the reconcile moves the
    units and reports FILLED. Day 2 the book already matches, so the same stale
    row would re-derive as DID NOT FILL — a false alarm inviting a double buy.
    Once marked, day 2 says nothing."""
    store = tmp_path / "resolved.json"
    rows = [_smh_row(MON_1300.isoformat())]

    day1 = _watch(tmp_path, rows,
                  [{"ticker": "SMH", "action": "added", "units": 31,
                    "detail": ""}],
                  resolved=ex._load_resolved_watch(store))
    assert day1[0]["verdict"].startswith("FILLED")
    ex._mark_watch_resolved(store, day1)

    day2 = _watch(tmp_path, rows,
                  [{"ticker": "SMH", "action": "ok", "units": 0, "detail": ""}],
                  resolved=ex._load_resolved_watch(store))
    assert day2 == []

    saved = json.loads(store.read_text(encoding="utf-8"))
    assert saved["perm:634394398"]["verdict"].startswith("FILLED")
    assert saved["perm:634394398"]["resolved_at"]


def test_non_terminal_verdicts_are_never_marked(tmp_path):
    """STILL WORKING is waiting on a close and UNRESOLVED may yet gain a
    position to compare against — marking either would bury a live order."""
    store = tmp_path / "resolved.json"
    ex._mark_watch_resolved(store, [
        {"ticker": "SMH", "side": "BUY", "qty": 31, "batch": "b", "key": "k1",
         "verdict": "STILL WORKING — its market has not closed"},
        {"ticker": "VEA", "side": "BUY", "qty": 19, "batch": "b", "key": "k2",
         "verdict": "UNRESOLVED — no broker position to compare against"}])
    assert not store.exists()
    assert ex._load_resolved_watch(store) == {}


def test_did_not_fill_is_reported_once_then_marked(tmp_path):
    store = tmp_path / "resolved.json"
    old = datetime(2026, 7, 20, 9, 35).isoformat()
    rows = [_smh_row(old)]
    actions = [{"ticker": "SMH", "action": "ok", "units": 0, "detail": ""}]
    first = _watch(tmp_path, rows, actions,
                   resolved=ex._load_resolved_watch(store))
    assert first[0]["verdict"].startswith("DID NOT FILL")
    ex._mark_watch_resolved(store, first)
    assert _watch(tmp_path, rows, actions,
                  resolved=ex._load_resolved_watch(store)) == []


def test_corrupt_or_missing_store_reads_empty_not_fatal(tmp_path):
    """Fail toward re-reporting: a duplicate email is recoverable, a swallowed
    non-fill is not."""
    assert ex._load_resolved_watch(tmp_path / "nope.json") == {}
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    assert ex._load_resolved_watch(bad) == {}
    notdict = tmp_path / "list.json"
    notdict.write_text("[1, 2]", encoding="utf-8")
    assert ex._load_resolved_watch(notdict) == {}


def test_store_is_pruned_to_newest_entries(tmp_path):
    store = tmp_path / "resolved.json"
    entries = {f"perm:{i}": {"verdict": "FILLED", "resolved_at":
                             f"2026-08-{(i % 28) + 1:02d}T09:00:00"}
               for i in range(12)}
    ex._save_resolved_watch(store, dict(entries), keep=5)
    kept = json.loads(store.read_text(encoding="utf-8"))
    assert len(kept) == 5
    newest = sorted(entries.items(), key=lambda kv: kv[1]["resolved_at"],
                    reverse=True)[:5]
    assert set(kept) == {k for k, _ in newest}

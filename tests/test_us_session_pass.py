"""Executing US legs in the US session, against the approved target weights.

The daily run fires at 10:20 AEST = 20:20 EDT the previous day — 4h20m after the
US close, and ~13h before the next US open. Every US leg was therefore an
overnight DAY order priced off a stale close, with no retry: SMH's repeated
non-fills, the frozen qty_filled=0 rows, and the false "DID NOT FILL" all trace
back to that one fact.

The fix is a second pass in the US session that executes the SAME approved plan.
What it must NOT do is re-optimise: the target weights are frozen from the
morning, and only the arithmetic turning them into units is redone at live
prices. These tests pin that distinction and the three refusals that bound it.
"""
from __future__ import annotations

import pytest

import ibkr_paper_exec as ex


# --- venue split ----------------------------------------------------------

@pytest.mark.parametrize("tkr,venue", [
    ("VLUE.AX", "ASX"), ("GOLD.AX", "ASX"), ("bear.ax", "ASX"),
    ("SMH", "US"), ("VEA", "US"), ("PDBC", "US"),
])
def test_venue_classification(tkr, venue):
    assert ex._venue_of(tkr) == venue


def test_open_order_guard_scopes_to_the_venue():
    """A leftover ASX order must not veto the US pass hours later — the ASX is
    shut by then and the order cannot stack with anything being submitted."""
    working = {"VLUE.AX": 430.0, "SMH": 31.0}
    assert ex._scope_open_orders(working, "US") == {"SMH": 31.0}
    assert ex._scope_open_orders(working, "ASX") == {"VLUE.AX": 430.0}


def test_unscoped_guard_is_unchanged():
    """Single-pass behaviour must be untouched when --venue is absent."""
    working = {"VLUE.AX": 430.0, "SMH": 31.0}
    assert ex._scope_open_orders(working, "") == working
    assert ex._scope_open_orders({}, "US") == {}


# --- re-solving units from the approved weights ---------------------------

NAV = 251_022.0


def _approved(tkr, delta, px, brokerage=1.5):
    return {tkr: {"delta_units": delta, "px_aud": px,
                  "brokerage_aud": brokerage}}


W, PX0, CUR = 0.2307, 767.24, 50
# What the morning engine itself would have produced at the stale price — the
# only honest baseline to compare a re-derivation against.
APPROVED_DELTA = int(round((W * NAV - CUR * PX0) / PX0))


def _gap(pct, nav=NAV):
    px1 = PX0 * (1.0 + pct)
    rows, findings = ex._rederive_to_targets(
        {"SMH": W}, {"SMH": CUR}, {"SMH": px1}, nav,
        _approved("SMH", APPROVED_DELTA, PX0), sigma={"SMH": 0.024},
        max_sigma=4.0)
    return px1, rows, findings


def test_gap_up_buys_fewer_units_for_the_same_weight():
    """THE case. Fixed units through a gap-up overshoot the target twice — the
    existing holding is worth more AND the same count costs more. Re-solving
    keeps the weight and leaves the AUD spend roughly flat."""
    px1, rows, _ = _gap(+0.05)
    r = rows[0]
    assert r["delta_units"] < APPROVED_DELTA
    assert (CUR + r["delta_units"]) * px1 / NAV == pytest.approx(W, abs=0.005)
    # Self-financing: fewer units at a higher price ≈ the same money.
    assert abs(r["delta_value_aud"]) < APPROVED_DELTA * px1
    # And decisively cheaper than executing the approved unit count would be.
    assert abs(r["delta_value_aud"]) == pytest.approx(
        APPROVED_DELTA * PX0, rel=0.30)


def test_gap_down_buys_more_units_for_the_same_weight():
    px1, rows, _ = _gap(-0.05)
    assert rows[0]["delta_units"] > APPROVED_DELTA
    assert (CUR + rows[0]["delta_units"]) * px1 / NAV == pytest.approx(
        W, abs=0.005)


def test_weight_still_lands_on_target_when_nav_moves_with_the_price():
    """The realistic case: NAV is read live, so a gap in a 23%-weight holding
    moves NAV too. The target is a SHARE of that NAV, so it must still land."""
    pct = 0.05
    nav_after = NAV + CUR * PX0 * pct          # the holding revalues
    px1, rows, _ = _gap(pct, nav=nav_after)
    got = (CUR + rows[0]["delta_units"]) * px1 / nav_after
    assert got == pytest.approx(W, abs=0.005)


def test_flat_price_reproduces_the_approved_trade():
    """No movement must mean no surprise: re-solving is a no-op."""
    w, px = 0.2307, 767.24
    cur = 50
    approved_delta = int(round((w * NAV - cur * px) / px))
    rows, _ = ex._rederive_to_targets(
        {"SMH": w}, {"SMH": cur}, {"SMH": px}, NAV,
        _approved("SMH", approved_delta, px), sigma={"SMH": 0.024})
    assert rows[0]["delta_units"] == approved_delta


def test_targets_are_frozen_not_reoptimised():
    """The weight comes from the approved plan. A ticker absent from
    target_weights is treated as target 0 and sold down — never invented."""
    rows, _ = ex._rederive_to_targets(
        {}, {"SMH": 50}, {"SMH": 800.0}, NAV, _approved("SMH", -50, 800.0))
    assert rows[0]["delta_units"] == -50


# --- the three refusals ---------------------------------------------------

def test_sign_flip_is_dropped_never_reversed():
    """A move big enough to turn the approved BUY into a SELL means the
    morning's decision is stale. Dropping is safe; reversing is not."""
    rows, findings = ex._rederive_to_targets(
        {"SMH": 0.10}, {"SMH": 50}, {"SMH": 900.0}, NAV,
        _approved("SMH", 31, 767.24), sigma={"SMH": 0.50})  # vol wide enough
    assert rows == []                                        # so drift lets it through
    assert any("reverses the approved direction" in f for f in findings)


def test_drift_beyond_the_vol_ceiling_is_dropped():
    rows, findings = ex._rederive_to_targets(
        {"SMH": 0.2307}, {"SMH": 50}, {"SMH": 767.24 * 1.20}, NAV,
        _approved("SMH", 31, 767.24), sigma={"SMH": 0.024}, max_sigma=3.0)
    assert rows == []
    assert any("moved 20.0%" in f and "3x its 2.4% daily vol" in f
               for f in findings)


def test_the_ceiling_is_vol_scaled_not_flat():
    """2% on HBRD and 2% on SMH are not the same event. A 6% move passes for a
    high-vol name and is refused for a low-vol one, at the same max_sigma."""
    common = dict(nav_aud=NAV, max_sigma=3.0)
    hi, _ = ex._rederive_to_targets(
        {"SMH": 0.23}, {"SMH": 50}, {"SMH": 767.24 * 1.06},
        approved=_approved("SMH", 31, 767.24), sigma={"SMH": 0.024}, **common)
    lo, findings = ex._rederive_to_targets(
        {"HBRD.AX": 0.06}, {"HBRD.AX": 1508}, {"HBRD.AX": 10.07 * 1.06},
        approved=_approved("HBRD.AX", -840, 10.07), sigma={"HBRD.AX": 0.003},
        **common)
    assert len(hi) == 1                       # 6% is 2.5 sigma for SMH — allowed
    assert lo == [] and any("HBRD.AX" in f for f in findings)   # 20 sigma — refused


def test_unknown_vol_falls_back_loudly_and_still_guards():
    """An unknown vol must not silently disable the check — that would leave the
    guard permanently inert, which is the same as never writing it."""
    rows, findings = ex._rederive_to_targets(
        {"SMH": 0.2307}, {"SMH": 50}, {"SMH": 767.24 * 1.30}, NAV,
        _approved("SMH", 31, 767.24), sigma={}, max_sigma=3.0,
        fallback_sigma=0.02)
    assert rows == []
    assert any("vol unknown" in f and "flat 2.0%/day assumed" in f
               for f in findings)


def test_unpriceable_leg_is_dropped_not_guessed():
    rows, findings = ex._rederive_to_targets(
        {"SMH": 0.23}, {"SMH": 50}, {"SMH": 0.0}, NAV,
        _approved("SMH", 31, 767.24))
    assert rows == [] and "no usable live price" in findings[0]


def test_missing_nav_aborts_everything():
    """Without NAV a weight cannot become a unit count. Refuse, never assume."""
    for bad in (0, None, -1, "x"):
        rows, findings = ex._rederive_to_targets(
            {"SMH": 0.23}, {"SMH": 50}, {"SMH": 800.0}, bad,
            _approved("SMH", 31, 767.24))
        assert rows == [] and "REPRICE ABORT" in findings[0]


def test_missing_reference_price_reports_drift_unchecked():
    rows, findings = ex._rederive_to_targets(
        {"SMH": 0.2307}, {"SMH": 50}, {"SMH": 800.0}, NAV,
        {"SMH": {"delta_units": 31, "px_aud": 0}}, sigma={"SMH": 0.024})
    assert any("drift UNCHECKED" in f for f in findings)
    assert len(rows) == 1          # reported, but not blocked


# --- housekeeping ---------------------------------------------------------

def test_zero_delta_legs_are_dropped_silently():
    w, px, cur = 0.2307, 767.24, None
    cur = int(round(w * NAV / px))
    rows, findings = ex._rederive_to_targets(
        {"SMH": w}, {"SMH": cur}, {"SMH": px}, NAV, _approved("SMH", 0, px))
    assert rows == [] and findings == []


def test_min_parcel_skips_are_reported():
    rows, findings = ex._rederive_to_targets(
        {"A200.AX": 0.0006}, {"A200.AX": 0}, {"A200.AX": 154.0}, NAV,
        _approved("A200.AX", 1, 154.0), min_trade_aud=500.0)
    assert rows == [] and any("below the $500" in f for f in findings)


def test_brokerage_is_carried_from_the_approved_plan():
    rows, _ = ex._rederive_to_targets(
        {"SMH": 0.2307}, {"SMH": 50}, {"SMH": 780.0}, NAV,
        _approved("SMH", 31, 767.24, brokerage=1.82), sigma={"SMH": 0.024})
    assert rows[0]["brokerage_aud"] == 1.82


def test_rows_carry_the_approved_delta_for_the_audit_trail():
    rows, _ = ex._rederive_to_targets(
        {"SMH": 0.2307}, {"SMH": 50}, {"SMH": 780.0}, NAV,
        _approved("SMH", 31, 767.24), sigma={"SMH": 0.024})
    assert rows[0]["approved_delta_units"] == 31
    assert rows[0]["side"] == "buy"

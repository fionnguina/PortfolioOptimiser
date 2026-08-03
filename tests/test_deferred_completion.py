"""Guard logic for the cash-deferred-buy auto-completer (ibkr_paper_exec).

The decision function is unattended-trade-execution-critical: it must fail SAFE
(refuse) whenever it can't verify price/staleness/funds. These tests pin every
branch so a future edit can't silently loosen a guard.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import ibkr_paper_exec as ex

NOW = datetime(2026, 7, 21, 9, 30, 0)               # a TUESDAY
FRESH = (NOW - timedelta(hours=6)).isoformat()      # 6h ago — within age guard
# 60h back from Tue 09:30 lands on a Saturday — which no longer ages the plan.
# Use a genuine weekday-spanning gap instead: Thu 2026-07-16 18:00 -> Tue 09:30
# is 63.5 BUSINESS hours (beyond the 48h guard) and 111.5h wall (inside the
# 120h ceiling), so it exercises the business-hours branch specifically.
STALE = datetime(2026, 7, 16, 18, 0, 0).isoformat()


def _decide(**kw):
    base = dict(
        approved_price_local=100.0, current_price_local=101.0,
        need_aud=50_000.0, funds_aud=90_000.0,
        deferred_at_iso=FRESH, now=NOW, drift_pct=3.0, max_age_hours=48.0,
    )
    base.update(kw)
    return ex._deferred_completion_decision(**base)


def test_all_guards_pass_submits():
    action, _ = _decide()
    assert action == "submit"


def test_price_drift_over_threshold_aborts():
    # 100 -> 110 = +10% > 3%
    action, reason = _decide(current_price_local=110.0)
    assert action == "abort_drift"
    assert "moved" in reason


def test_price_drift_just_under_threshold_submits():
    # 100 -> 102.9 = +2.9% < 3%
    action, _ = _decide(current_price_local=102.9)
    assert action == "submit"


def test_missing_current_price_refuses():
    action, _ = _decide(current_price_local=None)
    assert action == "abort_no_price"


def test_missing_approved_price_refuses():
    action, _ = _decide(approved_price_local=None)
    assert action == "abort_no_price"


def test_stale_deferral_aborts_before_price_check():
    # even with a fine price, an old deferral is refused
    action, reason = _decide(deferred_at_iso=STALE, current_price_local=100.5)
    assert action == "abort_stale"
    assert "too old" in reason


def test_insufficient_funds_defers_for_retry():
    action, reason = _decide(need_aud=95_000.0, funds_aud=90_000.0)
    assert action == "defer_funds"
    assert "insufficient" in reason


def test_funds_exactly_sufficient_submits():
    action, _ = _decide(need_aud=90_000.0, funds_aud=90_000.0)
    assert action == "submit"


def test_unknown_funds_does_not_block_submit():
    # funds_aud None (query failed) → funds guard is skipped, price/age still gate
    action, _ = _decide(funds_aud=None)
    assert action == "submit"


def test_drift_checked_in_local_ccy_symmetric():
    # a downward move also trips the guard
    action, _ = _decide(approved_price_local=100.0, current_price_local=90.0)
    assert action == "abort_drift"


# --- Weekend-aware staleness (2026-08-03) -----------------------------------
# A buy deferred on a Friday was next checked on Monday at 72h wall-clock and
# aborted by the 48h guard EVERY time — a deterministic kill that binned SOXX
# (2026-07-24 -> 07-27) and SMH (2026-07-31 -> 08-03). The market was shut for
# 2 of those 3 days, so the plan had not actually gone stale.

FRI_1735 = datetime(2026, 7, 31, 9, 35, 0)    # Friday, the 09:35 exec window
MON_1020 = datetime(2026, 8, 3, 10, 20, 0)    # the next scheduled run


def test_friday_deferral_survives_to_monday():
    """THE regression: 72h wall-clock, but only ~25 business hours."""
    action, _ = _decide(deferred_at_iso=FRI_1735.isoformat(), now=MON_1020)
    assert action == "submit"


def test_friday_deferral_was_killed_under_wall_clock_rule():
    """Pins WHY the fix was needed: the old wall-clock age really was >48h."""
    assert (MON_1020 - FRI_1735).total_seconds() / 3600.0 > 48.0


def test_business_hours_skips_whole_weekend():
    # Fri 09:35 -> Mon 10:20: Fri tail 14.42h + Mon 10.33h, Sat/Sun excluded
    got = ex._business_hours_between(FRI_1735, MON_1020)
    assert abs(got - (14.4167 + 10.3333)) < 0.01


def test_business_hours_pure_weekend_is_zero():
    sat = datetime(2026, 8, 1, 0, 0, 0)
    mon = datetime(2026, 8, 3, 0, 0, 0)
    assert ex._business_hours_between(sat, mon) == 0.0


def test_business_hours_reversed_range_is_zero():
    assert ex._business_hours_between(MON_1020, FRI_1735) == 0.0


def test_wall_clock_ceiling_aborts_even_when_business_hours_pass():
    """The ceiling bounds a loosened --max-age-hours: business guard wide open,
    but 6 wall days is past the 120h hard stop."""
    old = (MON_1020 - timedelta(days=6)).isoformat()
    action, reason = _decide(deferred_at_iso=old, now=MON_1020,
                             max_age_hours=500.0)
    assert action == "abort_stale"
    assert "wall-clock ceiling" in reason


def test_unparseable_deferred_at_does_not_crash_guard():
    # a corrupt timestamp must fall through to the price/funds guards, not raise
    action, _ = _decide(deferred_at_iso="not-a-timestamp")
    assert action == "submit"

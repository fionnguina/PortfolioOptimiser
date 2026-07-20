"""Guard logic for the cash-deferred-buy auto-completer (ibkr_paper_exec).

The decision function is unattended-trade-execution-critical: it must fail SAFE
(refuse) whenever it can't verify price/staleness/funds. These tests pin every
branch so a future edit can't silently loosen a guard.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import ibkr_paper_exec as ex

NOW = datetime(2026, 7, 21, 9, 30, 0)
FRESH = (NOW - timedelta(hours=6)).isoformat()      # 6h ago — within age guard
STALE = (NOW - timedelta(hours=60)).isoformat()     # 60h ago — beyond 48h guard


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

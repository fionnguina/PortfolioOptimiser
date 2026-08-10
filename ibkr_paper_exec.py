r"""IBKR Phase 3 — paper-account execution.

Submits marketable LIMIT orders (touch ± LIMIT_COLLAR_PCT) to the connected
PAPER IBKR account for every line in the most recent
trade_recommendation_log.jsonl entry. Tracks fills, writes
ibkr_fills_log.jsonl, and prints a reconciliation summary (slippage vs
last_price from the rec log, fill rate, status counts).

USAGE:
  # Preview only — never places orders even if TWS is connected (Phase 2 behaviour):
  & ".\.venv\Scripts\python.exe" ibkr_paper_exec.py

  # Actually execute — requires typed YES confirmation:
  & ".\.venv\Scripts\python.exe" ibkr_paper_exec.py --execute

  # Use a non-default rec log path:
  & ".\.venv\Scripts\python.exe" ibkr_paper_exec.py --execute --rec-log my_run.jsonl

  # Don't wait for fills (fire-and-forget, useful when MKT orders fire and
  # close events take >60s; not recommended for first runs):
  & ".\.venv\Scripts\python.exe" ibkr_paper_exec.py --execute --no-wait

SAFETY (layered):
  1. PORT constant hardcoded to 7497 (PAPER). The live port 7496 appears
     ONLY in safety-documentation comments — never assigned to PORT and
     never passed to ib.connect(). The single ib.connect call uses PORT
     directly with no override.
  2. Account-prefix check (_refuse_if_live): ib.managedAccounts() must
     return at least one account starting with 'DU'. Any other prefix
     calls SystemExit. This runs BEFORE qualifying contracts and BEFORE
     any order is built.
  3. --execute defaults to False. Without it, main() returns from the
     preview branch before ever reaching the connection / placeOrder
     code path. With it, the preview still prints and the user must pass
     the typed-YES gate (step 4) before placeOrder is reached.
  4. Interactive YES gate (_confirm_typed_yes): after preview prints, we
     prompt "Type YES to place N orders". Input must be exactly the
     four-character string "YES" (case-sensitive). Anything else aborts
     before the order-submit loop.
  5. The only ib.placeOrder() call is inside _submit_orders(), which is
     only called from main() after steps 2, 3, and 4 all pass. Per-order
     try/except so one rejection doesn't abort the batch mid-flight
     (which could leave the book in a half-rebalanced state).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Reuse the proven contract-builder + preview formatter + safety check from
# the Phase 2 dry-run. Keeping them in one place means a fix to one (e.g.
# ASX.AX -> ASX symbol stripping) propagates to both flows.
from ibkr_dry_run import (
    HOST,
    _load_latest_run,
    _ticker_to_contract,
    _refuse_if_live,
    _print_preview,
)


PORT = 7497              # PAPER (live = 7496, never used here)
CLIENT_ID = 12           # distinct from dry_run (11), engine (10), price_check (9), seed (8), paper_test (7)
CONNECT_TIMEOUT = 12
FILL_WAIT_SECONDS = 60   # how long to wait for orders to settle before summarising
# Marketable-LIMIT collar (%): orders go in as LIMIT, not MARKET, priced at the
# current local quote ± this band. Rationale (2026-07-24): a MARKET order placed
# off-hours fills at the next open at WHATEVER price it gaps to — unbounded
# slippage on an unattended order. A marketable limit still fills immediately
# under normal conditions (spread on these ETFs « 1%) but REFUSES to fill beyond
# the band, so an abnormal gap defers to the next run instead of executing blind.
# The band caps the WORST fill; the order still fills at the better touch price.
LIMIT_COLLAR_PCT = 1.0
FILLS_LOG_FILENAME = "ibkr_fills_log.jsonl"
REC_LOG_FILENAME = "trade_recommendation_log.jsonl"
DEFERRED_ORDERS_FILENAME = "deferred_orders.json"  # cash-deferred buys awaiting completion
# Terminal verdicts already reported for pending orders, keyed by broker perm id.
# Without this the same stale fills-log row is re-resolved every run, and once
# its fill has been absorbed into the seed the units comparison flips FILLED ->
# DID NOT FILL — telling the operator to re-place an order they already own.
PENDING_WATCH_FILENAME = "pending_watch_resolved.json"

# --- Guarded auto-completion of cash-deferred buys (2026-07-20) --------------
# When a rebalance defers a buy (e.g. a US buy funded by US-sell proceeds while
# the US market is shut), the morning wrapper can auto-complete it — but ONLY
# behind these guards, because it is unattended trade execution:
#   * price-drift: skip if the price moved more than this since the plan was
#     approved (the approved dollar-exposure is then stale);
#   * staleness: skip if the deferral is older than this (the plan is no longer
#     current — a regime may have turned);
#   * funds: skip if the sells still haven't freed enough cash.
# Fail-safe: if the price can't be verified at all, REFUSE (never auto-trade
# blind). All defaults overridable via CLI flags.
#
# Staleness is measured in BUSINESS hours, not wall-clock (2026-08-03). The
# 48h wall-clock guard could not survive a weekend: a buy deferred on a Friday
# was next checked on Monday at 72h and aborted EVERY time — a deterministic
# kill, observed on SOXX (2026-07-24 -> 07-27) and SMH (2026-07-31 -> 08-03).
# A plan does not go stale while the market is shut, so weekend hours don't
# count. WALL-clock is still bounded by a hard ceiling so the relaxation can
# never let a genuinely ancient plan through unattended.
DEFERRED_DRIFT_PCT_DEFAULT = 3.0        # abort if |Δprice| > 3% vs approved
DEFERRED_MAX_AGE_HOURS_DEFAULT = 48     # abort if older than 48 BUSINESS hours
DEFERRED_MAX_WALL_AGE_HOURS_DEFAULT = 120  # hard ceiling: 5 wall days, regardless

# Anchor log writes to this script's directory, NOT the process CWD. The daily
# scheduled task runs with WorkingDir unset (CWD = C:\Windows\System32), so a
# relative "ibkr_nav_log.jsonl" write there fails with PermissionError — which
# is why the broker-NAV log stopped accumulating after the initial manual row.
_SCRIPT_DIR = Path(__file__).resolve().parent


def _confirm_typed_yes(n_orders: int) -> bool:
    """Block until user types exactly 'YES' (case-sensitive). Anything else aborts.
    Print is unconditional so the user can see what they're confirming even when
    stdout is piped."""
    prompt = (
        f"\n[exec][CONFIRM] About to submit {n_orders} marketable LIMIT order(s) to the "
        f"PAPER account.\n"
        f"           Type the four-character string YES (uppercase) to proceed,\n"
        f"           or anything else to abort.\n"
        f"           > "
    )
    try:
        reply = input(prompt)
    except EOFError:
        print("[exec][SAFETY] stdin closed — refusing to proceed.")
        return False
    if reply != "YES":
        print(f"[exec][SAFETY] confirmation was '{reply}', expected 'YES'. Aborting.")
        return False
    return True


def _submit_orders(ib, plan: list, *, wait: bool) -> list:
    """Place every order in the plan and (optionally) wait for them to settle.
    Returns the list of (rec, Trade) tuples.

    Flushes the event loop between submissions and after the batch so that
    IBKR's PreSubmitted -> Submitted -> Filled/Cancelled transitions land on
    each Trade's orderStatus before we read it. The 2026-06-22 first-run bug
    (all 12 trades logged as 'Cancelled' with permId=0 despite 6 actually
    filling per TWS) was caused by reading orderStatus before the async
    status events were processed."""
    trades = []
    for i, (rec, contract, order) in enumerate(plan, 1):
        try:
            trade = ib.placeOrder(contract, order)
            trades.append((rec, trade))
            print(f"[exec] [{i:>3}/{len(plan)}] submitted "
                  f"{order.action} {int(order.totalQuantity):>6} {rec['ticker']:<10} "
                  f"orderId={trade.order.orderId}")
            # Yield to the event loop briefly so the placeOrder ack and any
            # immediate status updates flow before we submit the next order.
            ib.sleep(0.1)
        except Exception as e:
            print(f"[exec][ERR] {rec['ticker']} placeOrder failed "
                  f"({type(e).__name__}): {e}")
            continue

    if not wait:
        print(f"[exec] --no-wait set: returning before fills settle.")
        return trades

    if not trades:
        return trades

    # Initial 2s flush so PreSubmitted/Submitted statuses + permIds land
    # before our first read.
    ib.sleep(2)
    print()
    print("[exec] post-submit status snapshot:")
    for rec, t in trades:
        try:
            permid = int(getattr(t.order, "permId", 0) or 0)
            print(f"  {rec['ticker']:<10} orderId={t.order.orderId:>6} "
                  f"status={t.orderStatus.status:<15} "
                  f"permId={permid:>10} "
                  f"filled={int(t.filled() or 0):>6} "
                  f"remaining={int(t.remaining() or 0):>6}")
        except Exception as e:
            print(f"  {rec['ticker']:<10} (status read failed: {e})")
    print()

    print(f"[exec] waiting up to {FILL_WAIT_SECONDS}s for orders to reach a "
          f"terminal state (isDone)...")
    deadline = time.time() + FILL_WAIT_SECONDS
    last_print = time.time()
    while time.time() < deadline:
        # Trade.isDone() is the canonical terminal-state check — catches
        # Filled, Cancelled, ApiCancelled, Inactive, and other terminal
        # variants without us hardcoding the list.
        try:
            all_done = all(t.isDone() for _, t in trades)
        except Exception:
            all_done = False
        if all_done:
            print(f"[exec] all {len(trades)} orders reached terminal state.")
            break
        # Periodic status print every ~10s so a long wait isn't silent.
        if time.time() - last_print >= 10:
            pending = [(r["ticker"], t.orderStatus.status)
                       for r, t in trades if not t.isDone()]
            elapsed = int(FILL_WAIT_SECONDS - (deadline - time.time()))
            print(f"[exec] [{elapsed:>3}s] still pending: "
                  + ", ".join(f"{tk}={st}" for tk, st in pending[:10])
                  + (f" (+{len(pending)-10} more)" if len(pending) > 10 else ""))
            last_print = time.time()
        # waitOnUpdate returns as soon as ANY event arrives, more responsive
        # than a fixed-interval sleep.
        try:
            ib.waitOnUpdate(timeout=1)
        except Exception:
            ib.sleep(1)
    else:
        not_done = [(r["ticker"], t.orderStatus.status)
                    for r, t in trades if not t.isDone()]
        print(f"[exec][WARN] {len(not_done)}/{len(trades)} orders did not reach "
              f"terminal state within {FILL_WAIT_SECONDS}s. Logged as pending: "
              + ", ".join(f"{tk}={st}" for tk, st in not_done))

    # Final 2s flush to catch any straggler fill events before the log write.
    ib.sleep(2)
    return trades


# ============================================================================
# Guarded auto-completion of cash-deferred buys
# ============================================================================

def _current_local_price(ib, contract) -> "float | None":
    """Best-effort current local-ccy price via IBKR market data. Returns None on
    any failure (no subscription, NaN) so callers can fail-safe."""
    try:
        tks = ib.reqTickers(contract)
        if tks:
            t = tks[0]
            for v in (t.marketPrice(), getattr(t, "last", None),
                      getattr(t, "close", None)):
                try:
                    fv = float(v)
                    if fv == fv and fv > 0:   # not NaN, positive
                        return fv
                except (TypeError, ValueError):
                    continue
    except Exception:
        pass
    return None


def _last_close_via_history(ib, contract) -> "float | None":
    """Last daily close via reqHistoricalData — a subscription-INDEPENDENT price
    reference. Historical bars come from the HMDS farm, which is available without
    a top-of-book market-data subscription, so this works for the cap-0.0 names a
    flatten targets (SOXX tripped Error 354/10168 on both live and delayed
    top-of-book). Returns the most recent valid daily close, or None on any
    failure (fail-safe — caller drops the order rather than price it blind)."""
    try:
        bars = ib.reqHistoricalData(
            contract, endDateTime="", durationStr="5 D",
            barSizeSetting="1 day", whatToShow="TRADES",
            useRTH=True, formatDate=1)
        for b in reversed(bars or []):
            try:
                px = float(b.close)
                if px == px and px > 0:   # not NaN, positive
                    return px
            except (TypeError, ValueError):
                continue
    except Exception:
        pass
    return None


def _daily_vol_via_history(ib, contract, lookback: int = 40) -> "float | None":
    """Fractional daily return vol from HMDS daily bars, or None.

    Computed here rather than read off the engine's Sigma deliberately: Sigma is
    annualised and lives in the frozen exe, so consuming it would couple this
    guard to a rebuild AND to an annualisation convention that has to be got
    right silently. Daily bars are unambiguous and come from the same
    subscription-independent farm the price fallback already uses.

    None means 'unknown' — the caller must NOT read that as 'no drift'."""
    try:
        bars = ib.reqHistoricalData(
            contract, endDateTime="", durationStr=f"{int(lookback)} D",
            barSizeSetting="1 day", whatToShow="TRADES",
            useRTH=True, formatDate=1)
        closes = []
        for b in bars or []:
            try:
                px = float(b.close)
                if px == px and px > 0:
                    closes.append(px)
            except (TypeError, ValueError):
                continue
        if len(closes) < 10:
            return None
        rets = [closes[i] / closes[i - 1] - 1.0 for i in range(1, len(closes))]
        n = len(rets)
        mean = sum(rets) / n
        var = sum((r - mean) ** 2 for r in rets) / (n - 1)
        sd = var ** 0.5
        return sd if sd > 0 else None
    except Exception:
        return None


def _ref_local_price(ib, contract) -> "float | None":
    """Local-ccy reference price with a subscription-independent fallback: live/
    delayed top-of-book first, then the last daily close from the HMDS farm. The
    fallback is what makes a US name priceable during an ASX-hours run (US market
    shut → no top-of-book quote); without it US legs are silently dropped. Mirrors
    the --flatten pricing chain. Returns None only if BOTH sources fail."""
    return _ref_local_price_sourced(ib, contract)[0]


def _ref_local_price_sourced(ib, contract) -> tuple:
    """As _ref_local_price, but returns (price, source) where source is
    'live' | 'hist' | None.

    The caller usually does not care. The US-session re-pricing pass DOES: its
    whole premise is re-solving units against a LIVE price, and this account has
    no US real-time market-data subscription (verified 2026-07-30 — reqTickers
    on SMH/PDBC/VEA returns Error 10089/10168), so the live leg returns None for
    US names and the HMDS fallback quietly supplies a historical bar instead.
    That degradation is invisible in the price alone, which is exactly why it
    has to be reported rather than assumed."""
    ref = _current_local_price(ib, contract)
    if ref is not None and ref > 0:
        return (ref, "live")
    ref = _last_close_via_history(ib, contract)
    if ref is not None and ref > 0:
        return (ref, "hist")
    return (None, None)


def _marketable_limit_price(ref_local: float, side: str, collar_pct: float) -> float:
    """Marketable LIMIT price (local ccy): BUY caps at ref*(1+collar), SELL floors
    at ref*(1-collar). The band bounds the WORST acceptable fill (gap protection)
    — a limit order still fills at the better touch price. Rounded to 2dp: every
    traded ETF here is >$2, so a 1-cent tick is always valid."""
    mult = (1.0 + collar_pct / 100.0) if str(side).upper() == "BUY" else (1.0 - collar_pct / 100.0)
    return round(float(ref_local) * mult, 2)


def _price_orders_as_limits(ib, plan: list, collar_pct: float) -> tuple:
    """Set a marketable-LIMIT price on every order in `plan` from its current
    local quote. Returns (priced, unpriceable). An order whose price can't be
    verified is DROPPED into `unpriceable` — never submit a limit blind (fail-safe;
    it retries next run). For .AX the rec's px_aud IS the local (AUD) price, a
    safe fallback when the live quote is momentarily unavailable; US names have no
    local price in the rec log (px_aud is AUD), so they fall back to the last daily
    close from the HMDS farm (subscription-independent, available off-hours) — this
    is what lets a US buy placed during ASX hours (US market shut) be priced as a
    resting marketable LIMIT instead of being silently dropped every run."""
    priced, unpriceable = [], []
    for rec, contract, order in plan:
        ref = _current_local_price(ib, contract)
        if ref is None or ref <= 0:
            if str(rec.get("ticker", "")).endswith(".AX"):
                try:
                    ref = float(rec.get("px_aud") or 0) or None
                except (TypeError, ValueError):
                    ref = None
            else:
                # US name, no live quote (market shut) → historical close.
                ref = _last_close_via_history(ib, contract)
        if ref is None or ref <= 0:
            unpriceable.append((rec, contract, order))
            continue
        order.lmtPrice = _marketable_limit_price(ref, order.action, collar_pct)
        priced.append((rec, contract, order))
    return priced, unpriceable


def _business_hours_between(start: datetime, end: datetime) -> float:
    """Hours between start and end EXCLUDING whole weekend days (Sat/Sun).

    PURE. Used for the deferral staleness guard: a plan approved on Friday is
    not 72h stale on Monday morning — the market was shut for 2 of those days,
    so nothing could have moved. Walks midnight-to-midnight so partial first/
    last days are counted correctly. Returns 0.0 if end <= start."""
    if end <= start:
        return 0.0
    total = 0.0
    cur = start
    while cur < end:
        next_midnight = (cur.replace(hour=0, minute=0, second=0, microsecond=0)
                         + timedelta(days=1))
        seg_end = min(next_midnight, end)
        if cur.weekday() < 5:          # Mon=0 .. Fri=4
            total += (seg_end - cur).total_seconds() / 3600.0
        cur = seg_end
    return total


def _deferred_completion_decision(*, approved_price_local, current_price_local,
                                  need_aud, funds_aud, deferred_at_iso, now,
                                  drift_pct, max_age_hours,
                                  max_wall_age_hours=DEFERRED_MAX_WALL_AGE_HOURS_DEFAULT):
    """PURE guard for auto-completing one deferred buy. No I/O — unit-tested.

    Returns (action, reason) where action is one of:
      submit          — all guards pass, safe to place the order
      abort_stale     — older than max_age_hours BUSINESS hours, or past the
                        max_wall_age_hours wall-clock ceiling (plan too old)
      abort_drift     — price moved more than drift_pct since approval
      abort_no_price  — price can't be verified → refuse to auto-trade blind
      defer_funds     — sells still haven't freed enough cash → try again later
    Order of checks is deliberate: staleness → price → funds. Fail-safe: any
    missing price is a REFUSAL, never a silent proceed."""
    # 1) staleness — business hours (weekends don't age a plan), plus a hard
    #    wall-clock ceiling so the weekend allowance can't be stretched forever.
    if deferred_at_iso:
        try:
            _at = datetime.fromisoformat(deferred_at_iso)
            wall_h = (now - _at).total_seconds() / 3600.0
            biz_h = _business_hours_between(_at, now)
            if wall_h > max_wall_age_hours:
                return "abort_stale", (f"deferred {wall_h:.0f}h ago > {max_wall_age_hours}h "
                                       f"wall-clock ceiling — plan too old, "
                                       f"complete manually")
            if biz_h > max_age_hours:
                return "abort_stale", (f"deferred {biz_h:.0f} business-h ago "
                                       f"({wall_h:.0f}h wall) > {max_age_hours}h guard "
                                       f"— plan too old, complete manually")
        except Exception:
            pass
    # 2) price drift — BOTH prices required; missing either = refuse
    if not approved_price_local or not current_price_local:
        return "abort_no_price", ("could not verify current vs approved price — "
                                  "refusing to auto-execute blind, complete manually")
    drift = abs(current_price_local - approved_price_local) / approved_price_local
    if drift > drift_pct / 100.0:
        return "abort_drift", (f"price moved {drift*100:+.1f}% "
                               f"({approved_price_local:.2f} -> {current_price_local:.2f}) "
                               f"> {drift_pct:.1f}% guard — review manually")
    # 3) funds
    if funds_aud is not None and need_aud is not None and need_aud > funds_aud:
        return "defer_funds", (f"still insufficient (need ${need_aud:,.0f} > "
                               f"${funds_aud:,.0f}) — will retry next run")
    return "submit", (f"drift {drift*100:+.1f}% within {drift_pct:.1f}%, funds OK")


# ============================================================================
# Pre-trade validation gate (broker-truth; the guardrail for safe autonomy)
# ============================================================================

def validate_pre_trade(trades: list, assumed_positions: dict, broker_positions: dict,
                       available_cash_aud, nav_aud, *, max_turnover: float = 2.0,
                       unit_tol: float = 1.0, open_orders: "dict | None" = None,
                       data_farm_broken: bool = False, data_farm_reason: str = "") -> tuple[bool, list]:
    """PURE pre-trade safety gate — runs against BROKER TRUTH before any order.
    Returns (ok, failures). The engine's _validate_trade_plan_sanity runs on the
    SHEET; this runs on the broker, catching the exact 2026-07-23 failure the
    sheet-side check couldn't (a stale sheet made turnover look fine while the
    real book held a naked SOXX short the plan ignored).

    trades:            [{ticker, delta_units, delta_value_aud}, ...] (the plan)
    assumed_positions: {ticker: units} the plan was built on (rec-log current_units)
    broker_positions:  {ticker: units} truth from ib.positions()
    open_orders:       {ticker: signed working units} from ib.openTrades() — orders
                       from a prior run NOT yet in a terminal state. Optional; None
                       skips the check (back-compat + unit tests that don't set it).
    data_farm_broken:  True if IBKR's market-data farm reported BROKEN (code 2103)
                       on this session — the feed is down, so market orders would
                       sit unfilled (the 2026-07-24 incident: TWS up, data farm
                       down, orders stuck PreSubmitted for 4 days). data_farm_reason
                       carries the human-readable status. Default False = no gate.
    Checks (any failure ⇒ abort): (1) reconciliation — plan basis == broker;
    (2) no resulting/uncovered short in the long-only book; (3) turnover bound;
    (4) net buys ≤ available cash; (5) no working orders — the book must be
    quiescent so a new plan can't STACK on unfilled orders (the daily-churn
    failure autonomy must never cause); (6) market-data feed live — never submit
    into a dead feed where orders can't fill."""
    fails: list[str] = []
    delta = {}
    for t in trades:
        try:
            delta[str(t["ticker"])] = delta.get(str(t["ticker"]), 0.0) + float(t.get("delta_units", 0.0))
        except (KeyError, TypeError, ValueError):
            continue

    # 1) RECONCILIATION — the plan's assumed holdings must match the broker.
    all_tk = set(assumed_positions) | set(broker_positions) | set(delta)
    for tk in sorted(all_tk):
        a = float(assumed_positions.get(tk, 0.0) or 0.0)
        b = float(broker_positions.get(tk, 0.0) or 0.0)
        if abs(a - b) > unit_tol:
            fails.append(f"RECONCILE: {tk} plan-assumed {a:g}u != broker {b:g}u "
                         f"— plan built on stale holdings (reconcile first)")

    # 2) SHORTS — long-only book; no position may end (or stay) short.
    for tk in sorted(set(broker_positions) | set(delta)):
        b = float(broker_positions.get(tk, 0.0) or 0.0)
        resulting = b + float(delta.get(tk, 0.0))
        if resulting < -unit_tol:
            if b < -unit_tol and abs(float(delta.get(tk, 0.0))) <= unit_tol:
                fails.append(f"UNCOVERED-SHORT: {tk} broker {b:g}u short and the plan "
                             f"doesn't flatten it")
            else:
                fails.append(f"SHORT: {tk} would end at {resulting:g}u (short) in a "
                             f"long-only book")

    # 3) TURNOVER bound (on broker NAV).
    if nav_aud and float(nav_aud) > 0:
        turnover = sum(abs(float(t.get("delta_value_aud", 0.0) or 0.0)) for t in trades) / float(nav_aud)
        if turnover > max_turnover:
            fails.append(f"TURNOVER: Σ|trade|/NAV = {turnover:.2f} > {max_turnover} "
                         f"— would churn {turnover*100:.0f}% of NAV in one run")

    # 4) CASH — net buys must fit available cash.
    if available_cash_aud is not None:
        net_buy = sum(float(t.get("delta_value_aud", 0.0) or 0.0) for t in trades)
        if net_buy > float(available_cash_aud) * 1.01:
            fails.append(f"CASH: net buys ${net_buy:,.0f} > available ${float(available_cash_aud):,.0f}")

    # 5) OPEN ORDERS — a clean rebalance must start from a QUIESCENT book. A
    #    working order left over from a prior run (unfilled/PreSubmitted) is
    #    invisible to the position-based checks above, so submitting a new plan
    #    would STACK a fresh batch on top of it — the exact daily-churn failure
    #    autonomy must never cause (broken calendar anchor keeps the verdict RUN,
    #    drift stays high while orders sit unfilled → re-submit every day). Any
    #    working order aborts; the fail-safe email tells the user to clear it.
    if open_orders:
        for tk in sorted(open_orders):
            q = float(open_orders.get(tk, 0.0) or 0.0)
            if abs(q) > unit_tol:
                fails.append(f"OPEN-ORDER: {tk} has a working order ({q:g}u) at the "
                             f"broker — a prior run hasn't settled; cancel/resolve it "
                             f"before submitting (stacking risk)")

    # 6) MARKET-DATA FEED — a market order submitted into a dead data farm sits
    #    PreSubmitted and never fills (the 2026-07-24 incident: 7 orders stuck 4
    #    days while the account was 'not connected to the market data system').
    #    Delayed cached prices still return in that state, so a price probe is a
    #    FALSE all-clear — the farm connection status (code 2103 vs 2104) is the
    #    only reliable signal. If it's broken, refuse: better to abort+email than
    #    submit orders that can't fill and pile up.
    if data_farm_broken:
        fails.append(f"MKT-DATA: {data_farm_reason or 'market-data farm connection is broken'} "
                     f"— the feed is down; orders would sit unfilled. Not trading.")

    return (len(fails) == 0, fails)


def _fx_local_to_aud(ib, currency: str) -> "float | None":
    """Rate converting one unit of `currency` into AUD, or None.

    Primary source is IBKR's own ExchangeRate tag, which is quoted to the
    account's base (AUD). Fallback is the rate implied by the latest logged NAV
    snapshot — the same account-identity derivation the lot reconcile trusts,
    and deliberately NOT anything derived from mkt_value_base, which is local
    currency despite its name and yields exactly 1.0 for USD rows.

    Returns None rather than a guess: a wrong FX rate silently mis-sizes every
    US leg, and refusing costs one skipped pass."""
    ccy = str(currency or "").strip().upper()
    if ccy in ("AUD", ""):
        return 1.0
    try:
        for v in ib.accountValues():
            if (str(getattr(v, "tag", "")) == "ExchangeRate"
                    and str(getattr(v, "currency", "")).upper() == ccy):
                r = float(v.value)
                if 0.1 < r < 10.0:
                    return r
    except Exception:
        pass
    try:
        from lots import derive_fx_from_snapshot
        path = _SCRIPT_DIR / "ibkr_nav_log.jsonl"
        snap = None
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        snap = json.loads(line)
                    except json.JSONDecodeError:
                        continue
        if snap:
            r = float((derive_fx_from_snapshot(snap) or {}).get(ccy, 0) or 0)
            if 0.1 < r < 10.0:
                print(f"[exec][WARN] FX {ccy}->AUD from the last NAV snapshot "
                      f"({r:.4f}); live ExchangeRate was unavailable.")
                return r
    except Exception:
        pass
    return None


def _broker_positions(ib) -> dict:
    """{engine-ticker: units} from ib.positions(); ASX bare symbols → .AX
    (mirrors _run_sync_holdings_mode's mapping). Best-effort → {} on failure."""
    out: dict[str, float] = {}
    try:
        for pos in ib.positions():
            c = pos.contract
            if str(getattr(c, "secType", "STK")).upper() != "STK":
                continue
            sym = str(c.symbol).strip().upper()
            if (str(getattr(c, "currency", "")).upper() == "AUD"
                    or str(getattr(c, "primaryExchange", "")).upper() == "ASX"):
                sym = f"{sym}.AX"
            out[sym] = out.get(sym, 0.0) + float(pos.position)
    except Exception:
        pass
    return out


def _flatten_targets(broker_positions: dict, wanted_norm: set,
                     *, unit_tol: float = 1.0) -> list:
    """PURE (no IO): the to-zero orders needed to flatten the requested names,
    read from broker truth. Returns [(ticker, held_units, side, qty, signed_delta)]
    for every requested name held beyond unit_tol. BUY covers a short, SELL closes
    a long. Names already flat (|units| <= tol) or not requested are dropped.

    `wanted_norm` is the set of requested tickers with any .AX suffix stripped and
    upper-cased (suffix-tolerant match, mirrors the exec path). Factored out so the
    signed-delta logic — the part that has to be exactly right — is unit-testable
    without a broker connection."""
    def _strip_ax(s: str) -> str:
        s = str(s).upper()
        return s[:-3] if s.endswith(".AX") else s
    out = []
    for tk, units in broker_positions.items():
        if _strip_ax(tk) not in wanted_norm:
            continue
        u = float(units or 0.0)
        if abs(u) <= unit_tol:
            continue
        qty = int(round(abs(u)))
        side = "BUY" if u < 0 else "SELL"
        signed = qty if side == "BUY" else -qty
        out.append((tk, u, side, qty, signed))
    return out


def _broker_open_orders(ib) -> dict:
    """{engine-ticker: signed working units} for orders NOT in a terminal state
    (PreSubmitted/Submitted/PendingSubmit). A non-empty result means the book is
    mid-adjustment — a prior run's orders haven't filled or cancelled — so a new
    plan submitted now would STACK on top of them. ASX bare symbols → .AX
    (mirrors _broker_positions). Best-effort → {} on failure (fail-open here is
    safe: the validation gate treats {} as 'no working orders', and a real stuck
    order also trips the reconcile/drift paths on the next run)."""
    out: dict[str, float] = {}
    try:
        try:
            ib.reqAllOpenOrders()
            ib.sleep(1)
        except Exception:
            pass
        for tr in ib.openTrades():
            try:
                if tr.isDone():
                    continue
            except Exception:
                pass
            c = getattr(tr, "contract", None)
            if c is None or str(getattr(c, "secType", "STK")).upper() != "STK":
                continue
            sym = str(c.symbol).strip().upper()
            if (str(getattr(c, "currency", "")).upper() == "AUD"
                    or str(getattr(c, "primaryExchange", "")).upper() == "ASX"):
                sym = f"{sym}.AX"
            try:
                rem = float(tr.remaining() or 0)
            except Exception:
                rem = float(getattr(tr.order, "totalQuantity", 0) or 0)
            sign = -1.0 if str(getattr(tr.order, "action", "BUY")).upper() == "SELL" else 1.0
            out[sym] = out.get(sym, 0.0) + sign * abs(rem)
    except Exception:
        pass
    return out


class _DataFarmMonitor:
    """Tracks IBKR's market-data farm connection status from ib.errorEvent so the
    auto-execute gate can refuse to trade when the feed is DOWN (orders would sit
    unfilled — the 2026-07-24 incident). Subscribe RIGHT AFTER connect: IBKR emits
    the farm-status burst (2103/2104 …) once, moments after connecting, so a
    handler registered later would miss it. Farm status can also flip mid-session,
    which this keeps tracking. Only the MARKET-DATA farm gates trading — HMDS
    (historical) and sec-def (contract defs) don't block live fills."""
    _BROKEN = {2103: "market-data farm", 2105: "HMDS farm", 2157: "sec-def farm"}
    _OK = {2104: "market-data farm", 2106: "HMDS farm", 2158: "sec-def farm"}

    def __init__(self, ib):
        self._ib = ib
        self._status: dict = {}   # farm-name -> (ok: bool, msg: str)
        try:
            ib.errorEvent += self._on
        except Exception:
            pass

    def _on(self, reqId, code, msg, contract):
        if code in self._BROKEN:
            self._status[self._BROKEN[code]] = (False, str(msg))
        elif code in self._OK:
            self._status[self._OK[code]] = (True, str(msg))

    def mktdata_ok(self, settle: float = 3.0) -> "tuple[bool, str]":
        """(ok, reason). Waits `settle`s for the post-connect status burst, then
        reports the MARKET-DATA farm only. Fail-OPEN on a missing status message
        (the farm reliably reports on connect, and a genuinely-down feed reports
        BROKEN, not silence) — don't halt a run on an absent message; check (5)
        already blocks stacking, and a WARN is logged upstream."""
        try:
            self._ib.sleep(settle)
        except Exception:
            pass
        st = self._status.get("market-data farm")
        if st is None:
            return True, "no market-data farm status seen (assumed ok)"
        return st[0], ("market-data farm OK" if st[0]
                       else f"market-data farm connection is broken ({st[1]})")

    def detach(self):
        try:
            self._ib.errorEvent -= self._on
        except Exception:
            pass


def _broker_net_liquidation_aud(ib) -> "float | None":
    """Broker NetLiquidation (AUD) for the turnover check. None on failure."""
    try:
        rows = [v for v in ib.accountSummary()
                if v.tag == "NetLiquidation" and str(v.currency).upper() == "AUD"]
        return float(rows[0].value) if rows else None
    except Exception:
        return None


def _auto_email(subject: str, body: str) -> None:
    """Email an autonomous-run outcome (executed / blocked / aborted). Non-fatal;
    silent if the mailer is unconfigured. Auto-execution is unattended, so every
    outcome must reach the user."""
    try:
        from send_alert import send
        send(f"[Portfolio Optimiser] {subject}", body)
        print(f"[auto-exec] emailed: {subject}")
    except Exception as _e:
        print(f"[auto-exec] email failed ({_e}); non-fatal.")


# ============================================================================
# Shadow mode — dry-run the FULL autonomy decision, place NO orders
# ============================================================================

def _shadow_report_body(rec_entry: dict, trades_recs: list, ok: bool,
                        fails: list) -> str:
    """PURE report body for shadow mode (unit-tested). Shows exactly what the
    autonomous path WOULD do this cycle — execute or abort — without trading."""
    verdict = "WOULD EXECUTE" if ok else "WOULD ABORT (validation gate)"
    lines = [f"AUTONOMY SHADOW — plan @ {rec_entry.get('run_at', '?')}",
             f"Verdict: {verdict}", ""]
    if ok:
        lines.append("Orders it would place (pre-trade validation PASSED):")
        for r in trades_recs:
            try:
                du = int(r.get("delta_units", 0) or 0)
            except (TypeError, ValueError):
                du = 0
            if du == 0:
                continue
            side = "BUY " if du > 0 else "SELL"
            lines.append(f"  {side} {abs(du):>6} {str(r.get('ticker','?')):<10} "
                         f"~${abs(float(r.get('delta_value_aud', 0.0) or 0.0)):,.0f}")
    else:
        lines.append("Validation gate FAILED — it would NOT execute:")
        for f in fails:
            lines.append(f"  x {f}")
    lines.append("")
    lines.append("SHADOW MODE — no orders were placed. This is a dry run of the "
                 "autonomous path; enable it deliberately once you trust these calls.")
    return "\n".join(lines)


def _run_shadow_execute_mode(email: bool = False) -> int:
    """Dry-run the autonomous decision: load the latest plan, check it against
    BROKER TRUTH via the same pre-trade gate the real path uses, and report what
    it WOULD do. Places NO orders — the rung between the validation gate and the
    live AUTO_EXECUTE switch, so the user can watch it before trusting it."""
    rec_entry = _load_latest_run(Path(_SCRIPT_DIR / REC_LOG_FILENAME))
    trades_recs = rec_entry.get("recommended_trades", []) if rec_entry else []
    if not trades_recs:
        print("[shadow] latest run has no recommended_trades — nothing to shadow.")
        return 0
    try:
        from ib_insync import IB
    except ImportError:
        print("[shadow] ib_insync not installed.")
        return 1
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT)
    except Exception as e:
        print(f"[shadow][ERR] connect failed ({e}); TWS/Gateway up?")
        return 2
    _farm_mon = _DataFarmMonitor(ib)   # capture the post-connect farm-status burst
    try:
        managed = ib.managedAccounts() or []
        if not managed:
            print("[shadow][SAFETY] no managed account.")
            return 3
        _refuse_if_live(managed[0])
        _broker_pos = _broker_positions(ib)
        _assumed = dict(rec_entry.get("current_units", {}) or {})
        _val_trades = [{"ticker": r["ticker"],
                        "delta_units": r.get("delta_units", 0),
                        "delta_value_aud": r.get("delta_value_aud", 0.0)}
                       for r in trades_recs]
        _df_ok, _df_reason = _farm_mon.mktdata_ok()
        ok, fails = validate_pre_trade(
            _val_trades, _assumed, _broker_pos,
            available_cash_aud=_available_funds_aud(ib),
            nav_aud=_broker_net_liquidation_aud(ib),
            open_orders=_broker_open_orders(ib),
            data_farm_broken=(not _df_ok), data_farm_reason=_df_reason)
    finally:
        if ib.isConnected():
            ib.disconnect()

    body = _shadow_report_body(rec_entry, trades_recs, ok, fails)
    print("\n" + "=" * 96)
    print(body)
    print("=" * 96)
    if email:
        try:
            from send_alert import send
            subj = (f"[Portfolio Optimiser] SHADOW: would "
                    f"{'EXECUTE' if ok else 'ABORT'} ({len(trades_recs)} orders)")
            rc = send(subj, body)
            print(f"[shadow] report emailed (rc={rc}).")
        except Exception as e:
            print(f"[shadow] email failed ({e}); non-fatal.")
    return 0


def _write_deferred_orders(deferred: list, ib, rec_entry: dict, path: Path) -> int:
    """Persist cash-deferred buys so the guarded auto-completer can finish them.
    Captures the approved LOCAL price now (via IBKR) so completion can drift-check
    in local ccy without needing FX. Best-effort per order."""
    records = []
    _now = datetime.now().isoformat(timespec="seconds")
    _run_at = rec_entry.get("run_at") if isinstance(rec_entry, dict) else None
    for r, c, o in deferred:
        try:
            du = abs(int(r["delta_units"]))
            approved_aud = abs(float(r["delta_value_aud"])) / du if du else None
        except Exception:
            approved_aud = None
        records.append({
            "ticker": r["ticker"],
            "side": str(o.action),
            "qty": int(o.totalQuantity),
            "need_aud": abs(float(r.get("delta_value_aud") or 0.0)) * 1.01,
            "approved_price_aud": approved_aud,
            "approved_price_local": _ref_local_price(ib, c),
            "ccy": getattr(c, "currency", None),
            "rec_log_run_at": _run_at,
            "deferred_at": _now,
        })
    try:
        path.write_text(json.dumps({"deferred": records}, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[exec][WARN] could not persist deferred orders ({e}); "
              f"auto-completion won't run.")
        return 0
    return len(records)


def _run_complete_deferred_mode(drift_pct: float, max_age_hours: float,
                                email: bool = False) -> int:
    """Guarded auto-completion of previously cash-deferred buys. Reads
    deferred_orders.json, re-checks each behind the price-drift / staleness /
    funds guards, submits only those that pass, emails the outcome, and rewrites
    the file (completed + drift/stale aborts removed; funds-deferred kept)."""
    path = _SCRIPT_DIR / DEFERRED_ORDERS_FILENAME
    if not path.exists():
        print("[complete-deferred] no deferred_orders.json — nothing to complete.")
        return 0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        pending = list(data.get("deferred", []) or [])
    except Exception as e:
        print(f"[complete-deferred][ERR] unreadable deferred file ({e}).")
        return 1
    if not pending:
        print("[complete-deferred] deferred file is empty — nothing to complete.")
        return 0

    try:
        from ib_insync import IB, LimitOrder
    except ImportError:
        print("[complete-deferred] ib_insync not installed.")
        return 1

    print(f"[complete-deferred] {len(pending)} deferred order(s); guards: "
          f"drift<={drift_pct:.1f}%, age<={max_age_hours:.0f} business-h "
          f"(<={DEFERRED_MAX_WALL_AGE_HOURS_DEFAULT:.0f}h wall)")
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT)
    except Exception as e:
        print(f"[complete-deferred][ERR] connect failed ({e}); TWS/Gateway up?")
        return 2

    submitted, aborted, kept = [], [], []
    now = datetime.now()
    try:
        managed = ib.managedAccounts() or []
        if not managed:
            print("[complete-deferred][SAFETY] no managed account. Aborting.")
            return 3
        _refuse_if_live(managed[0])
        funds = _available_funds_aud(ib)
        print(f"[complete-deferred] paper {managed[0]}, available ${funds if funds is None else f'{funds:,.2f}'} AUD")
        for od in pending:
            contract = _ticker_to_contract(od["ticker"])
            if contract is None:
                aborted.append((od, "abort_no_contract", "could not build contract"))
                continue
            ib.qualifyContracts(contract)
            cur_local = _ref_local_price(ib, contract)
            action, reason = _deferred_completion_decision(
                approved_price_local=od.get("approved_price_local"),
                current_price_local=cur_local,
                need_aud=od.get("need_aud"),
                funds_aud=funds,
                deferred_at_iso=od.get("deferred_at"),
                now=now, drift_pct=drift_pct, max_age_hours=max_age_hours,
            )
            print(f"[complete-deferred] {od['ticker']}: {action} — {reason}")
            if action == "submit":
                # Marketable limit off the verified current price (cur_local is
                # guaranteed present here — the decision aborts_no_price otherwise).
                order = LimitOrder(od["side"], int(od["qty"]),
                                   _marketable_limit_price(cur_local, od["side"], LIMIT_COLLAR_PCT))
                order.tif = "DAY"
                _submit_orders(ib, [({"ticker": od["ticker"]}, contract, order)],
                               wait=True)
                submitted.append((od, reason))
                if funds is not None and od.get("need_aud"):
                    funds -= float(od["need_aud"])
            elif action == "defer_funds":
                kept.append((od, reason))
            else:  # abort_* (drift / stale / no_price / no_contract)
                aborted.append((od, action, reason))
    finally:
        if ib.isConnected():
            ib.disconnect()

    # Rewrite the file: keep only funds-deferred (retry next run). Completed and
    # aborted-on-guard are removed — aborts need human eyes, not auto-retries.
    try:
        path.write_text(json.dumps({"deferred": [od for od, _ in kept]}, indent=2),
                        encoding="utf-8")
    except Exception as e:
        print(f"[complete-deferred][WARN] could not rewrite deferred file ({e}).")

    print(f"[complete-deferred] submitted={len(submitted)} "
          f"aborted={len(aborted)} kept-for-retry={len(kept)}")
    if email and (submitted or aborted):
        _send_deferred_outcome_email(submitted, aborted, kept)
    return 0


def _send_deferred_outcome_email(submitted: list, aborted: list, kept: list) -> None:
    """Email the outcome of a guarded auto-completion run. Non-fatal/silent if
    the mailer is unavailable. Only called when something was submitted or a
    guard aborted (funds-only deferrals stay quiet)."""
    try:
        from send_alert import send
    except Exception as _e:
        print(f"  [complete-deferred] --email: send_alert unavailable ({_e}).")
        return
    lines = []
    if submitted:
        lines.append("AUTO-COMPLETED (guards passed, orders placed):")
        for od, reason in submitted:
            lines.append(f"  {od['ticker']:<10} {od['side']} {od['qty']}  ({reason})")
        lines.append("")
    if aborted:
        lines.append("NOT completed — needs your review:")
        for od, action, reason in aborted:
            lines.append(f"  {od['ticker']:<10} {od['side']} {od['qty']}  [{action}] {reason}")
        lines.append("")
        lines.append("To complete a reviewed one manually:")
        lines.append('  & ".\\.venv\\Scripts\\python.exe" ibkr_paper_exec.py --execute '
                     f"--only-tickers {','.join(od['ticker'] for od, _a, _r in aborted)}")
    if kept:
        lines.append("")
        lines.append(f"Still waiting on funds (will retry next run): "
                     f"{', '.join(od['ticker'] for od, _r in kept)}")
    n_sub, n_ab = len(submitted), len(aborted)
    subject = f"[Portfolio Optimiser] DEFERRED-BUY: {n_sub} completed, {n_ab} need review"
    try:
        rc = send(subject, "\n".join(lines))
        print(f"  [complete-deferred] --email: outcome sent (rc={rc}).")
    except Exception as _e:
        print(f"  [complete-deferred] --email: send failed ({_e}); non-fatal.")


def _write_fills_log(rec_entry: dict, trades: list, log_path: Path) -> int:
    """Append one JSONL row per submitted order to ibkr_fills_log.jsonl.
    Returns the number of rows written.

    Rich diagnostic capture (post 2026-06-22 bug):
    - is_done from Trade.isDone() instead of inferring from status string
    - qty_remaining alongside qty_filled so partial fills are unambiguous
    - status_log: the full transition history from Trade.log so we can
      see the PreSubmitted -> Cancelled or PreSubmitted -> Filled walk
      even if the final-status read is stale
    - avg_fill_price_local emitted as null (not NaN) for JSON safety
    """
    rec_ts = rec_entry.get("run_at", "?")
    now_iso = datetime.now().isoformat(timespec="seconds")
    n_written = 0
    with open(log_path, "a", encoding="utf-8") as f:
        for rec, trade in trades:
            try:
                fills = list(getattr(trade, "fills", []) or [])
                try:
                    filled_qty = float(trade.filled() if callable(getattr(trade, "filled", None)) else 0)
                except Exception:
                    filled_qty = 0.0
                try:
                    remaining_qty = float(trade.remaining() if callable(getattr(trade, "remaining", None)) else 0)
                except Exception:
                    remaining_qty = 0.0
                if filled_qty > 0 and fills:
                    avg_px = sum(float(fl.execution.price) * float(fl.execution.shares)
                                  for fl in fills) / filled_qty
                else:
                    avg_px = None  # null in JSON — NaN is invalid JSON
                # IBKR commission per fill lives on fl.commissionReport (arrives
                # asynchronously after the execution). Sum across fills; charged
                # in the instrument's trading currency (AUD for .AX, USD for US),
                # which is the same currency as avg_fill_price_local, so the
                # engine fx-converts both with one ticker->AUD rate. null when no
                # fills or the commission report hasn't arrived yet.
                fees_local = None
                if fills:
                    try:
                        _fee_sum = 0.0
                        _have_fee = False
                        for fl in fills:
                            _cr = getattr(fl, "commissionReport", None)
                            _c = getattr(_cr, "commission", None) if _cr is not None else None
                            if _c is not None:
                                _fee_sum += float(_c)
                                _have_fee = True
                        fees_local = _fee_sum if _have_fee else None
                    except Exception:
                        fees_local = None
                try:
                    is_done = bool(trade.isDone())
                except Exception:
                    is_done = False
                # Full status transition history from Trade.log. Each LogEntry
                # has time, status, message — capture all three, truncate msg
                # so a verbose IBKR error doesn't blow up the JSONL row.
                status_log = []
                for entry in getattr(trade, "log", []):
                    try:
                        status_log.append({
                            "ts": str(getattr(entry, "time", "")),
                            "status": str(getattr(entry, "status", "")),
                            "msg": str(getattr(entry, "message", ""))[:240],
                            "error_code": int(getattr(entry, "errorCode", 0) or 0),
                        })
                    except Exception:
                        continue
                row = {
                    "exec_timestamp": now_iso,
                    "rec_log_run_at": rec_ts,
                    "ticker": rec["ticker"],
                    "side": trade.order.action,
                    "qty_requested": int(trade.order.totalQuantity),
                    "qty_filled": int(filled_qty),
                    "qty_remaining": int(remaining_qty),
                    "avg_fill_price_local": avg_px,
                    "fees_local": fees_local,
                    "rec_px_aud": float(rec.get("px_aud", 0.0) or 0.0),
                    "rec_delta_value_aud": float(rec.get("delta_value_aud", 0.0) or 0.0),
                    "status_final": str(trade.orderStatus.status),
                    "is_done": is_done,
                    "order_id": int(trade.order.orderId),
                    "ibkr_perm_id": int(getattr(trade.order, "permId", 0) or 0),
                    "n_fills": len(fills),
                    "status_log": status_log,
                }
                f.write(json.dumps(row, default=str) + "\n")
                n_written += 1
            except Exception as e:
                print(f"[exec][WARN] fill-log row failed for {rec.get('ticker', '?')}: "
                      f"{type(e).__name__}: {e}")
    return n_written


def _print_reconciliation(trades: list) -> None:
    """Per-trade fill table + status counts + simple slippage estimate."""
    if not trades:
        print("[exec] no trades to reconcile.")
        return
    print()
    print("=" * 96)
    print("EXECUTION RECONCILIATION")
    print("=" * 96)
    print(f"  {'#':>3}  {'Ticker':<10} {'Side':<5} {'Req':>7} {'Filled':>7} "
          f"{'Avg Fill':>10} {'Status':<14} {'OrderId':>9}")
    print(f"  {'-'*3}  {'-'*10} {'-'*5} {'-'*7} {'-'*7} {'-'*10} {'-'*14} {'-'*9}")
    n_filled = 0
    n_partial = 0
    n_rejected = 0
    n_pending = 0
    for i, (rec, t) in enumerate(trades, 1):
        try:
            qty_req = int(t.order.totalQuantity)
            qty_filled = int(getattr(t, "filled", lambda: 0)() or 0)
            fills = list(getattr(t, "fills", []) or [])
            if qty_filled > 0 and fills:
                avg_px = sum(float(fl.execution.price) * float(fl.execution.shares)
                              for fl in fills) / qty_filled
                avg_px_str = f"{avg_px:>10.4f}"
            else:
                avg_px_str = f"{'—':>10}"
            status = t.orderStatus.status
            print(f"  {i:>3}  {rec['ticker']:<10} {t.order.action:<5} "
                  f"{qty_req:>7} {qty_filled:>7} {avg_px_str} "
                  f"{status:<14} {t.order.orderId:>9}")
            if status == "Filled":
                n_filled += 1
            elif status in ("PartiallyFilled", "Submitted", "PreSubmitted"):
                n_pending += 1 if qty_filled == 0 else 0
                n_partial += 1 if qty_filled > 0 and qty_filled < qty_req else 0
            elif status in ("Cancelled", "ApiCancelled", "Inactive"):
                n_rejected += 1
            else:
                n_pending += 1
        except Exception as e:
            print(f"[exec][WARN] reconciliation row failed for "
                  f"{rec.get('ticker', '?')}: {e}")
    print(f"  {'-'*3}  {'-'*10} {'-'*5} {'-'*7} {'-'*7} {'-'*10} {'-'*14} {'-'*9}")
    print()
    print(f"  Filled:    {n_filled:>3}")
    print(f"  Partial:   {n_partial:>3}")
    print(f"  Pending:   {n_pending:>3}  (may settle after script exits)")
    print(f"  Rejected:  {n_rejected:>3}")
    print()


def _append_backfill_rows(corrections: list, log_path) -> int:
    """Append corrected rows to the fills log. Append-only: the original rows
    stay for audit, and because _build_lots_from_fills_log skips qty_filled<=0
    they contribute nothing, so the appended rows do not duplicate them."""
    n = 0
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            for c in corrections:
                f.write(json.dumps(c) + "\n")
                n += 1
    except Exception as e:
        print(f"[check-fills][ERR] back-fill write failed: {e}")
    return n


def _build_backfill_row(row: dict, trade, filled: int, now_status: str,
                        permid: int) -> dict:
    """Build a corrected fills-log row from IBKR's live view of `trade`.

    Starts from the original logged row so nothing already captured is lost,
    then overrides what the original got wrong.

    exec_timestamp is the load-bearing field: it becomes the lot's AcqDate AND
    is what the seed watershed (lots.py) compares against. It MUST be the real
    execution time, not 'now' -- stamping now would push a historical fill past
    the watershed and double-count it against a freshly re-seeded book. IBKR
    serves execution.time in UTC; the fills log is naive local, so convert
    rather than mix conventions.
    """
    fills = list(getattr(trade, "fills", []) or [])
    avg_px = None
    if filled > 0 and fills:
        try:
            avg_px = sum(float(fl.execution.price) * float(fl.execution.shares)
                         for fl in fills) / filled
        except Exception:
            avg_px = None

    exec_ts = None
    try:
        times = [fl.execution.time for fl in fills if getattr(fl, "execution", None)
                 and getattr(fl.execution, "time", None)]
        if times:
            t = min(times)  # first fill = when the position was acquired
            if getattr(t, "tzinfo", None) is not None:
                t = t.astimezone().replace(tzinfo=None)
            exec_ts = t.isoformat(timespec="seconds")
    except Exception:
        exec_ts = None

    fees_local = None
    if fills:
        try:
            _sum, _have = 0.0, False
            for fl in fills:
                _cr = getattr(fl, "commissionReport", None)
                _c = getattr(_cr, "commission", None) if _cr is not None else None
                if _c is not None:
                    _sum += float(_c)
                    _have = True
            fees_local = _sum if _have else None
        except Exception:
            fees_local = None

    out = dict(row)
    out.update({
        "qty_filled": int(filled),
        "status_final": now_status,
        "avg_fill_price_local": avg_px,
        "perm_id": permid or row.get("perm_id"),
        "fees_local": fees_local if fees_local is not None else row.get("fees_local"),
        # Provenance: this row was reconstructed after the fact, not observed
        # at submission. Kept so a future reader can tell the two apart.
        "backfilled_at": datetime.now().isoformat(timespec="seconds"),
        "backfill_source": "check-fills",
    })
    if exec_ts:
        out["exec_timestamp"] = exec_ts
    return out


def _plan_check_fills_batch(rows: list) -> tuple:
    """Pure planning step for --check-fills. From all fills-log rows, return
    (latest_ts, batch, already_backfilled).

    - batch = the most recent SUBMISSION batch. Back-fill correction rows
      (stamped backfill_source) are EXCLUDED from batch selection: their
      exec_timestamp is the real historical fill time, which can post-date the
      submission and would otherwise collapse the batch to a single correction
      row, dropping every other outstanding order from the check.
    - already_backfilled = identity set (ticker, side, qty_requested) of orders
      that already have a correction row, so a second --write never appends a
      duplicate (the original qty_filled=0 row stays for audit).

    No IB, no I/O — unit-testable.
    """
    submission_rows = [r for r in rows if not r.get("backfill_source")]
    if not submission_rows:
        return (None, [], set())
    latest_ts = max(r["exec_timestamp"] for r in submission_rows)
    batch = [r for r in submission_rows if r["exec_timestamp"] == latest_ts]
    already_backfilled = {
        (str(r.get("ticker")), str(r.get("side")).upper(),
         int(float(r.get("qty_requested") or 0)))
        for r in rows if r.get("backfill_source")
    }
    return (latest_ts, batch, already_backfilled)


def _send_fill_confirmation_email(corrections: list, still_pending: list) -> None:
    """Email a fill confirmation for newly-Filled orders (used by --check-fills
    --email). Reuses send_alert.send(); non-fatal + silent if the mailer is
    unconfigured or unavailable. Only called when `corrections` is non-empty, so
    it never spams on a no-change re-check."""
    try:
        from send_alert import send
    except Exception as _e:
        print(f"  [check-fills] --email: send_alert unavailable ({_e}); skipping.")
        return
    lines = ["The following order(s) reached Filled since the fills log was last updated:", ""]
    for c in corrections:
        _px = c.get("avg_fill_price_local")
        _px_s = f"{float(_px):.4f}" if _px not in (None, "", "—") else "—"
        lines.append(f"  {c.get('ticker'):<10} {str(c.get('side')).upper():<4} "
                     f"{c.get('qty_filled'):>6} @ {_px_s}  "
                     f"exec={c.get('exec_timestamp')}")
    lines.append("")
    if still_pending:
        lines.append(f"Still pending ({len(still_pending)}): {', '.join(still_pending)}")
        lines.append("Re-run --check-fills --email later to confirm the rest.")
    else:
        lines.append("All orders in this batch are now Filled — rebalance complete.")
    subject = f"[Portfolio Optimiser] FILLS CONFIRMED — {len(corrections)} order(s) filled"
    try:
        rc = send(subject, "\n".join(lines))
        print(f"  [check-fills] --email: confirmation sent (rc={rc}).")
    except Exception as _e:
        print(f"  [check-fills] --email: send failed ({_e}); non-fatal.")


def _run_check_fills_mode(write: bool = False, email: bool = False) -> int:
    """Re-query IBKR for the current status of orders from the most recent
    batch in ibkr_fills_log.jsonl. Useful for:
      - Overnight US orders that were 'pending' when the original run exited
      - Diagnosing Cancelled-with-permId=0 rows (did the order actually fill
        despite our log saying otherwise?)

    Read-only — does not place orders, does not append to the fills log."""
    fills_path = _SCRIPT_DIR / FILLS_LOG_FILENAME  # anchored: scheduled-task CWD=System32
    if not fills_path.exists():
        print(f"[check-fills] {fills_path} not found. Nothing to check.")
        return 0

    rows = []
    with open(fills_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if not rows:
        print(f"[check-fills] {fills_path} is empty.")
        return 0

    # Most recent batch = highest exec_timestamp value AMONG SUBMISSION ROWS.
    # Back-fill rows (written by --write) carry the REAL historical fill time in
    # exec_timestamp (lots.py needs it for AcqDate), which can post-date the
    # submission and, if included here, would collapse "the latest batch" to the
    # single correction row — silently dropping every other outstanding order
    # from the check. Corrections are not submissions; exclude them from batch
    # selection. (Identified by the backfill_source stamp _build_backfill_row adds.)
    latest_ts, batch, _already_backfilled = _plan_check_fills_batch(rows)
    if not batch:
        print(f"[check-fills] {fills_path} has only back-fill rows, no submissions to check.")
        return 0
    print(f"[check-fills] checking {len(batch)} order(s) from batch "
          f"exec_timestamp={latest_ts}")

    try:
        from ib_insync import IB
    except ImportError:
        print("[check-fills] ib_insync not installed. Run: pip install ib_insync")
        return 1

    print(f"[check-fills] connecting to PAPER IBKR at {HOST}:{PORT} "
          f"(clientId={CLIENT_ID})...")
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT)
    except Exception as e:
        print(f"[check-fills][ERR] connect failed ({type(e).__name__}): {e}")
        print(f"[check-fills] make sure TWS / IB Gateway is on port {PORT} (paper).")
        return 2

    try:
        managed = ib.managedAccounts() or []
        if not managed:
            print("[check-fills][SAFETY] managedAccounts() returned nothing.")
            return 3
        _refuse_if_live(managed[0])
        print(f"[check-fills] paper account: {managed[0]}")

        # Ask IBKR for both currently-open and recently-completed orders.
        # ib_insync auto-subscribes to openOrders on connect; we also need
        # completed orders explicitly, then a short sleep so events arrive.
        try:
            ib.reqAllOpenOrders()
        except Exception as e:
            print(f"[check-fills][WARN] reqAllOpenOrders failed: {e}")
        try:
            ib.reqCompletedOrders(apiOnly=False)
        except Exception as e:
            print(f"[check-fills][WARN] reqCompletedOrders failed: {e}")
        ib.sleep(3)

        all_trades = list(ib.trades())
        print(f"[check-fills] IBKR returned {len(all_trades)} known trades for "
              f"clientId={CLIENT_ID}.")

        # Multi-strategy matching: orderId first (works for still-open orders),
        # then fall back to (ticker, side, qty) for orders TWS has reassigned
        # orderIds for. Completed orders re-served via reqCompletedOrders
        # often come back with orderId=0 (TWS clears it post-close) and the
        # contract symbol is the bare exchange code without our .AX suffix.
        # Normalise both sides by stripping .AX so VAE.AX matches VAE+AUD.
        def _norm_ticker(t: str) -> str:
            t = str(t or "").upper()
            return t[:-3] if t.endswith(".AX") else t

        def _ticker_from_contract(c) -> str:
            return _norm_ticker(str(getattr(c, "symbol", "") or ""))

        def _original_qty(t) -> int:
            """Best-effort recovery of original submission quantity.
            Filled trades come back with totalQuantity=0 because IBKR clears
            it post-fill; for those, the original qty = filled + remaining."""
            try:
                tq = int(t.order.totalQuantity or 0)
                fq = int(t.filled() or 0)
                rq = int(t.remaining() or 0)
                return max(tq, fq + rq)
            except Exception:
                return 0

        trades_by_id = {t.order.orderId: t for t in all_trades if t.order.orderId}
        trades_by_key = {}      # (ticker, side, qty)  — qty-aware match
        trades_by_ticker = {}   # (ticker, side)       — final fallback
        for t in all_trades:
            try:
                tk = _ticker_from_contract(t.contract)
                side = str(t.order.action)
                qty = _original_qty(t)
                # Qty-aware key first (handles batches with multiple orders
                # on the same ticker e.g. TLH swap).
                key = (tk, side, qty)
                existing = trades_by_key.get(key)
                if existing is None:
                    trades_by_key[key] = t
                else:
                    existing_oid = existing.order.orderId or 0
                    new_oid = t.order.orderId or 0
                    existing_pid = int(getattr(existing.order, "permId", 0) or 0)
                    new_pid = int(getattr(t.order, "permId", 0) or 0)
                    if new_oid > existing_oid or new_pid > existing_pid:
                        trades_by_key[key] = t
                # Ticker-only fallback. Engines normally submit one order per
                # ticker per batch, so this resolves cleanly. If collision,
                # prefer highest permId (latest broker-accepted).
                tkey = (tk, side)
                existing_tk = trades_by_ticker.get(tkey)
                if existing_tk is None:
                    trades_by_ticker[tkey] = t
                else:
                    if int(getattr(t.order, "permId", 0) or 0) > int(
                            getattr(existing_tk.order, "permId", 0) or 0):
                        trades_by_ticker[tkey] = t
            except Exception:
                continue

        print()
        print("=" * 100)
        print(f"ALL TRADES IBKR KNOWS ABOUT (clientId={CLIENT_ID}, this session)")
        print("=" * 100)
        print(f"  {'Ticker':<12} {'OrderId':>7} {'PermId':>11} "
              f"{'Side':<5} {'Status':<18} {'Filled':>8} {'Total':>8} "
              f"{'Avg Px':>10}")
        print(f"  {'-'*12} {'-'*7} {'-'*11} {'-'*5} {'-'*18} "
              f"{'-'*8} {'-'*8} {'-'*10}")
        for t in sorted(all_trades, key=lambda x: _ticker_from_contract(x.contract)):
            try:
                tk = _ticker_from_contract(t.contract)
                filled = int(t.filled() or 0)
                total = int(t.order.totalQuantity)
                permid = int(getattr(t.order, "permId", 0) or 0)
                fills = list(getattr(t, "fills", []) or [])
                if filled > 0 and fills:
                    avg_px = sum(float(fl.execution.price) * float(fl.execution.shares)
                                  for fl in fills) / filled
                    avg_px_str = f"{avg_px:>10.4f}"
                else:
                    avg_px_str = f"{'—':>10}"
                print(f"  {tk:<12} {t.order.orderId:>7} {permid:>11} "
                      f"{t.order.action:<5} {t.orderStatus.status:<18} "
                      f"{filled:>8} {total:>8} {avg_px_str}")
            except Exception as e:
                print(f"  (trade read failed: {e})")
        print(f"  {'-'*12} {'-'*7} {'-'*11} {'-'*5} {'-'*18} "
              f"{'-'*8} {'-'*8} {'-'*10}")

        print()
        print("=" * 100)
        print(f"BATCH STATUS RE-CHECK — fills_log entries from {latest_ts}")
        print("=" * 100)
        print(f"  {'Ticker':<12} {'OrderId':>7} {'PermId':>11} "
              f"{'Status (logged)':<18} {'Status (now)':<18} "
              f"{'Filled':>8} {'Avg Px':>10} {'Match':<10}")
        print(f"  {'-'*12} {'-'*7} {'-'*11} {'-'*18} {'-'*18} "
              f"{'-'*8} {'-'*10} {'-'*10}")
        n_changed = 0
        n_filled_now = 0
        corrections: list[dict] = []
        still_pending: list[str] = []   # tickers not yet Filled (for --email note)
        for row in batch:
            oid = int(row["order_id"])
            logged_status = row.get("status_final") or row.get("status") or "?"
            trade = trades_by_id.get(oid)
            match_method = "orderId"
            if not trade:
                # Fall back to (normalised-ticker, side, qty) match. Both
                # sides have .AX stripped so VAE.AX in the log finds the
                # bare VAE+AUD trade IBKR returns.
                norm_tk = _norm_ticker(row["ticker"])
                side = str(row["side"])
                key = (norm_tk, side, int(row["qty_requested"]))
                trade = trades_by_key.get(key)
                if trade:
                    match_method = "ticker+qty"
                else:
                    # Final fallback: ticker + side only. Safe for batches
                    # with one order per ticker (the engine's normal case).
                    trade = trades_by_ticker.get((norm_tk, side))
                    match_method = "ticker-only" if trade else "—"
            if not trade:
                print(f"  {row['ticker']:<12} {oid:>7} {'?':>11} "
                      f"{logged_status:<18} {'(not found)':<18} "
                      f"{'?':>8} {'?':>10} {match_method:<10}")
                continue
            try:
                now_status = str(trade.orderStatus.status)
                filled = int(trade.filled() or 0)
                permid = int(getattr(trade.order, "permId", 0) or 0)
                fills = list(getattr(trade, "fills", []) or [])
                if filled > 0 and fills:
                    avg_px = sum(float(fl.execution.price) * float(fl.execution.shares)
                                  for fl in fills) / filled
                    avg_px_str = f"{avg_px:>10.4f}"
                else:
                    avg_px_str = f"{'—':>10}"
                if now_status != logged_status:
                    n_changed += 1
                if now_status == "Filled":
                    n_filled_now += 1
                elif now_status not in ("Cancelled", "ApiCancelled", "Inactive"):
                    still_pending.append(str(row["ticker"]))
                print(f"  {row['ticker']:<12} {oid:>7} {permid:>11} "
                      f"{logged_status:<18} {now_status:<18} "
                      f"{filled:>8} {avg_px_str} {match_method:<10}")

                # BACK-FILL CANDIDATE. Narrow by design: only when the log says
                # NOTHING filled but IBKR says otherwise. That is exactly the
                # 2026-07-08 SOXL/VEA bug (script exited while PreSubmitted, so
                # the row froze at qty_filled=0) and the narrowness is what
                # makes appending safe -- _build_lots_from_fills_log skips rows
                # with qty_filled <= 0, so the stale row contributes nothing and
                # the appended row is not a duplicate. Never touch a row that
                # already claims a fill; correcting THAT needs supersede
                # semantics the log does not have.
                if float(row.get("qty_filled") or 0) <= 0 and filled > 0:
                    _ident = (str(row.get("ticker")), str(row.get("side")).upper(),
                              int(float(row.get("qty_requested") or 0)))
                    if _ident in _already_backfilled:
                        # Corrected on a prior --write run; the original
                        # qty_filled=0 row is still here for audit. Don't append
                        # a second correction (would double-count the lot).
                        print(f"    (already back-filled on a prior run — skipping)")
                    else:
                        corrections.append(_build_backfill_row(row, trade, filled,
                                                               now_status, permid))
            except Exception as e:
                print(f"  {row['ticker']:<12} {oid:>7} (read failed: {e})")
        print(f"  {'-'*12} {'-'*7} {'-'*11} {'-'*18} {'-'*18} "
              f"{'-'*8} {'-'*10} {'-'*10}")
        print()
        print(f"  Status changed since log:  {n_changed:>3}")
        print(f"  Filled now (truth):        {n_filled_now:>3}")
        print(f"  Missing from fills log:    {len(corrections):>3}")
        print()

        if corrections:
            print("  These orders FILLED but the log records qty_filled=0:")
            for c in corrections:
                print(f"    {c['ticker']:<12} {c['side']:<5} "
                      f"{c['qty_filled']:>6} @ {c.get('avg_fill_price_local')} "
                      f"exec={c.get('exec_timestamp')}")
            if write:
                n = _append_backfill_rows(corrections, fills_path)
                print(f"\n  [check-fills] BACK-FILLED {n} corrected row(s) -> "
                      f"{fills_path.name}")
                print("  [check-fills] NOTE: if a seed with SeedAsOf covers these "
                      "fills, lots.py will\n"
                      "                skip replaying them — the seed already "
                      "includes them. That is correct.")
            else:
                print("\n  [check-fills] NOT written (read-only). Re-run with "
                      "--write to back-fill them.")
            if email:
                _send_fill_confirmation_email(corrections, still_pending)
        elif email:
            print("  [check-fills] --email: no newly-filled orders — no email sent.")
    finally:
        if ib.isConnected():
            ib.disconnect()

    print("=" * 96)
    if write:
        print("CHECK-FILLS COMPLETE — no orders placed; fills log may have been "
              "back-filled.")
    else:
        print("CHECK-FILLS COMPLETE — read-only, no orders placed.")
    print("=" * 96)
    return 0


def _run_snapshot_nav_mode() -> int:
    """--snapshot-nav: append one broker-truth NAV row to ibkr_nav_log.jsonl.

    READ-ONLY by construction: queries account summary + portfolio marks,
    writes a log line, never builds a plan or touches an order. The broker's
    NetLiquidation is the fund's real performance record (user directive
    2026-07-08: performance from IBKR, not yfinance reconstructions —
    but broker reads must never trigger rebalances; the engine's flags
    remain the only rebalance driver).
    """
    try:
        from ib_insync import IB
    except ImportError:
        print("[nav] ib_insync not installed. Run: pip install ib_insync")
        return 1
    print(f"[nav] connecting to PAPER IBKR at {HOST}:{PORT} (clientId={CLIENT_ID})...")
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT)
    except Exception as e:
        print(f"[nav][ERR] connect failed ({type(e).__name__}): {e}")
        return 2
    try:
        managed = ib.managedAccounts() or []
        if not managed:
            print("[nav][SAFETY] managedAccounts() returned nothing. Aborting.")
            return 3
        _refuse_if_live(managed[0])
        tags = {}
        for v in ib.accountSummary():
            if str(v.currency).upper() in ("AUD", "BASE") and v.tag in (
                    "NetLiquidation", "TotalCashValue", "GrossPositionValue",
                    "UnrealizedPnL", "RealizedPnL", "AvailableFunds"):
                tags[v.tag] = float(v.value)
        positions = []
        for p in ib.portfolio():
            c = p.contract
            sym = str(c.symbol).strip().upper()
            if (str(getattr(c, "currency", "")).upper() == "AUD"
                    or str(getattr(c, "primaryExchange", "")).upper() == "ASX"):
                sym = f"{sym}.AX"
            # avg_cost_local is BROKER-TRUTH cost basis, per share, in the
            # instrument's trading currency (same units as mark_local). It is
            # the only cost-basis anchor the API will give us after the fact:
            # TWS serves NO historical executions (verified 2026-07-17 —
            # reqExecutions returns 0 even with a 30d ExecutionFilter), so once
            # the session that placed an order ends, its fills are gone. When
            # ibkr_fills_log.jsonl misses a fill (as it did for the 2026-07-08
            # SOXL/VEA pair), this is what catches the resulting lot-book drift.
            # Verified exact: units * (mark_local - avg_cost_local) reproduces
            # unrealized_pnl to the cent for every position.
            try:
                avg_cost_local = float(p.averageCost)
            except (TypeError, ValueError):
                avg_cost_local = None
            positions.append({
                "ticker": sym, "units": float(p.position),
                "mark_local": float(p.marketPrice),
                # NAME IS A MISNOMER, kept only because historical rows use it:
                # PortfolioItem.marketValue is in the contract's TRADING
                # currency, NOT the account base currency. It equals
                # units * mark_local exactly, USD rows included. Do NOT divide
                # by units*mark to get FX — that always yields 1.0. Use
                # lots.derive_fx_from_snapshot, which reads the rate off the
                # account-level gross_positions_aud identity instead.
                "mkt_value_base": float(p.marketValue),
                "unrealized_pnl": float(p.unrealizedPNL),
                "avg_cost_local": avg_cost_local,
                "currency": str(getattr(c, "currency", "") or "").upper() or None,
            })
    finally:
        ib.disconnect()

    row = {
        "ts": datetime.now().astimezone().isoformat(),
        "account": managed[0],
        "net_liquidation_aud": tags.get("NetLiquidation"),
        "cash_aud": tags.get("TotalCashValue"),
        "gross_positions_aud": tags.get("GrossPositionValue"),
        "unrealized_pnl_aud": tags.get("UnrealizedPnL"),
        "realized_pnl_aud": tags.get("RealizedPnL"),
        "n_positions": len(positions),
        "positions": positions,
    }
    with (_SCRIPT_DIR / "ibkr_nav_log.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")
    print(f"[nav] snapshot: NetLiq ${row['net_liquidation_aud']:,.2f} AUD "
          f"(cash ${row['cash_aud']:,.2f}, {len(positions)} positions) "
          f"-> ibkr_nav_log.jsonl")
    return 0


def _run_reconcile_lots_mode(write: bool = False, email: bool = False) -> int:
    """--reconcile-lots: realign lots_seed.json to the latest broker snapshot.

    The fills log freezes rows at qty_filled=0 whenever an order fills after the
    placing session ends (every US leg, and any ASX leg filling after the run),
    and the broker serves no historical executions to repair it from. The lot
    book therefore drifts from broker truth a little more every rebalance until
    someone re-seeds by hand — which is what happened on 2026-07-28, and by
    2026-08-03 VLUE.AX was already 400 units adrift again.

    This automates that re-seed properly: it PRESERVES the AcqDate and AUD cost
    of units that already exist (a blunt re-seed would restamp everything with
    today's date and reset the 12-month LT-discount clock) and only writes the
    difference. PREVIEW by default; --write applies it and backs up the old
    seed first."""
    seed_path = _SCRIPT_DIR / "lots_seed.json"
    nav_path = _SCRIPT_DIR / "ibkr_nav_log.jsonl"
    if not nav_path.exists():
        print(f"[reconcile-lots] {nav_path.name} not found — no broker truth "
              f"to reconcile against. Run --snapshot-nav first.")
        return 1

    snap = None
    with open(nav_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                snap = json.loads(line)
            except json.JSONDecodeError:
                continue
    if not snap or not snap.get("positions"):
        print("[reconcile-lots] latest NAV snapshot has no positions.")
        return 1

    seed = []
    if seed_path.exists():
        try:
            seed = json.loads(seed_path.read_text(encoding="utf-8")) or []
        except Exception as e:
            print(f"[reconcile-lots][ERR] unreadable {seed_path.name} ({e}); "
                  f"refusing to overwrite it.")
            return 1

    broker = {str(p.get("ticker", "")).strip().upper(): p
              for p in snap["positions"]}
    snap_ts = snap.get("ts")
    print(f"[reconcile-lots] seed {seed_path.name} ({len(seed)} lot(s)) vs "
          f"broker snapshot {snap_ts} ({len(broker)} position(s))")

    try:
        from lots import reconcile_seed_to_broker, derive_fx_from_snapshot
    except ImportError as e:
        print(f"[reconcile-lots][ERR] cannot import lots.py ({e}).")
        return 1

    # FX must come from the snapshot's ACCOUNT-level identity, not per position:
    # mkt_value_base is local currency despite its name (see derive_fx_from_
    # snapshot). Getting this wrong books US cost bases ~42% light.
    fx_map = derive_fx_from_snapshot(snap)
    print(f"[reconcile-lots] snapshot FX: "
          + ", ".join(f"{k}->AUD {v:.4f}" for k, v in sorted(fx_map.items())))
    foreign = {p.get("currency") for p in snap["positions"]
               if str(p.get("currency") or "").upper() not in ("", "AUD")}
    unresolved = {c for c in foreign if str(c).upper() not in fx_map}
    if unresolved:
        print(f"[reconcile-lots][WARN] no FX for {sorted(unresolved)} — those "
              f"positions will be reported unpriceable rather than guessed.")

    # AcqDate for new units is the SNAPSHOT time, not now: that is when the
    # units are first evidenced. Naive so it matches the seed's existing format.
    as_of = datetime.fromisoformat(snap_ts).replace(tzinfo=None) if snap_ts \
        else datetime.now()
    new_seed, actions = reconcile_seed_to_broker(seed, broker, as_of=as_of,
                                                 fx_map=fx_map)

    changed = [a for a in actions if a["action"] != "ok"]
    print()
    print("=" * 96)
    print("LOT SEED RECONCILIATION")
    print("=" * 96)
    print(f"  {'Ticker':<12} {'Action':<13} {'Units':>10}  Detail")
    print(f"  {'-'*12} {'-'*13} {'-'*10}  {'-'*54}")
    for a in actions:
        print(f"  {a['ticker']:<12} {a['action']:<13} {a['units']:>10,.0f}  "
              f"{a['detail']}")
    print(f"  {'-'*12} {'-'*13} {'-'*10}  {'-'*54}")
    print(f"\n  Changed: {len(changed)} of {len(actions)} ticker(s)")

    # A lost sell means a realised CGT event we can never price. Say so loudly
    # rather than letting a tidy-looking seed imply the books are complete.
    lost = [a for a in actions if a["action"] in ("reduced", "closed")]
    if lost:
        print()
        print("  [!] UNRECOVERABLE CGT: these positions shrank without a logged")
        print("      sale price, so the realised gain/loss cannot be computed.")
        print("      The FY tax ledger will understate them. Reconstruct from")
        print("      the IBKR account statement if the amounts are material:")
        for a in lost:
            print(f"        {a['ticker']:<12} {a['units']:>10,.0f} units")

    # --- Pending-order watch ---------------------------------------------
    # An order that fills after the placing session ends is never confirmed:
    # the row stays qty_filled=0 forever and --check-fills cannot see it. The
    # reconcile above resolves it either way, so say WHICH — affirmatively.
    # Silence used to mean "nothing changed", which is indistinguishable from
    # "the step didn't run", and the outcome that needs action (the order
    # DIED unfilled) was the silent one.
    watch_path = _SCRIPT_DIR / PENDING_WATCH_FILENAME
    watch = _pending_order_watch(_SCRIPT_DIR / FILLS_LOG_FILENAME, actions,
                                 resolved=_load_resolved_watch(watch_path))
    if watch:
        print()
        print("  PENDING ORDER WATCH — orders logged qty_filled=0, resolved")
        print("  against broker positions:")
        for w in watch:
            print(f"    {w['ticker']:<12} {w['side']:<5} {w['qty']:>6} "
                  f"{w['verdict']}")

    if not changed:
        print("\n[reconcile-lots] already reconciled — nothing to write.")
        if watch and email:
            _email_reconcile_outcome([], [], watch)
            # Dispatched to a human — never re-derive it.
            _mark_watch_resolved(watch_path, watch)
        return 0

    if not write:
        print("\n[reconcile-lots] PREVIEW only. Re-run with --write to apply.")
        return 0

    backup = seed_path.with_name(
        f"lots_seed.backup_{datetime.now():%Y-%m-%d_%H%M%S}.json")
    try:
        if seed_path.exists():
            backup.write_text(seed_path.read_text(encoding="utf-8"),
                              encoding="utf-8")
            print(f"\n[reconcile-lots] backed up old seed -> {backup.name}")
        seed_path.write_text(json.dumps(new_seed, indent=4), encoding="utf-8")
        print(f"[reconcile-lots] wrote {len(new_seed)} lot(s) -> {seed_path.name}")
    except Exception as e:
        print(f"[reconcile-lots][ERR] write failed ({e}); seed unchanged.")
        return 1

    # Mark BEFORE emailing: the seed on disk now absorbs the fill, so these
    # verdicts can no longer be re-derived correctly even if the mail fails.
    _mark_watch_resolved(watch_path, watch)

    if email:
        _email_reconcile_outcome(changed, lost, watch)
    return 0


def _session_has_closed(ticker: str, submitted_iso, now=None) -> bool:
    """Has the ticker's market closed at least once since the order was placed?

    Until it has, unchanged broker units mean nothing — the order simply has
    not had its chance. Local wall-clock is AEST (the machine's tz), and the
    universe is exactly two venues:
      .AX  -> ASX close 16:00 AEST
      else -> US close 16:00 ET, which is 06:00 AEST on EDT and 08:00 on EST.
              08:00 is used for both: erring LATE only delays a verdict by two
              hours, while erring early risks declaring a live order dead and
              prompting a duplicate buy.
    Unparseable timestamp -> True, so a corrupt row still gets a verdict from
    the position comparison rather than being silently dropped."""
    if not submitted_iso:
        return True
    try:
        placed = datetime.fromisoformat(str(submitted_iso)).replace(tzinfo=None)
    except (TypeError, ValueError):
        return True
    now = now or datetime.now()
    close_hour = 16 if str(ticker).upper().endswith(".AX") else 8
    close = placed.replace(hour=close_hour, minute=0, second=0, microsecond=0)
    if close <= placed:
        close += timedelta(days=1)
    return now >= close


def _venue_of(ticker: str) -> str:
    """ASX for .AX, US for everything else. The universe is exactly two venues,
    and they are NEVER open at the same time: ASX 10:00-16:00 AEST, US RTH
    23:30-06:00 AEST. That disjointness is the whole reason a single daily run
    cannot trade both well."""
    return "ASX" if str(ticker).strip().upper().endswith(".AX") else "US"


def _scope_open_orders(open_orders: dict, venue: str) -> dict:
    """Narrow the open-order guard to one venue.

    The guard exists to stop a new batch STACKING on working orders. Stacking is
    per-instrument, and the two venues never trade simultaneously — so a leftover
    ASX order must not veto the US pass hours later. Unscoped (venue falsy) it
    returns everything, preserving single-pass behaviour exactly."""
    if not venue:
        return dict(open_orders or {})
    return {k: v for k, v in (open_orders or {}).items()
            if _venue_of(k) == str(venue).upper()}


def _rederive_to_targets(target_weights: dict, current_units: dict,
                         prices_aud: dict, nav_aud: float,
                         approved: dict, *, sigma: dict = None,
                         max_sigma: float = 3.0,
                         min_trade_aud: float = 0.0,
                         fallback_sigma: float = 0.02) -> tuple:
    """Re-solve units so each leg hits its APPROVED WEIGHT at the LIVE price.

    Returns (rows, findings).

    WHY this and not the approved unit count: what the morning run approved is a
    target WEIGHT vector; the unit count is just that weight divided by a price
    which, for a US name priced off the previous close, is ~13h stale by the
    time the US opens. Executing fixed units through a 5% gap-up overshoots the
    target twice — the existing holding is worth more AND the same unit count
    costs more. Re-solving is also self-financing: a gap up means proportionally
    fewer units, so the AUD spend stays roughly flat and the cash budget holds.

    This is NOT re-optimising. target_weights are frozen from the approved plan;
    only the arithmetic that turns them into units is redone.

    Three refusals, because self-correcting is not the same as always-correct:
      - SIGN FLIP: if the re-derived trade reverses the approved direction, the
        morning's decision is stale. Drop the leg; never reverse it silently.
      - DRIFT: past max_sigma of the name's own overnight move, something
        regime-changing happened. sigma is per-ticker fractional daily vol, so
        2% on HBRD and 2% on SMH are not treated as the same event.
      - UNPRICEABLE: no live price, no trade. Never size a leg blind.
    """
    rows, findings = [], []
    sigma = sigma or {}
    try:
        nav = float(nav_aud)
    except (TypeError, ValueError):
        nav = 0.0
    if not (nav > 0):
        return ([], ["REPRICE ABORT: NAV unavailable or non-positive — cannot "
                     "convert target weights into units."])

    for tkr in sorted(prices_aud):
        px = prices_aud.get(tkr)
        try:
            px = float(px)
        except (TypeError, ValueError):
            px = 0.0
        if not (px > 0):
            findings.append(f"{tkr}: DROPPED — no usable live price.")
            continue

        w = float(target_weights.get(tkr, 0.0) or 0.0)
        cur = float(current_units.get(tkr, 0.0) or 0.0)
        appr = approved.get(tkr) or {}
        appr_delta = int(float(appr.get("delta_units", 0) or 0))

        # Drift: compare the live price against the one the plan was built on.
        # An unknown vol falls back to a flat ceiling and SAYS SO — silently
        # skipping the check would leave the guard permanently inert, which is
        # indistinguishable from not having written it.
        appr_px = float(appr.get("px_aud", 0) or 0)
        sig = float(sigma.get(tkr, 0.0) or 0.0)
        est = ""
        if sig <= 0:
            sig = float(fallback_sigma)
            est = f" (vol unknown — flat {sig*100:.1f}%/day assumed)"
        if appr_px > 0 and sig > 0:
            move = abs(px / appr_px - 1.0)
            if move > max_sigma * sig:
                findings.append(
                    f"{tkr}: DROPPED — moved {move*100:.1f}% since the plan "
                    f"({max_sigma:g}x its {sig*100:.1f}% daily vol{est}); the "
                    f"approved decision may no longer hold.")
                continue
        elif appr_px <= 0:
            findings.append(f"{tkr}: drift UNCHECKED — the approved plan "
                            f"recorded no reference price for it.")

        delta = int(round((w * nav - cur * px) / px))
        if delta == 0:
            continue
        if appr_delta and (delta > 0) != (appr_delta > 0):
            findings.append(
                f"{tkr}: DROPPED — re-derived trade ({delta:+d}u) reverses the "
                f"approved direction ({appr_delta:+d}u); plan is stale.")
            continue
        value = abs(delta) * px
        if min_trade_aud and value < float(min_trade_aud):
            findings.append(f"{tkr}: skipped — ${value:,.0f} below the "
                            f"${float(min_trade_aud):,.0f} minimum parcel.")
            continue

        rows.append({
            "ticker": tkr,
            "side": "buy" if delta > 0 else "sell",
            "delta_units": delta,
            "px_aud": round(px, 4),
            "delta_value_aud": round(delta * px, 2),
            # Carried from the approved plan: brokerage here is a fixed-minimum
            # fee that a small unit change does not move, and it feeds the
            # preview total only — never an order field.
            "brokerage_aud": float(appr.get("brokerage_aud", 0.0) or 0.0),
            "approved_delta_units": appr_delta,
        })
    return (rows, findings)


def _verdict_gate(rec_entry: dict, *, execute: bool, override: str = "") -> tuple:
    """Decide whether this plan is CLEARED to be submitted. Returns (ok, lines).

    The [rebal-trigger] verdict is the engine's decision about whether to trade
    at all — drift over threshold AND the 6W cadence satisfied. It used to exist
    only as a line in run.log that daily_auto.ps1 grepped, which meant the
    automated path was gated and the manual path was not: running `--execute` by
    hand submitted whatever was in the rec log. On 2026-08-10 that was 8 trades
    and $71k of volume the engine had gated as within-cadence.

    So the gate moves next to the money. RUN clears. SKIP refuses and says why.
    A plan with no verdict at all (written by an engine predating the stamp) is
    UNKNOWN, which also refuses — an unproven plan is not an approved one, and
    the cost of being wrong is asymmetric.

    PREVIEW is never blocked: reading the plan is how you decide. Only actual
    submission is gated, so `--execute` is the trigger.

    override is the deliberate escape hatch. It does not suppress the finding —
    the reason is echoed and lands in the fills log context, because overriding
    a gate silently is how gates stop meaning anything.

    COVERAGE (check this if the mode dispatch in main() is ever reordered):
      --execute        gated — the manual path this was written for.
      --auto-execute   gated — it sets args.execute=True before the dispatch,
                       so the unattended wrapper path lands here too.
      --shadow-execute NOT gated; returns earlier and places no orders. Its job
                       is to report what WOULD happen.
      --complete-deferred  NOT gated; returns earlier. A deferred buy belongs to
                       an OLDER plan that was already cleared, so judging it
                       against today's verdict would block a legitimate leg."""
    verdict = str((rec_entry or {}).get("verdict") or "UNKNOWN").upper()
    reason = str((rec_entry or {}).get("skip_reason") or "").strip()
    run_at = (rec_entry or {}).get("run_at", "?")
    lines = []

    if verdict == "RUN":
        return (True, [f"[exec] verdict gate: RUN (plan @ {run_at}) — cleared."])

    detail = f"verdict={verdict}" + (f", {reason}" if reason else "")
    if verdict == "UNKNOWN":
        detail += (" — this entry predates verdict stamping, or the engine's "
                   "verdict step failed; re-run the engine to get a stamped plan")

    if not execute:
        lines.append(f"[exec] verdict gate: {detail}.")
        lines.append("[exec] PREVIEW is not gated; --execute would REFUSE this plan.")
        return (True, lines)

    if override:
        lines.append(f"[exec][OVERRIDE] verdict gate bypassed: {detail}.")
        lines.append(f"[exec][OVERRIDE] operator reason: {override}")
        lines.append("[exec][OVERRIDE] proceeding against the engine's decision.")
        return (True, lines)

    lines.append(f"[exec] REFUSING TO EXECUTE — {detail}.")
    lines.append(f"[exec]   plan @ {run_at}")
    lines.append("[exec]   The engine did not clear this plan for execution. Re-run "
                 "the engine for a fresh verdict, or, if you intend to trade")
    lines.append("[exec]   anyway, re-run with: --override-verdict \"<why>\"")
    return (False, lines)


def _watch_key(row: dict) -> str:
    """Stable identity for a pending-order row.

    ibkr_perm_id is the broker's own PERMANENT order id: unique per order and
    stable across sessions, unlike order_id which restarts with the client
    connection and would collide across days. Rows that predate perm_id logging
    (or logged it as 0) fall back to the submission tuple, which is unique
    within a batch and stable because those fields are never rewritten."""
    try:
        perm = int(row.get("ibkr_perm_id") or 0)
    except (TypeError, ValueError):
        perm = 0
    if perm:
        return f"perm:{perm}"
    return "sub:{}|{}|{}|{}".format(
        row.get("exec_timestamp"), str(row.get("ticker", "")).strip().upper(),
        str(row.get("side", "")).strip().upper(),
        int(float(row.get("qty_requested") or 0)))


def _watch_is_terminal(verdict: str) -> bool:
    """FILLED and DID NOT FILL are final — the order's session is over and the
    answer cannot change. STILL WORKING and UNRESOLVED must be re-derived next
    run: the first is waiting on a close, the second may yet gain a broker
    position to compare against. Only terminal verdicts get marked."""
    return str(verdict).startswith(("FILLED", "DID NOT FILL"))


def _load_resolved_watch(path) -> dict:
    """Read the resolved-verdict store. A missing or corrupt file reads as
    empty: the cost is a repeated email, never a wrong verdict."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_resolved_watch(path, resolved: dict, keep: int = 500) -> None:
    """Persist the store, newest `keep` entries. Non-fatal — a failed write
    costs a duplicate report next run, not correctness."""
    try:
        if len(resolved) > keep:
            resolved = dict(sorted(
                resolved.items(),
                key=lambda kv: str((kv[1] or {}).get("resolved_at", "")),
                reverse=True)[:keep])
        with open(path, "w", encoding="utf-8") as f:
            json.dump(resolved, f, indent=2)
    except OSError as e:
        print(f"[reconcile-lots][WARN] could not persist pending-order "
              f"verdicts ({e}); a resolved order may be re-reported.")


def _mark_watch_resolved(path, watch: list) -> None:
    """Record terminal verdicts so they are never re-derived. Call once the
    verdict has been reported or the seed rewritten — after either, the units
    comparison that produced it can no longer be trusted."""
    terminal = [w for w in watch if _watch_is_terminal(w.get("verdict", ""))]
    if not terminal:
        return
    resolved = _load_resolved_watch(path)
    now = datetime.now().isoformat(timespec="seconds")
    for w in terminal:
        resolved[w["key"]] = {
            "ticker": w["ticker"], "side": w["side"], "qty": w["qty"],
            "verdict": w["verdict"], "batch": w["batch"], "resolved_at": now}
    _save_resolved_watch(path, resolved)


def _pending_order_watch(fills_path, actions: list, resolved=None) -> list:
    """Resolve the latest batch's unconfirmed orders against the reconcile.

    PURE-ish (one file read). An order logged qty_filled=0 either filled after
    the session died — in which case the reconcile moved that ticker's units —
    or it expired unfilled, in which case the units agree. DAY orders placed
    outside RTH die at the close of the next session and nothing retries them,
    so 'did not fill' is the verdict that needs a human.

    `resolved` is the perm-id store of verdicts already reported. Rows in it are
    skipped, because that units comparison is only meaningful BEFORE the fill is
    absorbed into the seed — afterwards 'unchanged units' means 'already
    accounted for', and re-deriving would flip FILLED into a bogus DID NOT FILL.
    Each returned row carries its `key` so the caller can mark it."""
    resolved = resolved or {}
    try:
        rows = []
        with open(fills_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except OSError:
        return []
    if not rows:
        return []

    latest_ts, batch, _ = _plan_check_fills_batch(rows)
    if not batch:
        return []

    by_tkr = {a["ticker"]: a for a in actions}
    out = []
    for r in batch:
        if float(r.get("qty_filled") or 0) > 0 or r.get("is_done"):
            continue
        key = _watch_key(r)
        if key in resolved:
            continue
        tkr = str(r.get("ticker", "")).strip().upper()
        qty = int(float(r.get("qty_requested") or 0))
        a = by_tkr.get(tkr)
        if a and a["action"] in ("added", "opened", "reduced", "closed"):
            verdict = (f"FILLED — broker moved {a['units']:+,.0f} units "
                       f"(logged as qty_filled=0)")
        elif a and a["action"] == "ok" and not _session_has_closed(
                tkr, r.get("exec_timestamp")):
            # Unchanged units prove nothing until the order has actually had
            # its session. Calling that a non-fill would invite a re-place on
            # top of a still-working order — a double buy.
            verdict = ("STILL WORKING — its market has not closed since the "
                       "order was placed; no verdict yet")
        elif a and a["action"] == "ok":
            verdict = ("DID NOT FILL — broker units unchanged. A DAY order "
                       "placed outside RTH expires at the next session close "
                       "and is NOT retried; re-place it if still wanted.")
        else:
            verdict = "UNRESOLVED — no broker position to compare against"
        out.append({"ticker": tkr, "side": str(r.get("side", "")),
                    "qty": qty, "verdict": verdict, "batch": latest_ts,
                    "key": key})
    return out


def _email_reconcile_outcome(changed: list, lost: list, watch: list) -> None:
    """Send the reconcile outcome. Fires when anything moved OR when there were
    unconfirmed orders to resolve — so a non-fill is reported, not just
    implied by silence. Non-fatal."""
    try:
        from send_alert import send
    except Exception as e:
        print(f"[reconcile-lots] --email: send_alert unavailable ({e}).")
        return

    parts = []
    if watch:
        parts.append("PENDING ORDERS RESOLVED\n" + "\n".join(
            f"  {w['ticker']:<12} {w['side']:<5} {w['qty']:>6}  {w['verdict']}"
            for w in watch))
    if changed:
        parts.append("LOT SEED CHANGES\n" + "\n".join(
            f"  {a['ticker']:<12} {a['action']:<13} {a['units']:>10,.0f}  "
            f"{a['detail']}" for a in changed))
    else:
        parts.append("LOT SEED CHANGES\n  none — book already matched broker.")
    if lost:
        parts.append("UNRECOVERABLE CGT — positions shrank with no logged "
                     "sale price:\n" + "\n".join(
                         f"  {a['ticker']} {a['units']:,.0f} units"
                         for a in lost))

    # Lead the subject with the thing that needs action.
    dead = [w for w in watch if w["verdict"].startswith("DID NOT FILL")]
    filled = [w for w in watch if w["verdict"].startswith("FILLED")]
    if dead:
        subj = (f"[Portfolio Optimiser] ORDER DID NOT FILL: "
                f"{', '.join(w['ticker'] for w in dead)}")
    elif filled:
        subj = (f"[Portfolio Optimiser] Order(s) filled: "
                f"{', '.join(w['ticker'] for w in filled)}")
    elif watch:
        subj = (f"[Portfolio Optimiser] Order(s) still working: "
                f"{', '.join(w['ticker'] for w in watch)}")
    else:
        subj = f"[Portfolio Optimiser] LOT SEED reconciled ({len(changed)} ticker(s))"
    try:
        rc = send(subj, "\n\n".join(parts))
        print(f"[reconcile-lots] --email: sent (rc={rc}).")
    except Exception as e:
        print(f"[reconcile-lots] --email: send failed ({e}); non-fatal.")


def _run_cancel_open_orders_mode(execute: bool = False, assume_yes: bool = False,
                                 only_tickers: str = "", email: bool = False) -> int:
    """Cancel WORKING (non-terminal) orders at the paper broker. PREVIEW by
    default (read-only); --execute + typed YES (or --assume-yes) actually cancels.

    Clears stale unfilled orders so the book is quiescent: the pre-trade open-order
    guard blocks trading while ANY working order remains, so a stuck order (e.g. the
    off-hours orders that sat PreSubmitted for days) wedges autonomy until cleared.
    Paper-only. Cancelling removes resting orders — it places NO trades and moves no
    funds. --only-tickers scopes; --email reports the outcome."""
    def _working(t) -> bool:
        try:
            return not t.isDone()
        except Exception:
            return True   # unknown state → treat as working (conservative)

    try:
        from ib_insync import IB
    except ImportError:
        print("[cancel] ib_insync not installed.")
        return 1
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT)
    except Exception as e:
        print(f"[cancel][ERR] connect failed ({type(e).__name__}): {e}")
        print(f"[cancel] make sure TWS / IB Gateway is on port {PORT} (paper).")
        return 2
    try:
        managed = ib.managedAccounts() or []
        if not managed:
            print("[cancel][SAFETY] managedAccounts() returned nothing.")
            return 3
        _refuse_if_live(managed[0])
        print(f"[cancel] paper account: {managed[0]}")

        ib.reqAllOpenOrders()
        ib.sleep(1.5)
        opens = [t for t in ib.openTrades() if _working(t)]

        # --only-tickers filter (suffix-tolerant, mirrors the exec path).
        if only_tickers:
            def _strip_ax(s: str) -> str:
                return s[:-3] if s.endswith(".AX") else s
            wanted = {_strip_ax(t.strip().upper()) for t in only_tickers.split(",") if t.strip()}
            opens = [t for t in opens
                     if _strip_ax(str(t.contract.symbol).upper()) in wanted]

        if not opens:
            print("[cancel] no working orders at the broker. Book is already quiescent.")
            return 0

        print("=" * 78)
        print(f"WORKING ORDERS ({len(opens)}):")
        for t in opens:
            o, c = t.order, t.contract
            try:
                rem = int(t.remaining() or 0)
            except Exception:
                rem = int(getattr(o, "totalQuantity", 0) or 0)
            print(f"  {c.symbol:<9} {o.action:<4} {int(o.totalQuantity or 0):>6} "
                  f"{o.orderType:<4} status={t.orderStatus.status:<12} "
                  f"remaining={rem:>6} orderId={o.orderId}")
        print("=" * 78)

        if not execute:
            print("[cancel] PREVIEW ONLY — re-run with --cancel-open-orders --execute to cancel.")
            return 0

        if not assume_yes:
            prompt = (f"\n[cancel][CONFIRM] About to CANCEL {len(opens)} working order(s) on "
                      f"PAPER account {managed[0]}.\n"
                      f"           Type YES (uppercase) to proceed: ")
            try:
                reply = input(prompt)
            except EOFError:
                reply = ""
            if reply != "YES":
                print(f"[cancel][SAFETY] confirmation was '{reply}', expected 'YES'. Aborting.")
                return 4

        cancelled = 0
        for t in opens:
            try:
                ib.cancelOrder(t.order)
                cancelled += 1
            except Exception as e:
                print(f"[cancel][WARN] {t.contract.symbol} cancel failed: {e}")
        ib.sleep(2)   # let cancel acks land
        ib.reqAllOpenOrders()
        ib.sleep(1.0)
        still = [t for t in ib.openTrades() if _working(t)]
        msg = (f"requested {cancelled} cancel(s); {len(still)} still working after settle"
               + (f" ({', '.join(str(t.contract.symbol) for t in still)})" if still else ""))
        print(f"[cancel] {msg}")
        if email:
            try:
                from send_alert import send
                rc = send(f"[Portfolio Optimiser] CANCEL: {cancelled} order(s) cancelled",
                          f"Paper account {managed[0]}.\n{msg}.")
                print(f"[cancel] outcome emailed (rc={rc}).")
            except Exception as e:
                print(f"[cancel] email failed ({e}); non-fatal.")
        return 0
    finally:
        if ib.isConnected():
            ib.disconnect()


def _run_flatten_mode(only_tickers: str, execute: bool = False,
                      assume_yes: bool = False, email: bool = False,
                      no_wait: bool = False) -> int:
    """Flatten a broker position the engine's own plan can NEVER close itself.

    A name with weight cap 0.0 is solver-EXCLUDED, so it never enters
    recommended_trades — which means a residual position in it (the 2026-07-24
    SOXX -53u short) is invisible to the normal exec path and, if it's a short,
    wedges autonomy on the pre-trade gate's UNCOVERED-SHORT check every run. This
    mode reads the SIGNED broker position directly, builds a to-zero order (BUY to
    cover a short, SELL to close a long) as a marketable LIMIT, runs it through the
    SAME broker-truth pre-trade gate, then submits.

    MUST be scoped with --only-tickers — never a blanket flatten. PREVIEW by
    default (read-only); --execute + typed YES (or --assume-yes) trades. Paper
    only. --email reports the outcome."""
    if not only_tickers.strip():
        print("[flatten] --flatten requires --only-tickers NAME[,NAME] "
              "(refusing to flatten the whole book).")
        return 1

    def _strip_ax(s: str) -> str:
        s = str(s).upper()
        return s[:-3] if s.endswith(".AX") else s
    wanted_norm = {_strip_ax(t.strip()) for t in only_tickers.split(",") if t.strip()}

    try:
        from ib_insync import IB, LimitOrder
    except ImportError:
        print("[flatten] ib_insync not installed.")
        return 1
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT)
    except Exception as e:
        print(f"[flatten][ERR] connect failed ({type(e).__name__}): {e}")
        print(f"[flatten] make sure TWS / IB Gateway is on port {PORT} (paper).")
        return 2
    # Subscribe to the data-farm status burst NOW (fires once, just after connect)
    # so the gate can refuse to trade into a dead feed.
    _farm_mon = _DataFarmMonitor(ib)
    try:
        managed = ib.managedAccounts() or []
        if not managed:
            print("[flatten][SAFETY] managedAccounts() returned nothing. Aborting.")
            return 3
        _refuse_if_live(managed[0])
        print(f"[flatten] paper account confirmed: {managed[0]}")

        broker_pos = _broker_positions(ib)
        targets = _flatten_targets(broker_pos, wanted_norm)
        found_norm = {_strip_ax(tk) for tk, *_ in targets}
        missing = wanted_norm - found_norm
        if missing:
            print(f"[flatten] no non-trivial broker position for {sorted(missing)} "
                  f"(already flat) — skipping those.")
        if not targets:
            print("[flatten] nothing to flatten — all requested names are already flat.")
            return 0

        plan = []
        for tk, held, side, qty, signed in targets:
            contract = _ticker_to_contract(tk)
            if contract is None:
                print(f"[flatten][WARN] {tk}: no contract (benchmark/unknown) — skipping.")
                continue
            order = LimitOrder(side, qty, 0.0)   # price set post-qualify
            order.tif = "DAY"
            rec = {"ticker": tk, "px_aud": 0.0, "delta_value_aud": 0.0,
                   "brokerage_aud": 0.0, "delta_units": signed}
            plan.append((rec, contract, order))
            print(f"[flatten] {tk}: broker {held:+g}u -> flatten via {side} {qty}u")
        if not plan:
            print("[flatten] no flattenable orders after contract build. Nothing to do.")
            return 0

        print(f"[flatten] qualifying {len(plan)} contract(s)...")
        ib.qualifyContracts(*[c for _, c, _ in plan])
        plan = [(r, c, o) for (r, c, o) in plan if getattr(c, "conId", 0)]
        if not plan:
            print("[flatten][WARN] no contracts qualified. Nothing to do.")
            return 0

        # A cap-0.0 name is never traded, so this paper account has no real-time
        # data subscription for it (SOXX tripped Error 10168). Request DELAYED-
        # FROZEN data (type 4) so the collar can price off the last available quote
        # — free, no subscription, and returns the prior close when the US market
        # is shut (the usual case for a morning-AEST flatten). Real-time names are
        # unaffected. Non-fatal.
        try:
            ib.reqMarketDataType(4)
            print("[flatten] market data: requested DELAYED-FROZEN (type 4) fallback "
                  "for unsubscribed names.")
        except Exception as _e:
            print(f"[flatten][WARN] reqMarketDataType failed ({_e}); non-fatal.")

        # Price each order for the marketable-LIMIT collar. Prefer a live/delayed
        # snapshot; fall back to the last daily close via historical data. A
        # cap-0.0 name has no top-of-book subscription, and the delayed snapshot is
        # intermittent (Error 354), so the history fallback (HMDS farm, no
        # subscription needed) is what makes a flatten reliably priceable. Never
        # submit a limit blind: an order with no verifiable reference is DROPPED.
        _df_ok, _df_reason = _farm_mon.mktdata_ok()
        if not _df_ok:
            print(f"[flatten][WARN] market-data feed: {_df_reason}")
        priced, unpriceable = [], []
        for rec, contract, order in plan:
            ref = _current_local_price(ib, contract)
            src = "live/delayed"
            if ref is None or ref <= 0:
                ref = _last_close_via_history(ib, contract)
                src = "hist-close"
            if ref is None or ref <= 0:
                unpriceable.append((rec, contract, order))
                continue
            order.lmtPrice = _marketable_limit_price(ref, order.action, LIMIT_COLLAR_PCT)
            print(f"[flatten] {rec['ticker']}: ref {ref:g} ({src}) -> "
                  f"limit {order.lmtPrice:g}")
            priced.append((rec, contract, order))
        plan = priced
        if unpriceable:
            _n = ", ".join(r["ticker"] for r, _c, _o in unpriceable)
            print(f"[flatten][WARN] {len(unpriceable)} order(s) DROPPED — no verifiable "
                  f"price (no live/delayed quote and no historical close): {_n}")
        if not plan:
            print("[flatten] no priceable orders remain. Nothing to submit.")
            return 0
        print("[flatten] priced (±{:.1f}% collar): ".format(LIMIT_COLLAR_PCT)
              + ", ".join(f"{r['ticker']} {o.action} {int(o.totalQuantity)}@{o.lmtPrice:g}"
                          for r, c, o in plan))

        # === Same broker-truth pre-trade gate as the main exec path ===
        # assumed == broker truth (the plan is built FROM the broker), so the
        # reconcile check passes by construction; the checks that MATTER for a
        # flatten still run: no resulting short, turnover, cash, open orders, live
        # feed. A BUY-to-cover clears the very UNCOVERED-SHORT check that blocks the
        # normal path (proven by tests.test_covering_an_existing_short_passes).
        _val_trades = [{"ticker": r["ticker"], "delta_units": r["delta_units"],
                        "delta_value_aud": r.get("delta_value_aud", 0.0)}
                       for r, c, o in plan]
        _val_ok, _val_fails = validate_pre_trade(
            _val_trades, dict(broker_pos), dict(broker_pos),
            available_cash_aud=_available_funds_aud(ib),
            nav_aud=_broker_net_liquidation_aud(ib),
            open_orders=_broker_open_orders(ib),
            data_farm_broken=(not _df_ok), data_farm_reason=_df_reason)
        if not _val_ok:
            print("\n" + "=" * 78)
            print("PRE-TRADE VALIDATION FAILED — NOT flattening. Fix these first:")
            for _f in _val_fails:
                print(f"  x {_f}")
            print("=" * 78)
            if email:
                _auto_email("FLATTEN BLOCKED by validation gate",
                            "The flatten refused to execute:\n\n"
                            + "\n".join(f"  x {f}" for f in _val_fails))
            return 6
        print("[flatten] pre-trade validation: PASS.")

        if not execute:
            print("[flatten] PREVIEW ONLY — re-run with --flatten --execute to submit.")
            return 0
        if not assume_yes and not _confirm_typed_yes(len(plan)):
            return 4

        print("=" * 78)
        print(f"FLATTENING {len(plan)} POSITION(S) — PAPER ACCOUNT {managed[0]}")
        print("=" * 78)
        # Sells first (free cash/margin), then buys — mirrors the main path.
        sells = [(r, c, o) for r, c, o in plan if o.action == "SELL"]
        buys = [(r, c, o) for r, c, o in plan if o.action == "BUY"]
        trades = []
        if sells:
            trades += _submit_orders(ib, sells, wait=(not no_wait))
        if buys:
            trades += _submit_orders(ib, buys, wait=(not no_wait))

        _print_reconciliation(trades)
        fills_path = _SCRIPT_DIR / FILLS_LOG_FILENAME
        rec_entry = {"run_at": f"flatten@{datetime.now().isoformat(timespec='seconds')}"}
        n_written = _write_fills_log(rec_entry, trades, fills_path)
        print(f"[flatten] {n_written} row(s) appended to {fills_path}")

        # Best-effort post-flatten residual check.
        ib.sleep(2)
        after = _broker_positions(ib)
        resid = {tk: after.get(tk, 0.0) for tk, *_ in targets
                 if abs(after.get(tk, 0.0)) > 1.0}
        msg = (f"flatten submitted {len(trades)} order(s); "
               + ("residual: " + ", ".join(f"{tk} {u:+g}u" for tk, u in resid.items())
                  if resid else "all target positions flat."))
        print(f"[flatten] {msg}")
        if email:
            _auto_email("FLATTEN complete", msg)
        return 0
    finally:
        if ib.isConnected():
            ib.disconnect()


def _run_sync_holdings_mode(workbook: str, execute: bool, assume_yes: bool = False) -> int:
    """--sync-holdings: pull broker-truth positions from paper TWS and write
    them into the Holdings sheet's Units column (typed-YES gated).

    Exists because manual TWS trades bypass ibkr_fills_log.jsonl entirely —
    without this, the engine plans against a book you no longer hold
    (2026-07-06: user part-executed a plan manually in TWS; the sheet still
    carried the pre-execution book). Read-only unless --execute AND typed YES.
    The engine itself never writes Units; this tool is the one deliberate,
    user-invoked exception.
    """
    try:
        from ib_insync import IB
    except ImportError:
        print("[sync] ib_insync not installed. Run: pip install ib_insync")
        return 1

    print(f"[sync] connecting to PAPER IBKR at {HOST}:{PORT} (clientId={CLIENT_ID})...")
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT)
    except Exception as e:
        print(f"[sync][ERR] connect failed ({type(e).__name__}): {e}")
        print(f"[sync] make sure TWS / IB Gateway is running on port {PORT} (paper).")
        return 2
    try:
        managed = ib.managedAccounts() or []
        if not managed:
            print("[sync][SAFETY] managedAccounts() returned nothing. Aborting.")
            return 3
        _refuse_if_live(managed[0])

        # Broker truth. ASX contracts come back as bare symbols in AUD;
        # map them back to the engine's .AX convention (inverse of
        # _ticker_to_contract).
        broker: dict[str, float] = {}
        for pos in ib.positions():
            c = pos.contract
            if str(getattr(c, "secType", "STK")).upper() != "STK":
                print(f"[sync] skipping non-stock position {c.symbol} ({c.secType})")
                continue
            sym = str(c.symbol).strip().upper()
            if (str(getattr(c, "currency", "")).upper() == "AUD"
                    or str(getattr(c, "primaryExchange", "")).upper() == "ASX"):
                sym = f"{sym}.AX"
            broker[sym] = broker.get(sym, 0.0) + float(pos.position)
        cash_rows = [v for v in ib.accountSummary()
                     if v.tag == "TotalCashValue" and v.currency == "AUD"]
        cash_aud = float(cash_rows[0].value) if cash_rows else float("nan")
    finally:
        ib.disconnect()

    import pandas as pd
    try:
        df = pd.read_excel(workbook, sheet_name="Holdings")
    except Exception as e:
        print(f"[sync][ERR] cannot read {workbook} Holdings sheet: {e}")
        return 4
    df.columns = [str(c).strip() for c in df.columns]
    if "Security" not in df.columns or "Units" not in df.columns:
        print(f"[sync][ERR] Holdings sheet needs Security + Units columns; "
              f"found {list(df.columns)}")
        return 4
    sheet_units = {
        str(s).strip().upper(): float(u)
        for s, u in zip(df["Security"],
                        pd.to_numeric(df["Units"], errors="coerce").fillna(0.0))
        if str(s).strip()
    }

    all_syms = sorted(set(sheet_units) | set(broker))
    changes = []       # (sym, sheet_units, broker_units)
    missing_rows = []  # broker positions with no sheet row
    print()
    print("=" * 78)
    print(f"HOLDINGS SYNC PREVIEW — broker truth vs {workbook}")
    print("=" * 78)
    print(f"  {'Security':<12} {'Sheet':>12} {'Broker':>12} {'Delta':>12}")
    for sym in all_syms:
        su = sheet_units.get(sym)
        bu = broker.get(sym, 0.0)
        if su is None:
            missing_rows.append(sym)
            print(f"  {sym:<12} {'(no row)':>12} {bu:>12,.0f}"
                  f"  <- NOT IN SHEET - add via dialog first")
            continue
        differs = abs(su - bu) > 1e-9
        if differs:
            changes.append((sym, su, bu))
        print(f"  {sym:<12} {su:>12,.0f} {bu:>12,.0f} {bu - su:>+12,.0f}"
              f"{'  <- UPDATE' if differs else ''}")
    print(f"  AUD cash at broker: ${cash_aud:,.2f} (info only — not written)")
    print("=" * 78)

    if not changes:
        print("[sync] sheet already matches broker. Nothing to write.")
        return 0
    if not execute:
        print(f"[sync] PREVIEW ONLY — {len(changes)} row(s) would change. "
              f"Re-run with --sync-holdings --execute to write.")
        return 0

    if assume_yes:
        # Non-interactive path for the unattended wrapper. This writes ONLY the
        # Holdings sheet Units to broker truth — it places NO orders, so it's a
        # safe reconciliation to auto-run. The morning wrapper calls it before
        # the engine so the plan is never built on stale holdings.
        print(f"[sync] --assume-yes: writing {len(changes)} Units value(s) to broker truth "
              f"(no orders placed).")
    else:
        prompt = (f"\n[sync][CONFIRM] About to write {len(changes)} Units value(s) into "
                  f"the Holdings sheet of {workbook}.\n"
                  f"           Type YES (uppercase) to proceed: ")
        try:
            reply = input(prompt)
        except EOFError:
            reply = ""
        if reply != "YES":
            print(f"[sync][SAFETY] confirmation was '{reply}', expected 'YES'. Aborting.")
            return 5

    # Write via xlwings so an already-open workbook + macros are respected.
    import xlwings as xw
    app_created = False
    book = None
    try:
        try:
            book = xw.Book(workbook)  # attaches to an open instance
        except Exception:
            app = xw.App(visible=False)
            app_created = True
            book = app.books.open(str(Path(workbook).resolve()))
        sht = book.sheets["Holdings"]
        hdr = [str(v).strip() if v is not None else ""
               for v in sht.range("A1").expand("right").value]
        sec_col = hdr.index("Security") + 1
        units_col = hdr.index("Units") + 1
        n_rows = sht.range((2, sec_col)).expand("down").last_cell.row
        want = {s: b for s, _old, b in changes}
        wrote = 0
        for r in range(2, n_rows + 1):
            sec = str(sht.range((r, sec_col)).value or "").strip().upper()
            if sec in want:
                sht.range((r, units_col)).value = want[sec]
                wrote += 1
        book.save()
        print(f"[sync] wrote {wrote} Units value(s); workbook saved.")
    finally:
        if app_created and book is not None:
            book.app.quit()
    if missing_rows:
        print(f"[sync][WARN] broker positions with NO sheet row (not written): "
              f"{missing_rows}")
        print("[sync][WARN] add these tickers via the Holdings dialog so the "
              "universe includes them.")
    return 0


def _available_funds_aud(ib) -> "float | None":
    """Funds available for BUYS, in base AUD — the CONSERVATIVE measure.

    IBKR's Error-201 rejection gate tracks settled cash (TotalCashValue),
    not the margin-based AvailableFunds: on 2026-07-08 AvailableFunds read
    $129k while the same account rejected a buy against 'Available
    converted to base: 67,929' (= TotalCashValue). Prefer TotalCashValue,
    fall back to AvailableFunds. Returns None if the query fails — callers
    treat None as 'unknown, submit everything (legacy)' rather than
    spuriously deferring the whole batch."""
    try:
        vals = {}
        for v in ib.accountSummary():
            if str(v.currency).upper() in ("AUD", "BASE"):
                vals[v.tag] = v.value
        for tag in ("TotalCashValue", "AvailableFunds"):
            if tag in vals:
                return float(vals[tag])
    except Exception as e:
        print(f"[exec][WARN] funds query failed: {e}")
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="IBKR Phase 3 — paper-account execution with safety gates."
    )
    parser.add_argument("--rec-log", type=str, default=str(_SCRIPT_DIR / REC_LOG_FILENAME),
                        help=f"Recommendation log path (default: {REC_LOG_FILENAME} beside the script)")
    parser.add_argument("--execute", action="store_true",
                        help="REQUIRED to actually place orders. Without this, "
                             "behaves as Phase 2 dry-run (preview only).")
    parser.add_argument("--no-wait", action="store_true",
                        help="Return immediately after submission without waiting "
                             "for fills. Reconciliation summary will be incomplete.")
    parser.add_argument("--no-qualify", action="store_true",
                        help="Skip IBKR connection and contract qualification "
                             "(preview only; --execute is ignored if set).")
    parser.add_argument("--skip-validation", action="store_true",
                        help="Override the broker-truth pre-trade validation gate "
                             "(reconciliation / no-short / turnover / cash). Only "
                             "use for a deliberate manual correction you understand.")
    parser.add_argument("--venue", type=str, default="", choices=["ASX", "US"],
                        help="Trade only this venue's legs of the approved plan, "
                             "and scope the open-order guard to it. The two "
                             "venues are never open together, so each is best "
                             "traded in its own session.")
    parser.add_argument("--reprice-to-targets", action="store_true",
                        help="Re-solve units from the plan's APPROVED TARGET "
                             "WEIGHTS at live prices, instead of executing the "
                             "approved unit counts. Use when executing a plan "
                             "hours after it was built (the US pass). Requires "
                             "live quotes — do NOT use while the venue is shut.")
    parser.add_argument("--drift-sigma-max", type=float, default=3.0,
                        metavar="N",
                        help="With --reprice-to-targets: drop a leg that has "
                             "moved more than N times its daily vol since the "
                             "plan was built (default 3).")
    parser.add_argument("--override-verdict", type=str, default="", metavar="REASON",
                        help="Execute a plan the engine did NOT clear (verdict "
                             "SKIP or UNKNOWN). Requires a written reason, which "
                             "is echoed to the log. Separate from "
                             "--skip-validation: that overrides the broker-truth "
                             "safety checks, this overrides the decision to trade "
                             "at all.")
    parser.add_argument("--check-fills", action="store_true",
                        help="Read the most recent batch from ibkr_fills_log.jsonl, "
                             "query IBKR for the current status of each orderId, "
                             "and print an updated reconciliation. Use to check on "
                             "orders that were pending when the original exec run "
                             "exited (e.g. US orders waiting for overnight US open) "
                             "or to diagnose Cancelled-with-permId=0 rows. Read-only "
                             "— never calls placeOrder.")
    parser.add_argument("--write", action="store_true",
                        help="With --check-fills: back-fill the fills log with any "
                             "order IBKR reports as filled while the log still says "
                             "qty_filled=0 (the 2026-07-08 SOXL/VEA bug — the exec "
                             "script exited while the order was PreSubmitted and froze "
                             "that status). Appends corrected rows stamped with the "
                             "REAL execution time; originals are kept for audit and "
                             "contribute nothing (lots.py skips qty_filled<=0). Only "
                             "recovers what TWS can still see — it serves NO historical "
                             "executions, so run it the same session/day. Never calls "
                             "placeOrder.")
    parser.add_argument("--email", action="store_true",
                        help="With --check-fills: send a confirmation email listing "
                             "any orders that reached Filled since the log was last "
                             "updated (reuses send_alert.py). Silent when nothing new "
                             "filled — safe to run on a schedule after the US session "
                             "to confirm the offshore legs went through. No-op if the "
                             "mailer is unconfigured.")
    parser.add_argument("--wait-for-funds", type=int, default=0, metavar="SECONDS",
                        help="After submitting sells and whatever buys fit current "
                             "AvailableFunds, poll every 60s for sell proceeds and "
                             "submit the deferred buys as cash arrives, up to this "
                             "many seconds. Use when submitting outside market hours "
                             "(sells can't fill until the open). 0 = don't wait; "
                             "deferred buys are listed with a re-run command.")
    parser.add_argument("--snapshot-nav", action="store_true",
                        help="Append one broker-truth NAV row (NetLiquidation, cash, "
                             "per-position marks) to ibkr_nav_log.jsonl. Read-only; "
                             "never builds a plan or touches orders. Run daily for a "
                             "real performance record.")
    parser.add_argument("--sync-holdings", action="store_true",
                        help="Query ib.positions() from paper TWS and reconcile the "
                             "Holdings sheet Units column to broker truth. Catches "
                             "manual TWS trades that never touch the fills log. "
                             "Preview by default; add --execute (+ typed YES) to "
                             "write the sheet.")
    parser.add_argument("--flatten", action="store_true",
                        help="Flatten a broker position the engine can't close "
                             "itself — a cap-0.0, solver-excluded name (e.g. a "
                             "residual SOXX short). Reads the SIGNED broker "
                             "position, builds a to-zero marketable-LIMIT order "
                             "(BUY covers a short, SELL closes a long), runs the "
                             "SAME pre-trade gate, then submits. MUST be scoped "
                             "with --only-tickers. Preview by default; add "
                             "--execute (+ typed YES or --assume-yes) to trade.")
    parser.add_argument("--cancel-open-orders", action="store_true",
                        help="Cancel WORKING (non-terminal) orders at the paper "
                             "broker so the book is quiescent for the next rebalance "
                             "(the pre-trade open-order guard blocks trading while any "
                             "remain, so a stuck order wedges autonomy). Preview by "
                             "default; add --execute (+ typed YES, or --assume-yes) to "
                             "cancel. Scope with --only-tickers; report with --email.")
    parser.add_argument("--assume-yes", action="store_true",
                        help="With --sync-holdings or --cancel-open-orders --execute: "
                             "skip the typed-YES prompt (for the unattended wrapper). "
                             "sync-holdings writes only sheet Units; cancel places NO "
                             "orders — both are safe to auto-run.")
    parser.add_argument("--workbook", type=str, default=str(_SCRIPT_DIR / "Stock Analysis.xlsm"),
                        help="Workbook path for --sync-holdings "
                             "(default: Stock Analysis.xlsm beside the script)")
    parser.add_argument("--only-tickers", type=str, default="",
                        help="Comma-separated list of tickers to keep from the "
                             "latest rec log. Use to retry specific orders after "
                             "a partial fill or permission rejection without "
                             "re-submitting the ones that already filled. "
                             "Matching is case-sensitive; .AX suffix is optional "
                             "(BEAR matches BEAR.AX, both match each other). "
                             "Example: --only-tickers HBRD,BEAR,BBUS")
    parser.add_argument("--reconcile-lots", action="store_true",
                        help="Realign lots_seed.json to the latest broker NAV "
                             "snapshot. The fills log freezes rows at "
                             "qty_filled=0 when an order fills after the placing "
                             "session ends, and the broker serves no historical "
                             "executions to repair it — so the lot book drifts "
                             "until re-seeded. PREVIEW by default; add --write "
                             "to apply (backs up the old seed). Preserves the "
                             "AcqDate + AUD cost of existing units, so the "
                             "12-month LT-discount clock is not reset. Pair with "
                             "--email to be told what moved.")
    parser.add_argument("--complete-deferred", action="store_true",
                        help="Guarded auto-completion of buys deferred on a prior "
                             "--execute for insufficient funds (deferred_orders.json). "
                             "Re-checks each behind price-drift / staleness / funds "
                             "guards and submits only those that pass; refuses to "
                             "auto-trade if a price can't be verified. Pair with "
                             "--email for an outcome report. Used by the morning "
                             "wrapper to finish offshore legs while you're asleep.")
    parser.add_argument("--drift-pct", type=float, default=DEFERRED_DRIFT_PCT_DEFAULT,
                        help=f"--complete-deferred: abort a buy if its price moved "
                             f"more than this %% since the plan was approved "
                             f"(default {DEFERRED_DRIFT_PCT_DEFAULT}).")
    parser.add_argument("--max-age-hours", type=float,
                        default=DEFERRED_MAX_AGE_HOURS_DEFAULT,
                        help=f"--complete-deferred: abort a buy deferred longer ago "
                             f"than this many BUSINESS hours — weekends don't age "
                             f"a plan (default {DEFERRED_MAX_AGE_HOURS_DEFAULT}h). "
                             f"A hard {DEFERRED_MAX_WALL_AGE_HOURS_DEFAULT:.0f}h "
                             f"wall-clock ceiling applies regardless. A stale plan "
                             f"needs a fresh run.")
    parser.add_argument("--record-tax-payment", action="store_true",
                        help="Record an ATO CGT payment so the tax provision RELEASES "
                             "that cash back to investable. Requires --fy and --amount. "
                             "Additive per FY (instalments accumulate). Writes "
                             "tax_settlements.json.")
    parser.add_argument("--fy", type=str, default="",
                        help="With --record-tax-payment: the AU FY label, e.g. FY2025-26.")
    parser.add_argument("--amount", type=float, default=0.0,
                        help="With --record-tax-payment: AUD paid to the ATO for that FY.")
    parser.add_argument("--shadow-execute", action="store_true",
                        help="Dry-run the AUTONOMOUS decision on the latest plan: "
                             "check it against broker truth via the pre-trade gate "
                             "and report (with --email) what it WOULD execute or "
                             "abort — places NO orders. The rung before live "
                             "auto-execution; run it a few cycles to build trust.")
    parser.add_argument("--auto-execute", action="store_true",
                        help="AUTONOMOUS execution: submit the latest plan with NO "
                             "human prompt — the broker-truth validation gate is the "
                             "approval (MANDATORY here; a gate failure or error aborts "
                             "and emails, never trades blind). Implies --execute. Pair "
                             "with --email. This is the live auto-trade switch.")
    args = parser.parse_args()
    # --auto-execute is a headless --execute; the validation gate replaces the prompt.
    if args.auto_execute:
        args.execute = True

    # === --shadow-execute: dry-run the autonomous decision, place NO orders ===
    if args.shadow_execute:
        return _run_shadow_execute_mode(email=bool(args.email))

    # === --record-tax-payment: release the CGT provision for a settled FY ===
    if args.record_tax_payment:
        from cgt import record_tax_settlement, TAX_SETTLEMENTS_FILENAME
        fy = str(args.fy).strip()
        amt = float(args.amount or 0.0)
        if not fy or amt <= 0:
            print("[tax-payment] need --fy FY2025-26 and --amount > 0.")
            return 1
        path = _SCRIPT_DIR / TAX_SETTLEMENTS_FILENAME
        s = record_tax_settlement(path, fy, amt)
        print(f"[tax-payment] recorded ${amt:,.2f} for {fy}. Settlements now: {s}")
        print(f"[tax-payment] the tax provision will drop by this at the next run.")
        return 0

    # === --complete-deferred mode: guarded finish of prior deferred buys ===
    if args.complete_deferred:
        return _run_complete_deferred_mode(
            drift_pct=float(args.drift_pct),
            max_age_hours=float(args.max_age_hours),
            email=bool(args.email),
        )

    # === --check-fills mode: read-only status query for previous orders ===
    # No rec log needed, no orders placed. Returns 0 on success.
    if args.check_fills:
        return _run_check_fills_mode(write=bool(args.write), email=bool(args.email))

    # === --snapshot-nav mode: read-only broker NAV logging ===
    if args.snapshot_nav:
        return _run_snapshot_nav_mode()

    # === --reconcile-lots mode: realign the lot seed to broker truth ===
    # Offline — reads lots_seed.json + ibkr_nav_log.jsonl, no IBKR connection.
    if args.reconcile_lots:
        return _run_reconcile_lots_mode(write=bool(args.write),
                                        email=bool(args.email))

    # === --sync-holdings mode: broker-truth Units reconciliation ===
    if args.sync_holdings:
        return _run_sync_holdings_mode(args.workbook, execute=args.execute,
                                       assume_yes=bool(args.assume_yes))

    # === --flatten mode: cover/close a position the engine can't touch ===
    if args.flatten:
        return _run_flatten_mode(only_tickers=args.only_tickers,
                                 execute=bool(args.execute),
                                 assume_yes=bool(args.assume_yes),
                                 email=bool(args.email),
                                 no_wait=bool(args.no_wait))

    # === --cancel-open-orders mode: clear stale working orders ===
    if args.cancel_open_orders:
        return _run_cancel_open_orders_mode(execute=args.execute,
                                            assume_yes=bool(args.assume_yes),
                                            only_tickers=args.only_tickers,
                                            email=bool(args.email))

    rec_entry = _load_latest_run(Path(args.rec_log))
    trades_recs = rec_entry.get("recommended_trades", [])
    if not trades_recs:
        print("[exec] latest run has no recommended_trades. Nothing to do.")
        return 0

    # Gate BEFORE contracts, connection or preview work: if the engine did not
    # clear this plan there is nothing further worth doing.
    _gate_ok, _gate_lines = _verdict_gate(
        rec_entry, execute=bool(args.execute),
        override=str(args.override_verdict or ""))
    for _l in _gate_lines:
        print(_l)
    if not _gate_ok:
        return 3

    # --only-tickers filter: keep only the requested tickers from the rec
    # log so we can retry specific orders (e.g. after a permission rejection
    # or partial fill) without re-submitting the ones that already filled.
    # Suffix-tolerant: BEAR matches BEAR.AX, both match each other.
    if args.only_tickers:
        def _strip_ax(s: str) -> str:
            return s[:-3] if s.endswith(".AX") else s
        wanted = {t.strip() for t in args.only_tickers.split(",") if t.strip()}
        wanted_norm = {_strip_ax(t) for t in wanted}
        available_norm = {_strip_ax(r["ticker"]) for r in trades_recs}
        missing = wanted_norm - available_norm
        if missing:
            print(f"[exec][WARN] --only-tickers requested {sorted(missing)} "
                  f"which are not in the rec log. Continuing with the rest.")
        original_count = len(trades_recs)
        trades_recs = [r for r in trades_recs
                        if _strip_ax(r["ticker"]) in wanted_norm]
        kept = [r["ticker"] for r in trades_recs]
        print(f"[exec] --only-tickers filtered {original_count} -> "
              f"{len(trades_recs)} trades. Kept: {kept}")
        if not trades_recs:
            print(f"[exec] No matching tickers after filter. Nothing to do.")
            return 0

    try:
        from ib_insync import IB
    except ImportError:
        print("[exec] ib_insync not installed. Run: pip install ib_insync")
        return 1

    # --venue filter: execute one venue's legs in its own session. Applied
    # after --only-tickers so the two compose.
    if args.venue:
        _before = len(trades_recs)
        trades_recs = [r for r in trades_recs
                       if _venue_of(r["ticker"]) == args.venue]
        print(f"[exec] --venue {args.venue}: {_before} -> {len(trades_recs)} "
              f"leg(s). Kept: {[r['ticker'] for r in trades_recs]}")
        if not trades_recs:
            print(f"[exec] no {args.venue} legs in this plan. Nothing to do.")
            return 0

    # === Build contracts + orders ===
    # Marketable LIMIT orders (lmtPrice set after connect+qualify, from the live
    # quote — see _price_orders_as_limits). LIMIT not MARKET so an unattended
    # order can't fill at an unbounded off-hours gap (2026-07-24).
    from ib_insync import LimitOrder
    plan = []
    n_skipped_bench = 0
    for rec in trades_recs:
        ticker = rec["ticker"]
        delta = int(rec["delta_units"])
        if delta == 0:
            continue
        contract = _ticker_to_contract(ticker)
        if contract is None:
            n_skipped_bench += 1
            continue
        side = "BUY" if delta > 0 else "SELL"
        order = LimitOrder(side, abs(delta), 0.0)   # price filled in post-qualify
        # Set TIF explicitly so IBKR doesn't apply the account order preset and
        # fire the noisy Error 10349 ("Order TIF was set to DAY based on order
        # preset") on every order — it prints a scary-looking "Canceled order"
        # line even though the order proceeds. DAY is what the preset resolves
        # to anyway (fills at the next session open for orders placed off-hours).
        order.tif = "DAY"
        plan.append((rec, contract, order))

    # === Always-on preview (Phase 2 behaviour) ===
    if args.no_qualify or not args.execute:
        # No connection, just preview — exec was never going to happen anyway.
        n_buy = sum(1 for r, c, o in plan if o.action == "BUY")
        n_sell = sum(1 for r, c, o in plan if o.action == "SELL")
        buy_aud = sum(float(r["delta_value_aud"]) for r, c, o in plan if o.action == "BUY")
        sell_aud = sum(abs(float(r["delta_value_aud"])) for r, c, o in plan if o.action == "SELL")
        brokerage = sum(float(r["brokerage_aud"]) for r, c, o in plan)
        totals = {
            "n_buy": n_buy, "n_sell": n_sell,
            "buy_aud": buy_aud, "sell_aud": sell_aud,
            "gross_aud": buy_aud - sell_aud,
            "brokerage": brokerage,
            "n_skipped_bench": n_skipped_bench,
            "unqualified": 0,
        }
        _print_preview(rec_entry, plan, totals)
        if not args.execute:
            print("=" * 96)
            print("PREVIEW ONLY — --execute was not passed, no orders submitted.")
            print("=" * 96)
        return 0

    # === Connect + qualify ===
    print(f"[exec] connecting to PAPER IBKR at {HOST}:{PORT} (clientId={CLIENT_ID})...")
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT)
    except Exception as e:
        print(f"[exec][ERR] connect failed ({type(e).__name__}): {e}")
        print(f"[exec] make sure TWS / IB Gateway is running on port {PORT} (paper).")
        return 2
    # Subscribe to the data-farm status burst NOW (it fires once, just after
    # connect) so the pre-trade gate can refuse to trade into a dead feed.
    _farm_mon = _DataFarmMonitor(ib)

    try:
        managed = ib.managedAccounts() or []
        if not managed:
            print("[exec][SAFETY] managedAccounts() returned nothing. Aborting.")
            return 3
        _refuse_if_live(managed[0])
        print(f"[exec] paper account confirmed: {managed[0]}")

        contracts = [c for _, c, _ in plan]
        print(f"[exec] qualifying {len(contracts)} contracts...")
        t0 = time.time()
        ib.qualifyContracts(*contracts)
        print(f"[exec] qualified in {time.time()-t0:.1f}s")
        unqualified = [rec["ticker"] for (rec, c, _) in plan if not getattr(c, "conId", 0)]
        if unqualified:
            print(f"[exec][WARN] {len(unqualified)} ticker(s) failed to qualify "
                  f"and will be skipped: {', '.join(unqualified)}")
            plan = [(r, c, o) for (r, c, o) in plan if getattr(c, "conId", 0)]

        # === Re-solve units from the approved TARGET WEIGHTS at live prices ===
        # Runs after qualify (needs contracts) and BEFORE limit pricing, so the
        # collar is applied to corrected quantities. Mutates each rec in place —
        # the same dict objects back trades_recs, so the validation gate and the
        # fills log see exactly what gets submitted, not the morning's numbers.
        if args.reprice_to_targets and plan:
            _prices_aud, _sigma, _fx_missing, _src = {}, {}, [], {}
            for _rec, _c, _o in plan:
                _tk = _rec["ticker"]
                _pl, _psrc = _ref_local_price_sourced(ib, _c)
                _src[_tk] = _psrc
                _fx = _fx_local_to_aud(ib, getattr(_c, "currency", "AUD"))
                if _pl and _fx:
                    _prices_aud[_tk] = float(_pl) * float(_fx)
                elif not _fx:
                    _fx_missing.append(_tk)
                _sv = _daily_vol_via_history(ib, _c)
                if _sv:
                    _sigma[_tk] = _sv
            if _fx_missing:
                print(f"[exec][WARN] no FX rate for {sorted(set(_fx_missing))} — "
                      f"those legs cannot be re-sized and are dropped.")
            # Say plainly which price each leg was re-solved against. A 'hist'
            # source during the venue's own session means there is no real-time
            # subscription for it, so this pass is re-solving against a stale
            # bar and delivers far less than it appears to.
            _stale = sorted(t for t, s in _src.items() if s == "hist")
            print("[exec][reprice] price source: "
                  + ", ".join(f"{t}={s or 'none'}" for t, s in sorted(_src.items())))
            if _stale:
                print(f"[exec][WARN] {len(_stale)} leg(s) re-solved against a "
                      f"HISTORICAL bar, not a live quote: {_stale}. No real-time "
                      f"market data for that venue — re-pricing is degraded and "
                      f"the limit collar is set off a stale reference.")
            if _sigma:
                print("[exec][reprice] daily vol: "
                      + ", ".join(f"{k} {v*100:.1f}%"
                                  for k, v in sorted(_sigma.items())))
            _approved = {r["ticker"]: dict(r) for r, _c, _o in plan}
            _rows, _findings = _rederive_to_targets(
                rec_entry.get("target_weights", {}) or {},
                _broker_positions(ib),
                _prices_aud,
                _broker_net_liquidation_aud(ib),
                _approved,
                sigma=_sigma,
                max_sigma=float(args.drift_sigma_max))
            for _f in _findings:
                print(f"[exec][reprice] {_f}")
            _by_tkr = {r["ticker"]: r for r in _rows}
            _new_plan = []
            for _rec, _c, _o in plan:
                _row = _by_tkr.get(_rec["ticker"])
                if _row is None:
                    continue
                _was = int(_rec.get("delta_units", 0))
                _rec.update(_row)
                _o.action = "BUY" if _row["delta_units"] > 0 else "SELL"
                _o.totalQuantity = abs(int(_row["delta_units"]))
                if _was != _row["delta_units"]:
                    print(f"[exec][reprice] {_rec['ticker']}: {_was:+d}u -> "
                          f"{_row['delta_units']:+d}u at live "
                          f"${_row['px_aud']:,.2f} (same target weight)")
                _new_plan.append((_rec, _c, _o))
            plan = _new_plan
            # Validate exactly what will be submitted.
            trades_recs = [r for r, _c, _o in plan]
            if not plan:
                print("[exec] no legs survived re-pricing. Nothing to submit.")
                return 0

        # === Price the marketable LIMIT orders from live quotes ===
        # Sets each order.lmtPrice at the current touch ± LIMIT_COLLAR_PCT. An
        # order whose price can't be verified is DROPPED (never submit a limit
        # blind) — it retries next run. Runs after the data-farm gate confirmed
        # the feed is live, so unpriceable should be rare.
        plan, _unpriceable = _price_orders_as_limits(ib, plan, LIMIT_COLLAR_PCT)
        if _unpriceable:
            _names = ", ".join(r["ticker"] for r, _c, _o in _unpriceable)
            print(f"[exec][WARN] {len(_unpriceable)} order(s) DROPPED — no verifiable "
                  f"price (retry next run): {_names}")
        if plan:
            print(f"[exec] priced {len(plan)} marketable LIMIT order(s) "
                  f"(±{LIMIT_COLLAR_PCT:.1f}% collar): "
                  + ", ".join(f"{r['ticker']}@{o.lmtPrice:g}" for r, c, o in plan[:12])
                  + (" …" if len(plan) > 12 else ""))
        else:
            print("[exec] no priceable orders remain. Nothing to submit.")
            return 0

        # === Preview before confirmation ===
        n_buy = sum(1 for r, c, o in plan if o.action == "BUY")
        n_sell = sum(1 for r, c, o in plan if o.action == "SELL")
        buy_aud = sum(float(r["delta_value_aud"]) for r, c, o in plan if o.action == "BUY")
        sell_aud = sum(abs(float(r["delta_value_aud"])) for r, c, o in plan if o.action == "SELL")
        brokerage = sum(float(r["brokerage_aud"]) for r, c, o in plan)
        totals = {
            "n_buy": n_buy, "n_sell": n_sell,
            "buy_aud": buy_aud, "sell_aud": sell_aud,
            "gross_aud": buy_aud - sell_aud,
            "brokerage": brokerage,
            "n_skipped_bench": n_skipped_bench,
            "unqualified": len(unqualified),
        }
        _print_preview(rec_entry, plan, totals)

        # === Pre-trade validation gate (broker truth) ===
        # Runs BEFORE approval so a bad plan never executes. Catches the
        # 2026-07-23 class (stale sheet → naked short) the engine's sheet-side
        # sanity check can't see. In AUTO mode this gate IS the approval and is
        # MANDATORY — --skip-validation is ignored and a validation ERROR aborts
        # (never auto-trade blind). Manual mode may override with --skip-validation.
        _val_ok, _val_fails, _val_errored = False, [], False
        try:
            _broker_pos = _broker_positions(ib)
            _assumed_pos = dict(rec_entry.get("current_units", {}) or {})
            _val_trades = [{"ticker": r["ticker"],
                            "delta_units": r.get("delta_units", 0),
                            "delta_value_aud": r.get("delta_value_aud", 0.0)}
                           for r in trades_recs]
            _df_ok, _df_reason = _farm_mon.mktdata_ok()
            if not _df_ok:
                print(f"[exec][WARN] market-data feed: {_df_reason}")
            _val_ok, _val_fails = validate_pre_trade(
                _val_trades, _assumed_pos, _broker_pos,
                available_cash_aud=_available_funds_aud(ib),
                nav_aud=_broker_net_liquidation_aud(ib),
                # Scoped to the venue being traded: a leftover ASX order must
                # not veto the US pass hours later, when the ASX is shut and
                # that order can no longer stack with anything here.
                open_orders=_scope_open_orders(_broker_open_orders(ib),
                                               args.venue),
                data_farm_broken=(not _df_ok), data_farm_reason=_df_reason)
        except Exception as _e_val:
            _val_errored = True
            print(f"[exec][WARN] pre-trade validation errored ({_e_val}).")

        if _val_errored:
            if args.auto_execute:
                _auto_email("AUTO-EXECUTE ABORTED — validation could not run",
                            "The pre-trade gate errored, so the autonomous run refused "
                            "to trade blind. No orders placed. Check the logs.")
                print("[exec] AUTO-EXECUTE aborted (validation errored — fail-safe). No orders.")
                return 6
            print("[exec][WARN] validation errored — falling through to the manual typed-YES gate.")
        elif not _val_ok:
            print("\n" + "=" * 96)
            print("PRE-TRADE VALIDATION FAILED — NOT executing. Fix these first:")
            for _f in _val_fails:
                print(f"  x {_f}")
            print("=" * 96)
            if args.auto_execute or not bool(args.skip_validation):
                if args.auto_execute:
                    _auto_email("AUTO-EXECUTE BLOCKED by validation gate",
                                "The autonomous run refused to execute:\n\n"
                                + "\n".join(f"  x {f}" for f in _val_fails)
                                + "\n\nNo orders placed. Fix: ibkr_paper_exec.py "
                                  "--sync-holdings --execute, then re-run the engine.")
                print("[exec] aborted by validation gate. Most likely fix: "
                      "ibkr_paper_exec.py --sync-holdings --execute, then re-run the engine.")
                return 6
            print("[exec][WARN] --skip-validation set — proceeding DESPITE the failures above.")
        else:
            print("[exec] pre-trade validation: PASS (reconciled, no shorts, turnover + cash OK)")

        # === Approval gate ===
        # AUTO mode: the validation gate above IS the approval (no human prompt).
        # Manual mode: require the typed-YES.
        if args.auto_execute:
            print(f"[exec] AUTO-EXECUTE: {len(plan)} order(s) auto-approved by the validation "
                  f"gate — submitting (no human prompt).")
        elif not _confirm_typed_yes(len(plan)):
            return 4

        # === Submit orders — funds-aware two phases (2026-07-08) ===
        # IBKR margin-checks each BUY against CURRENT available funds; queued
        # sells contribute nothing until they FILL, which outside market hours
        # is hours away. Submitting everything at once bounced a $71k SMH buy
        # with Error 201 against $13k cash. Sells go first, then only the
        # buys that fit AvailableFunds; the rest defer (optionally polled in
        # via --wait-for-funds).
        print()
        print("=" * 96)
        print(f"EXECUTING {len(plan)} ORDERS — PAPER ACCOUNT {managed[0]}")
        print("=" * 96)
        sells = [(r, c, o) for (r, c, o) in plan if o.action == "SELL"]
        buys = sorted(((r, c, o) for (r, c, o) in plan if o.action == "BUY"),
                      key=lambda x: abs(float(x[0]["delta_value_aud"])))
        trades = []
        if sells:
            print(f"[exec] phase 1 — {len(sells)} SELL order(s):")
            trades += _submit_orders(ib, sells, wait=(not args.no_wait))

        funds = _available_funds_aud(ib)
        deferred = []
        if funds is None:
            print("[exec][WARN] available funds unknown — submitting all buys (legacy behaviour)")
            if buys:
                trades += _submit_orders(ib, buys, wait=(not args.no_wait))
            buys = []
        else:
            print(f"[exec] available funds: ${funds:,.2f} AUD")
            fits = []
            for r, c, o in buys:
                need = abs(float(r["delta_value_aud"])) * 1.01  # +1% price/fee buffer
                if need <= funds:
                    fits.append((r, c, o))
                    funds -= need
                else:
                    deferred.append((r, c, o))
            if fits:
                print(f"[exec] phase 2 — {len(fits)} BUY order(s) within available funds:")
                trades += _submit_orders(ib, fits, wait=(not args.no_wait))

        if deferred and int(args.wait_for_funds or 0) > 0:
            _deadline = time.time() + int(args.wait_for_funds)
            print(f"[exec] --wait-for-funds: polling up to {int(args.wait_for_funds)}s "
                  f"for sell proceeds to cover {len(deferred)} deferred buy(s)...")
            while deferred and time.time() < _deadline:
                time.sleep(60)
                funds = _available_funds_aud(ib)
                if funds is None:
                    continue
                still = []
                for r, c, o in deferred:
                    need = abs(float(r["delta_value_aud"])) * 1.01
                    if need <= funds:
                        print(f"[exec] funds ${funds:,.2f} — submitting deferred "
                              f"{r['ticker']} ({o.action} {o.totalQuantity})")
                        trades += _submit_orders(ib, [(r, c, o)], wait=(not args.no_wait))
                        funds -= need
                    else:
                        still.append((r, c, o))
                deferred = still

        if deferred:
            _names = ",".join(r["ticker"] for r, _c, _o in deferred)
            print(f"[exec][WARN] {len(deferred)} BUY(s) DEFERRED — insufficient funds "
                  f"until sells settle: {_names}")
            print(f"[exec]        after the sells fill, re-run:")
            print(f"[exec]        ibkr_paper_exec.py --execute --only-tickers {_names}")
            # Persist so the morning wrapper's guarded auto-completer can finish
            # them (price-drift / staleness / funds guards). Captures the approved
            # local price now for the drift check.
            _dpath = _SCRIPT_DIR / DEFERRED_ORDERS_FILENAME
            _nd = _write_deferred_orders(deferred, ib, rec_entry, _dpath)
            if _nd:
                print(f"[exec]        ({_nd} deferred order(s) saved to "
                      f"{DEFERRED_ORDERS_FILENAME} for guarded auto-completion)")
        else:
            # No deferrals this run → clear any stale deferred file so the
            # auto-completer doesn't act on an obsolete plan.
            _dpath = _SCRIPT_DIR / DEFERRED_ORDERS_FILENAME
            if _dpath.exists():
                try:
                    _dpath.unlink()
                except Exception:
                    pass

        # === Reconcile + log ===
        _print_reconciliation(trades)
        fills_path = _SCRIPT_DIR / FILLS_LOG_FILENAME  # anchored: scheduled-task CWD=System32
        n_written = _write_fills_log(rec_entry, trades, fills_path)
        print(f"[exec] {n_written} row(s) appended to {fills_path}")

        # AUTO mode: email the outcome — this ran unattended, so every result must reach the user.
        if args.auto_execute:
            _lines = [f"AUTO-EXECUTE submitted {len(trades)} order(s) "
                      f"(gate PASSED, no human prompt)."]
            if deferred:
                _lines.append(f"{len(deferred)} deferred (insufficient funds until sells "
                              f"settle): {','.join(r['ticker'] for r, _c, _o in deferred)} "
                              f"— the guarded auto-completer finishes them next run.")
            _lines.append("")
            _lines.append("Orders may still be settling; the wrapper's --check-fills email "
                          "confirms fills. Review positions if anything looks off.")
            _auto_email("AUTO-EXECUTE complete", "\n".join(_lines))

    finally:
        if ib.isConnected():
            ib.disconnect()

    print()
    print("=" * 96)
    print("PHASE 3 EXECUTION COMPLETE")
    print("=" * 96)
    return 0


if __name__ == "__main__":
    sys.exit(main())

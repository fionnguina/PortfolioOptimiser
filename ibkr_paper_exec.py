r"""IBKR Phase 3 — paper-account execution.

Submits MarketOrders to the connected PAPER IBKR account for every line in
the most recent trade_recommendation_log.jsonl entry. Tracks fills, writes
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
from datetime import datetime
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
FILLS_LOG_FILENAME = "ibkr_fills_log.jsonl"
REC_LOG_FILENAME = "trade_recommendation_log.jsonl"

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
        f"\n[exec][CONFIRM] About to submit {n_orders} MarketOrder(s) to the PAPER "
        f"account.\n"
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


def _run_check_fills_mode(write: bool = False) -> int:
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


def _run_sync_holdings_mode(workbook: str, execute: bool) -> int:
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
    args = parser.parse_args()

    # === --check-fills mode: read-only status query for previous orders ===
    # No rec log needed, no orders placed. Returns 0 on success.
    if args.check_fills:
        return _run_check_fills_mode(write=bool(args.write))

    # === --snapshot-nav mode: read-only broker NAV logging ===
    if args.snapshot_nav:
        return _run_snapshot_nav_mode()

    # === --sync-holdings mode: broker-truth Units reconciliation ===
    if args.sync_holdings:
        return _run_sync_holdings_mode(args.workbook, execute=args.execute)

    rec_entry = _load_latest_run(Path(args.rec_log))
    trades_recs = rec_entry.get("recommended_trades", [])
    if not trades_recs:
        print("[exec] latest run has no recommended_trades. Nothing to do.")
        return 0

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

    # === Build contracts + orders ===
    from ib_insync import MarketOrder
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
        order = MarketOrder(side, abs(delta))
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

        # === Typed-YES gate ===
        if not _confirm_typed_yes(len(plan)):
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

        # === Reconcile + log ===
        _print_reconciliation(trades)
        fills_path = _SCRIPT_DIR / FILLS_LOG_FILENAME  # anchored: scheduled-task CWD=System32
        n_written = _write_fills_log(rec_entry, trades, fills_path)
        print(f"[exec] {n_written} row(s) appended to {fills_path}")

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

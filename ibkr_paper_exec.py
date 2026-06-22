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
    Returns the list of Trade objects (one per submitted order)."""
    trades = []
    for i, (rec, contract, order) in enumerate(plan, 1):
        try:
            trade = ib.placeOrder(contract, order)
            trades.append((rec, trade))
            print(f"[exec] [{i:>3}/{len(plan)}] submitted "
                  f"{order.action} {int(order.totalQuantity):>6} {rec['ticker']:<10} "
                  f"orderId={trade.order.orderId}")
        except Exception as e:
            print(f"[exec][ERR] {rec['ticker']} placeOrder failed "
                  f"({type(e).__name__}): {e}")
            continue

    if not wait:
        print(f"[exec] --no-wait set: returning before fills settle.")
        return trades

    if not trades:
        return trades

    print(f"[exec] waiting up to {FILL_WAIT_SECONDS}s for orders to settle...")
    deadline = time.time() + FILL_WAIT_SECONDS
    settled_states = {"Filled", "Cancelled", "Inactive", "ApiCancelled"}
    while time.time() < deadline:
        n_settled = sum(1 for _, t in trades
                        if t.orderStatus.status in settled_states)
        n_total = len(trades)
        if n_settled == n_total:
            print(f"[exec] all {n_total} orders settled.")
            break
        ib.sleep(1)
    else:
        remaining = [t for _, t in trades
                     if t.orderStatus.status not in settled_states]
        print(f"[exec][WARN] {len(remaining)}/{len(trades)} orders did not settle "
              f"within {FILL_WAIT_SECONDS}s. Summary will mark them as pending.")

    return trades


def _write_fills_log(rec_entry: dict, trades: list, log_path: Path) -> int:
    """Append one JSONL row per submitted order to ibkr_fills_log.jsonl.
    Returns the number of rows written."""
    rec_ts = rec_entry.get("run_at", "?")
    now_iso = datetime.now().isoformat(timespec="seconds")
    n_written = 0
    with open(log_path, "a", encoding="utf-8") as f:
        for rec, trade in trades:
            try:
                fills = list(getattr(trade, "fills", []) or [])
                filled_qty = float(getattr(trade, "filled", lambda: 0)() or 0)
                if filled_qty > 0 and fills:
                    avg_px = sum(float(fl.execution.price) * float(fl.execution.shares)
                                  for fl in fills) / filled_qty
                else:
                    avg_px = float("nan")
                row = {
                    "exec_timestamp": now_iso,
                    "rec_log_run_at": rec_ts,
                    "ticker": rec["ticker"],
                    "side": trade.order.action,
                    "qty_requested": int(trade.order.totalQuantity),
                    "qty_filled": int(filled_qty),
                    "avg_fill_price_local": avg_px,
                    "rec_px_aud": float(rec.get("px_aud", float("nan"))),
                    "rec_delta_value_aud": float(rec.get("delta_value_aud", float("nan"))),
                    "status": trade.orderStatus.status,
                    "order_id": int(trade.order.orderId),
                    "ibkr_perm_id": int(getattr(trade.order, "permId", 0) or 0),
                    "n_fills": len(fills),
                }
                f.write(json.dumps(row) + "\n")
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="IBKR Phase 3 — paper-account execution with safety gates."
    )
    parser.add_argument("--rec-log", type=str, default=REC_LOG_FILENAME,
                        help=f"Recommendation log path (default: {REC_LOG_FILENAME})")
    parser.add_argument("--execute", action="store_true",
                        help="REQUIRED to actually place orders. Without this, "
                             "behaves as Phase 2 dry-run (preview only).")
    parser.add_argument("--no-wait", action="store_true",
                        help="Return immediately after submission without waiting "
                             "for fills. Reconciliation summary will be incomplete.")
    parser.add_argument("--no-qualify", action="store_true",
                        help="Skip IBKR connection and contract qualification "
                             "(preview only; --execute is ignored if set).")
    args = parser.parse_args()

    rec_entry = _load_latest_run(Path(args.rec_log))
    trades_recs = rec_entry.get("recommended_trades", [])
    if not trades_recs:
        print("[exec] latest run has no recommended_trades. Nothing to do.")
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

        # === Submit orders ===
        print()
        print("=" * 96)
        print(f"EXECUTING {len(plan)} ORDERS — PAPER ACCOUNT {managed[0]}")
        print("=" * 96)
        trades = _submit_orders(ib, plan, wait=(not args.no_wait))

        # === Reconcile + log ===
        _print_reconciliation(trades)
        fills_path = Path(FILLS_LOG_FILENAME)
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

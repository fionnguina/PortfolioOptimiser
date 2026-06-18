r"""IBKR paper-account seeder (Phase 1.5).

Reads the engine's `current_units` from the most recent
trade_recommendation_log.jsonl entry, and "pretend trades" them as
MARKET BUY orders into the IBKR paper account so paper matches engine state.

After this runs cleanly, paper and engine are aligned. Subsequent rebalances
can mirror real-trade flow without divergence on day one.

USAGE:
  Dry-run (default — prints orders, submits NONE):
    & ".\.venv\Scripts\python.exe" ibkr_seed_paper.py

  Execute (will actually submit MARKET BUY orders to paper):
    & ".\.venv\Scripts\python.exe" ibkr_seed_paper.py --execute

PRE-EXECUTE CHECKLIST:
  1. Re-run the live optimiser ONCE so the latest current_units is in the log.
  2. In TWS: Edit -> Global Configuration -> API -> Settings:
       UNCHECK 'Read-Only API'   (otherwise orders are rejected with Error 321)
       Click Apply + OK, then RESTART TWS so the toggle takes effect.
  3. Confirm paper login still shows DUQ532705 + 'PAPER TRADING ACCOUNT' banner.
  4. Be aware of market hours (orders queue until open):
       ASX: 10:00 - 16:00 AEST/AEDT
       NYSE/NASDAQ: 09:30 - 16:00 ET  (= 23:30 - 06:00 AEST next day)
  5. Dry-run FIRST. Read the preview. Only then re-run with --execute.

POST-EXECUTE:
  1. In TWS: re-tick 'Read-Only API' to put the safety belt back on.
  2. Restart TWS.
  3. Re-run ibkr_paper_test.py — positions should now match.

SAFETY:
  - Hardcoded to paper port 7497.
  - Refuses to run if connected account does NOT start with 'DU'.
  - Default dry-run; --execute is the only way to submit.
  - Each order shown before submission; interactive y/N prompt before sending.
  - Only BUY orders (we're seeding from cash).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from ib_insync import IB, Stock, MarketOrder, util


HOST = "127.0.0.1"
PORT = 7497              # PAPER TWS
CLIENT_ID = 8            # Different from ibkr_paper_test.py (clientId=7)
CONNECT_TIMEOUT_SEC = 15
FILL_WAIT_TIMEOUT_SEC = 30  # per order; paper usually fills in <1s when market open

REC_LOG_FILENAME = "trade_recommendation_log.jsonl"


def _load_latest_current_units(log_path: Path) -> tuple[dict[str, int], str]:
    """Return (current_units, run_at_iso) from the most recent rec-log entry."""
    if not log_path.exists():
        raise SystemExit(f"[seed] {log_path} not found — run the engine at "
                         f"least once to produce it.")
    entries: list[dict] = []
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if not entries:
        raise SystemExit(f"[seed] {log_path} is empty.")
    latest = entries[-1]
    units = latest.get("current_units", {}) or {}
    if not units:
        raise SystemExit("[seed] latest log entry has no current_units field.")
    return ({str(k): int(v) for k, v in units.items() if int(v) != 0},
            str(latest.get("run_at", "?")))


def _ticker_to_contract(ticker: str) -> Stock | None:
    """Map engine ticker -> ib_insync Stock contract.

    Convention:
      *.AX     -> ASX stock, AUD, primaryExchange='ASX', exchange='SMART'
      ^*       -> benchmark, skip
      else     -> US stock, USD, exchange='SMART'
    """
    t = ticker.strip()
    if t.startswith("^"):
        return None  # benchmarks (^AORD, ^GSPC, etc.) — not tradeable
    if t.endswith(".AX"):
        base = t[:-3]
        return Stock(base, exchange="SMART", currency="AUD",
                     primaryExchange="ASX")
    return Stock(t, exchange="SMART", currency="USD")


def _refuse_if_live(account_id: str) -> None:
    if not account_id.startswith("DU"):
        raise SystemExit(
            f"[seed][SAFETY] Connected account '{account_id}' does not start "
            f"with 'DU'. Aborting before any orders are built. "
            f"Re-check TWS paper login + socket port 7497."
        )


def _print_preview(orders_plan: list[tuple[str, Stock, int]]) -> None:
    print("\n[seed] === ORDER PREVIEW ===")
    print(f"  {'#':>3}  {'Ticker':<10} {'IBKR symbol':<10} {'Exch':<6} "
          f"{'Ccy':<4} {'Action':<6} {'Qty':>10}")
    for i, (ticker, c, qty) in enumerate(orders_plan, 1):
        print(f"  {i:>3}  {ticker:<10} {c.symbol:<10} "
              f"{(c.primaryExchange or c.exchange):<6} {c.currency:<4} "
              f"{'BUY':<6} {qty:>10}")
    print(f"  total orders: {len(orders_plan)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed IBKR paper from engine current_units.")
    parser.add_argument("--execute", action="store_true",
                        help="Actually submit orders. Without this flag, dry-run only.")
    parser.add_argument("--rec-log", type=str, default=REC_LOG_FILENAME,
                        help=f"Path to recommendation log (default: {REC_LOG_FILENAME})")
    args = parser.parse_args()

    log_path = Path(args.rec_log)
    units, run_at = _load_latest_current_units(log_path)
    print(f"[seed] loaded {len(units)} non-zero positions from rec log entry "
          f"@ {run_at}:")
    for t, q in sorted(units.items()):
        print(f"  {t:<12} {q:>10}")

    # Build contract+order plan
    orders_plan: list[tuple[str, Stock, int]] = []
    skipped: list[str] = []
    for ticker, qty in sorted(units.items()):
        if qty <= 0:
            skipped.append(f"{ticker} (qty<=0)")
            continue
        contract = _ticker_to_contract(ticker)
        if contract is None:
            skipped.append(f"{ticker} (benchmark / not tradeable)")
            continue
        orders_plan.append((ticker, contract, qty))

    _print_preview(orders_plan)
    if skipped:
        print(f"\n[seed] skipped: {', '.join(skipped)}")

    if not args.execute:
        print("\n[seed] DRY-RUN ONLY. No orders submitted.")
        print("       Re-run with --execute to actually send to paper.")
        return 0

    # === EXECUTE PATH ===
    print("\n[seed] --execute flag set. Connecting to PAPER IBKR...")
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT_SEC)
    except Exception as e:
        print(f"[seed][ERROR] connect failed: {e!r}")
        return 2

    try:
        managed = ib.managedAccounts() or []
        if not managed:
            print("[seed][ERROR] no managed accounts returned.")
            return 3
        account = managed[0]
        _refuse_if_live(account)
        print(f"[seed] connected to PAPER account: {account}")

        # Qualify all contracts up-front (resolves conId, validates ticker exists).
        print("[seed] qualifying contracts (resolving conId)...")
        contracts = [c for _, c, _ in orders_plan]
        ib.qualifyContracts(*contracts)
        unresolved = [(t, c, q) for (t, c, q) in orders_plan if not c.conId]
        if unresolved:
            print(f"[seed][WARN] {len(unresolved)} contract(s) failed to "
                  f"qualify — they may not exist on the chosen exchange or "
                  f"the symbol mapping is wrong. NOT submitting these:")
            for t, c, _ in unresolved:
                print(f"  {t} -> {c}")
        viable = [(t, c, q) for (t, c, q) in orders_plan if c.conId]

        print(f"\n[seed] {len(viable)} contract(s) ready to submit.")
        confirm = input("[seed] type 'YES' (uppercase) to submit, anything else aborts: ")
        if confirm.strip() != "YES":
            print("[seed] aborted by user.")
            return 0

        # Submit one at a time + wait for fills (paper usually fills fast).
        trades = []
        for ticker, contract, qty in viable:
            order = MarketOrder("BUY", qty)
            print(f"[seed] BUY {qty} {ticker} ({contract.symbol} on "
                  f"{contract.primaryExchange or contract.exchange}, {contract.currency})...")
            tr = ib.placeOrder(contract, order)
            trades.append((ticker, tr))

        # Wait for fills or timeout.
        deadline = time.time() + FILL_WAIT_TIMEOUT_SEC * len(trades)
        while time.time() < deadline:
            pending = [tr for _, tr in trades
                       if tr.orderStatus.status not in {"Filled", "Cancelled", "Inactive"}]
            if not pending:
                break
            ib.sleep(1.0)

        print("\n[seed] === FINAL STATUS ===")
        for ticker, tr in trades:
            st = tr.orderStatus
            print(f"  {ticker:<12}  status={st.status:<10}  filled={st.filled}  "
                  f"remaining={st.remaining}  avgFillPx={st.avgFillPrice}")

        # Re-read positions to confirm.
        print("\n[seed] === POST-SEED PAPER POSITIONS ===")
        positions = ib.positions(account)
        if not positions:
            print("  (no positions — orders may not have filled yet; "
                  "market may be closed)")
        else:
            for p in positions:
                c = p.contract
                sym = c.localSymbol or c.symbol
                print(f"  {sym:<12} qty={p.position:>10.2f}  "
                      f"avgCost={p.avgCost:>10.4f}  ccy={c.currency}")

        return 0

    except Exception as e:
        print(f"[seed][ERROR] {e!r}")
        return 1
    finally:
        if ib.isConnected():
            ib.disconnect()
            print("[seed] disconnected")


if __name__ == "__main__":
    sys.exit(main())

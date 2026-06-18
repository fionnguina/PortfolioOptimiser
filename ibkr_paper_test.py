r"""IBKR paper-account read-only connection test (Phase 0).

What this does:
  - Connects to TWS or IB Gateway running PAPER credentials on 127.0.0.1:7497.
  - Prints account ID, account summary (cash, NAV, buying power, etc.).
  - Prints any positions and any open orders.
  - Disconnects cleanly.

What this does NOT do:
  - It does NOT submit any orders. There is no placeOrder() call in this file.
  - It does NOT modify any positions or settings.
  - It will refuse to run against a non-paper account (sanity check on the
    account-ID prefix).

Pre-flight checklist:
  1. TWS (or IB Gateway) is open and logged into your PAPER account.
  2. Title bar shows "THIS IS A PAPER TRADING ACCOUNT FOR SIMULATED TRADING".
  3. API enabled: Edit → Global Configuration → API → Settings:
       - Enable ActiveX and Socket Clients: ON
       - Read-Only API: ON  (extra safety belt)
       - Socket port: 7497
       - Allow connections from localhost only: ON
  4. You've clicked Apply + OK; the "API announcements" dialog has been
     dismissed.

Run with:
  & ".\.venv\Scripts\python.exe" ibkr_paper_test.py
"""
from __future__ import annotations

import sys
from ib_insync import IB


HOST = "127.0.0.1"
PORT = 7497              # 7497 = TWS paper. 4002 = IB Gateway paper. 7496/4001 = LIVE.
CLIENT_ID = 7            # Any int 1-31. 0 is reserved for the user's manual TWS.
CONNECT_TIMEOUT_SEC = 10

INTERESTING_TAGS = (
    "AccountType", "Currency",
    "NetLiquidation", "TotalCashValue", "AvailableFunds",
    "GrossPositionValue", "BuyingPower", "ExcessLiquidity",
    "UnrealizedPnL", "RealizedPnL", "Cushion",
    "DayTradesRemaining",
)


def _refuse_if_live(account_id: str) -> None:
    """Paper accounts start with 'DU' (Demo User). Anything else is rejected
    here so a misconfigured port can't accidentally hit the live account."""
    if not account_id.startswith("DU"):
        raise SystemExit(
            f"[ibkr][SAFETY] Connected account '{account_id}' does not start "
            f"with 'DU' — this looks like a LIVE account. Aborting. "
            f"Check TWS is logged into your paper login and socket port is 7497."
        )


def main() -> int:
    print(f"[ibkr] connecting to {HOST}:{PORT} (clientId={CLIENT_ID}, expecting PAPER)...")
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT_SEC)
    except ConnectionRefusedError:
        print("[ibkr][ERROR] Connection refused.")
        print("  - Is TWS / IB Gateway running?")
        print("  - Is 'Enable ActiveX and Socket Clients' ticked?")
        print("  - Did you click Apply + OK after enabling?")
        return 2
    except Exception as e:
        print(f"[ibkr][ERROR] Connect failed: {e!r}")
        return 2

    try:
        print(f"[ibkr] connected. serverVersion={ib.client.serverVersion()}, "
              f"twsTime={ib.reqCurrentTime()}")

        managed = ib.managedAccounts() or []
        print(f"[ibkr] managed accounts: {managed}")
        if not managed:
            print("[ibkr][ERROR] No managed account returned. Is TWS logged in?")
            return 3
        account = managed[0]
        _refuse_if_live(account)
        print(f"[ibkr] using PAPER account: {account}")

        # --- Account summary ---
        summary = ib.accountSummary(account)
        print(f"\n[ibkr] Account summary ({account}):")
        wanted = set(INTERESTING_TAGS)
        rows = {s.tag: s for s in summary if s.tag in wanted}
        for tag in INTERESTING_TAGS:
            s = rows.get(tag)
            if s is None:
                continue
            ccy = f" {s.currency}" if s.currency else ""
            print(f"  {tag:22s}: {s.value}{ccy}")

        # --- Positions ---
        positions = ib.positions(account)
        print(f"\n[ibkr] Positions ({len(positions)}):")
        if not positions:
            print("  (none — a fresh paper account)")
        else:
            print(f"  {'Symbol':<12} {'Sec':>5}  {'Qty':>14}  {'AvgCost':>14}  {'Ccy':>4}  {'Exchange':>12}")
            for p in positions:
                c = p.contract
                symbol = c.localSymbol or c.symbol or "?"
                print(
                    f"  {symbol:<12} {str(c.secType):>5}  "
                    f"{p.position:>14.4f}  {p.avgCost:>14.4f}  "
                    f"{str(c.currency):>4}  {str(c.exchange or c.primaryExchange):>12}"
                )

        # NOTE: openOrders() and completedOrders() are blocked by the
        # "Read-Only API" toggle (Error 321). That's the safety belt working
        # as intended — they're treated as trading-interface reads. We skip
        # them in Phase 0; we'll re-enable when we move past read-only mode.

        print("\n[ibkr] Phase 0 OK — read-only path verified.")
        return 0

    except Exception as e:
        print(f"[ibkr][ERROR] {e!r}")
        return 1
    finally:
        if ib.isConnected():
            ib.disconnect()
            print("[ibkr] disconnected")


if __name__ == "__main__":
    sys.exit(main())

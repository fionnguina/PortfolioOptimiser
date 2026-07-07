"""Exit 0 iff the IBKR API is genuinely ALIVE on the paper port.

"Port open" is not "API alive": on 2026-07-06 TWS accepted sockets and named
the account while every data request timed out (wedged session / modal
dialog). ib_insync's connect() only returns after the full handshake
including the initial data sync, so a successful connect within the timeout
is a trustworthy liveness signal. Used by daily_auto.ps1 before and after
launching IBC/Gateway.
"""
import sys

HOST, PORT, CLIENT_ID, TIMEOUT = "127.0.0.1", 7497, 97, 10

try:
    from ib_insync import IB
    ib = IB()
    ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=TIMEOUT)
    alive = ib.isConnected()
    accounts = ib.managedAccounts() or []
    ib.disconnect()
    if alive and accounts:
        print(f"[probe] API alive; accounts={accounts}")
        sys.exit(0)
    print(f"[probe] connected={alive} accounts={accounts} — not healthy")
    sys.exit(1)
except Exception as e:
    print(f"[probe] {type(e).__name__}: {e}")
    sys.exit(1)

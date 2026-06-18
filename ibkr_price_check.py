r"""IBKR market-data proof-of-concept (paper account).

Pulls DELAYED quotes from IBKR for a sample universe + compares to yfinance.
Delayed (15-min lag) data is FREE on paper accounts — no subscriptions needed.

Goals:
  - Verify the connection works for market data, not just account reads.
  - Confirm contract mapping (.AX -> ASX/AUD, others -> SMART/USD) returns sane prices.
  - Measure how far yfinance's last close drifts from IBKR's delayed last.
  - Time how long it takes to pull N tickers (informs whether we can do it on every engine run).

NO orders, NO writes — pure read-only.

Usage:
  & ".\.venv\Scripts\python.exe" ibkr_price_check.py
"""
from __future__ import annotations

import sys
import time
import math

import pandas as pd
import yfinance as yf
from ib_insync import IB, Stock


HOST = "127.0.0.1"
PORT = 7497
CLIENT_ID = 9   # different from paper_test (7) and seed_paper (8)
CONNECT_TIMEOUT = 15
SNAPSHOT_WAIT_SEC = 8   # delayed snapshots can take a few seconds

# Sample covers: ASX equity, US equity, US bond, US commodity ETF, AU commodity, AU bond
SAMPLE_TICKERS = [
    "VLUE.AX",  # AU listed value factor ETF
    "BEAR.AX",  # AU inverse SPY
    "HBRD.AX",  # AU hybrid bonds
    "GOLD.AX",  # AU gold ETF
    "SMH",      # US semiconductor ETF
    "COPX",     # US copper miners ETF
    "IEF",      # US 7-10y Treasuries
    "SPY",      # US S&P 500 — the benchmark
    "QQQ",      # US NASDAQ
    "PDBC",     # US broad commodity ETF
]


def _ticker_to_contract(ticker: str) -> Stock | None:
    t = ticker.strip()
    if t.startswith("^"):
        return None
    if t.endswith(".AX"):
        return Stock(t[:-3], exchange="SMART", currency="AUD",
                     primaryExchange="ASX")
    return Stock(t, exchange="SMART", currency="USD")


def _refuse_if_live(account_id: str) -> None:
    if not account_id.startswith("DU"):
        raise SystemExit(
            f"[price-check][SAFETY] Connected account '{account_id}' does "
            f"not start with 'DU'. Aborting — paper only."
        )


def _pick_price(tk) -> tuple[float | None, str]:
    """Choose the best available price from an ib_insync Ticker. Returns
    (price, source). Sources are tried in order of preference."""
    # last/close/midpoint can all be NaN; check explicitly.
    def _ok(v): return v is not None and isinstance(v, (int, float)) and math.isfinite(v)
    if _ok(tk.last):
        return float(tk.last), "last"
    if _ok(tk.close):
        return float(tk.close), "close"
    if _ok(tk.bid) and _ok(tk.ask):
        return (float(tk.bid) + float(tk.ask)) / 2.0, "mid"
    if _ok(tk.marketPrice()):
        return float(tk.marketPrice()), "mp"
    return None, "none"


def main() -> int:
    # === yfinance baseline ===
    print(f"[price-check] yfinance baseline: pulling {len(SAMPLE_TICKERS)} tickers...")
    t0 = time.time()
    yf_raw = yf.download(SAMPLE_TICKERS, period="5d", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    yf_close = yf_raw["Close"] if isinstance(yf_raw.columns, pd.MultiIndex) else yf_raw
    yf_last = yf_close.ffill().iloc[-1]
    print(f"  done in {time.time()-t0:.1f}s.")

    # === IBKR connection ===
    print(f"\n[price-check] connecting to PAPER IBKR at {HOST}:{PORT}...")
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT)
    except Exception as e:
        print(f"[price-check][ERROR] connect failed: {e!r}")
        print("  - Is TWS open + logged into paper?")
        print("  - Is 'Enable ActiveX and Socket Clients' ticked?")
        return 2

    try:
        managed = ib.managedAccounts() or []
        if not managed:
            print("[price-check][ERROR] no managed accounts.")
            return 3
        _refuse_if_live(managed[0])
        print(f"[price-check] paper account: {managed[0]}")

        # Mode 3 = DELAYED data. Free for paper. Mode 1 needs subscriptions.
        ib.reqMarketDataType(3)
        print("[price-check] market data mode: DELAYED (free, 15-min lag)")

        # Build + qualify contracts
        plan: list[tuple[str, Stock]] = []
        for t in SAMPLE_TICKERS:
            c = _ticker_to_contract(t)
            if c is None:
                continue
            plan.append((t, c))
        print(f"[price-check] qualifying {len(plan)} contracts...")
        t0 = time.time()
        ib.qualifyContracts(*[c for _, c in plan])
        print(f"  done in {time.time()-t0:.1f}s.")
        unresolved = [t for t, c in plan if not c.conId]
        if unresolved:
            print(f"[price-check][WARN] could not qualify: {unresolved}")

        # Request snapshots
        viable = [(t, c) for t, c in plan if c.conId]
        print(f"\n[price-check] requesting delayed snapshots for {len(viable)} contracts...")
        t0 = time.time()
        tickers = []
        for ticker, contract in viable:
            tk = ib.reqMktData(contract, "", snapshot=True, regulatorySnapshot=False)
            tickers.append((ticker, contract, tk))
        # Wait for snapshots — IB pushes them asynchronously.
        deadline = time.time() + SNAPSHOT_WAIT_SEC
        while time.time() < deadline:
            # check how many have a usable price
            ready = sum(1 for _, _, tk in tickers
                        if _pick_price(tk)[0] is not None)
            if ready == len(tickers):
                break
            ib.sleep(0.5)
        elapsed = time.time() - t0
        print(f"  snapshots collected in {elapsed:.1f}s.\n")

        # === Side-by-side comparison ===
        print(f"  {'Ticker':<10} {'Ccy':<4} {'IBKR':>12} {'(src)':>8} "
              f"{'yfinance':>12} {'Δ bps':>10}")
        print(f"  {'-'*10} {'-'*4} {'-'*12} {'-'*8} {'-'*12} {'-'*10}")
        divergence: list[tuple[str, float]] = []
        for ticker, contract, tk in tickers:
            ibkr_px, src = _pick_price(tk)
            yfx = yf_last.get(ticker)
            ccy = contract.currency
            ibkr_str = f"{ibkr_px:>12.4f}" if ibkr_px is not None else f"{'(none)':>12}"
            yf_str = f"{float(yfx):>12.4f}" if pd.notna(yfx) else f"{'(none)':>12}"
            if ibkr_px is not None and pd.notna(yfx) and float(yfx) > 0:
                diff_bps = (ibkr_px - float(yfx)) / float(yfx) * 10_000
                divergence.append((ticker, diff_bps))
                diff_str = f"{diff_bps:>+10.1f}"
            else:
                diff_str = f"{'?':>10}"
            print(f"  {ticker:<10} {ccy:<4} {ibkr_str} {src:>8} {yf_str} {diff_str}")

        # Summary diagnostics
        if divergence:
            abs_diffs = [abs(d) for _, d in divergence]
            print(f"\n[price-check] divergence summary across {len(divergence)} tickers:")
            print(f"  median |Δ|: {sorted(abs_diffs)[len(abs_diffs)//2]:.1f} bps")
            print(f"  max    |Δ|: {max(abs_diffs):.1f} bps  ({max(divergence, key=lambda x: abs(x[1]))[0]})")
            print(f"  >25 bps:    {sum(1 for d in abs_diffs if d > 25)} ticker(s)")
        print(f"\n[price-check] IBKR snapshot path verified in {elapsed:.1f}s "
              f"for {len(viable)} tickers — scaling: ~{elapsed/max(len(viable),1)*45:.0f}s "
              f"for the full 45-ticker live universe.")
        return 0

    finally:
        if ib.isConnected():
            ib.disconnect()
            print("[price-check] disconnected")


if __name__ == "__main__":
    sys.exit(main())

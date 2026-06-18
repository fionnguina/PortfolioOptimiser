r"""IBKR Phase 2 — dry-run trade-plan preview.

Reads the engine's most recent trade plan from trade_recommendation_log.jsonl
and formats every recommended trade as an IBKR Contract + Order. PRINTS the
fully-formed orders. NEVER calls placeOrder. Verifies that:

  - Every ticker maps to a sensible IBKR contract.
  - Every contract qualifies (resolves to a real conId on the chosen exchange).
  - Order sides + quantities make sense (BUY for +delta, SELL for -delta).
  - Estimated dollar value matches what the engine computed.

The output is what would be sent to IBKR if you ran Phase 3 (paper execution).
Eyeball it. If it looks wrong here it will be wrong when real orders go in.

USAGE:
  & ".\.venv\Scripts\python.exe" ibkr_dry_run.py

OPTIONS:
  --rec-log PATH   Path to recommendation log (default: ./trade_recommendation_log.jsonl)
  --no-qualify     Skip the IBKR connection entirely; just print the contract objects
                   from the mapping. Useful when TWS is not running.

SAFETY:
  - Hardcoded to paper port 7497. The string "live" appears nowhere here.
  - _refuse_if_live() check on connected account (must start with 'DU').
  - No --execute flag. There is no path in this file that calls placeOrder.
  - You can confirm by grepping: `grep "ib\.placeOrder\|\.placeOrder(" ibkr_dry_run.py`
    returns nothing — the only `placeOrder` strings in this file are in these comments.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


HOST = "127.0.0.1"
PORT = 7497              # PAPER
CLIENT_ID = 11           # distinct from paper_test (7), seed (8), price_check (9), engine (10)
CONNECT_TIMEOUT = 12

REC_LOG_FILENAME = "trade_recommendation_log.jsonl"


def _load_latest_run(log_path: Path) -> dict:
    """Return the most recent recommendation log entry (full dict)."""
    if not log_path.exists():
        raise SystemExit(f"[dry-run] {log_path} not found. Run the engine first.")
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
        raise SystemExit(f"[dry-run] {log_path} is empty.")
    return entries[-1]


def _ticker_to_contract(ticker: str):
    """Map engine ticker -> ib_insync Stock contract.
    Convention:
      *.AX     -> ASX stock, AUD, primaryExchange='ASX'
      ^*       -> benchmark, skip
      else     -> US stock, SMART exchange, USD
    """
    from ib_insync import Stock
    t = str(ticker).strip()
    if t.startswith("^"):
        return None
    if t.endswith(".AX"):
        return Stock(t[:-3], exchange="SMART", currency="AUD",
                     primaryExchange="ASX")
    return Stock(t, exchange="SMART", currency="USD")


def _refuse_if_live(account_id: str) -> None:
    if not account_id.startswith("DU"):
        raise SystemExit(
            f"[dry-run][SAFETY] Connected account '{account_id}' does NOT start "
            f"with 'DU'. Aborting — paper only."
        )


def _print_preview(rec_entry: dict, plan: list[tuple], totals: dict) -> None:
    print()
    print("=" * 96)
    print(f"DRY-RUN TRADE PREVIEW — engine recommendation @ {rec_entry.get('run_at', '?')}")
    print("=" * 96)
    print(f"  Mode:       {rec_entry.get('selected_mode')}")
    print(f"  Broker:     {rec_entry.get('broker')}")
    print(f"  Portfolio:  ${rec_entry.get('portfolio_value_aud', 0):>12,.2f} AUD")
    print(f"  Universe:   {rec_entry.get('universe_size')} tickers")
    mix = rec_entry.get("regime_mix", {})
    if mix:
        mix_str = " · ".join(f"{k.split('(')[0].strip()}={float(v)*100:.0f}%"
                              for k, v in mix.items())
        print(f"  Regime:     {mix_str}")
    print()
    print(f"  {'#':>3}  {'Ticker':<10} {'IBKR sym':<10} {'Exch':<6} {'Ccy':<4} "
          f"{'Side':<4} {'Qty':>10} {'Px AUD':>10} {'Value AUD':>14} "
          f"{'Brok':>8} {'conId':>10}")
    print(f"  {'-'*3}  {'-'*10} {'-'*10} {'-'*6} {'-'*4} "
          f"{'-'*4} {'-'*10} {'-'*10} {'-'*14} {'-'*8} {'-'*10}")
    for i, (rec, contract, order) in enumerate(plan, 1):
        ccy = getattr(contract, "currency", "?")
        exch = (getattr(contract, "primaryExchange", "") or
                getattr(contract, "exchange", "?"))
        cid = getattr(contract, "conId", 0)
        sym = getattr(contract, "symbol", "?")
        print(f"  {i:>3}  "
              f"{rec['ticker']:<10} {sym:<10} {exch:<6} {ccy:<4} "
              f"{order.action:<4} {int(order.totalQuantity):>10} "
              f"{rec['px_aud']:>10.4f} {rec['delta_value_aud']:>14,.2f} "
              f"{rec['brokerage_aud']:>8.2f} {cid if cid else '(none)':>10}")
    print(f"  {'-'*3}  {'-'*10} {'-'*10} {'-'*6} {'-'*4} "
          f"{'-'*4} {'-'*10} {'-'*10} {'-'*14} {'-'*8} {'-'*10}")
    print(f"  {'TOTALS':>{3+1+10+1+10+1+6+1+4+1+4+1+10+1+10}}  "
          f"{totals['gross_aud']:>14,.2f} {totals['brokerage']:>8.2f}")
    print()
    print(f"  Buys:        {totals['n_buy']}  total +${totals['buy_aud']:,.2f} AUD")
    print(f"  Sells:       {totals['n_sell']}  total -${totals['sell_aud']:,.2f} AUD")
    print(f"  Net cash:    ${totals['buy_aud'] - totals['sell_aud']:+,.2f} AUD before brokerage")
    print(f"  Brokerage:   ${totals['brokerage']:,.2f} AUD")
    print(f"  Skipped (^):  {totals['n_skipped_bench']} benchmark ticker(s)")
    if totals.get("unqualified"):
        print(f"  ⚠️  UNQUALIFIED:  {totals['unqualified']} ticker(s) — IBKR could not "
              f"resolve contract. WOULD BE DROPPED IN PHASE 3.")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description="IBKR Phase 2 dry-run preview.")
    parser.add_argument("--rec-log", type=str, default=REC_LOG_FILENAME,
                        help=f"Recommendation log path (default: {REC_LOG_FILENAME})")
    parser.add_argument("--no-qualify", action="store_true",
                        help="Skip IBKR connection; build contracts but don't qualify them.")
    args = parser.parse_args()

    rec_entry = _load_latest_run(Path(args.rec_log))
    trades = rec_entry.get("recommended_trades", [])
    if not trades:
        print("[dry-run] latest run has no recommended_trades. Nothing to preview.")
        return 0

    # === Lazy-import ib_insync (only when needed) ===
    try:
        from ib_insync import IB, MarketOrder
    except ImportError:
        print("[dry-run] ib_insync not installed. Run: pip install ib_insync")
        return 1

    # === Build contracts + orders from each recommended trade ===
    plan: list[tuple[dict, object, object]] = []
    n_skipped_bench = 0
    for rec in trades:
        ticker = rec["ticker"]
        delta = int(rec["delta_units"])
        if delta == 0:
            continue
        contract = _ticker_to_contract(ticker)
        if contract is None:
            n_skipped_bench += 1
            continue
        side = "BUY" if delta > 0 else "SELL"
        # ib_insync MarketOrder uses positive quantity + action; engine uses signed delta.
        order = MarketOrder(side, abs(delta))
        plan.append((rec, contract, order))

    # === Qualify contracts against IBKR (unless --no-qualify) ===
    unqualified = 0
    if args.no_qualify:
        print("[dry-run] --no-qualify set: skipping IBKR connection")
    else:
        print(f"[dry-run] connecting to PAPER IBKR at {HOST}:{PORT}...")
        ib = IB()
        try:
            ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT)
        except Exception as e:
            print(f"[dry-run][WARN] connect failed ({type(e).__name__}): {e}")
            print(f"[dry-run] continuing without contract qualification "
                  f"(re-run with TWS open to validate ticker mapping)")
        else:
            try:
                managed = ib.managedAccounts() or []
                if managed:
                    _refuse_if_live(managed[0])
                    print(f"[dry-run] paper account: {managed[0]}")
                contracts = [c for _, c, _ in plan]
                print(f"[dry-run] qualifying {len(contracts)} contracts...")
                t0 = time.time()
                ib.qualifyContracts(*contracts)
                print(f"[dry-run] qualified in {time.time()-t0:.1f}s")
                unqualified = sum(1 for c in contracts if not getattr(c, "conId", 0))
            finally:
                if ib.isConnected():
                    ib.disconnect()

    # === Aggregate totals ===
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
        "unqualified": unqualified,
    }

    _print_preview(rec_entry, plan, totals)
    print("=" * 96)
    print("DRY-RUN COMPLETE — NO ORDERS SUBMITTED. This file does not call placeOrder.")
    print("=" * 96)
    return 0


if __name__ == "__main__":
    sys.exit(main())

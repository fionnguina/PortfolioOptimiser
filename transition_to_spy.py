"""One-off transition script: liquidate engine paper positions and buy SPY.

User decision (2026-06-26): park engine operation at sub-wholesale scale
where IBKR friction (the $5 min commission) eats the strategy's alpha.
Hold SPY direct (USD, unhedged) to match the chart's "SPY (AUD)"
benchmark exactly. Resume engine when wholesale capital arrives.

The engine itself keeps running daily — captures live OOS evidence
without us executing — but its rebalance recommendations are ignored.
Only the trade plan emitted by THIS script gets executed via
`python ibkr_paper_exec.py --execute`.

Writes one entry to trade_recommendation_log.jsonl containing:
  - SELL <every current position> at market
  - BUY  SPY at market with the AUD-equivalent proceeds (IBKR auto-FX)

Position source: live IBKR query first (TWS at 127.0.0.1:7497). If TWS
isn't reachable, falls back to a hardcoded snapshot — risky if your
account has shifted, so review the printed plan carefully before
running `ibkr_paper_exec.py --execute`. The Holdings sheet in
Stock Analysis.xlsm is NOT trusted as a source because the engine
overwrites it with target weights every run.

IBKR contract dispatch (per ibkr_dry_run.py _ticker_to_contract):
  *.AX   -> ASX stock, AUD
  others -> SMART US stock, USD
IBKR auto-converts AUD->USD when buying SPY with insufficient USD cash
(paper account allows margin during settlement), so no explicit FX
trade is needed.

Run once. Not idempotent.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
REC_LOG = APP_DIR / "trade_recommendation_log.jsonl"

# Fallback snapshot — last user-confirmed positions as of 2026-06-26
# morning. Used ONLY if the live IBKR query fails. If your paper account
# has changed since, edit these before running.
HARDCODED_POSITIONS = {
    "BBUS.AX": 531,
    "BEAR.AX": 21_590,
    "HBRD.AX": 2_034,
}

# AUD buffer kept aside so we don't over-spend on SPY given IBKR's
# $1.50 USD commission per US order + ~10 bps FX spread.
USD_CASH_BUFFER_AUD = 1500.0

IBKR_HOST = "127.0.0.1"
IBKR_PORT = 7497   # paper
IBKR_CLIENT_ID = 13  # distinct from engine (10), price_check (9), paper_exec (12), dry_run (11)


def _fetch_live_account_via_ibkr() -> tuple[dict[str, int], float] | None:
    """Connect to TWS, read paper positions + AUD cash. Returns
    (positions_dict, aud_cash) or None on any failure."""
    try:
        from ib_insync import IB
    except ImportError:
        print("[transition] ib_insync not installed — skipping live query")
        return None
    ib = IB()
    try:
        ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID, timeout=10)
    except Exception as e:
        print(f"[transition] IBKR connect failed ({type(e).__name__}: {e})")
        return None
    try:
        acct = ib.managedAccounts()
        if acct and not str(acct[0]).startswith("DU"):
            print(f"[transition][SAFETY] Connected account '{acct[0]}' does NOT "
                  f"start with 'DU' — aborting live query.")
            return None
        positions = {}
        for p in ib.positions():
            sym = p.contract.symbol
            if p.contract.exchange in ("ASX", "SMART") and p.contract.currency == "AUD":
                key = sym if sym.endswith(".AX") else f"{sym}.AX"
            else:
                key = sym
            try:
                qty = int(round(float(p.position)))
            except Exception:
                qty = 0
            if qty != 0:
                positions[key] = qty

        # Read AUD cash balance. IBKR exposes this under "TotalCashValue"
        # (not "TotalCashBalance" — that tag exists too but is broken out
        # under the $LEDGER- prefix). The first AUD row wins.
        aud_cash = 0.0
        for row in ib.accountSummary():
            if row.tag == "TotalCashValue" and row.currency == "AUD":
                try:
                    aud_cash = float(row.value)
                except Exception:
                    pass
                break
        return positions, aud_cash
    except Exception as e:
        print(f"[transition] live account read failed ({type(e).__name__}: {e})")
        return None
    finally:
        try: ib.disconnect()
        except Exception: pass


def _fetch_live_prices(tickers: list[str]) -> dict[str, tuple[float, float]] | None:
    """Pull last-trade price + FX-to-AUD per ticker via yfinance. Returns
    {ticker: (px_local, fx_to_aud)} or None on failure."""
    try:
        import yfinance as yf
    except ImportError:
        print("[transition] yfinance not installed — cannot fetch prices")
        return None
    out: dict[str, tuple[float, float]] = {}
    # AUDUSD for FX: fetch once.
    try:
        usd_aud = float(yf.Ticker("AUDUSD=X").history(period="5d")["Close"].iloc[-1])
        fx_aud_per_usd = 1.0 / usd_aud if usd_aud > 0 else 1.4281
    except Exception:
        fx_aud_per_usd = 1.4281  # last known fallback
    for tk in tickers:
        try:
            hist = yf.Ticker(tk).history(period="5d")["Close"]
            px = float(hist.iloc[-1])
        except Exception:
            px = 0.0
        if tk.endswith(".AX") or tk == "^AORD":
            out[tk] = (px, 1.0)
        else:
            out[tk] = (px, fx_aud_per_usd)
    return out


def main() -> None:
    # 1) Resolve current positions + AUD cash
    live = _fetch_live_account_via_ibkr()
    if live is None:
        print(f"[transition] Falling back to hardcoded snapshot: {HARDCODED_POSITIONS}")
        positions = dict(HARDCODED_POSITIONS)
        existing_aud_cash = 0.0
        print(f"[transition] AUD cash unknown (fallback) — treating as 0.")
    else:
        positions, existing_aud_cash = live
        print(f"[transition] Live IBKR positions: {positions}")
        print(f"[transition] Live IBKR AUD cash: ${existing_aud_cash:,.2f}")
    if not positions:
        raise SystemExit("[transition] No positions found — nothing to liquidate.")

    # 2) Resolve current prices (we need px for SPY + each held ticker)
    tickers_needed = list(positions.keys()) + ["SPY"]
    pxmap = _fetch_live_prices(tickers_needed)
    if pxmap is None or any(pxmap[t][0] <= 0 for t in tickers_needed):
        raise SystemExit("[transition] Price fetch failed — aborting before "
                         "writing the rec-log entry.")

    # 3) Liquidation table
    print()
    print("[transition] Current positions to liquidate:")
    gross_aud_proceeds = 0.0
    for sec, units in positions.items():
        px_local, fx = pxmap[sec]
        value_aud = units * px_local * fx
        gross_aud_proceeds += value_aud
        print(f"  SELL {units:>7,} {sec:10s}  px={px_local:.4f}  value=${value_aud:,.0f} AUD")

    spy_px_usd, fx_aud_per_usd = pxmap["SPY"]
    spy_px_aud = spy_px_usd * fx_aud_per_usd
    print()
    print(f"[transition] Gross AUD proceeds from sells:     ${gross_aud_proceeds:,.0f}")
    print(f"[transition] + existing AUD cash:               ${existing_aud_cash:,.0f}")
    total_aud_deployable = gross_aud_proceeds + existing_aud_cash
    print(f"[transition] = total AUD deployable into SPY:   ${total_aud_deployable:,.0f}")
    print(f"[transition] SPY @ ${spy_px_usd:.2f} USD x FX {fx_aud_per_usd:.4f} = ${spy_px_aud:,.2f} AUD/share")

    # 4) Size the SPY order conservatively
    estimated_au_brokerage_aud = 5.0 * len(positions)
    aud_available_for_spy = (total_aud_deployable
                             - estimated_au_brokerage_aud
                             - USD_CASH_BUFFER_AUD)
    spy_target_units = int(aud_available_for_spy / spy_px_aud)
    print(f"[transition] After ${estimated_au_brokerage_aud:.0f} sell brokerage + "
          f"${USD_CASH_BUFFER_AUD:.0f} USD buffer: ${aud_available_for_spy:,.0f} AUD for SPY")
    print(f"[transition] Target SPY units: {spy_target_units} "
          f"(value ~${spy_target_units * spy_px_aud:,.0f} AUD / "
          f"${spy_target_units * spy_px_usd:,.0f} USD)")

    if spy_target_units <= 0:
        raise SystemExit("[transition] Target SPY units <= 0 — proceeds too low; aborting.")

    # 5) Build the rec-log entry
    now_iso = datetime.now().isoformat(timespec="seconds")
    recommended = []
    for sec, units in positions.items():
        px_local, fx = pxmap[sec]
        px_aud = px_local * fx
        recommended.append({
            "ticker": sec,
            "side": "sell",
            "delta_units": -int(units),
            "px_aud": round(px_aud, 4),
            "delta_value_aud": round(-int(units) * px_aud, 2),
            "brokerage_aud": 5.0,
        })
    recommended.append({
        "ticker": "SPY",
        "side": "buy",
        "delta_units": spy_target_units,
        "px_aud": round(spy_px_aud, 4),
        "delta_value_aud": round(spy_target_units * spy_px_aud, 2),
        "brokerage_aud": round(1.5 * fx_aud_per_usd, 2),
    })

    entry = {
        "run_at": now_iso,
        "selected_mode": "spy_hold_transition",
        "broker": "Interactive Brokers (Pro AU)",
        "cgt_mtr": 0.30,
        "portfolio_value_aud": round(total_aud_deployable, 2),
        "universe_size": 1,
        "regime_mix": {},
        "target_weights": {"SPY": 1.0},
        "current_units": {sec: int(u) for sec, u in positions.items()},
        "expected_brokerage_aud": round(
            estimated_au_brokerage_aud + 1.5 * fx_aud_per_usd, 2),
        "expected_cgt_aud": 0.0,
        "recommended_trades": recommended,
        "tlh_swaps": [],
    }

    REC_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(REC_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
    print()
    print(f"[transition] Appended one entry to {REC_LOG.name} "
          f"({len(recommended)} orders, mode=spy_hold_transition)")
    print()
    print("Next step (TWS must be running):")
    print('  ./.venv/Scripts/python.exe ibkr_paper_exec.py --execute')
    print()
    print("That will read the LATEST rec-log entry (this transition plan),")
    print("show a preview, and prompt for the typed 'YES' confirmation.")


if __name__ == "__main__":
    main()

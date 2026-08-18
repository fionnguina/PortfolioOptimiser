"""Parse an IBKR Activity Statement CSV into trades, lots and FX.

WHY THIS EXISTS: the TWS API serves no execution history — verified 2026-08-17,
reqExecutions returns 0 at 7/30/90-day filters and reqCompletedOrders returns 0,
while still reporting all 7 open positions. So once the session that placed an
order ends, its fills are gone from the API forever. lots_seed.json is therefore
a position SNAPSHOT with nominal AcqDates (most read 2026-07-08, the re-seed
date), which is why the NAV reconstruction could not derive a path and why the
lot-book cost check showed a residual traced to those placeholder dates.

The Activity Statement is the one place the real trade history survives. It
gives, per fill: Date/Time to the second, Quantity, T. Price, Comm/Fee, and
Basis — where Basis ALREADY INCLUDES commission, which is exactly the AU CGT
cost base (s110-25: incidental costs form part of the cost base). It also
carries `Listing Exch`, needed to map bare ASX symbols to the engine's `.AX`
convention, and IBKR's own Base Currency Exchange Rate, which beats a yfinance
daily close for translating a USD cost base.

Format notes that bite:
  * Multi-section CSV. Every row starts with the section name and a row type
    (Header/Data/SubTotal/Total). Only Data rows matter, and SubTotal/Total
    rows repeat the same columns — including them double-counts.
  * One section can carry SEVERAL different headers. `Trades` emits one layout
    for stocks and another for forex, so columns must be resolved against the
    most recent Header row of that section, not a fixed index.
  * Quantities are thousands-separated inside quotes ("3,259") and Date/Time
    embeds a comma ("2026-06-22, 11:53:26"), so this must go through csv, never
    a naive split.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd


def _sections(path):
    """Yield (section, rowtype, {column: value}) resolving per-section headers."""
    headers: dict[str, list] = {}
    with open(path, encoding="utf-8-sig", newline="") as fp:
        for row in csv.reader(fp):
            if len(row) < 2:
                continue
            sec, kind = row[0], row[1]
            if kind == "Header":
                headers[sec] = row[2:]
                continue
            cols = headers.get(sec)
            if not cols:
                continue
            yield sec, kind, dict(zip(cols, row[2:]))


def _num(v):
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None


def statement_period(path) -> str | None:
    for sec, _k, d in _sections(path):
        if sec == "Statement" and d.get("Field Name") == "Period":
            return d.get("Field Value")
    return None


def exchange_map(path) -> dict:
    """{SYMBOL: listing exchange} from Financial Instrument Information."""
    out = {}
    for sec, _k, d in _sections(path):
        if sec == "Financial Instrument Information" and d.get("Symbol"):
            out[d["Symbol"].strip().upper()] = str(d.get("Listing Exch", "")).strip().upper()
    return out


def fx_to_base(path) -> dict:
    """{CURRENCY: units of BASE per 1 unit of CURRENCY}, IBKR's own rates."""
    out = {}
    for sec, _k, d in _sections(path):
        if sec == "Base Currency Exchange Rate":
            r = _num(d.get("Rate"))
            if d.get("Currency") and r:
                out[d["Currency"].strip().upper()] = r
    return out


def engine_symbol(sym: str, exch: str) -> str:
    """Map a statement symbol to the engine's convention (ASX -> .AX)."""
    s = str(sym).strip().upper()
    return f"{s}.AX" if exch == "ASX" and not s.endswith(".AX") else s


def parse_trades(path) -> pd.DataFrame:
    """Executed STOCK trades. Forex conversions are excluded — they are not
    acquisitions of a CGT asset and would otherwise appear as a phantom lot."""
    exch = exchange_map(path)
    rows = []
    for sec, kind, d in _sections(path):
        # SubTotal/Total repeat the same columns; counting them doubles volume.
        if sec != "Trades" or kind != "Data":
            continue
        if d.get("DataDiscriminator") != "Order":
            continue
        if str(d.get("Asset Category", "")).strip() != "Stocks":
            continue
        sym = str(d.get("Symbol", "")).strip().upper()
        dt = str(d.get("Date/Time", "")).strip()
        qty = _num(d.get("Quantity"))
        if not sym or not dt or qty is None:
            continue
        rows.append({
            "Security": engine_symbol(sym, exch.get(sym, "")),
            "RawSymbol": sym,
            "DateTime": pd.to_datetime(dt.replace(",", " "), errors="coerce"),
            "Units": qty,                        # signed: negative = sell
            "Currency": str(d.get("Currency", "")).strip().upper(),
            "PriceLocal": _num(d.get("T. Price")),
            "CommLocal": _num(d.get("Comm/Fee")),
            # Basis already includes commission — the CGT cost base.
            "BasisLocal": _num(d.get("Basis")),
            "RealizedLocal": _num(d.get("Realized P/L")),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("DateTime").reset_index(drop=True) if not df.empty else df


def build_lots(trades: pd.DataFrame, fx: dict | None = None) -> pd.DataFrame:
    """Lot book in the engine schema: Security | AcqDate | Units | CostBaseAUD.

    Only BUYs open lots. Sells are left to the existing FIFO machinery in
    cgt.LotBook rather than being netted here, so the disposal order stays the
    engine's single source of truth.
    """
    if trades is None or trades.empty:
        return pd.DataFrame(columns=["Security", "AcqDate", "Units", "CostBaseAUD"])
    fx = fx or {}
    out = []
    for _, r in trades[trades["Units"] > 0].iterrows():
        rate = 1.0 if r["Currency"] == "AUD" else float(fx.get(r["Currency"], 0) or 0)
        basis = r["BasisLocal"]
        if not rate or basis is None or not r["Units"]:
            continue
        out.append({
            "Security": r["Security"],
            "AcqDate": r["DateTime"].isoformat(),
            "Units": float(r["Units"]),
            "CostBaseAUD": float(basis) * rate / float(r["Units"]),
        })
    return pd.DataFrame(out)

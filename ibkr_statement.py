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


def fx_from_statement(path) -> pd.Series:
    """USDAUD implied by IBKR's OWN AUD.USD conversions in the statement.

    The executor converts AUD to USD to fund a USD purchase, so a conversion
    usually sits on the same day as the trade it funds — meaning the exact
    rate IBKR applied is recoverable rather than approximated. It matters:
    against yfinance daily closes these differ by a mean 17bps and up to 76bps
    on 2026-08-03, which is a date lots were actually acquired. Rows are
    AUD.USD (USD per AUD), so USDAUD is the reciprocal.
    """
    rows = {}
    for sec, kind, d in _sections(path):
        if sec != "Trades" or kind != "Data":
            continue
        if str(d.get("Asset Category", "")).strip() != "Forex":
            continue
        px = _num(d.get("T. Price"))
        dt = str(d.get("Date/Time", "")).strip()
        if not px or px <= 0 or not dt:
            continue
        day = pd.to_datetime(dt.replace(",", " "), errors="coerce")
        if pd.isna(day):
            continue
        rows[day.normalize()] = 1.0 / px
    return pd.Series(rows).sort_index() if rows else pd.Series(dtype=float)


def cash_events(path) -> pd.DataFrame:
    """Every dated cash movement, per currency: date | Currency | Amount.

    Positions alone do not make a NAV — NetLiquidation is positions PLUS cash,
    and a reconstruction that omits cash is wrong by the whole balance and
    biases every return. Signs follow the statement: trade Proceeds are already
    negative for a purchase, commissions are negative, withdrawals negative.

    Forex conversions ARE included. They net to ~zero in base currency but move
    the balance between AUD and USD, and each currency is translated at its own
    rate, so dropping them leaves both legs wrong.
    """
    rows = []

    def add(day, ccy, amt):
        if day is None or amt is None or not ccy:
            return
        d = pd.to_datetime(str(day).replace(",", " "), errors="coerce")
        if pd.isna(d):
            return
        rows.append({"Date": d.normalize(), "Currency": str(ccy).strip().upper(),
                     "Amount": float(amt)})

    for sec, kind, d in _sections(path):
        if kind != "Data":
            continue
        if sec == "Trades" and d.get("DataDiscriminator") == "Order":
            when = d.get("Date/Time")
            ccy = d.get("Currency")
            if str(d.get("Asset Category", "")).strip() == "Forex":
                # A conversion has TWO legs and they are in different
                # currencies. For symbol AUD.USD, Quantity is the AUD sold and
                # Proceeds the USD received; the commission column is
                # explicitly "Comm in AUD" whatever the row currency says.
                # Booking only the Proceeds leg left AUD overstated by the
                # entire 183,563 that was converted away.
                base = str(d.get("Symbol", "")).split(".")[0].strip().upper()
                add(when, base, _num(d.get("Quantity")))
                add(when, ccy, _num(d.get("Proceeds")))
                add(when, "AUD", _num(d.get("Comm in AUD")))
            else:
                add(when, ccy, _num(d.get("Proceeds")))
                add(when, ccy, _num(d.get("Comm/Fee")))
        elif sec == "Deposits & Withdrawals" and d.get("Settle Date"):
            add(d.get("Settle Date"), d.get("Currency"), _num(d.get("Amount")))
        elif sec == "Dividends" and d.get("Date"):
            add(d.get("Date"), d.get("Currency"), _num(d.get("Amount")))
        elif sec == "Interest" and d.get("Date"):
            add(d.get("Date"), d.get("Currency"), _num(d.get("Amount")))
        # Borrow fees are deliberately NOT added: they are already inside the
        # Interest rows ("Net Short Stock Interest"). Counting them again
        # overstated the drain by exactly their total, -38.87 AUD / -3.29 USD.

    df = pd.DataFrame(rows, columns=["Date", "Currency", "Amount"])
    if df.empty:
        return df
    # "Total"/"Total in AUD" rows have no usable currency and are dropped by the
    # guards above; anything left with a non-currency label is not a movement.
    df = df[df["Currency"].str.len() == 3]
    return df.sort_values("Date").reset_index(drop=True)


def external_flows(path, min_abs: float = 1000.0) -> pd.DataFrame:
    """Deposits and withdrawals — capital movements, not performance.

    A NAV series containing these cannot be used to measure returns or
    drawdown. This account was reset on 2026-06-23 with a -189,334 withdrawal
    and a +250,000 deposit; read naively that is a -75% "drawdown" which is
    really just capital leaving. `min_abs` ignores the sub-dollar FX sweeps
    that also land in this section.
    """
    rows = []
    for sec, kind, d in _sections(path):
        if sec != "Deposits & Withdrawals" or kind != "Data":
            continue
        if not d.get("Settle Date"):
            continue
        amt = _num(d.get("Amount"))
        if amt is None or abs(amt) < min_abs:
            continue
        day = pd.to_datetime(str(d["Settle Date"]).strip(), errors="coerce")
        if pd.isna(day):
            continue
        rows.append({"Date": day.normalize(),
                     "Currency": str(d.get("Currency", "")).strip().upper(),
                     "Amount": amt})
    return pd.DataFrame(rows, columns=["Date", "Currency", "Amount"])


def performance_start(path, min_abs: float = 1000.0):
    """First date after the last external capital flow.

    Before it, the NAV path reflects money moving in and out rather than the
    strategy, so it is not a live track record and must not feed drift or
    drawdown.
    """
    fl = external_flows(path, min_abs=min_abs)
    if fl.empty:
        return None
    return pd.Timestamp(fl["Date"].max()) + pd.Timedelta(days=1)


def starting_cash(path) -> dict:
    """{CCY: opening balance} from the Cash Report."""
    out = {}
    for sec, kind, d in _sections(path):
        if sec == "Cash Report" and kind == "Data"                 and d.get("Currency Summary") == "Starting Cash":
            c = str(d.get("Currency", "")).strip().upper()
            if len(c) == 3:
                out[c] = _num(d.get("Total")) or 0.0
    return out


def open_lots(trades: pd.DataFrame) -> pd.DataFrame:
    """Surviving lots after signed FIFO, in LOCAL currency.

    Buys-only is not an option: over this statement BEAR nets 1,644 units from
    22,200 bought and 20,556 sold, so ignoring sells overstates it 13x. Sells
    consume the OLDEST lot first (the engine's LOT_MATCH_METHOD default), and
    each surviving lot keeps its own acquisition date and per-unit cost —
    which is the point, since AcqDate drives the 12-month CGT discount.

    SHORTS ARE SIGNED, not dropped. SOXX was sold 53 short on 2026-07-20 and
    covered on 2026-07-27 (the Borrow Fee Details section bills the borrow for
    exactly those days). A long-only FIFO silently discards a sale made against
    an empty book and then books the covering purchase as a NEW LONG — which
    invented a phantom 53-unit SOXX position that the broker does not hold.
    This project has been bitten by phantom lots before (the 3.4M SMH
    corruption, 2026-06-26), and a phantom lot is not cosmetic: it carries a
    fabricated cost base straight into the CGT calculation.

    The 2026-06-23 rows priced at 0 with 0 proceeds are the paper-account reset
    (paired with the -189,334 withdrawal and +250,000 deposit that day). They
    are ordinary disposals to FIFO and correctly close every pre-reset lot.
    """
    cols = ["Security", "AcqDate", "Units", "CostBaseLocal", "Currency"]
    if trades is None or trades.empty:
        return pd.DataFrame(columns=cols)
    eps = 1e-9
    out = []
    for sec, g in trades.sort_values("DateTime").groupby("Security", sort=True):
        book: list[list] = []          # [signed units, per-unit cost, date, ccy]
        for _, r in g.iterrows():
            u = float(r["Units"])
            basis = r["BasisLocal"]
            if abs(u) <= eps or basis is None:
                continue
            unit_cost = float(basis) / u          # sign-safe: both flip together
            if not book or (book[0][0] > 0) == (u > 0):
                book.append([u, unit_cost, r["DateTime"], r["Currency"]])
                continue
            # Opposite direction: close oldest first, then flip if it overruns.
            rem = abs(u)
            while rem > eps and book:
                avail = abs(book[0][0])
                take = min(rem, avail)
                book[0][0] -= (1.0 if book[0][0] > 0 else -1.0) * take
                rem -= take
                if abs(book[0][0]) <= eps:
                    book.pop(0)
            if rem > eps:
                book.append([(1.0 if u > 0 else -1.0) * rem, unit_cost,
                             r["DateTime"], r["Currency"]])
        for units, cost, dt, ccy in book:
            if abs(units) > eps:
                out.append({"Security": sec, "AcqDate": dt, "Units": units,
                            "CostBaseLocal": cost, "Currency": ccy})
    return pd.DataFrame(out, columns=cols)


def to_aud(lots: pd.DataFrame, fx_usdaud=None, flat_rates: dict | None = None,
           stmt_fx=None) -> pd.DataFrame:
    """Add CostBaseAUD, translating at each lot's ACQUISITION-date rate.

    AU CGT translates a foreign cost base at the rate prevailing on the
    acquisition date, not today's — the very thing that made the lot-book
    reconciliation drift. `fx_usdaud` is a dated Series (preferred);
    `flat_rates` is a {CCY: rate} fallback from the statement's own table.
    """
    if lots is None or lots.empty:
        return lots
    df = lots.copy()
    ser = None
    if fx_usdaud is not None and len(fx_usdaud):
        ser = pd.Series(fx_usdaud).copy()
        ser.index = pd.to_datetime(ser.index).tz_localize(None).normalize()
        ser = ser[~ser.index.duplicated(keep="last")].sort_index()
    # IBKR's own conversions take precedence on the days they exist — they are
    # the rate actually applied, not a daily close standing in for it.
    if stmt_fx is not None and len(stmt_fx):
        sf = pd.Series(stmt_fx).copy()
        sf.index = pd.to_datetime(sf.index).tz_localize(None).normalize()
        sf = sf[~sf.index.duplicated(keep="last")].sort_index()
        ser = sf if ser is None else sf.combine_first(ser).sort_index()

    def rate(row):
        if str(row["Currency"]).upper() == "AUD":
            return 1.0
        if ser is not None:
            try:
                v = float(ser.asof(pd.Timestamp(row["AcqDate"]).tz_localize(None).normalize()))
                if v == v and v > 0:
                    return v
            except Exception:
                pass
        return float((flat_rates or {}).get(str(row["Currency"]).upper(), 0) or 0)

    df["FxAtAcq"] = df.apply(rate, axis=1)
    df["CostBaseAUD"] = df["CostBaseLocal"] * df["FxAtAcq"]
    return df


def build_lots(trades: pd.DataFrame, fx: dict | None = None) -> pd.DataFrame:
    """Engine-schema seed: Security | AcqDate | Units | CostBaseAUD."""
    lots = to_aud(open_lots(trades), flat_rates=fx)
    if lots is None or lots.empty:
        return pd.DataFrame(columns=["Security", "AcqDate", "Units", "CostBaseAUD"])
    lots = lots[lots["FxAtAcq"] > 0].copy()
    lots["AcqDate"] = lots["AcqDate"].apply(lambda d: pd.Timestamp(d).isoformat())
    return lots[["Security", "AcqDate", "Units", "CostBaseAUD"]].reset_index(drop=True)

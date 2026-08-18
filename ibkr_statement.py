"""Parse an IBKR account statement into trades, lots and FX.

TWO WIRE FORMATS, ONE SET OF RULES: the Client Portal's Activity Statement CSV
(documented below) and the Flex Web Service's XML (documented at the bottom of
this file). `_sections` dispatches on format and the XML is translated into the
CSV's column vocabulary, so every consumer — and every trap those consumers
encode — is written exactly once. The Flex feed exists because it refreshes
without a human; the CSV remains the archival record and the fallback.

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
    """Yield (section, rowtype, {column: value}) for EITHER statement format.

    The Flex Web Service delivers XML, the Client Portal download delivers the
    multi-section CSV above. Rather than fork every consumer, the XML is
    translated INTO this CSV row vocabulary (see `_flex_sections`) — so
    parse_trades, cash_events, starting_cash and external_flows are written
    once and the hard-won traps they encode (the forex two-leg split, the
    SubTotal double-count, the borrow-fee exclusion) hold for both sources.
    """
    if is_flex_xml(path):
        yield from _flex_sections(path)
    else:
        yield from _csv_sections(path)


def _csv_sections(path):
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


# ---------------------------------------------------------------------------
# IBKR Flex Web Service (XML) support
#
# Same account, same trades, different wire format — and the Flex feed is the
# only one that can refresh unattended (the Client Portal CSV needs a human to
# click through Statements -> Activity -> date range -> export, which is why
# `ibkr_activity_statement.csv` silently aged out the moment nobody clicked).
#
# Everything below is a TRANSLATION LAYER, not a second parser: it re-labels
# Flex attributes into the Activity-Statement column vocabulary and hands them
# to the same consumers unchanged. Field differences that bite:
#   * assetCategory is "STK"/"CASH", not "Stocks"/"Forex".
#   * `cost` is the Activity Statement's `Basis` — commission included, signed
#     with quantity, which is what open_lots' `basis / units` relies on.
#   * dates default to yyyyMMdd and dateTime to "yyyyMMdd;HHmmss", but the
#     format is settable per query, so parse defensively and emit ISO.
#   * FX commission carries its own `ibCommissionCurrency` — strictly better
#     than the CSV, whose column is hardcoded "Comm in AUD".
# ---------------------------------------------------------------------------

FLEX_XML_NAME = "ibkr_flex_statement.xml"


def is_flex_xml(path) -> bool:
    """True if `path` is a Flex Web Service XML response rather than the CSV."""
    try:
        p = Path(path)
        if p.suffix.lower() != ".xml":
            return False
        with open(p, "rb") as fp:
            return b"FlexQueryResponse" in fp.read(4096)
    except Exception:
        return False


def _flex_dt(v, date_only: bool = False):
    """Normalise a Flex date/dateTime to ISO, or None.

    Flex writes "20260703;103012" by default but the query can be configured
    for "yyyy-MM-dd" and a space or comma separator, so this must not assume.
    Downstream does `.replace(",", " ")` before pd.to_datetime, and ISO
    survives that untouched.
    """
    s = str(v or "").strip()
    if not s:
        return None
    s = s.replace(";", " ").replace(",", " ")
    parts = s.split()
    d = parts[0].replace("/", "-")
    t = parts[1] if len(parts) > 1 else ""
    if len(d) == 8 and d.isdigit():                      # 20260703
        d = f"{d[:4]}-{d[4:6]}-{d[6:]}"
    if date_only or not t:
        return d
    if len(t) == 6 and t.isdigit():                      # 103012
        t = f"{t[:2]}:{t[2:4]}:{t[4:]}"
    return f"{d} {t}"


def _flex_trades(root):
    """The order-level rows — the CSV's `DataDiscriminator == "Order"`.

    THE TAG IS THE LEVEL. Flex does not distinguish level of detail by an
    attribute on a common element: order-level rows come as `<Order>` and
    execution-level rows as `<Trade>`. A query with both levels ticked emits
    both, and reading only `<Trade>` then silently reports EXECUTIONS — which
    is not double-counting so much as a different unit. Against this account
    that read 75 stock rows where the statement has 60 orders, because a
    partially-filled order arrives as several executions.

    Orders are the right unit: they are what the CSV records, what the lot book
    is built from, and what carries one commission rather than a per-fill
    allocation of it. Executions remain the fallback for a query configured
    with only that level.
    """
    orders = [n for n in root.iter("Order")
              if str(n.attrib.get("levelOfDetail", "ORDER")).upper() == "ORDER"
              and n.attrib.get("assetCategory")]
    if orders:
        return orders
    nodes = list(root.iter("Trade"))
    if not nodes:
        return []
    levels = {str(n.attrib.get("levelOfDetail", "")).upper() for n in nodes}
    for want in ("ORDER", "EXECUTION"):
        if want in levels:
            return [n for n in nodes
                    if str(n.attrib.get("levelOfDetail", "")).upper() == want]
    return nodes


def _flex_comm(a):
    """Commission the way the CSV states it: brokerage AND tax, one figure.

    Flex splits what the Activity Statement's `Comm/Fee` column combines —
    `ibCommission` is brokerage, `taxes` is the GST on it (every ASX fill here
    carries one). Mapping `Comm/Fee` to `ibCommission` alone left the cash
    reconstruction short by exactly the GST total, 120.83 AUD across 60 orders,
    while cost bases stayed right because Flex's `cost` already includes both
    (verified: tradeMoney + ibCommission + taxes == cost, to the cent).
    """
    c, t = _num(a.get("ibCommission")), _num(a.get("taxes"))
    if c is None and t is None:
        return None
    return (c or 0.0) + (t or 0.0)


def _flex_sections(path):
    """Yield Flex XML as (section, "Data", {CSV column: value}) rows."""
    import xml.etree.ElementTree as ET

    root = ET.parse(path).getroot()

    # --- statement period ---------------------------------------------------
    for st in root.iter("FlexStatement"):
        a, b = st.attrib.get("fromDate"), st.attrib.get("toDate")
        if a and b:
            yield "Statement", "Data", {
                "Field Name": "Period",
                "Field Value": f"{_flex_dt(a, True)} - {_flex_dt(b, True)}",
            }
        break

    trades = _flex_trades(root)

    # --- listing exchanges --------------------------------------------------
    # SecuritiesInfo is an optional Flex section; every Trade node carries
    # listingExchange anyway, so derive it and the user needn't tick an extra
    # box to get ASX symbols mapped onto the engine's .AX convention.
    seen = {}
    for n in root.iter("SecurityInfo"):
        s = str(n.attrib.get("symbol", "")).strip().upper()
        if s:
            seen[s] = str(n.attrib.get("listingExchange", "")).strip().upper()
    for n in trades:
        s = str(n.attrib.get("symbol", "")).strip().upper()
        if s and s not in seen:
            seen[s] = str(n.attrib.get("listingExchange", "")).strip().upper()
    for s, ex in seen.items():
        yield "Financial Instrument Information", "Data", {
            "Symbol": s, "Listing Exch": ex}

    # --- base-currency rates ------------------------------------------------
    # The CSV's Base Currency Exchange Rate section lists FOREIGN currencies
    # only — the base's own rate of 1.0 is implied, never printed. Trades carry
    # fxRateToBase for every currency including the base, so filter it out or
    # the two formats disagree on a row that means nothing either way.
    base = next((str(n.attrib.get("currency", "")).strip().upper()
                 for n in root.iter("AccountInformation")
                 if n.attrib.get("currency")), None)
    rates = {}
    for n in root.iter("ConversionRate"):
        c = str(n.attrib.get("fromCurrency", "")).strip().upper()
        r = _num(n.attrib.get("rate"))
        if len(c) == 3 and r and c != base:
            rates[c] = r
    for n in trades:
        c = str(n.attrib.get("currency", "")).strip().upper()
        r = _num(n.attrib.get("fxRateToBase"))
        if len(c) == 3 and r and c not in rates and c != base and r != 1.0:
            rates[c] = r
    for c, r in rates.items():
        yield "Base Currency Exchange Rate", "Data", {"Currency": c, "Rate": r}

    # --- trades -------------------------------------------------------------
    for n in trades:
        a = n.attrib
        cat = str(a.get("assetCategory", "")).strip().upper()
        when = _flex_dt(a.get("dateTime") or a.get("tradeDate"))
        sym = str(a.get("symbol", "")).strip().upper()
        ccy = str(a.get("currency", "")).strip().upper()
        if not when or not sym:
            continue
        if cat == "CASH":
            # Symbol is a pair (AUD.USD): quantity is the BASE currency sold,
            # proceeds the QUOTE currency received. Both legs must be booked or
            # the balance moves in one currency only.
            comm_ccy = str(a.get("ibCommissionCurrency", "")).strip().upper()
            row = {
                "DataDiscriminator": "Order", "Asset Category": "Forex",
                "Currency": ccy, "Symbol": sym, "Date/Time": when,
                "Quantity": a.get("quantity"), "T. Price": a.get("tradePrice"),
                "Proceeds": a.get("proceeds"),
            }
            # cash_events books this leg to AUD unconditionally, matching the
            # CSV's hardcoded "Comm in AUD" column. Only populate it when the
            # broker agrees, rather than silently crediting the wrong currency.
            if comm_ccy == "AUD":
                row["Comm in AUD"] = _flex_comm(a)
            elif _flex_comm(a):
                print(f"[flex][WARN] FX commission in {comm_ccy}, not AUD "
                      f"({sym} {when}) - not booked to cash.")
            yield "Trades", "Data", row
        elif cat == "STK":
            yield "Trades", "Data", {
                "DataDiscriminator": "Order", "Asset Category": "Stocks",
                "Currency": ccy, "Symbol": sym, "Date/Time": when,
                "Quantity": a.get("quantity"), "T. Price": a.get("tradePrice"),
                "Proceeds": a.get("proceeds"), "Comm/Fee": _flex_comm(a),
                # Flex `cost` == Activity Statement `Basis`: commission
                # included (AU ITAA s110-25) and signed with quantity.
                "Basis": a.get("cost"),
                "Realized P/L": a.get("fifoPnlRealized"),
            }

    # --- cash movements -----------------------------------------------------
    # Borrow fees are deliberately NOT mapped into Interest: as in the CSV they
    # already sit inside the broker-interest rows, and counting them twice
    # overstated the drain by exactly their total (-38.87 AUD / -3.29 USD).
    for n in root.iter("CashTransaction"):
        a = n.attrib
        kind = str(a.get("type", "")).strip().lower()
        ccy = str(a.get("currency", "")).strip().upper()
        amt = a.get("amount")
        settle = _flex_dt(a.get("settleDate") or a.get("dateTime"), True)
        when = _flex_dt(a.get("dateTime") or a.get("settleDate"), True)
        if "deposit" in kind or "withdrawal" in kind:
            yield "Deposits & Withdrawals", "Data", {
                "Settle Date": settle, "Currency": ccy, "Amount": amt}
        elif "dividend" in kind or "lieu" in kind or "withholding" in kind:
            yield "Dividends", "Data", {
                "Date": when, "Currency": ccy, "Amount": amt}
        elif "interest" in kind:
            yield "Interest", "Data", {
                "Date": when, "Currency": ccy, "Amount": amt}
        else:
            # "Other Fees", "Commission Adjustments" — the CSV lands these in a
            # Fees section that cash_events ignores. Mirror that exactly rather
            # than improve it here: the reconciliation that closes to the cent
            # was validated against the CSV's behaviour.
            yield "Fees", "Data", {
                "Date": when, "Currency": ccy, "Amount": amt}

    # --- opening balances ---------------------------------------------------
    for n in root.iter("CashReportCurrency"):
        a = n.attrib
        yield "Cash Report", "Data", {
            "Currency Summary": "Starting Cash",
            "Currency": str(a.get("currency", "")).strip().upper(),
            "Total": a.get("startingCash"),
        }


def resolve_statement_path(app_dir):
    """The statement to read: the auto-refreshed Flex XML, else the manual CSV.

    Both are the same account's history from the same broker; the XML is
    preferred only because it can be refreshed without a human. The CSV stays
    as the archival record and the fallback, so losing the token or the network
    degrades to yesterday's behaviour rather than to no statement at all.
    """
    app_dir = Path(app_dir)
    xml = app_dir / FLEX_XML_NAME
    csv_path = app_dir / "ibkr_activity_statement.csv"
    if not is_flex_xml(xml):
        return csv_path
    if not csv_path.exists():
        return xml
    # A Flex query with too short a date range covers LESS than the CSV, which
    # would silently amputate history — the same class of failure as trimming
    # before accumulating in compute_nav_from_statement. Say so and fall back
    # rather than pick the shorter source silently.
    try:
        x0 = parse_trades(xml)["DateTime"].min()
        c0 = parse_trades(csv_path)["DateTime"].min()
        if pd.notna(x0) and pd.notna(c0) and x0 > c0:
            print(f"[flex][WARN] Flex XML starts {x0:%Y-%m-%d} but the CSV "
                  f"starts {c0:%Y-%m-%d} - widen the Flex query date range. "
                  f"Using the CSV.")
            return csv_path
    except Exception as e:
        print(f"[flex][WARN] could not compare statement coverage ({e}); using XML.")
    return xml

"""IBKR Flex Web Service client — refreshes the account statement unattended.

WHY THIS EXISTS: the trade history has exactly one durable home. The TWS API
serves none of it (verified 2026-08-17: reqExecutions returns 0 at 7/30/90-day
filters), so `ibkr_statement.py` reads an Activity Statement instead — but that
statement arrives only when a human clicks through Client Portal -> Statements
-> Activity -> date range -> CSV. Between clicks the lot book, the CGT cost
bases and the reconstructed NAV all quietly describe a portfolio that has since
moved on. The Flex Web Service is the same data over REST, on a token, which is
the difference between a record that keeps itself current and one that decays
until somebody notices.

RUNS FROM SOURCE. Like `ibkr_paper_exec.py` this is live-ops, not engine logic:
editing it takes effect on the next scheduled run and must NOT flag the exe
stale. It is deliberately absent from `ops_expected.json -> exe.engine_sources`.
The XML it writes is parsed by `ibkr_statement.py`, which IS compiled in.

THE PROTOCOL is two calls. SendRequest hands back a reference code; GetStatement
exchanges that code for the report, and answers "still generating" until the
report is ready. Both are GETs carrying the token in the query string, so
neither the token nor the response may be echoed into a log that gets committed
or emailed — see `_mask`.

CREDENTIALS come from the environment (IBKR_FLEX_TOKEN / IBKR_FLEX_QUERY_ID) or
`flex_config.json` beside this file, which is gitignored. Generate both in
Client Portal: Settings -> Account Reporting -> Flex Web Service (token) and
Performance & Reports -> Flex Queries -> Activity Flex (query id). A newly
issued token needs a few minutes before it authenticates.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

APP_DIR = Path(__file__).resolve().parent
CONFIG_PATH = APP_DIR / "flex_config.json"
OUT_PATH = APP_DIR / "ibkr_flex_statement.xml"
CSV_PATH = APP_DIR / "ibkr_activity_statement.csv"

BASE_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"
# Connect and read timeouts. urlopen without a timeout — ib_insync's approach —
# can block a scheduled task indefinitely on a half-open socket, and this runs
# ahead of the engine in an unattended wrapper where a hang costs the whole run.
TIMEOUT = (10, 60)
MAX_WAIT_SEC = 180          # generation is usually <10s; this is the ceiling
POLL_SEC = 3

# Codes worth another attempt rather than a failure. 1019 is the documented
# "still generating" answer and 1018 is the rate limiter, which trips when a
# query is requested twice in quick succession — both resolve by waiting.
RETRY_CODES = {"1018", "1019"}


def _mask(tok: str) -> str:
    """A token is a bearer credential — enough to identify it, not to use it."""
    t = str(tok or "")
    return f"...{t[-4:]}" if len(t) > 4 else "(unset)"


EXPIRY_WARN_DAYS = 45


def expiry_warning(config_path=CONFIG_PATH, warn_days: int = EXPIRY_WARN_DAYS):
    """Nag as the token's activation period runs out, or None.

    Expiry is the one failure this design cannot detect for itself. Every other
    fault is loud — a bad query fails validation, a wrong field fails the
    cross-check — but an expired token just errors the fetch, and the engine
    then falls back to the CSV and runs perfectly normally. That is the
    original silent-decay problem returning by the back door, roughly a year
    after anyone last thought about it. The API does not report the expiry, so
    `flex_config.json` records it and this counts down.
    """
    try:
        cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
    except Exception:
        return None
    raw = str(cfg.get("expires", "")).strip()
    if not raw:
        return None
    try:
        import datetime as _dt
        # Accept a plain date; anything more precise than the day is noise for
        # a once-a-year renewal.
        exp = _dt.date.fromisoformat(raw[:10])
    except Exception:
        return f"flex_config.json 'expires' is not an ISO date: {raw!r}"
    import datetime as _dt
    left = (exp - _dt.date.today()).days
    if left < 0:
        return (f"Flex token EXPIRED {-left}d ago ({exp}). The statement is no "
                f"longer refreshing — regenerate it in Client Portal.")
    if left <= warn_days:
        return (f"Flex token expires in {left}d ({exp}). Regenerate it before "
                f"then, or the statement silently stops refreshing.")
    return None


def load_credentials(config_path=CONFIG_PATH) -> tuple[str, str]:
    """(token, query_id) from the environment, else the gitignored config."""
    tok = os.environ.get("IBKR_FLEX_TOKEN", "").strip()
    qid = os.environ.get("IBKR_FLEX_QUERY_ID", "").strip()
    if tok and qid:
        return tok, qid
    try:
        cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return tok, qid
    except Exception as e:
        raise RuntimeError(f"{config_path} is not readable JSON: {e}") from e
    return (tok or str(cfg.get("token", "")).strip(),
            qid or str(cfg.get("query_id", "")).strip())


class FlexError(RuntimeError):
    pass


def _error_of(root):
    """(code, message) if the response is an IBKR error, else (None, None)."""
    code = root.findtext("ErrorCode") or root.findtext("./code")
    msg = root.findtext("ErrorMessage") or root.findtext("./message")
    if code or (msg and root.tag != "FlexQueryResponse"):
        return (str(code or "").strip(), str(msg or "").strip())
    return (None, None)


def _get(url: str, params: dict) -> tuple[ET.Element, bytes]:
    """(parsed root, RAW bytes). The raw response is what gets saved: the file
    on disk should be the broker's own artefact, byte for byte, not this
    program's re-serialisation of its understanding of it."""
    r = requests.get(url, params=params, timeout=TIMEOUT,
                     headers={"User-Agent": "PortfolioOptimiser/1.0"})
    r.raise_for_status()
    try:
        return ET.fromstring(r.content), r.content
    except ET.ParseError as e:
        # An HTML login page or a maintenance notice lands here. Show a little
        # of it — never the URL, which carries the token.
        head = r.text[:160].replace("\n", " ")
        raise FlexError(f"non-XML response from IBKR: {head!r}") from e


def fetch_report(token: str, query_id: str, *, base_url: str = BASE_URL,
                 max_wait: float = MAX_WAIT_SEC, verbose: bool = True) -> bytes:
    """Download one Flex report. Returns the raw XML bytes."""
    if not token or not query_id:
        raise FlexError(
            "no Flex credentials. Set IBKR_FLEX_TOKEN + IBKR_FLEX_QUERY_ID, or "
            f"create {CONFIG_PATH.name} — see --status.")
    # The Flex Queries list shows each query's NAME and a description of the
    # sections it contains, and the numeric ID is easy to miss between them.
    # IBKR answers a wrong one with "1020: Invalid request or unable to
    # validate request", which names neither the field nor the problem.
    if not str(query_id).strip().isdigit():
        raise FlexError(
            f"query_id must be the NUMERIC Flex Query ID, got {query_id!r}. "
            "That looks like the query's name or section list — the ID is the "
            "digits shown beside the query in Client Portal.")

    deadline = time.monotonic() + max_wait
    if verbose:
        print(f"[flex] SendRequest query={query_id} token={_mask(token)}")

    ref, url = None, f"{base_url}/GetStatement"
    while True:
        root, _ = _get(f"{base_url}/SendRequest",
                       {"t": token, "q": query_id, "v": "3"})
        # A captive portal, proxy notice or maintenance page can be perfectly
        # well-formed XML — `<!DOCTYPE html><html>…</html>` parses cleanly — and
        # would otherwise fail several steps later as "no ReferenceCode", which
        # says nothing about the actual problem.
        if root.tag not in ("FlexStatementResponse", "FlexQueryResponse"):
            raise FlexError(f"unexpected response root <{root.tag}> — this is not "
                            f"the Flex Web Service (login or maintenance page?)")
        code, msg = _error_of(root)
        if code in RETRY_CODES and time.monotonic() < deadline:
            if verbose:
                print(f"[flex] {code}: {msg} — retrying in {POLL_SEC}s")
            time.sleep(POLL_SEC)
            continue
        if code:
            raise FlexError(f"SendRequest {code}: {msg}")
        ref = (root.findtext("ReferenceCode") or "").strip()
        # IBKR names the collection host in the response; it is not always the
        # host that accepted the request, so follow it rather than assume.
        url = (root.findtext("Url") or url).strip()
        break
    if not ref:
        raise FlexError("SendRequest succeeded but returned no ReferenceCode")

    while True:
        root, raw = _get(url, {"q": ref, "t": token, "v": "3"})
        code, msg = _error_of(root)
        if code is None and root.tag == "FlexQueryResponse":
            if verbose:
                print("[flex] statement retrieved")
            return raw
        if code not in RETRY_CODES or time.monotonic() >= deadline:
            raise FlexError(f"GetStatement {code or '?'}: {msg or 'unknown'}")
        if verbose:
            print(f"[flex] {code}: {msg} — waiting {POLL_SEC}s")
        time.sleep(POLL_SEC)


# --------------------------------------------------------------------------
# Validation — never overwrite a good statement with a worse one
# --------------------------------------------------------------------------

def inspect(xml_bytes: bytes) -> dict:
    """Summarise a report: period, trade/cash counts, cash-report presence.

    Counts the rows that will actually be PARSED — orders, not executions —
    by asking ibkr_statement rather than re-deriving the rule here. Reporting
    raw <Trade> nodes said "75 stock trades" for a statement of 60 orders.
    """
    import ibkr_statement as S

    root = ET.fromstring(xml_bytes)
    st = next(iter(root.iter("FlexStatement")), None)
    trades = S._flex_trades(root)
    return {
        "account": (st.attrib.get("accountId") if st is not None else None),
        "from": (st.attrib.get("fromDate") if st is not None else None),
        "to": (st.attrib.get("toDate") if st is not None else None),
        "trades": len(trades),
        "stock_trades": sum(1 for n in trades
                            if str(n.attrib.get("assetCategory", "")).upper() == "STK"),
        "fx_trades": sum(1 for n in trades
                         if str(n.attrib.get("assetCategory", "")).upper() == "CASH"),
        "cash_txns": sum(1 for _ in root.iter("CashTransaction")),
        "cash_report": sum(1 for _ in root.iter("CashReportCurrency")),
    }


def validate(xml_bytes: bytes) -> list[str]:
    """Problems that make a report unfit to replace the current one.

    A Flex query is a set of tickboxes and a wrong one produces a well-formed
    document that is missing a whole section — a report with no Cash Report is
    silently wrong by the entire opening balance, which is exactly the class of
    error that put the NAV series at $994,850 instead of $247,000.
    """
    try:
        info = inspect(xml_bytes)
    except ET.ParseError as e:
        return [f"not parseable XML: {e}"]
    problems = []
    if not info["from"] or not info["to"]:
        problems.append("no FlexStatement period — is this an Activity Flex query?")
    if info["stock_trades"] == 0:
        problems.append("no stock trades — tick Trades (and 'Orders') in the query")
    if info["cash_report"] == 0:
        problems.append("no Cash Report — opening balances missing, NAV would be "
                        "wrong by the whole cash position")
    if info["cash_txns"] == 0:
        problems.append("no Cash Transactions — deposits/dividends/interest missing")
    return problems


def write_report(xml_bytes: bytes, out_path=OUT_PATH, *, force: bool = False) -> Path:
    """Validate, then replace `out_path` atomically, keeping one backup."""
    problems = validate(xml_bytes)
    if problems and not force:
        raise FlexError("report rejected:\n  - " + "\n  - ".join(problems))
    for p in problems:
        print(f"[flex][WARN] {p}")

    out_path = Path(out_path)
    tmp = out_path.with_suffix(".xml.tmp")
    tmp.write_bytes(xml_bytes)
    if out_path.exists():
        bak = out_path.with_suffix(".xml.bak")
        bak.unlink(missing_ok=True)
        out_path.replace(bak)
    tmp.replace(out_path)
    return out_path


# --------------------------------------------------------------------------
# Cross-check — two independent exports of the same account must agree
# --------------------------------------------------------------------------

def _ending_cash(xml_path) -> dict:
    """{CCY: closing balance} from the report's own Cash Report — broker truth."""
    out = {}
    try:
        root = ET.parse(str(xml_path)).getroot()
    except Exception:
        return out
    for n in root.iter("CashReportCurrency"):
        ccy = str(n.attrib.get("currency", "")).strip().upper()
        try:
            if len(ccy) == 3:          # skips BASE_SUMMARY
                out[ccy] = float(n.attrib["endingCash"])
        except Exception:
            continue
    return out


def cross_check(xml_path=OUT_PATH, csv_path=CSV_PATH) -> tuple[bool, list[str]]:
    """Compare the Flex XML against the CSV over the window BOTH cover.

    This is the only real proof the translation layer is right. The CSV path is
    reconciled to the cent against the broker's own closing balances, so if the
    XML reproduces its trades, its net units and its cost bases over the
    overlap, the XML is reconciled too — and if it does not, the difference is
    a parser bug, not a data update.
    """
    import pandas as pd
    import ibkr_statement as S

    lines = []
    if not S.is_flex_xml(xml_path):
        return False, [f"{Path(xml_path).name} is not a Flex XML report"]
    if not Path(csv_path).exists():
        return True, [f"{Path(csv_path).name} absent — nothing to compare against"]

    x, c = S.parse_trades(xml_path), S.parse_trades(csv_path)
    if x.empty or c.empty:
        return False, ["one source parsed to zero trades"]

    lo = max(x["DateTime"].min(), c["DateTime"].min())
    hi = min(x["DateTime"].max(), c["DateTime"].max())
    xw = x[(x["DateTime"] >= lo) & (x["DateTime"] <= hi)]
    cw = c[(c["DateTime"] >= lo) & (c["DateTime"] <= hi)]
    lines.append(f"overlap {lo:%Y-%m-%d} .. {hi:%Y-%m-%d}: "
                 f"flex {len(xw)} trades vs csv {len(cw)}")

    ok = True
    if len(xw) != len(cw):
        ok = False
        lines.append(f"  MISMATCH trade count: {len(xw)} vs {len(cw)}")

    for col, tol in (("Units", 1e-6), ("BasisLocal", 0.01)):
        xs = xw.groupby("Security")[col].sum()
        cs = cw.groupby("Security")[col].sum()
        for sec in sorted(set(xs.index) | set(cs.index)):
            a, b = float(xs.get(sec, 0.0)), float(cs.get(sec, 0.0))
            if abs(a - b) > tol:
                ok = False
                lines.append(f"  MISMATCH {sec} {col}: flex {a:,.2f} vs csv {b:,.2f}")

    xf, cf = S.fx_from_statement(xml_path), S.fx_from_statement(csv_path)
    both = xf.index.intersection(cf.index)
    if len(both):
        worst = (xf[both] - cf[both]).abs().max()
        lines.append(f"FX: {len(both)} shared conversion days, worst diff {worst:.6f}")
        if worst > 1e-4:
            ok = False
            lines.append("  MISMATCH FX rates differ beyond rounding")
    elif not cf.empty:
        lines.append(f"FX: csv has {len(cf)} conversion days, flex has {len(xf)}")

    xc, cc = S.cash_events(xml_path), S.cash_events(csv_path)
    for ccy in sorted(set(cc["Currency"]) | set(xc["Currency"])):
        xa = xc[(xc["Currency"] == ccy) & xc["Date"].between(lo.normalize(), hi.normalize())]["Amount"].sum()
        ca = cc[(cc["Currency"] == ccy) & cc["Date"].between(lo.normalize(), hi.normalize())]["Amount"].sum()
        flag = "" if abs(xa - ca) <= 0.01 else "  <-- MISMATCH"
        if flag:
            ok = False
        lines.append(f"cash {ccy}: flex {xa:,.2f} vs csv {ca:,.2f}{flag}")

    # Opening balances are NOT comparable — the two reports start on different
    # days, so the Flex figure is 0 (the account was empty a year ago) against
    # the CSV's 1,000,000 (it opens mid-life). Comparing them directly prints
    # an alarming line about a non-problem. What IS comparable, and is the real
    # reconciliation, is where each source ENDS: opening balance plus every
    # movement must land on the broker's own closing cash.
    xs_, cs_ = S.starting_cash(xml_path), S.starting_cash(csv_path)
    closing = _ending_cash(xml_path)
    if closing:
        for ccy, want in sorted(closing.items()):
            for src, st, ev in (("flex", xs_, xc), ("csv", cs_, cc)):
                got = st.get(ccy, 0.0) + ev[ev["Currency"] == ccy]["Amount"].sum()
                if abs(got - want) > 0.01:
                    ok = False
                    lines.append(f"  MISMATCH {src} {ccy} closes at {got:,.2f}, "
                                 f"broker says {want:,.2f}")
        lines.append(f"closing cash vs broker CashReport: both sources reconcile "
                     f"({', '.join(f'{c} {v:,.2f}' for c, v in sorted(closing.items()))})"
                     if ok else "closing cash: see mismatches above")
    lines.append(f"opening cash (periods differ, not comparable): "
                 f"flex {xs_} vs csv {cs_}")

    # Reported, not asserted. The production CSV carries no Base Currency
    # Exchange Rate section at all, while a Flex query normally does — so the
    # XML having MORE rates is expected and additive, not a disagreement.
    # to_aud only consults these when no dated conversion covers the lot's
    # acquisition date, so extra rates can improve a cost base, never move one
    # that IBKR's own dated conversion already fixed.
    xr, cr = S.fx_to_base(xml_path), S.fx_to_base(csv_path)
    if xr != cr:
        lines.append(f"conversion rates: flex {xr or '{}'} vs csv {cr or '{}'} "
                     f"(fallback only — dated conversions take precedence)")
    return ok, lines


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def _cmd_status(args) -> int:
    tok, qid = load_credentials()
    print(f"config file : {CONFIG_PATH} "
          f"({'present' if CONFIG_PATH.exists() else 'ABSENT'})")
    print(f"token       : {_mask(tok)}")
    print(f"query id    : {qid or '(unset)'}")
    if qid and not str(qid).strip().isdigit():
        print("  [!] not numeric — this is the query's NAME or its section list, "
              "not its ID.\n      IBKR answers a wrong one with an opaque 1020. "
              "The ID is the digits\n      shown with the query in Client Portal.")
    warn = expiry_warning()
    print(f"token expiry: {warn or 'not near expiry (or no \"expires\" recorded)'}")
    print(f"statement   : {OUT_PATH.name} "
          f"({'present' if OUT_PATH.exists() else 'ABSENT'})")
    if OUT_PATH.exists():
        try:
            info = inspect(OUT_PATH.read_bytes())
            print(f"  period    : {info['from']} .. {info['to']}  "
                  f"account {info['account']}")
            print(f"  contents  : {info['stock_trades']} stock trades, "
                  f"{info['fx_trades']} fx, {info['cash_txns']} cash txns, "
                  f"{info['cash_report']} cash-report rows")
        except Exception as e:
            print(f"  UNREADABLE: {e}")
    if not (tok and qid):
        print("\nMissing credentials. Either set the environment variables")
        print("  IBKR_FLEX_TOKEN / IBKR_FLEX_QUERY_ID")
        print(f"or write {CONFIG_PATH.name} (gitignored):")
        print('  {"token": "<digits>", "query_id": "<digits>", '
              '"expires": "YYYY-MM-DD"}')
        print('  ("expires" is optional — the end of the token\'s Activation')
        print("   Period in Client Portal. Recording it is what turns a silent")
        print("   lapse into a warning 45 days out.)")
        return 1
    return 0


def _cmd_fetch(args) -> int:
    tok, qid = load_credentials()
    # Printed before the fetch, so it lands in daily_auto.log even on a run
    # that then fails for the very reason being warned about.
    warn = expiry_warning()
    if warn:
        print(f"[flex][WARN] {warn}")
    try:
        data = fetch_report(tok, qid, max_wait=args.max_wait)
    except FlexError as e:
        print(f"[flex] FAILED: {e}")
        return 2
    except requests.RequestException as e:
        print(f"[flex] FAILED: network error: {type(e).__name__}: {e}")
        return 2

    info = inspect(data)
    print(f"[flex] {info['from']}..{info['to']} account {info['account']}: "
          f"{info['stock_trades']} stock trades, {info['fx_trades']} fx, "
          f"{info['cash_txns']} cash txns")
    if args.no_write:
        print("[flex] --no-write: not saving")
        return 0
    try:
        p = write_report(data, args.out, force=args.force)
    except FlexError as e:
        print(f"[flex] {e}")
        print("[flex] existing statement left untouched; --force to override")
        return 3
    print(f"[flex] wrote {p.name}")
    return 0


def _cmd_verify(args) -> int:
    if not args.offline:
        rc = _cmd_fetch(args)
        if rc != 0:
            return rc
    ok, lines = cross_check(args.out)
    print("\n--- Flex vs Activity Statement CSV ---")
    for ln in lines:
        print(ln)
    print(f"--- {'AGREE' if ok else 'DISAGREE'} ---")
    return 0 if ok else 4


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--fetch", action="store_true",
                    help="download the report and save it (default)")
    ap.add_argument("--verify", action="store_true",
                    help="download, then cross-check against the CSV")
    ap.add_argument("--status", action="store_true",
                    help="show credential + statement state (token masked)")
    ap.add_argument("--offline", action="store_true",
                    help="with --verify, check the on-disk XML without fetching")
    ap.add_argument("--no-write", action="store_true", help="fetch but do not save")
    ap.add_argument("--force", action="store_true",
                    help="save even if validation finds missing sections")
    ap.add_argument("--out", default=str(OUT_PATH), help="output path")
    ap.add_argument("--max-wait", type=float, default=MAX_WAIT_SEC,
                    help=f"seconds to wait for generation (default {MAX_WAIT_SEC})")
    args = ap.parse_args(argv)

    if args.status:
        return _cmd_status(args)
    if args.verify:
        return _cmd_verify(args)
    return _cmd_fetch(args)


if __name__ == "__main__":
    sys.exit(main())

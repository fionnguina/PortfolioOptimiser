"""Flex Web Service: the XML must parse to exactly what the CSV parses to.

The Flex feed replaces a human clicking through Client Portal, so it is only
worth having if it is the SAME data. `_FLEX` below is a hand-written Flex report
carrying the identical three fills as the CSV fixture in
test_lockbox_and_reporting (`_STMT`) — same quantities, same commissions, same
cost bases, same conversion. Every parity test asserts the two formats produce
byte-identical answers; if IBKR's real attribute names differ from the ones
assumed here, these are the tests that will say so.

The runtime equivalent of this file is `ibkr_flex.py --verify`, which runs the
same comparison against the live download and the real CSV.
"""
import pandas as pd
import pytest

import ibkr_flex
import ibkr_statement as S

# The CSV fixture this mirrors, kept verbatim so the parity tests compare two
# expressions of one statement rather than two different statements.
_CSV = '''Statement,Header,Field Name,Field Value
Statement,Data,Period,"June 22, 2026"
Trades,Header,DataDiscriminator,Asset Category,Currency,Account,Symbol,Date/Time,Quantity,T. Price,C. Price,Proceeds,Comm/Fee,Basis,Realized P/L,MTM P/L,Code
Trades,Data,Order,Stocks,AUD,DUQ1,GOLD,"2026-06-22, 11:53:26","3,259",54.72,54.8,-178332.48,-156.9325824,178489.4125824,0,260.72,O
Trades,SubTotal,,Stocks,AUD,GOLD,,,3259,,,-178332.48,-156.9325824,178489.4125824,0,260.72,
Trades,Data,Order,Stocks,USD,DUQ1,SMH,"2026-06-22, 23:30:09",50,670.42,668.91,-33521,-1.00015,33522.00015,0,-75.5,O
Trades,Header,DataDiscriminator,Asset Category,Currency,Account,Symbol,Date/Time,Quantity,T. Price,,Proceeds,Comm in AUD,,,,Code
Trades,Data,Order,Forex,USD,DUQ1,AUD.USD,"2026-06-22, 23:31:09","-102,177.8",0.70044,,71569.418232,-2.041016669,,,,L
Financial Instrument Information,Header,Asset Category,Symbol,Description,Conid,Security ID,Underlying,Listing Exch,Multiplier,Type,Code
Financial Instrument Information,Data,Stocks,GOLD,GLOBAL X,45127612,AU00000GOLD7,GOLD,ASX,1,ETF,
Financial Instrument Information,Data,Stocks,SMH,VANECK,229725622,US92189F6768,SMH,NASDAQ,1,ETF,
Base Currency Exchange Rate,Header,Currency,Rate
Base Currency Exchange Rate,Data,USD,1.428400
'''

# Flex defaults: dates yyyyMMdd, dateTime "yyyyMMdd;HHmmss", assetCategory
# STK/CASH, and `cost` carrying the commission the way `Basis` does.
_FLEX = '''<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="PortfolioOptimiser" type="AF">
<FlexStatements count="1">
<FlexStatement accountId="DUQ1" fromDate="20260622" toDate="20260622" period="LastNDays">
<ConversionRates>
  <ConversionRate reportDate="20260622" fromCurrency="USD" toCurrency="AUD" rate="1.428400" />
</ConversionRates>
<Trades>
  <Trade accountId="DUQ1" currency="AUD" assetCategory="STK" symbol="GOLD"
         listingExchange="ASX" levelOfDetail="ORDER" dateTime="20260622;115326"
         tradeDate="20260622" quantity="3259" tradePrice="54.72"
         proceeds="-178332.48" ibCommission="-156.9325824" ibCommissionCurrency="AUD"
         cost="178489.4125824" fifoPnlRealized="0" fxRateToBase="1" buySell="BUY" />
  <Trade accountId="DUQ1" currency="USD" assetCategory="STK" symbol="SMH"
         listingExchange="NASDAQ" levelOfDetail="ORDER" dateTime="20260622;233009"
         tradeDate="20260622" quantity="50" tradePrice="670.42"
         proceeds="-33521" ibCommission="-1.00015" ibCommissionCurrency="USD"
         cost="33522.00015" fifoPnlRealized="0" fxRateToBase="1.428400" buySell="BUY" />
  <Trade accountId="DUQ1" currency="USD" assetCategory="CASH" symbol="AUD.USD"
         listingExchange="IDEALPRO" levelOfDetail="ORDER" dateTime="20260622;233109"
         tradeDate="20260622" quantity="-102177.8" tradePrice="0.70044"
         proceeds="71569.418232" ibCommission="-2.041016669" ibCommissionCurrency="AUD"
         cost="0" fifoPnlRealized="0" fxRateToBase="1.428400" buySell="SELL" />
</Trades>
</FlexStatement>
</FlexStatements>
</FlexQueryResponse>
'''


def _pair(tmp_path):
    c = tmp_path / "stmt.csv"
    c.write_text(_CSV, encoding="utf-8")
    x = tmp_path / "ibkr_flex_statement.xml"
    x.write_text(_FLEX, encoding="utf-8")
    return c, x


# --------------------------------------------------------------------------
# Parity — the whole justification for the translation layer
# --------------------------------------------------------------------------

def test_flex_trades_match_the_csv_exactly(tmp_path):
    c, x = _pair(tmp_path)
    a, b = S.parse_trades(x), S.parse_trades(c)
    pd.testing.assert_frame_equal(a, b, check_exact=False, rtol=1e-12)


def test_flex_maps_asx_listings_to_the_engine_symbol(tmp_path):
    """Without listingExchange the engine looks up 'GOLD' and finds nothing —
    the sheet, the lot book and the solver all speak GOLD.AX."""
    _, x = _pair(tmp_path)
    assert list(S.parse_trades(x)["Security"]) == ["GOLD.AX", "SMH"]


def test_flex_cost_is_the_csv_basis_commission_included(tmp_path):
    """Flex `cost` == Activity Statement `Basis`. If it were `tradeMoney` the
    cost base would drop the commission and understate every CGT cost base."""
    _, x = _pair(tmp_path)
    gold = S.parse_trades(x).iloc[0]
    assert gold["BasisLocal"] == pytest.approx(178332.48 + 156.9325824)


def test_flex_forex_is_excluded_from_trades_but_yields_the_rate(tmp_path):
    """A conversion is not the acquisition of a CGT asset — but it IS the
    exact rate IBKR applied, which beats a yfinance daily close."""
    c, x = _pair(tmp_path)
    assert "AUD.USD" not in set(S.parse_trades(x)["RawSymbol"])
    pd.testing.assert_series_equal(S.fx_from_statement(x), S.fx_from_statement(c))
    assert S.fx_from_statement(x).iloc[0] == pytest.approx(1.0 / 0.70044)


def test_flex_cash_events_match_the_csv(tmp_path):
    """Both legs of the conversion, both signs, and the commission booked to
    AUD — dropping the base leg left AUD overstated by the whole conversion."""
    c, x = _pair(tmp_path)
    pd.testing.assert_frame_equal(S.cash_events(x), S.cash_events(c),
                                  check_exact=False, rtol=1e-12)


def test_flex_lots_match_the_csv(tmp_path):
    c, x = _pair(tmp_path)
    pd.testing.assert_frame_equal(S.build_lots(S.parse_trades(x), S.fx_to_base(x)),
                                  S.build_lots(S.parse_trades(c), S.fx_to_base(c)),
                                  check_exact=False, rtol=1e-12)


def test_flex_conversion_rate_matches_the_csv(tmp_path):
    c, x = _pair(tmp_path)
    assert S.fx_to_base(x) == S.fx_to_base(c) == {"USD": 1.4284}


# --------------------------------------------------------------------------
# Format handling
# --------------------------------------------------------------------------

def test_is_flex_xml_discriminates_on_content_not_extension(tmp_path):
    c, x = _pair(tmp_path)
    assert S.is_flex_xml(x) and not S.is_flex_xml(c)
    other = tmp_path / "other.xml"
    other.write_text("<html><body>login</body></html>", encoding="utf-8")
    assert not S.is_flex_xml(other), "an HTML error page must not read as a report"
    assert not S.is_flex_xml(tmp_path / "missing.xml")


@pytest.mark.parametrize("raw,want", [
    ("20260703;103012", "2026-07-03 10:30:12"),   # Flex default
    ("20260703", "2026-07-03"),
    ("2026-07-03;10:30:12", "2026-07-03 10:30:12"),
    ("2026-07-03, 10:30:12", "2026-07-03 10:30:12"),  # comma separator option
    ("2026/07/03", "2026-07-03"),
    ("", None),
])
def test_flex_dates_survive_every_query_format(raw, want):
    """The date format is a per-query setting, so a wrong guess would silently
    NaT every fill and empty the lot book rather than fail loudly."""
    assert S._flex_dt(raw) == want


def test_flex_ignores_execution_rows_when_orders_are_present(tmp_path):
    """A query with both levels ticked reports each fill twice — the Flex
    incarnation of the CSV's SubTotal double-count."""
    dbl = _FLEX.replace("</Trades>", '''
  <Trade accountId="DUQ1" currency="AUD" assetCategory="STK" symbol="GOLD"
         listingExchange="ASX" levelOfDetail="EXECUTION" dateTime="20260622;115326"
         quantity="3259" tradePrice="54.72" proceeds="-178332.48"
         ibCommission="-156.9325824" cost="178489.4125824" fifoPnlRealized="0" />
</Trades>''')
    p = tmp_path / "dbl.xml"
    p.write_text(dbl, encoding="utf-8")
    t = S.parse_trades(p)
    assert len(t) == 2, f"expected 2 orders, got {len(t)} — executions double-counted"
    assert t[t["Security"] == "GOLD.AX"]["Units"].sum() == 3259


def test_flex_falls_back_to_executions_when_no_orders_exist(tmp_path):
    p = tmp_path / "exec.xml"
    p.write_text(_FLEX.replace('levelOfDetail="ORDER"', 'levelOfDetail="EXECUTION"'),
                 encoding="utf-8")
    assert len(S.parse_trades(p)) == 2


def test_flex_cash_transactions_route_to_the_right_sections(tmp_path):
    p = tmp_path / "cash.xml"
    p.write_text(_FLEX.replace("</FlexStatement>", '''
<CashTransactions>
  <CashTransaction type="Deposits/Withdrawals" currency="AUD" amount="250000"
                   dateTime="20260623" settleDate="20260623" />
  <CashTransaction type="Dividends" currency="USD" amount="12.34" dateTime="20260701" />
  <CashTransaction type="Broker Interest Received" currency="AUD" amount="5.67"
                   dateTime="20260702" />
  <CashTransaction type="Other Fees" currency="AUD" amount="-9.99" dateTime="20260703" />
</CashTransactions>
<CashReport>
  <CashReportCurrency currency="BASE_SUMMARY" startingCash="1000" />
  <CashReportCurrency currency="AUD" startingCash="900" />
  <CashReportCurrency currency="USD" startingCash="70" />
</CashReport>
</FlexStatement>'''), encoding="utf-8")

    ce = S.cash_events(p)
    got = {(str(r.Date.date()), r.Currency, r.Amount) for r in ce.itertuples()}
    assert ("2026-06-23", "AUD", 250000.0) in got
    assert ("2026-07-01", "USD", 12.34) in got
    assert ("2026-07-02", "AUD", 5.67) in got
    # Other Fees lands in the CSV's Fees section, which cash_events ignores.
    # Mirrored deliberately: the reconciliation that closes to the cent was
    # validated against that behaviour, not against an improved version of it.
    assert not any(a == -9.99 for _, _, a in got)

    assert S.starting_cash(p) == {"AUD": 900.0, "USD": 70.0}, "BASE_SUMMARY is not a currency"
    assert not S.external_flows(p).empty
    assert S.performance_start(p) == pd.Timestamp("2026-06-24")


def test_flex_fx_commission_in_a_foreign_currency_is_not_booked_to_aud(tmp_path, capsys):
    """cash_events credits this leg to AUD unconditionally (the CSV column is
    literally 'Comm in AUD'). Booking a USD commission there would corrupt the
    AUD balance silently, so it is dropped loudly instead."""
    p = tmp_path / "usdcomm.xml"
    p.write_text(_FLEX.replace('ibCommission="-2.041016669" ibCommissionCurrency="AUD"',
                               'ibCommission="-2.041016669" ibCommissionCurrency="USD"'),
                 encoding="utf-8")
    ce = S.cash_events(p)
    assert "[flex][WARN]" in capsys.readouterr().out
    assert not any(abs(a + 2.041016669) < 1e-9 for a in ce["Amount"])


# --------------------------------------------------------------------------
# Source selection
# --------------------------------------------------------------------------

def test_resolver_prefers_flex_but_falls_back_to_the_csv(tmp_path):
    c, x = _pair(tmp_path)
    c.rename(tmp_path / "ibkr_activity_statement.csv")
    assert S.resolve_statement_path(tmp_path) == x
    x.unlink()
    assert S.resolve_statement_path(tmp_path).name == "ibkr_activity_statement.csv"


def test_resolver_refuses_a_flex_query_that_covers_less_than_the_csv(tmp_path, capsys):
    """A 30-day query against a 2-month account silently amputates history —
    the same class of failure as trimming before accumulating in
    compute_nav_from_statement, which put NAV at $994,850 instead of $247,000."""
    _pair(tmp_path)
    (tmp_path / "stmt.csv").rename(tmp_path / "ibkr_activity_statement.csv")
    short = _FLEX.replace('dateTime="20260622;115326"', 'dateTime="20260801;115326"') \
                 .replace('dateTime="20260622;233009"', 'dateTime="20260801;233009"')
    (tmp_path / "ibkr_flex_statement.xml").write_text(short, encoding="utf-8")
    assert S.resolve_statement_path(tmp_path).name == "ibkr_activity_statement.csv"
    assert "widen the Flex query" in capsys.readouterr().out


# --------------------------------------------------------------------------
# Client: credentials, validation, and not leaking the token
# --------------------------------------------------------------------------

def test_token_is_never_printed_in_full():
    assert ibkr_flex._mask("123456789012345678") == "...5678"
    assert ibkr_flex._mask("") == "(unset)"


def test_environment_beats_the_config_file(tmp_path, monkeypatch):
    cfg = tmp_path / "flex_config.json"
    cfg.write_text('{"token": "filetok", "query_id": "111"}', encoding="utf-8")
    monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
    monkeypatch.delenv("IBKR_FLEX_QUERY_ID", raising=False)
    assert ibkr_flex.load_credentials(cfg) == ("filetok", "111")
    monkeypatch.setenv("IBKR_FLEX_TOKEN", "envtok")
    monkeypatch.setenv("IBKR_FLEX_QUERY_ID", "222")
    assert ibkr_flex.load_credentials(cfg) == ("envtok", "222")


def test_missing_credentials_are_a_clear_error_not_a_crash(tmp_path, monkeypatch):
    monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
    monkeypatch.delenv("IBKR_FLEX_QUERY_ID", raising=False)
    assert ibkr_flex.load_credentials(tmp_path / "nope.json") == ("", "")
    with pytest.raises(ibkr_flex.FlexError, match="no Flex credentials"):
        ibkr_flex.fetch_report("", "", verbose=False)


def test_validation_rejects_a_query_missing_its_sections():
    """A Flex query is a page of tickboxes. Miss one and the report is
    well-formed and wrong — no Cash Report means NAV is off by the entire
    opening balance, with nothing in the output to say so."""
    problems = ibkr_flex.validate(_FLEX.encode())
    assert any("Cash Report" in p for p in problems)
    assert any("Cash Transactions" in p for p in problems)
    assert not any("stock trades" in p for p in problems)

    empty = '<FlexQueryResponse><FlexStatements count="0"/></FlexQueryResponse>'
    assert any("Activity Flex" in p for p in ibkr_flex.validate(empty.encode()))
    assert ibkr_flex.validate(b"<html>nope</html>")


def test_a_rejected_report_never_overwrites_the_good_one(tmp_path):
    out = tmp_path / "ibkr_flex_statement.xml"
    out.write_text("<FlexQueryResponse>GOOD</FlexQueryResponse>", encoding="utf-8")
    with pytest.raises(ibkr_flex.FlexError, match="rejected"):
        ibkr_flex.write_report(_FLEX.encode(), out)
    assert "GOOD" in out.read_text(encoding="utf-8")

    ibkr_flex.write_report(_FLEX.encode(), out, force=True)
    assert "FlexStatement" in out.read_text(encoding="utf-8")
    assert "GOOD" in out.with_suffix(".xml.bak").read_text(encoding="utf-8"), \
        "the replaced statement must survive as a backup"


def test_inspect_summarises_what_the_query_actually_returned():
    info = ibkr_flex.inspect(_FLEX.encode())
    assert (info["stock_trades"], info["fx_trades"]) == (2, 1)
    assert info["account"] == "DUQ1" and info["from"] == "20260622"


def test_cross_check_agrees_when_both_sources_hold_the_same_trades(tmp_path):
    """The runtime proof: --verify runs exactly this against the live feed."""
    c, x = _pair(tmp_path)
    ok, lines = ibkr_flex.cross_check(x, c)
    assert ok, "\n".join(lines)
    assert any("overlap" in ln for ln in lines)


def test_cross_check_catches_a_quantity_that_drifted(tmp_path):
    c, x = _pair(tmp_path)
    x.write_text(_FLEX.replace('quantity="3259"', 'quantity="3260"'), encoding="utf-8")
    ok, lines = ibkr_flex.cross_check(x, c)
    assert not ok and any("MISMATCH GOLD.AX" in ln for ln in lines)


def test_spliced_nav_never_reaches_outside_the_paths_it_was_given(tmp_path, monkeypatch):
    """statement_path=None means NO statement, not "go and find one".

    Resolution was briefly wired into the None default, which read as tidier
    and was a footgun: nav.APP_DIR defaults to Path("."), so a caller passing
    its own fills and seed silently picked up whichever real account statement
    sat in the working directory and rebuilt NAV from that instead. Caught only
    because two unrelated tests started reporting a $1,000,102 portfolio.
    """
    import json
    import nav

    monkeypatch.chdir(tmp_path)
    (tmp_path / "ibkr_activity_statement.csv").write_text(_CSV, encoding="utf-8")
    (tmp_path / "ibkr_flex_statement.xml").write_text(_FLEX, encoding="utf-8")

    seed = tmp_path / "lots_seed.json"
    seed.write_text(json.dumps([{"Security": "IVV.AX", "AcqDate": "2026-07-01",
                                 "Units": 100}]), encoding="utf-8")
    fills = tmp_path / "fills.jsonl"
    fills.write_text("", encoding="utf-8")
    prices = pd.DataFrame({"IVV.AX": [40, 41, 42, 43]},
                          index=pd.date_range("2026-07-01", periods=4))

    got = nav.compute_actual_nav_series_spliced(prices, fills, seed)
    assert list(got) == [4000.0, 4100.0, 4200.0, 4300.0], \
        "the seed's 100 units of IVV.AX is the whole book; anything else means " \
        "a statement leaked in from the working directory"


def test_statement_resolver_takes_app_dir_explicitly(tmp_path):
    """The engine passes its own APP_DIR; nothing depends on the CWD."""
    import nav

    (tmp_path / "ibkr_flex_statement.xml").write_text(_FLEX, encoding="utf-8")
    assert nav.statement_path_for(tmp_path).name == "ibkr_flex_statement.xml"
    assert nav.statement_path_for(tmp_path / "empty").name == "ibkr_activity_statement.csv"


# --------------------------------------------------------------------------
# The two-call protocol, without a live token
# --------------------------------------------------------------------------

class _Resp:
    def __init__(self, body):
        self.content = body.encode() if isinstance(body, str) else body
        self.text = self.content.decode("utf-8", "replace")

    def raise_for_status(self):
        pass


def _scripted(monkeypatch, *bodies):
    """Serve `bodies` in order, recording the params each call was made with."""
    calls = []
    seq = list(bodies)

    def fake_get(url, params=None, **kw):
        calls.append((url, dict(params or {})))
        return _Resp(seq.pop(0))

    monkeypatch.setattr(ibkr_flex.requests, "get", fake_get)
    monkeypatch.setattr(ibkr_flex, "POLL_SEC", 0)
    return calls


_SEND_OK = ('<FlexStatementResponse><Status>Success</Status>'
            '<ReferenceCode>987654</ReferenceCode>'
            '<Url>https://example.invalid/GetStatement</Url></FlexStatementResponse>')
_IN_PROGRESS = ('<FlexStatementResponse><Status>Warn</Status><ErrorCode>1019</ErrorCode>'
                '<ErrorMessage>Statement generation in progress</ErrorMessage>'
                '</FlexStatementResponse>')


def test_fetch_completes_the_two_call_handshake(monkeypatch):
    """SendRequest returns a reference code; GetStatement exchanges it for the
    report and says 'still generating' until it is ready."""
    calls = _scripted(monkeypatch, _SEND_OK, _IN_PROGRESS, _FLEX)
    got = ibkr_flex.fetch_report("tok123456", "42", verbose=False)

    assert got == _FLEX.encode(), "the broker's own bytes must be what is saved"
    assert len(calls) == 3
    assert calls[0][1] == {"t": "tok123456", "q": "42", "v": "3"}
    # The collection host comes from the response, not from an assumption.
    assert calls[1][0] == "https://example.invalid/GetStatement"
    assert calls[1][1] == {"q": "987654", "t": "tok123456", "v": "3"}


def test_fetch_surfaces_an_expired_token_instead_of_hanging(monkeypatch):
    _scripted(monkeypatch, '<FlexStatementResponse><Status>Fail</Status>'
                           '<ErrorCode>1012</ErrorCode>'
                           '<ErrorMessage>Token has expired</ErrorMessage>'
                           '</FlexStatementResponse>')
    with pytest.raises(ibkr_flex.FlexError, match="1012.*expired"):
        ibkr_flex.fetch_report("tok", "42", verbose=False)


def test_fetch_gives_up_rather_than_polling_forever(monkeypatch):
    """A statement stuck in generation must not block the morning run. The
    wrapper runs this ahead of the engine; an unbounded wait costs the run."""
    _scripted(monkeypatch, _SEND_OK, *[_IN_PROGRESS] * 20)
    with pytest.raises(ibkr_flex.FlexError, match="1019"):
        ibkr_flex.fetch_report("tok", "42", max_wait=0, verbose=False)


def test_an_html_login_page_is_not_mistaken_for_a_report(monkeypatch):
    """A captive portal or maintenance page can be well-formed XML and parse
    cleanly, so the root tag is the discriminator — otherwise this surfaces
    several steps later as 'no ReferenceCode', which names the wrong problem."""
    _scripted(monkeypatch, "<!DOCTYPE html><html><body>Please log in</body></html>")
    with pytest.raises(ibkr_flex.FlexError, match="not the Flex Web Service"):
        ibkr_flex.fetch_report("tok", "42", verbose=False)


def test_a_malformed_response_is_reported_as_such(monkeypatch):
    _scripted(monkeypatch, "<html><br>unclosed")
    with pytest.raises(ibkr_flex.FlexError, match="non-XML"):
        ibkr_flex.fetch_report("tok", "42", verbose=False)


def test_the_token_never_reaches_stdout(monkeypatch, capsys):
    """Wrapper output goes to daily_auto.log, which is emailed on failure."""
    _scripted(monkeypatch, _SEND_OK, _FLEX)
    ibkr_flex.fetch_report("98765432109876543210", "42", verbose=True)
    out = capsys.readouterr().out
    assert "98765432109876543210" not in out
    assert "...3210" in out


# --------------------------------------------------------------------------
# Token expiry — the one failure mode that is otherwise silent
# --------------------------------------------------------------------------

def _cfg(tmp_path, expires):
    p = tmp_path / "flex_config.json"
    p.write_text('{"token": "t", "query_id": "1", "expires": "%s"}' % expires,
                 encoding="utf-8")
    return p


def test_expiry_warns_before_it_lapses_not_after(tmp_path):
    """Every other fault here is loud: a bad query fails validation, a wrong
    field fails the cross-check. An expired token just errors the fetch and the
    engine carries on with the CSV — silent decay returning by the back door a
    year after anyone last thought about it."""
    import datetime as dt
    today = dt.date.today()

    assert ibkr_flex.expiry_warning(_cfg(tmp_path, today + dt.timedelta(days=300))) is None
    soon = ibkr_flex.expiry_warning(_cfg(tmp_path, today + dt.timedelta(days=10)))
    assert soon and "expires in 10d" in soon
    gone = ibkr_flex.expiry_warning(_cfg(tmp_path, today - dt.timedelta(days=3)))
    assert gone and "EXPIRED 3d ago" in gone


def test_expiry_is_optional_and_never_blocks_a_run(tmp_path):
    """A config without the field is the normal case, not an error."""
    p = tmp_path / "flex_config.json"
    p.write_text('{"token": "t", "query_id": "1"}', encoding="utf-8")
    assert ibkr_flex.expiry_warning(p) is None
    assert ibkr_flex.expiry_warning(tmp_path / "absent.json") is None
    assert "not an ISO date" in ibkr_flex.expiry_warning(_cfg(tmp_path, "next July"))


def test_a_non_numeric_query_id_is_caught_before_the_request(monkeypatch):
    """IBKR answers a wrong query id with '1020: Invalid request or unable to
    validate request', which names neither the field nor the problem. The Flex
    Queries list shows a name and a section description either side of the
    numeric ID, so picking up the wrong string is the expected mistake."""
    with pytest.raises(ibkr_flex.FlexError, match="NUMERIC Flex Query ID"):
        ibkr_flex.fetch_report("tok", "Trades Cash Transactions Cash Repo",
                               verbose=False)
    # And it must not have hit the network to find that out.
    def boom(*a, **k):
        raise AssertionError("no request should be made with a bad query id")
    monkeypatch.setattr(ibkr_flex.requests, "get", boom)
    with pytest.raises(ibkr_flex.FlexError):
        ibkr_flex.fetch_report("tok", "MyQueryName", verbose=False)


# --------------------------------------------------------------------------
# The real wire format — both of these were found by --verify against the live
# feed on 2026-08-18, not by reading the schema.
# --------------------------------------------------------------------------

# Order-level rows come as <Order>, execution-level as <Trade>. This document
# holds ONE order that filled in TWO executions, plus the separate `taxes`
# field IBKR reports alongside commission on ASX fills.
_FLEX_ORDERS = '''<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="PortfolioOptimiser" type="AF">
<FlexStatements count="1">
<FlexStatement accountId="DUQ1" fromDate="20250818" toDate="20260817">
<AccountInformation accountId="DUQ1" currency="AUD" />
<Trades>
  <Order accountId="DUQ1" currency="AUD" assetCategory="STK" symbol="BBUS"
         listingExchange="ASX" levelOfDetail="ORDER" dateTime="20260624;095953 AEST"
         quantity="531" tradePrice="24.25" tradeMoney="12876.75" proceeds="-12876.75"
         ibCommission="-10.3014" ibCommissionCurrency="AUD" taxes="-1.03014"
         cost="12888.08154" fifoPnlRealized="0" fxRateToBase="1" buySell="BUY" />
  <Trade accountId="DUQ1" currency="AUD" assetCategory="STK" symbol="BBUS"
         listingExchange="ASX" levelOfDetail="EXECUTION" dateTime="20260624;095953 AEST"
         quantity="331" tradePrice="24.25" proceeds="-8026.75" ibCommission="-6.42"
         taxes="-0.642" cost="8033.812" fifoPnlRealized="0" />
  <Trade accountId="DUQ1" currency="AUD" assetCategory="STK" symbol="BBUS"
         listingExchange="ASX" levelOfDetail="EXECUTION" dateTime="20260624;100104 AEST"
         quantity="200" tradePrice="24.25" proceeds="-4850.00" ibCommission="-3.8814"
         taxes="-0.38814" cost="4854.26954" fifoPnlRealized="0" />
</Trades>
</FlexStatement>
</FlexStatements>
</FlexQueryResponse>
'''


def test_order_rows_win_over_execution_rows(tmp_path):
    """THE TAG IS THE LEVEL — <Order> vs <Trade>, not an attribute on one
    element. Reading only <Trade> reported EXECUTIONS: 75 stock rows against
    the statement's 60 orders, because a partially filled order arrives as
    several executions. Orders are the unit the CSV records and the lot book
    is built from."""
    p = tmp_path / "orders.xml"
    p.write_text(_FLEX_ORDERS, encoding="utf-8")
    t = S.parse_trades(p)
    assert len(t) == 1, f"expected the 1 order, got {len(t)} (executions leaked in)"
    assert t.iloc[0]["Units"] == 531, "531 is the order; 331+200 are its fills"


def test_commission_includes_the_tax_the_csv_folds_into_it(tmp_path):
    """Flex splits what `Comm/Fee` combines: ibCommission is brokerage, taxes
    is the GST on it. Mapping Comm/Fee to ibCommission alone left the cash
    reconstruction short by exactly the GST — 120.83 AUD over 60 orders —
    while cost bases stayed right, because Flex's `cost` already includes both."""
    p = tmp_path / "orders.xml"
    p.write_text(_FLEX_ORDERS, encoding="utf-8")
    t = S.parse_trades(p)
    assert t.iloc[0]["CommLocal"] == pytest.approx(-10.3014 + -1.03014)
    # cost = tradeMoney + commission + taxes, so the cost base needs no fixing.
    assert t.iloc[0]["BasisLocal"] == pytest.approx(12876.75 + 10.3014 + 1.03014)

    cash = S.cash_events(p)
    aud = cash[cash["Currency"] == "AUD"]["Amount"].sum()
    assert aud == pytest.approx(-12888.08154), \
        "cash must move by the full cost including GST, or it drifts by the tax"


def test_commission_is_none_when_the_query_omits_both_fields(tmp_path):
    """`_FLEX` carries no taxes attribute at all — a query can omit it."""
    _, x = _pair(tmp_path)
    assert S.parse_trades(x).iloc[0]["CommLocal"] == pytest.approx(-156.9325824)


def test_a_timezone_suffixed_datetime_parses(tmp_path):
    """Live Flex writes '20260624;095953 AEST'. The label is dropped, not
    applied — the CSV is in the same local convention and the two must sort
    identically or FIFO consumes different lots."""
    assert S._flex_dt("20260624;095953 AEST") == "2026-06-24 09:59:53"
    assert S._flex_dt("20260703;233009 EDT") == "2026-07-03 23:30:09"
    p = tmp_path / "orders.xml"
    p.write_text(_FLEX_ORDERS, encoding="utf-8")
    assert str(S.parse_trades(p).iloc[0]["DateTime"]) == "2026-06-24 09:59:53"


def test_cross_check_reconciles_both_sources_to_the_brokers_closing_cash(tmp_path):
    """Opening balances are not comparable — the reports start on different
    days, so flex opens at 0 (empty account a year ago) against the CSV's
    mid-life balance. Where they END is comparable, and is the real test:
    opening plus every movement must land on the broker's own closing figure."""
    c, x = _pair(tmp_path)
    body = _FLEX.replace("</FlexStatement>", '''
<CashReport>
  <CashReportCurrency currency="BASE_SUMMARY" startingCash="0" endingCash="-280669.25" />
  <CashReportCurrency currency="AUD" startingCash="0" endingCash="-280669.253599069" />
</CashReport>
</FlexStatement>''')
    x.write_text(body, encoding="utf-8")
    # Every AUD movement in the fixture: the GOLD purchase including
    # commission, the AUD leg sold in the conversion, and its commission.
    assert ibkr_flex._ending_cash(x) == {
        "AUD": pytest.approx(-(178489.4125824 + 102177.8 + 2.041016669))}
    ok, lines = ibkr_flex.cross_check(x, c)
    assert ok, "\n".join(lines)
    assert any("reconcile" in ln for ln in lines)

    # A closing figure the movements cannot reach is a real disagreement.
    x.write_text(body.replace('endingCash="-280669.253599069"', 'endingCash="-999.00"'),
                 encoding="utf-8")
    ok, lines = ibkr_flex.cross_check(x, c)
    assert not ok and any("broker says -999.00" in ln for ln in lines)


def test_cash_before_the_first_trade_is_opening_balance_not_noise(tmp_path):
    """`reindex(dates)` drops cash dated before the first trade, while `start`
    describes a different day — so between them the movement vanishes.

    Invisible while the statement began mid-life with its opening balance
    already stated (the CSV: 1,000,000 at 2026-06-22, nothing earlier). The
    Flex feed starts from a genuinely empty account and carries the funding
    deposit as an EVENT three weeks before the first trade, so the same code
    computed closing cash of -988,327 instead of +11,673 — understated by
    exactly the deposit.
    """
    import nav

    p = tmp_path / "funded.xml"
    p.write_text(_FLEX.replace("</FlexStatement>", '''
<CashTransactions>
  <CashTransaction type="Deposits/Withdrawals" currency="AUD" amount="300000"
                   dateTime="20260530" settleDate="20260530" />
</CashTransactions>
<CashReport>
  <CashReportCurrency currency="AUD" startingCash="0" endingCash="19330.75" />
</CashReport>
</FlexStatement>'''), encoding="utf-8")

    prices = pd.DataFrame({"GOLD.AX": [54.72] * 5},
                          index=pd.date_range("2026-06-22", periods=5))
    got = nav.compute_nav_from_statement(prices, p)
    assert not got.empty

    # 300,000 deposited, 178,489.41 spent on GOLD (incl. commission) and
    # 102,177.80 converted away with 2.04 commission => 19,330.75 cash left,
    # plus 3,259 units of GOLD marked at 54.72.
    expected_cash = 300000 - 178489.4125824 - 102177.8 - 2.041016669
    assert got.iloc[-1] == pytest.approx(expected_cash + 3259 * 54.72, abs=0.01), \
        "the pre-window deposit must survive as opening balance"


def test_positions_bought_before_the_price_panel_are_not_zeroed(tmp_path):
    """The symmetric case to the cash fix above: a trade dated before `dates`
    begins is an opening POSITION, and reindexing drops it the same way.

    Cannot trigger while the panel reaches back past the first trade — which
    is why it went unnoticed — so the test forces the condition rather than
    waiting for it. With flat prices the answer must not depend on where the
    panel happens to start; without the guard the holding vanishes entirely.
    """
    import nav

    p = tmp_path / "funded.xml"
    p.write_text(_FLEX.replace("</FlexStatement>", '''
<CashTransactions>
  <CashTransaction type="Deposits/Withdrawals" currency="AUD" amount="300000"
                   dateTime="20260530" settleDate="20260530" />
</CashTransactions>
</FlexStatement>'''), encoding="utf-8")

    def nav_from(start):
        px = pd.DataFrame({"GOLD.AX": [54.72] * 6},
                          index=pd.date_range(start, periods=6))
        return nav.compute_nav_from_statement(px, p)

    on_trade_date = nav_from("2026-06-22")      # panel opens with the trade
    after = nav_from("2026-06-25")              # panel opens three days later
    assert not on_trade_date.empty and not after.empty
    assert after.iloc[-1] == pytest.approx(on_trade_date.iloc[-1], abs=0.01), \
        "a later panel start must not erase the GOLD position"
    # And the holding is actually in there, not merely equal-and-both-wrong.
    cash = 300000 - 178332.48 - 156.9325824 - 102177.8 - 2.041016669
    assert after.iloc[-1] == pytest.approx(cash + 3259 * 54.72, abs=0.01)

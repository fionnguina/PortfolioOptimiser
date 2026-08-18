# IBKR Flex Web Service — setup

The account statement is the only surviving record of what this fund has
traded. The TWS API serves none of it (verified 2026-08-17: `reqExecutions`
returns 0 at 7/30/90-day filters), so the lot book, every CGT cost base and the
reconstructed NAV all descend from a statement file on disk. Until now that
file arrived only when a human exported it from Client Portal — which means
between exports the engine reasons about a portfolio that has already moved on.

The Flex Web Service is the same data over REST. Once the token exists,
`daily_auto.ps1` refreshes the statement before every engine run and the record
stops decaying.

**Two things can only be created by you, in your own logged-in Client Portal
session: the token and the query.** Everything else is built and tested.

---

All of this is in **Client Portal, in a browser** — not TWS. TWS shows
statements but has no Flex query builder and no token.

## 1. Generate the token

**Performance & Reports** → **Flex Queries** → **Flex Web Service
Configuration**. (Or: **Menu** → Reporting → Flex Queries → Flex Web Service
Configuration.) Same screen as the queries themselves.

- Turn the service **ON**.
- **Set the expiry explicitly to the longest option offered.** The default is
  **six hours**, which is fine for a one-off script and useless for this: the
  10:20 job would authenticate once and fail every morning after. This is the
  single easiest thing to get wrong here.
- There is an optional **IP restriction**. Leave it blank unless this machine
  has a static address — a dynamic IP that changes will lock the job out with
  an authentication error that looks exactly like an expired token.
- Generate. **The token is displayed once.** It is a long digit string.
- A new token needs roughly **10–15 minutes** before it authenticates. Until
  then you will get an authentication error; that is expected, not a fault.

The token is a bearer credential for the brokerage account. It is masked in
every log line this repo writes, and `flex_config.json` is gitignored — but
treat it like a password: don't paste it into a commit, an email, or a chat.

## 2. Create the Activity Flex query

Client Portal → **Performance & Reports** → **Flex Queries** →
**Activity Flex Query** → create new.

**Sections and fields.** Tick these sections; within each, the fields the
parser reads:

| Section | Fields |
|---|---|
| **Trades** | `assetCategory`, `symbol`, `listingExchange`, `currency`, `dateTime`, `tradeDate`, `quantity`, `tradePrice`, `proceeds`, `ibCommission`, `ibCommissionCurrency`, **`taxes`**, `cost`, `fifoPnlRealized`, `fxRateToBase`, `buySell`, `levelOfDetail` |
| **Cash Transactions** | `type`, `currency`, `amount`, `dateTime`, `settleDate` — and in that section's options include Deposits/Withdrawals, Dividends, Payment In Lieu, Withholding Tax, Broker Interest |
| **Cash Report** | `currency`, `startingCash`, `endingCash` |
| **Account Information** | `currency` (identifies the base currency) |

In the **Trades** section options set level of detail to **Orders**. Executions
may also be ticked without harm — the parser prefers orders — but orders are
required. They are the unit the CSV records and the lot book is built from; a
partially filled order arrives as several executions, which is a different
count of a different thing (the live account: 60 orders, 75 executions).

**Delivery configuration:**

- Format: **XML**
- Period: **Last 365 Calendar Days** (the maximum, and more than the account's
  whole life so far)
- Date format: any — `yyyyMMdd`, `yyyy-MM-dd` and both separators are handled
- Include canceled trades: **No**

Save, then note the **Query ID** from the list — a shorter digit string.

Three fields carry more weight than the rest, and all three are easy to miss:

- **`cost`** is the cost base *including commission and tax*, which is exactly
  what AU CGT requires (ITAA s110-25). Its near-neighbour `tradeMoney` excludes
  both and would understate every cost base.
- **`taxes`** is the GST on brokerage, which Flex reports SEPARATELY from
  `ibCommission` where the CSV's `Comm/Fee` column combines them. Omit it and
  the cash reconstruction runs short by exactly the GST — it was 120.83 AUD
  across 60 orders here — while cost bases stay right, so nothing looks wrong.
- **`listingExchange`** is what maps a bare `GOLD` to the engine's `GOLD.AX`.
  Without it the sheet, the lot book and the solver stop agreeing on what a
  holding is called.
- **Cash Report** carries the opening balances. A statement without it is
  well-formed and wrong by the entire opening cash position — the same class of
  error that once put the NAV series at $994,850 instead of $247,000.

You do not have to get this perfect first time. `--verify` (step 4) names
whatever is missing.

## 3. Store the credentials

Either set environment variables (these win if both are present):

```bash
setx IBKR_FLEX_TOKEN "<token digits>"
```

...or — simpler for a scheduled task — create `flex_config.json` in the repo
root. It is gitignored:

```json
{"token": "<token digits>", "query_id": "<query id digits>", "expires": "2027-07-20"}
```

`expires` is optional but worth filling in: it is the end of the **Activation
Period** shown in Client Portal when you generate the token. Expiry is the one
failure this design cannot detect for itself — a lapsed token just errors the
fetch, and the engine then falls back to the CSV and runs perfectly normally,
which is the original silent-decay problem returning a year later by the back
door. Recording the date makes `--status` and every scheduled run warn from 45
days out.

Check what the tooling can see. The token is masked to its last four digits:

```bash
./.venv/Scripts/python.exe ibkr_flex.py --status
```

## 4. Verify against the CSV — the acceptance test

This is the step that matters. `ibkr_activity_statement.csv` is reconciled to
the cent against the broker's own closing balances, so it is a known-good
answer. `--verify` downloads the Flex report and compares the two over the
window both cover: trade counts, net units per security, cost bases, FX rates
and cash movements per currency.

```bash
./.venv/Scripts/python.exe ibkr_flex.py --verify
```

`AGREE` means the Flex feed reproduces a statement already proven correct, and
the engine can be left to refresh itself. `DISAGREE` prints each mismatched
security and figure — that is a query misconfiguration or a parser bug, and it
should be resolved before trusting the feed.

The first fetch will not overwrite anything if validation fails: a report
missing a section is rejected, the existing statement is left alone, and the
run says why.

## 5. Nothing else to do

`daily_auto.ps1` now refreshes the statement before the engine, and
`nav.statement_path_for()` prefers the Flex XML whenever it exists. Both steps
are non-fatal: no token, an expired token or a network failure and the engine
falls back to `ibkr_activity_statement.csv` exactly as it did before. A stale
statement is a worse answer, not no answer.

---

## Operating notes

- **The statement is a T+1 record.** IBKR generates activity statements after
  the close, so trades placed today generally appear tomorrow. This is fine —
  the statement is the historical record; live positions come from the broker
  directly. It does mean the morning refresh picks up the *previous* session,
  including the 02:00 US pass.
- **Keep the CSV.** It stays in git as the archival copy and the fallback. The
  XML is gitignored: it is rewritten every run, and tracking a file that churns
  on every run is what made the build's `-dirty` marker meaningless once
  before.
- **The token expires, and the default is six hours.** Set the longest expiry
  offered when generating it, and diarise the renewal. The failure mode is an
  authentication error in `daily_auto.log`, not a crash — the engine keeps
  running on the CSV — so it will not announce itself loudly. Re-generate and
  update `flex_config.json`.
- **Don't hammer the endpoint.** IBKR rate-limits repeated requests for the
  same query; the client backs off and retries (error 1018) rather than
  failing, but there is no reason to run `--fetch` in a loop.
- **A shorter query than the CSV is refused.** If the Flex date range starts
  later than the CSV's first trade, `resolve_statement_path` warns and keeps
  using the CSV rather than silently amputating history.

## Commands

```bash
./.venv/Scripts/python.exe ibkr_flex.py --status
```

```bash
./.venv/Scripts/python.exe ibkr_flex.py --fetch
```

```bash
./.venv/Scripts/python.exe ibkr_flex.py --verify
```

```bash
./.venv/Scripts/python.exe ibkr_flex.py --verify --offline
```

`--offline` re-runs the cross-check against the XML already on disk without
downloading. `--no-write` fetches and reports without saving. `--force` saves a
report that failed validation (use only when you know why it failed).

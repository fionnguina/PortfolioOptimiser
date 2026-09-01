# Handoff — Portfolio Optimiser, session of 2026-08-13 → 08-18

Paste this into a new conversation to pick up where this one ended.
Repo conventions live in `CLAUDE.md`; this covers only what changed and what's open.

---

## Where things stand

Branch `fix/fills-log-gap-and-deferral-race`, **30 commits ahead of `main`**,
pushed, [PR #1](https://github.com/fionnguina/PortfolioOptimiser/pull/1) open.
560 tests. Exe rebuilt and clean-stamped. Latest run:

```
[nav] reconstruction PASSED validation — worst monthly error 0.70%
[drift] NAV samples=34, src=fills recon (validated) + broker NetLiq, DD -0.29%, warnings=0
Warnings in log: 0     Errors in log: 0
OPS ASSERTIONS: All declared expectations hold.
```

## The engine's identity — RESTATED, do not quote older figures

Published deck, lockbox 2026-07-30, back-fill look-ahead removed, rf =
contemporaneous RBA cash rate:

| 10Y | Return | Vol | Sharpe | MaxDD |
|---|---|---|---|---|
| **Strategy** | **+13.28%** | 13.63% | **0.83** | **−22.02%** |
| SPY (AUD) | +15.79% | 20.37% | 0.72 | −24.22% |
| AU equities (TR) | +8.91% | 14.45% | 0.52 | −35.75% |

Beats Australian equities outright with a 13-point shallower drawdown; beats
SPY risk-adjusted while trailing ~2.5pp on absolute return. Figures move
±0.5pp run to run — the 12y panel start rolls daily and yfinance jitters.

**Anything citing ~0.94 Sharpe, +13-14%/yr, "trails SPY ~2%/yr", or a
"+7.31% alpha" row is superseded.** That last one is CAPM alpha at β≈0.35 and
reads backwards to a non-quant.

## Overfitting position

- **Deflated Sharpe 0.99** against 47 distinct variants (sd 0.040) recovered
  from `logs/*.log`. Do NOT use `metrics_history.jsonl` — those are production
  re-runs of one config (sd 0.077 = yfinance jitter) and set the null near zero.
- **MinTRL 4.05 years** of live data to establish Sharpe > 0 at 95%. ~0.2 years
  elapsed. This is the number to put in front of investors.
- **Universe vintage −2.98%/yr, −0.21 Sharpe** restricted to the 31 tickers that
  existed in 2016 — an upper bound on hindsight ETF selection, entirely
  post-2021. Corollary: the 3Y row is the LEAST trustworthy figure on the deck.
- **PBO** implemented (`validation.probability_of_backtest_overfitting`) but
  needs ~10 distinct configs on ONE window. `variant_store.pbo_readiness()`
  reports the shortfall. Sweeps must run in one sitting — the panel start rolls
  daily so runs on different days land on different `data_key`s.

## Open items

1. **PR #1 is not merged** and the branch name predates most of its contents.
2. **PBO needs ~10 configs in one sitting** — the next sweep contributes
   automatically.
3. **Statement refresh is manual.** `ibkr_activity_statement.csv` covers
   2026-06-22 → 08-17. New trades will not appear until it is re-downloaded
   (Client Portal → Statements → Activity → date range → CSV). The durable fix
   is IBKR Flex Web Service: `requests` is already installed and it is two REST
   calls, but the token must be generated in Client Portal by the user.
4. **`gh` is not installed** and cannot be installed by the assistant — writes
   outside the project directory are sandboxed and invisible to the user's real
   shell (see `reference_sandbox_filesystem` memory).
5. **Live book is 74% in two names** (VLUE.AX 53%, SMH 21%), uncapped and
   deliberately so. Disclosed on the deck footnote.

## Traps that cost real time here

- **TWS serves NO execution history.** Verified: `reqExecutions` returns 0 at
  7/30/90 days. The Activity Statement is the only source.
- **Signed FIFO, always.** SOXX was shorted and covered; long-only FIFO invents
  a phantom long with a fabricated cost base.
- **Gate at the consumer's timescale.** Daily bars cannot match an intraday
  broker snapshot (~0.65% floor). The drift tracker uses monthly returns.
- **Exclude external capital flows** before measuring anything. The 2026-06-23
  reset read as a −69% drawdown.
- **A recurring warning is a live bug.** This whole session started from one
  `[lots][WARN]` that had been printing for weeks.

---

# Session 2026-08-18 → 08-20 — Flex Web Service + four correctness fixes

Branch `fix/broker-truth-and-reporting-integrity` (renamed from
`fix/fills-log-gap-and-deferral-race`; PR #1 closed when the old remote branch
was deleted). 616 tests.

```
8ee10cb fix: a five-day stub is not a month, and construction is not tracking error
ef75276 fix: a max over one monthly return is not a validation gate
dba7951 fix: the NAV reconstruction was a day ahead of the broker it validates against
d82faa8 feat: the statement refreshes itself — IBKR Flex Web Service
```

## The statement now refreshes itself

`ibkr_flex.py` (source-run, no rebuild) fetches the Activity Flex report before
each 10:20 run. `ibkr_statement._sections()` dispatches CSV vs Flex XML and
translates the XML into the CSV's column vocabulary, so every consumer and
every trap it encodes is written once. Non-fatal throughout: no token, an
expired one or no network falls back to `ibkr_activity_statement.csv`.

Setup and the Flex query field list: `FLEX_SETUP.md`.
**Token expires 2027-07-20** — recorded in the gitignored `flex_config.json`;
`--status` and every scheduled run warn from 45 days out.

## Each fix uncovered the next — none were visible from the code

1. **`<Order>` vs `<Trade>` is the level of detail**, not an attribute on one
   element. Reading `<Trade>` reported 75 executions for 60 orders.
2. **`taxes` is separate from `ibCommission`** where the CSV's `Comm/Fee`
   combines them. Cash ran short by exactly the GST — 120.83 AUD over 60
   orders — while cost bases stayed correct, so nothing looked wrong.
3. **The reconstruction was a day ahead.** A 10:27 AEST NetLiq sees the
   PREVIOUS close on both venues. `RECON_SNAPSHOT_LAG_DAYS=1` took median daily
   error 0.36% -> 0.14%. The prior note in nav.py said this was worth only
   0.03% — true of the US leg it tested; the book is 53% VLUE.AX and the AU leg
   needed the same lag.
4. **The validation gate maxed over ONE monthly observation** and failed at
   1.48% on a volatile month. Below `RECON_MIN_MONTHS_TO_GATE=3` the median
   daily error decides instead. Series went 24 samples -> 35.
5. **The restored June head then breached the drift gate** — five trading days
   with the book a third built (39 of 60 trades came after 30 June).
   `PARTIAL_MONTH_COVERAGE=0.80` marks a stub `Partial`: reported, never warned
   on. `LIVE_TRADING_START_DATE` 2026-06-22 -> **2026-07-01**.

## Open  *(SUPERSEDED — see the 08-25 → 09-01 section below; two of these are
now closed)*

- **Daily interim gate (0.5%) governs until ~November**, when three monthly
  observations exist and the monthly gate resumes automatically. Set from one
  account over 25 days; untested across a genuinely volatile stretch.
- **`LIVE_TRADING_START_DATE` could defensibly be 2026-07-09**, after the
  07-06/07-08 build-out finished. One line.
- **A rebuild wipes `dist/`**, taking the timestamped engine logs with it —
  lost the ability to diff against the prior run on 08-19.
- **7 `[ibkr-price][WARN]`/run** (IBKR live vs yfinance previous close at
  10:20). Structural, not a defect, but it is most of the warning count.

## Confirmed in production — 2026-08-21 and 08-24

Both runs on the 8ee10cb binary, both clean:

```
[nav] reconstruction PASSED validation — median daily error 0.26% / 0.31% vs 0.50%
[drift] tracker: NAV samples=36 / 37 ... warnings=0
Warnings in log: 2      Errors in log: 0
```

The drift table now measures something real — June's stub gone, both months
`Partial=False`, and July's FY tax settlement netted out correctly (OOS -7.53%
raw, +6.91% tax, -0.76% ex-tax), which is the convention difference that used
to breach every July:

| Month | Live | OOS ex-Tax | Drift |
|---|---|---|---|
| 2026-07 | -1.52% | -0.76% | -0.76% |
| 2026-08 | +2.88% | +3.12% | -0.24% |

Cumulative drift -1.00% against a +/-5% threshold. Warning count fell 8 -> 2,
both structural `[ibkr-price]` (IBKR live quote vs yfinance previous close at
10:20).

**Watch the reconstruction's median daily error**: 0.26% -> 0.31% across those
two runs, 8 of 26 days above 50bps. Headroom to the 0.50% interim gate is
0.19pp. If it drifts toward 0.45%, find out why rather than raising the number.

## Verified live

60/60 trades matching the CSV, every per-security unit and cost base, FX worst
diff 0.000000, both sources reconciling to the broker's own closing cash
(AUD 11,672.93). `ibkr_flex.py --verify` re-runs that comparison and is the
acceptance test — **run it after ANY change to the statement parser.**

---

# Session 2026-08-25 → 09-01 — five defects, each found by reviewing a healthy-looking run

`main`, pushed, 636 tests. The Flex work from the previous session is live and
has needed no attention since.

```
386cb28 fix: staleness by content, so a checkout stops crying wolf about the binary
7f85356 fix: the build destroyed the evidence trail it exists to produce
5701381 fix: the live NAV line was shredded by days the pipeline never ran
869b84d fix: the unattended pipeline could not see its own missed day, or its own sweep
```

Every one of these came out of a run that reported `warnings=0` and `exit=0`.
None were visible from reading the code. **The daily review is what finds
these** — the pipeline reporting itself healthy is not evidence that it is.

## The one that could have cost money

The 02:00 US pass loads the **latest** rec-log entry. The 18:00 evidence run
also runs the engine and wrote its own entry, so the US legs would have chased
the evening sweep rather than the morning's approved plan — contradicting the
invariant in CLAUDE.md that both halves of the day chase the same target.

Never diverged across 39 days, because every one was a cadence-gated SKIP. On a
RUN day it inverts: the morning fills the ASX legs → the 10:30 snapshot moves
`last_position_change_date` to today → the 18:00 run sees 0 days since the last
fill and writes SKIP → at 02:00 the US legs load that SKIP and refuse. ASX
traded, US not, and the anchor now claims a fresh rebalance, so no retry for six
weeks. **That is the standing SMH-underweight symptom, and this is its
mechanism.**

`PORTOPT_NO_REC_LOG=1`, set by `evidence_run.ps1`. Verified: 202 rec-log lines
before and after, verdict still stamped, metrics still written.

**The next RUN day is ~14 September** (28 of 42 days elapsed as of 09-01) and
would have been the first time that path was ever exercised. Watch that run.

## The others

- **The heartbeat could not see a job's own missed day.** `daily_auto.ps1`
  stamps the ledger *before* checking, so the latest success always satisfied
  yesterday's due time. 2026-08-13 was missed by all three jobs; only
  `evidence_run` was reported, purely because it runs at 18:00 and had not yet
  stamped. Now checks every due occurrence over 7 days, bounded by the job's
  first ledger entry (a job cannot miss a day before it existed).
- **Staleness compared mtimes**, so the 08-24 merge checkout reported 11 stale
  sources against a byte-identical binary. Now compares SHA-256 against
  `dist/build_manifest.json`, written at build time from `ops_expected.json`'s
  `engine_sources`. Falls back to mtime when no manifest exists, and says which
  comparison ran.
- **A rebuild wiped `dist/`** and its engine logs — cost a review twice.
  `build_helper._preserve_run_logs()` moves them to `logs/engine_runs/` first.
- **The live NAV line was shredded** by six days with no broker snapshot.
  `bridge_short_gaps()` fills holes of ≤3 days entirely or not at all; a longer
  outage stays visible. Plot only.

## Open — current

- **Median daily reconstruction error is trending up**: 0.26% → 0.31% → 0.35%
  against the 0.50% interim gate. Self-resolves ~October when the third monthly
  observation arrives and the monthly gate resumes. If it reaches ~0.45%,
  diagnose it — do not raise the threshold.
- **Slide 5 overstates divergence ~5x.** The strategy line is rebased to the
  chart window start (25 May), Actual NAV to its own first point (24 Jun), so
  the orange line carries a **+5.19%** head start before the blue line begins.
  It is also today's target weights projected backwards, not what was held, and
  gross of ~296bps/yr costs. The drift table is the honest number: **cumulative
  -1.00%**. Rebasing the strategy line to the NAV's first date was offered and
  not actioned — investor-facing, so it is a judgement call.
- **`LIVE_TRADING_START_DATE` could defensibly be 2026-07-09**, after the
  07-06/07-08 build-out. One line. (Carried forward.)

## Closed since the previous section

- `dist/` log wiping — fixed (7f85356).
- The `[ibkr-price][WARN]` cluster is **explained, not a defect**: at 10:20 AEST
  IBKR quotes a US close yfinance has not published yet. The leveraged pairs
  prove it — TQQQ/QQQ = 2.96x, SOXL/SOXX = 2.90x, both landing on their 3x
  factor. The engine uses the *fresher* price; the warning measures yfinance's
  lag.

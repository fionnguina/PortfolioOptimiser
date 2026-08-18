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

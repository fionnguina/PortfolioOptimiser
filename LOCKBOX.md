# LOCKBOX — Validation discipline + live evidence accumulation

The engine's only honest measure of generalisation is the **dev/validation
split** with a hard peek budget. Without that discipline you cannot tell
whether a knob change improves the strategy or just overfits the test
window. This file captures (a) where the lock-box stands today, (b) the
trigger conditions for refreshing it, and (c) the cadence by which live
evidence accumulates against the engine's recommendations.

See [memory project_dev_validation_2026_06_18](memory/project_dev_validation_2026_06_18.md)
for the original framework + first results.

---

## Current state — as of 2026-06-22

| Item | Value |
|---|---|
| Dev window | 2015-2020 |
| Validation window | 2020-2026 |
| Peek budget | 7 |
| Peeks used | 5 |
| Peeks remaining | 2 |
| Last peek | TLH validation pass (2026-06-18) |
| Live trading start | **2026-06-22 (paper)** |
| Real-money start | TBC, blocked on AFSL |

### What "peek" means here

A peek is **any time we look at validation-window results and use that
information to decide whether to ship something.** It does not count if
we look at validation purely for monitoring without taking action — but
in practice it is hard to look without acting, so we count generously.

### Peek history (rough)

1. Initial validation pass on the 5-slot ensemble baseline (2026-06-18).
2. TLH layer validation (2026-06-18) — uplift confirmed ~0.
3. Cost-aware solver validation (2026-06-18) — dead-end.
4. SKIP_REBAL_DELTA tuning validation — dead-end.
5. Stretch+hedge validation (2026-06-19) — shipped, reverted same day
   after full-period MaxDD revealed the fold-mean lie.

---

## When to refresh the lock-box

Three trigger conditions, **any one of which kicks off the refresh procedure**:

1. **Peek budget exhausted** — we hit 7/7 on the current val window.
2. **Major engine architecture change** — e.g. swap the regime signal,
   change the universe in a structural way, replace the optimiser. A
   knob tweak inside the existing structure does not count.
3. **18 months wall-clock since last refresh** — even without using
   peeks, time-based decay of the dev/val split's relevance matters.
   The market regime that defined 2015-2020 may not be the regime that
   defines 2026 onwards.

The first condition is the most likely trigger given current pace
(~2 peeks per major work session). At 2 peeks left, **one more shipping
decision** triggers a refresh.

---

## Refresh procedure

1. **Snapshot the current validation results** to `LOCKBOX_HISTORY.md`
   before any window change so we keep a record of what the old
   val window said.
2. **Expand the dev window** to include the current val window:
   `dev = 2015 → today` (= 2015-2026 as of June 2026).
3. **Carve a new val window** from "today + buffer" to "today + 24
   months". Buffer = 1 month to avoid the engine being trained on
   data we are about to "validate" against during the buffer.
4. **Reset peek budget** to 7.
5. **Log the refresh** to [memory reference_pending_work](memory/reference_pending_work.md)
   and update this file's "Current state" table.

This means after the first refresh:
- Dev: 2015 → 2026-07
- Val: 2026-08 → 2028-08
- Peeks: 7

Caveat: the new val window does not have observed data yet — every
peek is **waiting for live evidence to accumulate** rather than running
a backtest. This is by design. The discipline shifts from "don't peek
at backtest" to "don't make engine changes that you cannot justify
purely from dev/in-sample evidence."

---

## Live evidence accumulation cadence

Now that paper trading has started (2026-06-22), four artifacts accumulate
automatically on every engine run:

| Artifact | Captures | Refresh |
|---|---|---|
| `metrics_history.jsonl` | Sharpe, α vs SPY, MaxDD per run | every engine run |
| `live_nav_history.jsonl` | Daily NAV snapshot | engine + drift tracker |
| `cash_ledger.jsonl` | Cumulative cost vs expectation | engine run |
| `trade_recommendation_log.jsonl` | Recommended trades + TLH swaps per run | engine run |
| `ibkr_fills_log.jsonl` | Actual broker fills | Phase 3 paper exec runs |

### Recommended cadence

- **Daily** — `--preflight` before market open to confirm config + connectivity.
- **Every rebalance** — full engine pipeline → `dist/Portfolio Optimiser.exe`,
  then `ibkr_paper_exec.py` (preview → typed-YES → execute).
- **Weekly** — `ibkr_paper_exec.py --check-fills` to reconcile broker
  truth against the fills_log (works around the script-side state
  capture bug for orderId-drifted closed orders).
- **Monthly** — `--show-metrics-history` to look for regression drift.
  Any `[metrics-warn]` flags get investigated, not silenced.
- **Quarterly** — re-read this file and confirm the peek budget +
  refresh triggers still match reality.

### What we are NOT doing yet

- Excel `Actual_Fills` sheet auto-refreshes every engine run (this
  works — sheet writes from `ibkr_fills_log.jsonl`).
- No automated slippage report. To compute, join `trade_recommendation_log.jsonl`
  against `ibkr_fills_log.jsonl` by `rec_log_run_at` + `ticker` and
  diff `avg_fill_price_local` against `px_aud`. Easy follow-up build
  once we have a few weeks of fills.
- No paper-vs-engine-backtest divergence chart. Compute: backtest the
  engine over the same `[LIVE_TRADING_START_DATE, today]` window with
  the same regime mix, compare cumulative return to `live_nav_history.jsonl`.
  Follow-up.

---

## The discipline rule (don't forget)

**Any knob change that affects return distribution must go through
`--dev-validation` before being shipped as a `PRODUCTION_*` default.**

The 2026-06-19 Stretch+hedge revert (`a660598`) is the canonical lesson.
Fold-mean MaxDD lies — multi-year drawdowns split across calendar-year
folds and never show up in single-year MaxDD. **Always validate with
full-period peak-to-trough on the live OOS run** before flipping any
`PRODUCTION_*` constant. See
[memory feedback_validate_full_period_maxdd](memory/feedback_validate_full_period_maxdd.md).

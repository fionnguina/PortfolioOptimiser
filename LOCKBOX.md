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

## Current state — as of 2026-08-13 (REFRESH #2 executed)

| Item | Value |
|---|---|
| Dev window | 2015 → 2026-07-30 (= data lockbox date) |
| Buffer | August 2026 |
| Validation window | 2026-09-01 → 2028-09-01 (**forward — live evidence only**) |
| Peek budget | 7 (reset at refresh) |
| Peeks used | 0 |
| Last refresh | 2026-08-13 — trigger: **user directive** (see below) |
| Prior window record | [LOCKBOX_HISTORY.md](LOCKBOX_HISTORY.md) |
| Live trading start | **2026-06-22 (paper)** |
| Real-money start | TBC, blocked on AFSL |

### Two lockboxes, different jobs

| Constant | Governs | Default | Why |
|---|---|---|---|
| `DATA_LOCKBOX_DATE` | the 15 research CLI modes | 2026-07-30 | research honesty — no parameter selected off post-boundary data |
| `REPORT_LOCKBOX_DATE` | the **published** backtest: deck chart, metrics table, Excel | 2026-07-30 | what you show investors must not creep forward into sealed dates |
| *(neither)* | the **live solve** — regime score, crash-hedge check, trade plan | full current data | blinding this is what forced the 2026-07-06 revert |

The live solve and the published backtest now read **different frames**:
`oos_prices_aud_long` (full) vs `oos_prices_report` (truncated). The engine
prints both ranges each run. Override with `PORTOPT_REPORT_LOCKBOX=YYYY-MM-DD`,
or `PORTOPT_REPORT_LOCKBOX=""` to publish a to-today backtest again.

**Refresh #2 trigger was a user directive, not one of the three documented
conditions.** It followed a 2026-08-13 review of the headline performance
slide which established that the published backtest ran to *today* — i.e.
outside the lockbox, and 12 days into this window's own sealed dates. The
boundary moved 2026-06-30 → 2026-07-30 and the reporting scope was added.
Note the cost, deliberately accepted: the July 2026 buffer is consumed, and
one month of already-observed paper evidence is now inside the dev window.
The new August buffer restores the seam.

**The new validation window has no backtest.** Every val "peek" is now a
read of accumulated live/paper evidence, not a backtest run. Shipping
decisions must be justified purely from dev-window (2015→2026-07-30)
evidence: full-window CV + full-period MaxDD + the regime-split harness.

**`--dev-validation` harness reinterpretation:** its two windows
(2015→Feb 2020 / Feb 2020→now) are BOTH inside the new dev window. It is
no longer a validation peek — it is an in-sample **regime-robustness
split** (bull vs post-COVID), and remains the strongest in-sample gate we
have. Use it freely; it costs no budget. But it can no longer catch
overfitting to 2020-2026 — only forward live evidence can.

### What "peek" means here

A peek is **any time we look at validation-window results and use that
information to decide whether to ship something.** It does not count if
we look at validation purely for monitoring without taking action — but
in practice it is hard to look without acting, so we count generously.
Under the forward window this means: any ship/no-ship decision that
cites live-window performance.

### Peek history (current window)

_None yet. Window opened 2026-09-01; evidence accumulates from paper
trading. See [LOCKBOX_HISTORY.md](LOCKBOX_HISTORY.md) for the 7 peeks
spent on the retired 2020-2026 window, and for Window 2 (retired unpeeked)._

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

**Refresh #1 executed 2026-07-03** (trigger: peek budget exhausted 7/7):
- Dev: 2015 → 2026-06-30 (aligned to DATA_LOCKBOX_DATE)
- Buffer: July 2026
- Val: 2026-08-01 → 2028-08-01
- Peeks: reset to 7
- Old-window snapshot: [LOCKBOX_HISTORY.md](LOCKBOX_HISTORY.md)

**Refresh #2 executed 2026-08-13** (trigger: user directive after the
headline-slide review — the published backtest was running to today):
- Dev: 2015 → 2026-07-30 (aligned to DATA_LOCKBOX_DATE)
- Buffer: August 2026
- Val: 2026-09-01 → 2028-09-01
- Peeks: reset to 7
- New: `REPORT_LOCKBOX_DATE` scopes the lockbox to the PUBLISHED backtest
- Old-window snapshot: [LOCKBOX_HISTORY.md](LOCKBOX_HISTORY.md) (Window 2)

Caveat: the new val window does not have observed data yet — every
peek is **waiting for live evidence to accumulate** rather than running
a backtest. This is by design. The discipline shifts from "don't peek
at backtest" to "don't make engine changes that you cannot justify
purely from dev/in-sample evidence."

The DATA_LOCKBOX_DATE stays at 2026-07-30 for all backtest work. When the
val window matures (2028-09) or a refresh trigger fires, the next refresh
moves the lockbox forward in the same step — never in between.

### Lockbox scope (2026-07-06 directive)

The lockbox governs **research honesty, not live operations**. Once today
moved past the boundary, a globally-applied lockbox had the live engine
trading week-old regimes (Stretch 68% solved before a semis selloff).
Scope as implemented:

- **Research CLI modes** (`--walk-forward-cv`, `--dev-validation`, sweeps,
  stress/attribution/tilt tests) → data truncated at 2026-07-30.
- **Published backtest** (deck chart + metrics table + Excel) → truncated at
  `REPORT_LOCKBOX_DATE` (2026-07-30). Added 2026-08-13; before that the deck
  ran to today.
- **Live pipeline, `--auto-pipeline`, diagnostics** (preflight,
  factor-recs, metrics-history) → full current data.
- Kernel workers inherit the parent's resolved state via env, never
  re-decide from their own flags.
- `DATA_LOCKBOX_DATE` env var still overrides in any mode (manual
  research extensions or deliberate freezes).

What keeps the forward val window honest is the **peek budget** — no
parameter may be selected off a backtest that saw post-boundary data —
not blinding the engine that generates the live evidence.

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

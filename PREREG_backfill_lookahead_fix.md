# PRE-REGISTRATION — remove the pre-inception back-fill look-ahead

**Written 2026-08-13, BEFORE running any variant.** Registered per the
validation protocol in CLAUDE.md + LOCKBOX.md.

---

## The defect

Price panels are `.ffill().bfill()`-ed **before** returns are computed:

- `Portfolio_Optimiser.py` — the raw OOS panel, then again after FX conversion
- `oos_engine.py:459` — `px = px.sort_index().ffill().bfill()` inside
  `run_oos_ensemble_walk_forward`

`bfill()` synthesises a flat price series backwards to the panel start, so
`pct_change()` on a ticker that had not yet listed returns **0.0, not NaN**.
The availability gate at `oos_engine.py:697-701` tests `notna()`, so it never
fires:

```python
coverage = train_rets.notna().sum() / max(len(train_rets), 1)
good_cols = coverage[coverage >= 0.8].index.tolist()
```

Verified empirically at a 2018-08-16 rebalance: VLUE.AX (first traded
2021-03-08) and MTUM.AX (first traded 2024-07-22) both score coverage
**1.0000** and present to the solver as **μ = 0.00%, σ = 0.00%** — a
synthetic risk-free asset, which a long-only max-Sharpe optimiser with a
shrunk covariance matrix is maximally attracted to.

Measured exposure in the cached headline run (114 rebalances):
- weight in tickers that **did not exist yet**: >0.1% at **15/114**, max **3.21%**
- weight in tickers with **<24m of real history** (understated variance):
  >1% at **50/114**, peaking **47.6%**

## The fix

Drop `bfill()` from the panels that feed return computation; keep `ffill()`
(carrying the last real price across a non-trading day is legitimate). Leading
NaNs then survive to `pct_change()`, the coverage gate fires as designed, and
a ticker enters the opportunity set only once it has ≥80% real observations in
the trailing training window.

Scope: the OOS/backtest path only. The live panel is examined separately and
reported, not silently changed in this step.

## Hypothesis and expected direction

**This is a correctness fix, not a performance lever.** The pre-fix numbers
are wrong, so the post-fix numbers are the truth regardless of sign. I predict:

1. **Headline metrics DEGRADE modestly.** Removing a zero-variance,
   zero-correlation phantom asset removes free diversification. Expect
   Sharpe −0.02 to −0.10 and MaxDD to deepen slightly.
2. **Degradation is concentrated in 2016-2019**, where the most tickers were
   pre-inception, and shrinks toward zero by 2022+ when nearly all had listed.
3. **Early-fold weights become more concentrated**, because the effective
   universe in early years is genuinely smaller (31 of 47 names at the start).
4. **No change at all after ~2024-07**, once the last late-lister (MTUM.AX)
   has 24 months of history.

## Gate — deliberately NOT the usual "must improve" bar

Applying the standard improvement gate to a bug fix would reject it for
making the number smaller, which is exactly backwards. The gate here is
**correctness and comprehension**:

| # | Criterion | Pass condition |
|---|---|---|
| G1 | The gate now fires | At a pre-inception rebalance, the affected ticker's coverage is <0.8 and it is EXCLUDED from `good_cols`. Verified directly, not inferred. |
| G2 | No new NaN pathology | No NaN reaches weights, prices-at-rebalance, lot/CGT accounting or the NAV series. Rebalance count stays in family (114 ± a few). |
| G3 | Degradation is bounded and explained | Full-period Sharpe drop ≤ 0.15 and return drop ≤ 2.0%/yr. Anything larger means the old result depended on the phantom asset far more than measured, which is a finding in its own right — report, do not silently accept. |
| G4 | Time profile matches prediction 2 | Per-fold deltas are largest in 2016-2019 and ≈0 (within the 10-30bps / 0.02-Sharpe noise floor) in 2024-2026. If the deltas are uniform across time, the mechanism is NOT what I think it is — stop and re-diagnose. |
| G5 | Regime-robustness split | `--dev-validation` runs clean on both windows. Since both windows sit inside the dev period post-Refresh #2, this is an **in-sample regime-robustness check and costs ZERO peek budget** (LOCKBOX.md). It cannot certify generalisation; it can catch a fix that breaks one regime. |

**Ship condition:** G1 and G2 must pass outright. G3/G4 failing does not block
the fix — the fix is correct either way — but a failure must be reported
prominently and the headline numbers restated accordingly.

**No re-rolls.** I will not tune anything in response to these results. If the
fix degrades the strategy, the degradation is the truth and the previously
published numbers were overstated by that amount.

## What this does NOT do

- It does not address **universe selection bias** — the 47 tickers were chosen
  in 2026 knowing their outcomes. That is governed by the peek budget, not by
  any data-hygiene fix, and remains the largest open degree of freedom.
- It does not make the 10Y record a record of *today's* portfolio (VLUE.AX is
  55.8% of the live book and has 5.4 of the 10 years).
- It costs no validation peek and provides no evidence of generalisation.

---

# RESULTS (recorded as they landed; no re-rolls)

All runs lockboxed at 2026-07-30. LEGACY arm = `PORTOPT_LEGACY_BACKFILL=1`.

## Walk-forward CV — FULL-PERIOD (2016-01-04 → 2026-07-30)

| | LEGACY (buggy) | FIXED | Δ |
|---|---|---|---|
| Ann return | +13.17% | **+11.44%** | **−1.73 pp/yr** |
| Sharpe | +0.96 | **+0.85** | **−0.11** |
| MaxDD (peak-to-trough) | −20.49% | **−22.27%** | **1.78 pp deeper** |
| α vs SPY | −1.77% | **−3.49%** | −1.72 pp |

Fold-mean aggregates: Sharpe 0.99±1.24 → 0.91±1.27 (SE ~0.40);
α −3.52% → −4.68%; years with α>0 4/10 → 3/10.

**The look-ahead was worth ~1.7%/yr of return and 0.11 Sharpe** — roughly
6-8× the documented run-to-run noise floor (10-30bps / 0.02 Sharpe). It was
not a rounding artifact.

## Per-fold time profile (gate G4)

| period | mean Δ return | mean Δ Sharpe |
|---|---|---|
| 2016-2019 | **−4.16 pp** | **−0.360** |
| 2020-2023 | +1.01 pp | +0.152 |
| 2024-2025 | +0.46 pp | +0.005 |

2016 alone: −10.56 pp return, −0.81 Sharpe. Monotone decay to ≈0 by 2024-2025,
exactly as predicted — the phantom asset mattered most when the most tickers
were pre-inception and is gone once they had all listed. The positive
2021/2023 folds are re-shuffling noise from a genuinely different opportunity
set, not a mechanism failure.

## Gate outcomes

| gate | result |
|---|---|
| G1 gate now fires | **PASS** — at 2018-08-16, VLUE.AX and MTUM.AX go from coverage 1.000 (both admitted) to 0.000 (both excluded); SMH/SPY still admitted |
| G2 no NaN pathology | **PASS** — exit 0 both arms, zero tracebacks, complete 10-fold table, finite metrics. The `[ff5-setup] FAILED` notice is pre-existing in both arms (expected in skip-pipeline research mode) |
| G3 degradation bounded | **PASS, near the limit** — Sharpe −0.11 (≤0.15), return −1.73pp (≤2.0pp) |
| G4 time profile | **PASS** — concentrated 2016-2019, ≈0 by 2024-2025 |
| G5 regime-robustness split | see below |

## Live-path finding (pre-registered as "examined separately")

The same `.ffill().bfill()` sits on the LIVE panel (`PRICE_DOWNLOAD_PERIOD="2y"`),
the live covariance panel feeding `Sigma_opt`/`mu_vec_opt`, and the live AUD
panel. Verified against the current 2-year window (2024-08-13 → 2026-08-13):
**every one of the 48 tickers has real data from the window start, so the fix
is a provable NO-OP for today's trade plan.** The most recent lister, MTUM.AX
(2024-07-22), passed 24 months of history in July 2026 — the live path WAS
exposed until then. Fixed under the same flag: zero effect now, and it stops
the bug re-appearing the next time a ticker is added to the Holdings sheet.

Portfolio-value display series (`port_prices`, `_pv_series`) still back-fill
deliberately — they are derived NAV history for charts, not solver inputs.

## G5 — dev/validation regime-robustness split (zero peek cost)

Both windows sit inside the dev period post-Refresh #2, so this is an
in-sample regime split, not a validation peek.

| | LEGACY dev | FIXED dev | LEGACY val | FIXED val |
|---|---|---|---|---|
| Ann return | +13.08% | **+12.05%** | +14.54% | **+13.28%** |
| Sharpe | 0.96 | **0.90** | 1.07 | **0.90** |
| MaxDD | −20.08% | −20.80% | −16.07% | −15.48% |
| α vs SPY | −3.44% | **−4.47%** | **+0.37%** | **−0.90%** |

**G5: PASS.** The fixed engine's verdict block reads *"Stable. Engine
generalises well across the two windows"* — Sharpe degradation dev→val is
−0.01 (0.90 → 0.90). Removing the look-ahead does not break the structure.

### But it retires a load-bearing claim

The look-ahead flattered the **validation** window roughly 3× harder than dev
(Sharpe −0.17 vs −0.06), because Feb-2020-onward is exactly when VLUE.AX,
VVLU.AX, VMIN.AX, AGVT.AX, QHAL.AX and REIT.AX were freshly listed or still
pre-inception.

Consequence: **validation α vs SPY flips sign, +0.37% → −0.90%.**

`LOCKBOX_HISTORY.md` Window 1 records the founding result as *"Loses to SPY in
the bull dev window, beats SPY when the regime turns (val α +1.07%/yr).
Volatility-managed-beta thesis vindicated."* That vindication was, in part, an
artifact of the back-fill. Post-fix the engine does **not** beat SPY on
absolute return in either window. The defensible claim is now narrower and
should be stated as such: **lower volatility and shallower drawdowns at a cost
of ~3.5%/yr of absolute return**, not "beats SPY when the regime turns."

This is a finding, not a gate failure — it is the truth the fix revealed.

## Reproduction + final state

Re-ran `--walk-forward-cv` after the live-path edits: **+11.44%/yr, Sharpe
0.85, MaxDD −22.27%, α −3.49%** — identical to the basis point. The result is
deterministic, not yfinance jitter, and the live-path fix provably does not
touch the backtest.

486 tests pass (17 new guards in `tests/test_lockbox_and_reporting.py`,
including a behavioural one that fails if the coverage gate stops firing).
Exe rebuilt. `metrics_history.jsonl` now records `lockbox`, `report_lockbox`,
`legacy_backfill` and `au_benchmark` per run, so a stored metric row can be
attributed to its data-hygiene state — the gap that made this audit slow.

## SHIPPED. Restated engine identity

**+11.44%/yr, Sharpe 0.85, MaxDD −22.27%, trailing SPY 3.49%/yr absolute.**
dev/val stable (0.90 → 0.90). Every prior citation of ~0.94-0.97 Sharpe /
+13-14%/yr / "trails SPY ~2%/yr" is superseded — updated in CLAUDE.md,
LOCKBOX_HISTORY.md, both agent definitions, and the memory files.

## Still open (unchanged by this fix)

1. **Universe selection bias** — the 47 tickers were chosen in 2026 knowing
   their outcomes. Largest remaining degree of freedom; governed by the peek
   budget, not by any data fix.
2. **The 10Y record is not a record of today's portfolio** — 31.8% of the live
   book by weight existed at the backtest start; VLUE.AX is 55.8% of the book
   with 5.4 of the 10 years.
3. **~45-50 tuned scalars against ~2 independent market cycles.** Walk-forward
   at the estimator level, in-sample at the config level.

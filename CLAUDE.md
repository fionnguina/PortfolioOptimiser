# Portfolio Optimiser — CLAUDE.md

Quant portfolio engine for an Australian investor (Guina Family Managed Investments,
wholesale-only, AFSL pending). Regime-adaptive 5-slot ensemble (Modest→Stretch) over a
~46-ticker ETF universe, walk-forward validated, net of IBKR brokerage + AU CGT
(personal_30pc: 30% MTR + 2% Medicare, 50% LT discount, FY netting).

## Runtime — non-negotiable
- Python: `./.venv/Scripts/python.exe` (3.12). NEVER bare `python`.
- Production runs the PyInstaller exe in `dist/` (`./.venv/Scripts/python.exe build_helper.py`)
  — **rebuild after changing anything in `ops_expected.json → exe.engine_sources`**.
  NOT everything needs one: `ibkr_paper_exec.py`, `ops_assertions.py` and every `*.ps1`
  wrapper run from SOURCE and are live on the next scheduled run. Getting this wrong in
  either direction costs a day — a stale exe silently runs old logic; an unnecessary
  rebuild is 5 minutes and a new sha.
- `[build]` in run.log carries the sha/time. `-dirty` now means real CODE drift: runtime
  state files that churn every run are excluded (`build_helper.RUNTIME_STATE`), and the
  offending files are listed by name at build time.
- Syntax check after edits: `./.venv/Scripts/python.exe -m py_compile Portfolio_Optimiser.py cgt.py`
- Tests: `./.venv/Scripts/python.exe -m pytest tests/` (469 tests).

## File map
- `Portfolio_Optimiser.py` — the monolith (~16k lines). Config constants ~400-700,
  solvers ~4200-5200, walk-forward engine ~7300-7900, CLI modes ~8200-9700,
  live pipeline ~11200-12800, Excel/PPT export ~13000-16000.
- `cgt.py` — CGT profiles, LotBook (FIFO lots, `protect=` shielding), compute_cgt_tax,
  compute_fy_tax_ledger (per-FY ledger from actual fills).
- Extracted modules (constants canonical in-module, engine imports/re-exports back):
  `metrics.py`, `ensemble.py`, `tlh.py`, `brokerage.py`, `drift.py`, `factors.py`
  (FF5/MOM + region mapping), `dialogs.py` (holdings/tilts Tk dialogs),
  `solvers.py` (MV core: max_sharpe/frontier/candidates + Ledoit-Wolf — engine syncs
  caps + ENSEMBLE_SLOTS into it), `lots.py` (lot-book construction from fills/holdings),
  `nav.py` (broker-truth actual-NAV series — engine syncs APP_DIR),
  `excel_sheets.py` (xlwings sheet writers + pptx formatting — engine syncs
  TARGET_PORTFOLIO_VALUE_AUD; imports fx), `fx.py` (USD/AUD conversion — engine syncs
  the runtime fx_usdaud series), `ppt_utils.py` (pure PPT/date primitives),
  `ppt_export.py` (the 2,243-line deck builder — FUSED to engine state; engine syncs
  ~55 globals via `_sync_ppt_export()` before each call, back-compat shim delegates),
  `oos_engine.py` (the walk-forward backtest core: run_oos + 7 analytics helpers —
  engine syncs ~25 config values via `_sync_oos_engine()` after config is defined),
  `research_modes.py` (the 12 `_run_*` diagnostic CLI drivers — engine injects 6 shared
  helper fns + 9 config values via `_sync_research_modes()` before dispatch; diagnostic only).
- `Stock Analysis.xlsm` — **Holdings sheet Security column IS the ticker universe.**
  Engine writes Holdings back each run (Units stay sticky; engine never writes Units).
- `tlh_pairs.json` — TLH substitutes. Keys must match engine symbols EXACTLY
  (`IVV.AX` not `IVV`); substitutes must be in the sheet to be priced/buyable.
- `lots_seed.json` + `ibkr_fills_log.jsonl` — lot book truth (rebuilt fresh every run).
- `portfolio_state.json` — NAV state; drives OOS starting NAV. `regions.json` — FF5 region overrides.
- **Live-ops (all run from SOURCE — no rebuild):**
  `ibkr_paper_exec.py` (the executor: order build/price/submit, reconcile, deferrals),
  `jsonl_logs.py` (rec-log writer — carries the verdict, see below),
  `ops_assertions.py` + `ops_expected.json` (declared intent vs reality + run-ledger
  heartbeat — **`ops_expected.json` is the source of truth**; if reality is right and it
  is wrong, change it deliberately), `ops_power.ps1` (blocks idle sleep for a run),
  and the three wrappers `daily_auto.ps1` / `evidence_run.ps1` / `us_session_run.ps1`.
- `pending_watch_resolved.json` — terminal verdicts for orders logged `qty_filled=0`,
  keyed on `ibkr_perm_id`. Without it the same stale row is re-resolved every run and
  flips FILLED → DID NOT FILL once the fill is absorbed into the seed.
- `run_ledger.jsonl` — which scheduled jobs actually ran (gitignored, backed up).
- `.cache/oos/` — OOS backtest cache. Fingerprint = data shape/cols/dates + NAV + kwargs
  + git sha + caps + sector caps + tlh_pairs bytes + μ-shrink λ + LT-defer window.

## CLI modes (all skip dialog + live pipeline)
`--walk-forward-cv` (fold table + FULL-PERIOD block + JSON), `--dev-validation`
(dev 2015→Feb 2020 / validation Feb 2020→now), `--stress-test` (GFC, 7-ticker),
`--scale-analysis`, `--attribution`, `--preflight`, `--auto-pipeline` (scheduled runs).

## Live pipeline — three scheduled tasks (2026-08-10)
| when | task | does |
|---|---|---|
| 10:20 AEST MON–FRI | `daily_auto.ps1` | engine → verdict → **ASX** execution → reconcile → ops check |
| 18:00 AEST MON–FRI | `evidence_run.ps1` | scale-sensitivity sweep. Places NO orders. |
| 02:00 local TUE–SAT | `us_session_run.ps1` | **US** legs of the morning's approved plan |

- **10:20 is load-bearing** — at 09:30 the run fires before the ASX opens, `--wait-for-funds`
  expires and the largest buy defers every rebalance.
- **02:00 / TUE–SAT is not a typo.** The US session in AU local time moves with two DST
  switches (23:30–06:00, 00:30–07:00, or 01:30–08:00); 02:00 is the only hour inside RTH
  in all three. And Monday's plan trades in the session still open at 02:00 **Tuesday**,
  so Friday's runs at 02:00 **Saturday**.
- The US pass **never runs the engine** — no re-optimisation, no new verdict. It reloads
  the morning's rec-log entry so both halves of the day chase the same target.
- `ops_assertions.py --check` is the first thing to run when anything looks off: it answers
  "is the pipeline configured as intended" in one command. Changing a Windows task? See
  the memory note — always via `/XML`, never flag-based `/Create` (it silently defaults
  `WakeToRun=False`), and **quote the script path** (this repo's home dir has a space).

## Executor flags (`ibkr_paper_exec.py`; source-run, no rebuild)
- `--execute` / `--auto-execute` — the latter is a headless `--execute` (validation gate
  replaces the typed prompt). Both are **verdict-gated**.
- `--override-verdict "<why>"` — execute a plan the engine did NOT clear. Requires a
  written reason, echoed loudly. Distinct from `--skip-validation`, which overrides the
  broker-truth safety checks; this overrides the decision to trade at all.
- `--venue ASX|US` — trade one venue's legs; also scopes the open-order guard to it, so a
  leftover ASX order can't veto the US pass hours later.
- `--reprice-to-targets` — re-solve units from the plan's frozen `target_weights` at live
  prices. Use ONLY while the venue is open. Refuses a leg on sign-flip, on drift past
  `--drift-sigma-max` (default 3) × its daily vol, or if unpriceable.
- Read-only/repair: `--shadow-execute`, `--check-fills --write`, `--reconcile-lots`,
  `--snapshot-nav`, `--sync-holdings`, `--cancel-open-orders`, `--flatten`,
  `--complete-deferred`, `--only-tickers`.

## Env overrides (sweeps without code edits; all cache-fingerprint aware)
- `PORTOPT_CAP_OVERRIDES='{"SOXX":0.08}'` — merge into PER_ASSET_WEIGHT_CAPS
- `PORTOPT_SECTOR_CAPS_DISABLE=1` — empty SECTOR_GROUP_CAPS
- `PORTOPT_MU_SHRINKAGE=0.5` — μ→median shrinkage (prod 0; FAILED dev/val 2026-07-02)
- `PORTOPT_LT_DEFER_DAYS=126` — LT-discount sell deferral (prod 0; FAILED dev/val 2026-07-02)
- `PORTOPT_REPORT_LOCKBOX=YYYY-MM-DD` — truncate the PUBLISHED backtest (deck +
  Excel). Default = the lockbox boundary. `=""` publishes a to-today backtest.
- `PORTOPT_UNIVERSE_VINTAGE=2016-08-16` — restrict the panel to instruments
  already trading then ("could this have been run in 2016?"). Research only;
  measured -2.98%/yr and -0.21 Sharpe, all of it post-2021 (VALIDATION_2026_08_14.md).
- `PORTOPT_VARIANT_STORE=0` — disable per-variant return-series capture.
- `PORTOPT_LEGACY_BACKFILL=1` — restore the pre-inception price back-fill.
  **A/B ONLY — it is a known look-ahead** (see PREREG_backfill_lookahead_fix.md).

## Data lockbox — THREE scopes, not one (Refresh #2, 2026-08-13)
| scope | governs | boundary |
|---|---|---|
| `DATA_LOCKBOX_DATE` | the 15 research CLI modes | 2026-07-30 |
| `REPORT_LOCKBOX_DATE` | the **published** backtest: deck chart, metrics table, Excel | 2026-07-30 |
| *(neither)* | the **live solve** — regime score, crash-hedge, trade plan | full current data |

Two frames: `oos_prices_aud_long` (full — live regime/crash-hedge read it;
blinding them forced the 2026-07-06 revert) vs `oos_prices_report` (truncated
— the published backtest and its benchmark rows). Both ranges print each run.
`LOCKBOX_BOUNDARY` is the single constant; no other hardcoded date.
**A lockbox scoped to research modes does NOT govern what you publish** — for
six weeks the deck's backtest ran to today, outside the boundary. See LOCKBOX.md.

## Overfitting instrumentation (`validation.py`, `variant_store.py`)
- **Deflated Sharpe / PSR / MinTRL** — prices the search that found the config.
  DSR 0.99 at the observed 47-variant spread (sd 0.040); the spread would have
  to be ~5x larger before the edge fails. **MinTRL: 4.05 yrs of LIVE data to
  establish Sharpe>0 at 95%** — the number that should govern what you tell
  investors. Do NOT feed `metrics_history.jsonl` to DSR: those are production
  re-runs of ONE config (sd 0.077 = yfinance jitter) and set the null near zero.
  The honest trial spread comes from distinct variants in `logs/*.log`.
- **PBO / CSCV** — `probability_of_backtest_overfitting()`. Needs each variant's
  full return SERIES, which is why `variant_store` exists.
- **`variant_store`** — one hook (`oos_engine.VARIANT_SINK`, fired once per
  walk-forward) captures every variant to `.cache/variants/`. Keys config and
  data SEPARATELY: PBO holds `data_key` fixed and varies `config_key`. Use
  `load_trial_matrix()`, never compare configs across different windows.
  **Sweep in ONE sitting** — the panel start rolls with `period="12y"`, so runs
  on different days land on different windows and do NOT accumulate into one
  comparable matrix. `pbo_readiness()` says where you stand (needs ~10 configs
  on one window). NAV is part of `data_key`, not config: the scale sweep's
  100k→1M are the same strategy at genuinely different brokerage drag
  (Sharpe 0.98→1.03).

## Validation protocol (hard-won; do not shortcut)
1. Gate on the PRODUCTION frame (exe slide metrics at user NAV), not the CV harness alone.
2. Gate on FULL-PERIOD peak-to-trough MaxDD, never fold-mean (understates multi-year DDs).
3. Fold-mean deltas within ~2×SE are noise. Run-to-run noise floor: ~10-30bps return,
   ~0.00-0.02 Sharpe (yfinance re-download jitter); deltas must exceed it.
4. Sweep = selection phase. Then dev/validation split: pick on DEV, open VALIDATION **once**
   per change family. No variant re-rolls after seeing validation.
5. Decompose bundled changes with intermediate runs before attributing cause.
6. Universe doctrine: expand freely on low-vol diversifier (Σ) side; high-vol thematic (μ)
   side measured NET-NEGATIVE (error maximization) — don't re-add without new evidence.

## Config anchors (Portfolio_Optimiser.py)
- `PER_ASSET_WEIGHT_CAPS` — leveraged/vol products 5%/0%; 9 thematics at 0.0 (2026-07-02
  revert — solver-excluded, still priced as TLH substitutes); PMGOLD.AX 0.0 (TLH-only).
- `SECTOR_GROUP_CAPS = {}` — EMPTY. Re-adding groups containing SMH/SOXL re-introduces
  a measured -4.2%/yr 3Y semis haircut. Machinery works; config deliberately off.
- `REBALANCE_FREQ="6W"`, `SKIP_REBAL_DELTA=0.03`, early trigger 5% DD deepen / 10d min.
- TLH: -5% / $100 min / 31d cooldown; 21 pairs; worth ~70bps/yr gross at $251k NAV.
- `LT_DEFER_WINDOW_DAYS=0`, `MU_SHRINKAGE_LAMBDA=0.0` — dormant experiment knobs.

## Log-line glossary (run.log; single lines are load-bearing — do not skim)
- `[rebal-trigger] summed_|Δw|=… verdict=SKIP/RUN` — 0.0000 with position deltas = bug.
- `[oos-cache] HIT/MISS key=…` — HIT after a config change = stale-cache bug.
- `[lt-defer]`, `[tlh]`, `[oos] CGT/brokerage/TOTAL drag`, `[config] …` snapshot block.
- `[drift][WARN]` — live-vs-OOS drift; current large % artifacts vs $1M target are known.
- `[data] Dropped N return outlier(s)` — 30% filter is the canonical yfinance guard.
- Health summary block at end = quickest run triage.

Live-ops lines (`daily_auto.log`, `us_session_run.log`, `evidence_run.log`):
- `[exec] REFUSING TO EXECUTE — verdict=…` — the verdict gate. Exit 3. On a SKIP day this
  is the CORRECT outcome, not a failure; the wrappers record it as `ok`.
- `[exec][reprice] price source: SMH=live|hist` — **read this on any US run.** `hist` means
  no real-time subscription for that venue, so units were re-solved against a stale bar
  and the pass delivers far less than it appears to.
- `[exec][reprice] <TKR>: +31u -> +22u at live $… (same target weight)` — normal; a gap-up
  buys proportionally fewer units, which is the point.
- `OPS ASSERTIONS` / `OPS DRIFT` email — reality disagrees with `ops_expected.json`.
  `TASK TIME/DAYS/ACTION` name the cause directly; `NO RUN RECORDED` is the heartbeat and
  means a job did not run at all (check the task's `LastTaskResult` — `0xFFFD0000` = the
  action failed to launch, usually an unquoted path).
- Evidence run: `finished … Ns awake / Ns wall` is healthy. `ABANDONED` = the machine slept
  past the ceiling; `HUNG` = no sentinel. Machine sleep is excluded from the budget.

## Domain doctrine
- **The portfolio pays its own tax at lodgement** per the Tax_Ledger sheet figure (sim
  models FY-end settlement inside NAV via `_apply_fy_tax`; keep live on the same convention).
- Trade plan `Include?` column gates the TRADE PLAN only — NOT the solver/backtest.
  Solver exclusion = cap 0.0.
- Holdings dialog Cancel still runs the full pipeline on sheet seeds.
- Never execute a trade plan generated under a config you're about to change.
- **The plan and the permission to execute it travel together.** The rec-log entry carries
  `verdict` + `skip_reason`; a missing verdict reads as UNKNOWN and refuses. Don't add a
  path that consumes `recommended_trades` without passing it through `_verdict_gate`.
- **What was approved is a WEIGHT, not a unit count.** Units are that weight over a price
  that may be hours stale. Executing fixed units through a gap overshoots the target twice;
  re-solving is self-financing and is why no extra liquidity reserve was needed.
- The engine's identity — **RESTATED 2026-08-13** after the back-fill look-ahead fix
  (PREREG_backfill_lookahead_fix.md). Full-period CV, lockbox 2026-07-30:
  **+11.44%/yr, Sharpe 0.85, MaxDD -22.27%, α vs SPY -3.49%/yr.**
  Superseded figures (~0.94 Sharpe, +13%/yr, trails SPY ~2%/yr) were inflated by the
  look-ahead — worth ~1.7%/yr and 0.11 Sharpe, 6-8x the noise floor. DO NOT quote them.
  Still a Sharpe/drawdown machine (SPY(AUD) 10Y Sharpe ~0.84 at far higher vol), and
  dev/val remains STABLE post-fix (dev 0.90 -> val 0.90). It trails SPY in ABSOLUTE
  terms — a "+7.31% alpha" row is CAPM alpha at beta~0.35 and reads backwards to a
  non-quant. Benchmark note: the old "-37% AORD" is the PRICE index; investable AU
  equities (total return) did +9.35%/yr with MaxDD -34.31%.
  Levers tested and killed: thematics, μ-shrinkage, LT-deferral.
  User rejected SPY-buy-and-hold slot (wants risk-optimised only).

## Session habits
- Small targeted reads/greps of the monolith; never read it whole.
- Status updates terse; background long runs (`--walk-forward-cv` ≈ 2.5 min each).
- End sessions at natural boundaries; memory files carry state forward.

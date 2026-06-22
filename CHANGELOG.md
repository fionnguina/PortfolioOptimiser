# Changelog

All notable changes to the Portfolio Optimiser engine are recorded here. Format
loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) but
collapses dev-day churn into themed entries so the file stays scannable.

Each section header is the calendar date of the work, not a SemVer tag — the
engine doesn't yet ship release tags because the only consumer is the local
dist/ build. `HEAD` always tracks the current main; the **Last built exe** line
in [README.md](README.md) records which commit is bundled into the live `.exe`.

---

## [Unreleased]

### Added
- **Excel `Actual_Fills` sheet** populated on every engine run from
  `ibkr_fills_log.jsonl`. Header shows the latest batch summary
  (Submitted / Filled / Cancelled / Pending), full per-row ledger
  below sorted by exec timestamp descending. Cross-references the
  `--check-fills` mode for broker truth when the script's snapshot
  is stale.
- **[CONFIG.md](CONFIG.md)** — every operationally-meaningful knob
  documented in one place: build/paths, broker, CGT, drift, IBKR,
  rebalance triggers, weight caps, TLH, crash hedge, production
  toggles, fund economics, FF5 + universe filter. Mirrors the
  `_log_config_snapshot()` output so any `run.log` is reproducible
  without grepping the engine.
- **IBKR Phase 3 — paper-account execution** ([ibkr_paper_exec.py](ibkr_paper_exec.py)).
  Five-layer safety: hardcoded paper port 7497, account-prefix `DU` check,
  `--execute` flag (default OFF = Phase 2 preview), interactive typed-YES
  gate after preview, per-order try/except. Fills written to
  `ibkr_fills_log.jsonl` (one row per submitted order). Reconciliation
  summary prints fill table + status counts after orders settle (60s
  timeout, configurable). Reuses contract-builder, preview formatter,
  and `_refuse_if_live` from `ibkr_dry_run.py`.
- **PDS slide — Guina Family Managed Investments real fund details.** Trustee
  + AFSL-pending notice, wholesale-only (s708/s761G) language, quarterly
  redemption term, IBKR Australia custody, direct-payment distribution.
  Footer relabelled from generic "placeholder" to "DRAFT — pending AFSL +
  legal review."
- **Fund-fee scaffolding** (`FUND_FEES_ACTIVE`, `MANAGEMENT_FEE_PCT_ANN`,
  `PERFORMANCE_FEE_PCT`, `PERFORMANCE_FEE_HWM`, `PERFORMANCE_FEE_HURDLE_ANN`,
  `PERFORMANCE_FEE_CRYSTALLISE_FREQ`). Constants are surfaced on the PDS
  slide and config snapshot; `compute_fund_fees()` accrual helper is in
  the module but gated by `FUND_FEES_ACTIVE = False`. Default is 2/20 with
  HWM, quarterly crystallisation, no hurdle. Flip the gate when capital
  arrives + AFSL issues.
- **CHANGELOG.md** (this file).

### Changed
- **EF slide overhauled.** Old Current / Previous / Optimised / With Tilts
  markers removed. New view shows the 5 ensemble slots (Modest / Aggressive /
  Bold / Maximum / Stretch) projected onto a live-basis frontier (mu_ann_geo /
  Sigma_daily) — slots sit on the curve by construction, Ensemble star sits
  strictly inside (concavity tax). Added "Today's Weight" column to the
  Portfolio table so the live regime mix is visible alongside each slot's
  vol / return. Tilts comparison table removed (orphaned without the With
  Tilts marker).
- **TLH pairs:** added `SMH ↔ SOXX` (symmetric, both US semi) and
  `SEMI.AX → SMH` (cross-domicile substitute for AU semi sleeve).
- **Slide 2 (Fund Performance) table** flush against chart bottom (top
  11.59 → 11.39 cm).
- **Slide 4 (FF vs Portfolio) x-axis** pinned to `MonthLocator(bymonthday=1)`
  with `x_compat=True` on the pandas plot — fixes a misalignment where the
  "30 Apr 2026" vertical line landed between the 01-Mar and 01-Apr ticks.

---

## 2026-06-21 — Operational hardening + slide polish

### Added
- `_evaluate_sweep_result()` honest verdict helper — every sweep CLI
  (`--crash-hedge-test`, `--stretch-hedge-sweep`, `--turnover-penalty-sweep`,
  `--rebal-skip-sweep`, etc.) routes through one central judgement function
  that checks Sharpe + MaxDD + Alpha + Return together. Catches the "ship
  on Sharpe alone" trap that produced the Stretch+hedge regression.
- Run-health summary at end of every live pipeline (Excel ✓ / PPT ✓ /
  metrics ✓ / warnings / errors).
- OOS walk-forward progress beacons every ~10% of rebalances.
- `--preflight` CLI: 10 system checks in under 5 seconds (yfinance, Excel
  COM, IBKR port, disk space, config files, etc.).
- [RUNBOOK.md](RUNBOOK.md) — 11-section operational guide.
- [PDS slide](Portfolio_Optimiser.py) (placeholder draft, finalised in
  Unreleased above).

### Changed
- Slide layout precision (user-measured spec): Slide 2 chart 22.6×8.61 at
  (1.4, 2.78), Slide 3 textboxes positioned inside title ribbon, Slide 4
  table at 2.03×11.55 / 20.32×6.48.
- Slide 4 vertical line at FF cutoff (Portfolio line extends past).
- Engine Metrics Dashboard slide (final position-2).
- Auto factor tilts validated and kept **OFF** (Sharpe +0.18 for -5.5%/yr
  return cost — mirror image of the rejected Stretch+hedge trade).

### Fixed
- `faulthandler` enabled to surface hard crashes in dist.
- Timestamped log filenames (`run_YYYY-MM-DD_HH-MM-SS.log`) so multiple
  same-day runs don't overwrite.

---

## 2026-06-19/20 — Stretch+hedge ship/revert + factor diagnostics

### Reverted
- `a660598` rolled back Stretch+hedge production config to the 5-slot blend.
  Sweep results showed fold-mean MaxDD ~-17%; the full-period live run hit
  **-34.4% peak-to-trough vs the blend's -20.5%** for only +0.3%/yr return.
  Honest postmortem recorded; lesson is the
  `feedback_validate_full_period_maxdd` rule: **fold-mean MaxDD lies — always
  check FULL-PERIOD peak-to-trough on live OOS before flipping any
  PRODUCTION_* constant.**

### Added
- `metrics_history.jsonl` + `--show-metrics-history` CLI + `[metrics-warn]`
  regression warnings on every live run.
- `--factor-recs` CLI: trailing 3M/6M/12M factor Sharpes + auto-tilt
  recommender (rec the long tilt to MOM; engine validated as cost > benefit).

---

## 2026-06-17/18 — GFC stress, scale analysis, dev/val split, TLH

### Added
- **GFC stress test** (`--stress-test`): stripped 7-ticker pre-2006 universe.
  5-slot blend takes 68% of SPY's GFC drawdown (-25% vs -36.6%).
- **Crash-hedge basket** (`HBRD.AX 60% / GOLD.AX 40%`) + asymmetric
  release thresholds. Closes the 8% tail gap in Stretch+hedge GFC config,
  but in modern walk-forward proved net-negative (false-positive cost >
  true-positive benefit); kept `PRODUCTION_CRASH_HEDGE = False` by default.
- **Scale analysis** (10k → 10M AUM, `--scale-analysis` CLI): engine LOSES
  to SPY net of costs at all tested scales. CGT is 92% of drag.
- **Dev/validation split** (`--dev-validation`): peek-budget locked.
  Engine generalises (Sharpe 0.90 dev → 1.03 val). Loses SPY in bull
  (α -4.07%), beats SPY when regime turns (α +1.07%).
- **Tax-loss harvesting layer**: `tlh_pairs.json` + `_run_tlh_pass()` +
  `[tlh]` log + Excel `TLH_Log` sheet + PPT scorecard. Machinery works,
  but net Sharpe uplift ~0 — the engine's natural FY-end netting already
  does most of the offset work.
- **Drift tracker v2+v3**: fill slippage, monthly NAV drift, DD alert.
- **IBKR Phase 2 dry-run** ([ibkr_dry_run.py](ibkr_dry_run.py)) — builds
  Contract + MarketOrder per recommendation, qualifies against TWS, prints
  full preview. Never calls `placeOrder`.

---

## 2026-06-15/16 — Ensemble engine + dialog-first refactor

### Added
- **Regime-aware ensemble engine** (`c188282`): 5 slots (Modest / Aggressive
  / Bold / Maximum / Stretch), softmax-blended via rolling 12-month Sortino
  + forward SPY-regime signal. 10Y backtest Sharpe 0.97, MaxDD -20.5%,
  α vs SPY -2.0%.
- **Dialog-first architecture**: `_run_ff5_and_frontier_setup()` is the
  canonical FF5/frontier compute path; all initial module-level FF5 blocks
  collapsed.
- **Per-asset weight caps** (5% default, overridable per ticker).
- **Multi-region FF5+MOM** (US / AP-ex-Japan / Japan / Europe) with regional
  vol standardisation. `regions.json` overrides supported.

---

For deeper context on why specific paths were taken (or rejected), see:
- [ARCHITECTURE.md](ARCHITECTURE.md) §10 (dev/val + peek budget) and §11
  (production config rationale).
- [RUNBOOK.md](RUNBOOK.md) for operational drive.
- The `memory/` directory for session-by-session decision provenance.

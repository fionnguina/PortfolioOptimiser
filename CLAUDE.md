# Portfolio Optimiser — CLAUDE.md

Quant portfolio engine for an Australian investor (Guina Family Managed Investments,
wholesale-only, AFSL pending). Regime-adaptive 5-slot ensemble (Modest→Stretch) over a
~46-ticker ETF universe, walk-forward validated, net of IBKR brokerage + AU CGT
(personal_30pc: 30% MTR + 2% Medicare, 50% LT discount, FY netting).

## Runtime — non-negotiable
- Python: `./.venv/Scripts/python.exe` (3.12). NEVER bare `python`.
- Production runs use the PyInstaller exe in `dist/` — **rebuild required after any .py change**.
  The exe logs its build sha/time; check `[build]` in run.log for code/exe drift.
- Syntax check after edits: `./.venv/Scripts/python.exe -m py_compile Portfolio_Optimiser.py cgt.py`
- Tests: `./.venv/Scripts/python.exe -m pytest tests/` (~62 tests).

## File map
- `Portfolio_Optimiser.py` — the monolith (~16k lines). Config constants ~400-700,
  solvers ~4200-5200, walk-forward engine ~7300-7900, CLI modes ~8200-9700,
  live pipeline ~11200-12800, Excel/PPT export ~13000-16000.
- `cgt.py` — CGT profiles, LotBook (FIFO lots, `protect=` shielding), compute_cgt_tax,
  compute_fy_tax_ledger (per-FY ledger from actual fills).
- `tlh.py`, `brokerage.py`, `drift.py`, `ensemble.py` — extracted modules.
- `Stock Analysis.xlsm` — **Holdings sheet Security column IS the ticker universe.**
  Engine writes Holdings back each run (Units stay sticky; engine never writes Units).
- `tlh_pairs.json` — TLH substitutes. Keys must match engine symbols EXACTLY
  (`IVV.AX` not `IVV`); substitutes must be in the sheet to be priced/buyable.
- `lots_seed.json` + `ibkr_fills_log.jsonl` — lot book truth (rebuilt fresh every run).
- `portfolio_state.json` — NAV state; drives OOS starting NAV. `regions.json` — FF5 region overrides.
- `.cache/oos/` — OOS backtest cache. Fingerprint = data shape/cols/dates + NAV + kwargs
  + git sha + caps + sector caps + tlh_pairs bytes + μ-shrink λ + LT-defer window.

## CLI modes (all skip dialog + live pipeline)
`--walk-forward-cv` (fold table + FULL-PERIOD block + JSON), `--dev-validation`
(dev 2015→Feb 2020 / validation Feb 2020→now), `--stress-test` (GFC, 7-ticker),
`--scale-analysis`, `--attribution`, `--preflight`, `--auto-pipeline` (scheduled runs).

## Env overrides (sweeps without code edits; all cache-fingerprint aware)
- `PORTOPT_CAP_OVERRIDES='{"SOXX":0.08}'` — merge into PER_ASSET_WEIGHT_CAPS
- `PORTOPT_SECTOR_CAPS_DISABLE=1` — empty SECTOR_GROUP_CAPS
- `PORTOPT_MU_SHRINKAGE=0.5` — μ→median shrinkage (prod 0; FAILED dev/val 2026-07-02)
- `PORTOPT_LT_DEFER_DAYS=126` — LT-discount sell deferral (prod 0; FAILED dev/val 2026-07-02)

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

## Domain doctrine
- **The portfolio pays its own tax at lodgement** per the Tax_Ledger sheet figure (sim
  models FY-end settlement inside NAV via `_apply_fy_tax`; keep live on the same convention).
- Trade plan `Include?` column gates the TRADE PLAN only — NOT the solver/backtest.
  Solver exclusion = cap 0.0.
- Holdings dialog Cancel still runs the full pipeline on sheet seeds.
- Never execute a trade plan generated under a config you're about to change.
- The engine's identity: Sharpe/drawdown machine (10Y ~0.94 vs SPY 0.83, MaxDD -26% vs
  -37% AORD), trails SPY ~2%/yr absolute in exchange. Pre-tax it beats SPY — the gap is
  mostly CGT drag. Levers tested and killed: thematics, μ-shrinkage, LT-deferral.
  User rejected SPY-buy-and-hold slot (wants risk-optimised only).

## Session habits
- Small targeted reads/greps of the monolith; never read it whole.
- Status updates terse; background long runs (`--walk-forward-cv` ≈ 2.5 min each).
- End sessions at natural boundaries; memory files carry state forward.

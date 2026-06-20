# Portfolio Optimiser — RUNBOOK

Operational document for running the engine, interpreting output, and debugging issues. Pair with [ARCHITECTURE.md](ARCHITECTURE.md) for design context.

---

## 1. Before every live run

Quick mental checklist (or run `--preflight` to do it for you):

- [ ] **Close Excel** — any open instance of `Stock Analysis.xlsm` will block the script with a file lock. Kill stray `EXCEL.EXE` processes in Task Manager if needed.
- [ ] **Close PowerPoint** — same reason for the generated `Reports/Portfolio_Report.pptx`.
- [ ] **TWS / IBKR Gateway** — if you want live IBKR prices, start TWS or Gateway in paper mode (port 7497). If not running, the engine falls back to yfinance automatically (warning logged).
- [ ] **Network** — yfinance needs internet. Throttled/offline = no data download = no run.
- [ ] **Disk space** — engine + reports + history needs ~500 MB free.

**One-line preflight** (recommended before any heavy run):

```powershell
& "C:\Users\Fionn Guina\Portfolio_Optimiser\dist\Portfolio Optimiser.exe" --preflight
```

Runs all the above checks in <5 seconds and exits with clear PASS / WARN / FAIL. Exit code 1 if any FAIL.

---

## 2. Running the engine

### Normal mode (live trade plan + reports)

Just double-click `dist/Portfolio Optimiser.exe` or run:

```powershell
& "C:\Users\Fionn Guina\Portfolio_Optimiser\dist\Portfolio Optimiser.exe"
```

The script will:
1. Snapshot config + build stamp to log
2. Download prices + FF5 factor data (~15 s)
3. Show the holdings + tilts dialog (click Save to use your edits, Cancel to use sheet defaults)
4. Run FF5 universe build + frontier solve
5. Run OOS walk-forward backtest (~1 minute, with `[oos-progress]` beacons)
6. Compute live trade-plan recommendation
7. Write Excel (`Stock Analysis.xlsm`) — multiple sheets, ~30 seconds
8. Generate PowerPoint (`Reports/Portfolio_Report.pptx`) — ~10 seconds
9. Print the **run health summary** at the bottom (Item 7 below — must always be green)

Total runtime: typically 4–6 minutes.

### Diagnostic CLI modes (no dialog, no live trade plan)

All exit early without touching Excel/PPT:

| Flag | What it does |
|---|---|
| `--preflight` | System health check, ~5 sec |
| `--show-metrics-history` | Print last 12 live runs with metrics (regression diff) |
| `--factor-recs` | Print trailing-3M/6M/12M factor Sharpes + recommended tilts |
| `--dev-validation` | Engine ON dev (2015-2020) vs validation (2020-today) windows |
| `--walk-forward-cv` | 10-fold OOS Sharpe distribution (modern era) |
| `--attribution` | Where the engine earns its money: per-slot / per-asset / per-regime |
| `--scale-analysis` | Net return at 6 AUM levels ($10k → $10M) |
| `--stress-test` | GFC 2008-09 stress test on stripped 7-ticker universe |
| `--stress-test --stretch-only` | GFC test with Stretch-only allocation comparison |
| `--crash-hedge-test` | Walk-forward CV with crash hedge ON vs OFF |
| `--crash-hedge-release-sweep` | Sweep hedge-release threshold {-3, -5, -8, -10, -12}% |
| `--stretch-only-test` | Walk-forward CV with Stretch-only vs 5-slot blend |
| `--stretch-hedge-sweep` | Stretch base + hedge across release values |
| `--turnover-penalty-sweep` | Sweep γ_cgt penalty {0, 1e-4, 5e-4, 1e-3, 5e-3} |
| `--rebal-skip-sweep` | Sweep SKIP_REBAL_DELTA {3, 4, 5, 6, 7}% |
| `--tilted-ensemble-test` | Walk-forward CV with auto factor tilts ON vs OFF |

---

## 3. Output files — what each one is for

After a normal run, expect:

| File | Purpose | When to check |
|---|---|---|
| `Stock Analysis.xlsm` | Master workbook: Holdings, Tilts, Lots, Trade Plan, OOS_Validation, TLH_Log, etc. | Always — drives the next run + audit trail |
| `Reports/Portfolio_Report.pptx` | Generated deck: title, fund performance, trade plan, FF5, frontier, metrics dashboard | After every run |
| `dist/run.log` | Latest run log (overwritten each run) | Quick "what just happened" |
| `dist/run_YYYY-MM-DD_HH-MM-SS.log` | Timestamped per-run log, kept for last 10 runs | Historical debugging |
| `metrics_history.jsonl` | One JSON line per live run with 3Y/5Y/10Y metrics | Regression detection |
| `trade_recommendation_log.jsonl` | One line per recommendation set | Drift tracker / fill reconciliation |
| `cash_ledger.jsonl` | One line per recorded run with NAV state | Cash flow + drift tracking |
| `portfolio_state.json` | Latest portfolio state (NAV, invested, cum-cost) | Driven by drift tracker |
| `gfc_stress_summary.json` | Most recent `--stress-test` output | Tail-risk audit |
| `dev_validation_summary.json` | Most recent `--dev-validation` output | Generalisation gate |
| `walk_forward_cv_summary.json` | Most recent `--walk-forward-cv` output | Statistical hygiene |
| `attribution_summary.json` | Most recent `--attribution` output | Where money is earned |
| `tilted_ensemble_test_summary.json` | Most recent `--tilted-ensemble-test` output | Factor-tilt validation |
| `crash_hedge_*_summary.json` | Most recent hedge sweep output | Hedge sweep results |
| `*sweep_summary.json` | Each parameter sweep | Parameter-tuning audit trail |

---

## 4. Run-health summary — what every green line means

Printed at the bottom of every live run. **If any line says FAILED, WARNING, or shows `← INVESTIGATE`, do not act on the trade plan until you understand why.**

```
[health] === RUN HEALTH SUMMARY ===
  Runtime:              4m 23s              ← <10 min normal; >15 min = Excel COM hang
  Build:                <sha> at <time>     ← matches the .exe you launched
  Production config:    slot=5-slot blend  crash_hedge=off
  PPT generated:        OK (6 slides)       ← FAILED = PPT save error
  Excel workbook:       OK (updated 3s ago) ← WARNING = stale or lock issue
  Metrics snapshot:     OK (12 total runs)
  Live recommendation:  11 positions        ← if zero, optimiser failed
  TLH (backtest):       18 events ($X loss)
  Drift tracker:        ...
  Warnings in log:      1                   ← grep run.log for [WARN]
  Errors in log:        0                   ← if >0, INVESTIGATE
  Metrics regressions:  0                   ← if >0, REVIEW — engine changed
  Run log:              C:\...\run.log
```

---

## 5. Common errors — fixes

### "API connection failed: ConnectionRefusedError ... port 7497"
**Cause:** IBKR TWS or Gateway not running, or running in non-paper mode (port 7496).
**Fix:** Start TWS or Gateway in paper mode. Or set `USE_IBKR_LIVE_PRICES = False` in config.json to skip entirely.
**Impact:** Engine falls back to yfinance prices. Warning logged.

### "Excel workbook: LOCKED" in preflight
**Cause:** Excel has `Stock Analysis.xlsm` open somewhere.
**Fix:** Close Excel. Check Task Manager for stray `EXCEL.EXE` processes and end them.

### Run hangs for 5+ minutes after `[oos] metrics computed`
**Cause:** xlwings/Excel COM is slow to spawn the invisible Excel instance, especially first-time.
**Fix:** Wait it out — typical first-spawn is 1–3 minutes. If >10 min, kill the process and check Task Manager.
**Prevention:** Run `--preflight` first — the Excel COM check warms up the COM subsystem.

### `[metrics-warn] REGRESSION` lines in log
**Cause:** This run's 10Y Sharpe / MaxDD / Alpha worsened materially vs the prior run.
**Fix:** First, run `--show-metrics-history` to see exactly which dimension changed. Check `git log` for recent commits affecting the engine. **Do not act on the trade plan until you understand why it regressed.**

### "yfinance reachable: FAIL" in preflight
**Cause:** yfinance throttling, network issue, or yfinance API change.
**Fix:** Wait 15 minutes. If persistent, check `https://finance.yahoo.com` directly in a browser. Yahoo occasionally changes APIs that yfinance has to catch up to.

### PPT slide 5 (Frontier) missing tilts comparison
**Cause:** Per-asset factor regression failed for some securities (small sample, new ETF, missing data).
**Fix:** Usually self-resolves on next run as data catches up. Check the `[ff5-setup] Universe built → N securities` line in the log — if N < 30, the universe was unusually narrow.

### "Portfolio NAV drift > 5%" warning
**Cause:** Drift tracker detected the actual portfolio drifted >5% from the last recommendation.
**Fix:** Either execute the recommended trade plan, or update the Lots sheet with actual fills so the engine knows what really happened.

---

## 6. Debugging procedure — when something looks wrong

In order of likelihood:

1. **Run `--show-metrics-history`** — did the numbers change vs the prior run? If yes, look at recent commits.
2. **Check the run-health summary** at end of `run.log` — any FAILED / WARNING / INVESTIGATE lines?
3. **Grep run.log for `[WARN]` / `[ERROR]` / `Traceback`** — surfaces hidden issues.
4. **Run `--preflight`** — confirms basic system state.
5. **Run `--dev-validation`** — confirms engine generalisation hasn't broken.
6. **Compare to a prior timestamped run.log** in `dist/` — look at `dist/run_YYYY-MM-DD_*.log` for the last known-good run.
7. **Check `git log --oneline`** — what changed since the last working run? Use `git diff` against that commit.

---

## 7. Reverting a bad config change

If you flipped a `PRODUCTION_*` constant and the engine got worse:

1. **Run `--show-metrics-history`** — confirms the regression objectively.
2. **`git log --oneline`** — find the commit that introduced the change.
3. **Edit the constants** back to known-good values:
   ```python
   PRODUCTION_SLOT_OVERRIDE = None    # full 5-slot ensemble
   PRODUCTION_CRASH_HEDGE   = False   # no hedge overlay
   ```
   (these at the top of `Portfolio_Optimiser.py`)
4. **Commit the revert** with an honest postmortem in the commit message.
5. **Rebuild** the exe so the dist matches the source:
   ```powershell
   & ".\.venv\Scripts\python.exe" build_helper.py
   ```
6. **Re-run** the live pipeline. Confirm metrics are back via `--show-metrics-history` (latest entry should match the prior good run).

**Example:** see commits `1b8ccd3` (bad ship) and `a660598` (honest revert) from 2026-06-19.

---

## 8. Backup procedures

### Daily (automatic)
- `metrics_history.jsonl` — append-only, never overwritten. Survives all runs.
- `dist/run_*.log` — last 10 timestamped logs auto-kept by `Main.py:_setup_logging`.
- `trade_recommendation_log.jsonl` — append-only.
- `cash_ledger.jsonl` — append-only.

### Manual (recommended weekly)
- **Stock Analysis.xlsm** — copy to OneDrive / Google Drive / external. The Lots sheet is the only source of truth for cost basis history; losing it means losing CGT accuracy.
- **portfolio_state.json** — copy to backup.

### Before a major code change
- `git commit` everything first.
- `git push` if you have a remote.
- Optionally: tag the commit with `git tag pre-experiment-2026-06-19` so reverting is one command.

---

## 9. Build + ship procedure

1. **Validate** changes via diagnostic CLIs:
   ```powershell
   & ".\.venv\Scripts\python.exe" Portfolio_Optimiser.py --preflight
   & ".\.venv\Scripts\python.exe" Portfolio_Optimiser.py --walk-forward-cv
   & ".\.venv\Scripts\python.exe" Portfolio_Optimiser.py --dev-validation
   ```
2. **Commit** with descriptive message.
3. **Build**:
   ```powershell
   & ".\.venv\Scripts\python.exe" build_helper.py
   ```
   Outputs `dist/Portfolio Optimiser.exe` + copies `regions.json` + `tlh_pairs.json` to `dist/`.
4. **Smoke-test** the built exe:
   ```powershell
   & ".\dist\Portfolio Optimiser.exe" --preflight
   ```
5. **Run a live test** before relying on it for real trades.
6. **Check the metrics history** post-run for any regression warnings:
   ```powershell
   & ".\dist\Portfolio Optimiser.exe" --show-metrics-history
   ```

---

## 10. Where to look first when X is broken

| Symptom | First look |
|---|---|
| Trade plan is empty | `[ensemble] Live recommendation: 0 positions` — check optimiser logs |
| Numbers different from yesterday | `--show-metrics-history` |
| Excel sheet missing | `[excel] ... write skipped` lines in log |
| PPT slide missing | `[pptx] ... skipped` lines in log |
| Wrong prices | `[ibkr-price]` warnings; yfinance fallback usually safe |
| Wrong CGT | Check Lots sheet — is acquisition history complete? |
| Engine config differs from expected | Top of `run.log` for `[config]` snapshot block |
| .exe doesn't start | Check `dist/run.log` for early-init errors |
| .exe crashes silently | `faulthandler` writes traceback to `dist/run.log` |

---

## 11. Glossary of file paths

```
C:\Users\Fionn Guina\Portfolio_Optimiser\
├── Portfolio_Optimiser.py        # main engine source (15k+ lines)
├── Main.py                       # PyInstaller entry point + log setup
├── build_helper.py               # build script
├── ARCHITECTURE.md               # design rationale
├── RUNBOOK.md                    # this file
├── Stock Analysis.xlsm           # master workbook
├── regions.json                  # ticker → region override map
├── tlh_pairs.json                # TLH substitute mapping
├── config.json                   # user runtime config
├── _version.py                   # build stamp (auto-generated)
├── portfolio_state.json          # latest NAV state
├── metrics_history.jsonl         # per-run metrics snapshots
├── trade_recommendation_log.jsonl
├── cash_ledger.jsonl
├── Reports/
│   └── Portfolio_Report.pptx     # generated deck
└── dist/
    ├── Portfolio Optimiser.exe   # built executable
    ├── regions.json              # copy for exe
    ├── tlh_pairs.json            # copy for exe
    ├── run.log                   # latest run log
    └── run_*.log                 # timestamped historical logs
```

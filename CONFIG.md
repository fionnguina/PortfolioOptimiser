# CONFIG — every knob that changes engine behaviour

All knobs live as module-level constants near the top of
[Portfolio_Optimiser.py](Portfolio_Optimiser.py) (mostly between lines ~250 and
~700). Flipping any of them is a config change, not a code change. The startup
config snapshot — printed to `dist/run.log` on every live run — mirrors this
file so you can reconstruct exactly what produced any output without grepping.

**Validation discipline:** any knob change that affects *return distribution*
or *risk profile* must go through `--dev-validation` against the lock-box
window before being shipped as a `PRODUCTION_*` default. The 2026-06-19
Stretch+hedge revert (`a660598`) is the canonical lesson. See
[memory feedback rule](memory/feedback_validate_full_period_maxdd.md).

---

## CLI mode flags (read-only, exit early)

These select an alternative pipeline at startup and skip the live dialog.
None modifies the workbook unless explicitly noted.

| Flag | What it does |
|---|---|
| `--preflight` | 10-check system audit (yfinance, Excel COM, IBKR port, disk, config files). <5 seconds. |
| `--show-metrics-history` | Print the run-by-run regression diff from `metrics_history.jsonl`. |
| `--factor-recs` | Trailing 3M/6M/12M factor Sharpes + auto-tilt recommender. |
| `--dev-validation` | Two-window walk-forward (dev 2015-2020, val 2020-2026). The discipline gate — every ship-decision goes through this. |
| `--walk-forward-cv` | 10-fold walk-forward CV over the modern era. |
| `--attribution` | Per-slot return + tilt-realisation attribution. |
| `--scale-analysis` | Engine-vs-SPY net return across AUM scales 10k → 10M. |
| `--stress-test` | GFC stress (pre-2006 universe, 2007-2009). Add `--stretch-only` for the variant. |
| `--crash-hedge-test` | A/B test the crash-hedge overlay on the modern era. |
| `--crash-hedge-release-sweep` | Sweep the release threshold. |
| `--stretch-only-test` | Force `slot_weights_override = {"Stretch": 1.0}` against the blend. |
| `--stretch-hedge-sweep` | Stretch-only + crash hedge, sweep across release values. |
| `--turnover-penalty-sweep` | Sweep `turnover_penalty` in the cost-aware solver. Validated dead-end on the discipline gate. |
| `--rebal-skip-sweep` | Sweep `SKIP_REBAL_DELTA`. Validated dead-end. |
| `--tilted-ensemble-test` | Auto factor tilts ON for the ensemble. Validated -5.5% return cost; kept OFF. |

Live pipeline = no flags.

---

## Build + paths

| Constant | Default | What it controls |
|---|---|---|
| `APP_DIR` | `~/Portfolio_Optimiser` if it exists, else `_app_dir()` (PyInstaller bundle root) | Resolution root for `config.json`, `Reports/`, `dist/`, log files. |
| `CONFIG_PATH` | `APP_DIR / "config.json"` | Optional user-config JSON. Overrides `_DEFAULTS`. |
| `EXPORT_DIR` | `APP_DIR / "Reports"` | Where `Portfolio_Report.pptx` is written. |

---

## Broker — `BROKER_CONFIG` (auto-selected by `ACTIVE_BROKER_PROFILE`)

| Profile | AU live min/rate | US live min/rate |
|---|---|---|
| `ibkr_pro_au` (default) | $5.00 + 0.080% | $1.50 + 0.020% |
| `commsec_pocket` (preserved for backtest) | $10 + 0.12% | — |

Switch profile by changing `ACTIVE_BROKER_PROFILE` and re-running.

---

## CGT — `CGT_CONFIG` (auto-selected by `ACTIVE_CGT_PROFILE`)

| Profile | MTR | Medicare | LT discount | FY netting | Carry-fwd losses |
|---|---|---|---|---|---|
| `personal_30pc` (default — the user's actual MTR) | 30% | 2% | 50% | yes | yes |
| `personal_45pc` | 45% | 2% | 50% | yes | yes |
| `trust_split` (the AU 2026 budget loophole — **CLOSED**, do not use) | n/a | n/a | n/a | n/a | n/a |

See [memory au_tax_2026](memory/reference_au_tax_2026.md) — trust loophole closed; stick to `personal_30pc`.

---

## Drift tracker

| Constant | Default | What it controls |
|---|---|---|
| `LIVE_TRADING_START_DATE` | `None` | ISO date string when real trading starts. **Drift tracker is dormant until this is set.** Flip on first real fill. |
| `DRIFT_MONTHLY_THRESH` | `0.02` (2%) | Warn if `\|monthly drift\|` exceeds this. |
| `DRIFT_CUMULATIVE_THRESH` | `0.05` (5%) | Warn if `\|cumulative drift\|` exceeds this. |
| `DRIFT_DD_ALERT_THRESH` | `-0.10` (-10%) | Warn if live MaxDD breaches this. |
| `DRIFT_SLIPPAGE_BPS_THRESH` | `25.0` bps | Warn if `\|slippage\|` exceeds this. |
| `DRIFT_FEE_MULTIPLIER` | `2.0` | Warn if actual fees > N × expected. |
| `TARGET_PORTFOLIO_VALUE_AUD` | `1_000_000.0` | Anchor used by cash-ledger for "drift vs target" calculation. |

---

## IBKR live-price fetch (Phase 1)

| Constant | Default | What it controls |
|---|---|---|
| `USE_IBKR_LIVE_PRICES` | `True` | Pull delayed last-prices from TWS for the live trade plan. Falls back to yfinance on connection failure. |
| `IBKR_HOST` | `"127.0.0.1"` | TWS / IB Gateway host. |
| `IBKR_PORT` | `7497` | **PAPER**. `7496` = live; the engine never uses 7496. |
| `IBKR_CLIENT_ID` | `10` | Distinct from helper-script client IDs (7/8/9/11/12). |
| `IBKR_CONNECT_TIMEOUT` | `8` seconds | Bail-out on connect. |
| `IBKR_SNAPSHOT_WAIT_SEC` | `6` seconds | Per-ticker snapshot wait. |
| `IBKR_DIVERGENCE_WARN_BPS` | `100` | Per-ticker warning if IBKR vs yfinance > this. |

Phase 2 dry-run lives in [ibkr_dry_run.py](ibkr_dry_run.py) (client ID 11).
Phase 3 paper exec lives in [ibkr_paper_exec.py](ibkr_paper_exec.py) (client ID 12).

---

## Rebalance triggers + portfolio targets

| Constant | Default | What it controls |
|---|---|---|
| `REBALANCE_FREQ` | `"6W"` | Calendar rebalance frequency. Options: `"MS"`/`"QS"`/`"6W"`/`"YS"`. |
| `REBALANCES_PER_YEAR` | `8.67` (derived from above) | Used by Sharpe scaling + cost models. |
| `SKIP_REBAL_DELTA` | `0.03` (3%) | Skip the rebalance if summed `\|Δw\|` is below this. Validated dead-end at higher values. |
| `EARLY_TRIGGER_DD_DEEPEN` | `0.05` (5%) | Early re-trigger if SPY deepens DD by ≥5% since last rebalance. |
| `EARLY_TRIGGER_MIN_DAYS` | `10` | Minimum days between rebalances even with early-trigger. |
| `PER_ASSET_WEIGHT_CAPS` | `dict` (5% per asset by default) | Per-ticker hard cap on portfolio weight. Override individual tickers. |
| `TARGET_PORTFOLIO_VALUE_AUD` | `1_000_000.0` | Anchor for cash-ledger drift. |

---

## TLH (Tax-Loss Harvesting)

| Constant | Default | What it controls |
|---|---|---|
| `TLH_ENABLED` | `True` | Master switch. Engine runs `_run_tlh_pass()` per rebalance. |
| `TLH_MIN_LOSS_PCT` | `-0.05` (-5%) | Only harvest lots ≥5% in the red. |
| `TLH_COOLDOWN_DAYS` | `31` | ≥30d outside US wash-sale safe-harbour + comfortable under AU TR 2008/1. |
| `TLH_MIN_LOSS_AUD` | `100.0` | Don't bother for absolute losses below this (brokerage floor). |
| `TLH_PAIRS` | loaded from `tlh_pairs.json` | Ticker → substitute map. 13 pairs incl. semi-ETP triangle (SMH ↔ SOXX, SEMI.AX → SMH). |

See [project_tlh_2026_06_18](memory/project_tlh_2026_06_18.md) — net Sharpe uplift ~0; machinery works but FY-end netting already does most of the offset work. Optionality, not alpha.

---

## Crash hedge

| Constant | Default | What it controls |
|---|---|---|
| `CRASH_HEDGE_ENABLED` | `False` | Off by default. Set via test CLIs or live `PRODUCTION_CRASH_HEDGE`. |
| `CRASH_HEDGE_DD_TRIGGER` | `-0.15` (-15%) | Engage hedge when SPY peak-to-current DD ≤ this. |
| `CRASH_HEDGE_DD_RELEASE` | `-0.05` (-5%) | Release hedge when DD recovers above this. |
| `CRASH_HEDGE_LOOKBACK_DAYS` | `252` (1 year) | Rolling peak for DD calculation. |
| `CRASH_HEDGE_BASKET` | `{"HBRD.AX": 0.60, "GOLD.AX": 0.40}` | What to swap the ensemble into when triggered. |

See [project_session_2026_06_20_21](memory/project_session_2026_06_20_21.md) — closed the GFC tail gap (-26% vs SPY -36%) but in modern walk-forward false-positive cost > true-positive benefit (-34% MaxDD for +0.3%/yr return, reverted in `a660598`). Wired and available; default off.

---

## Production config (ship-toggles)

These two are the most consequential constants in the file — they replace the
default 5-slot blend if set. Validate with full-period MaxDD before changing.

| Constant | Default | What it controls |
|---|---|---|
| `PRODUCTION_SLOT_OVERRIDE` | `None` | If set (e.g. `{"Stretch (SPY+25%)": 1.0}`), forces softmax weight onto that slot. Bypasses the regime ensemble. Tested + reverted; do not flip without dev/val. |
| `PRODUCTION_CRASH_HEDGE` | `False` | Engage `_apply_crash_hedge` when trigger fires. Wired but proven net-negative on modern data. |

---

## Fund economics (wired, currently INACTIVE)

These do not touch any return / NAV calculation while `FUND_FEES_ACTIVE = False`.
They populate the PDS slide and config snapshot. Flip the gate when capital
arrives + AFSL issues.

| Constant | Default | What it controls |
|---|---|---|
| `FUND_FEES_ACTIVE` | `False` | Master gate. While `False`, the helper `compute_fund_fees()` is callable but the live pipeline does not deduct fees. |
| `MANAGEMENT_FEE_PCT_ANN` | `0.02` (2%) | Management fee per annum, accrued daily on NAV. |
| `PERFORMANCE_FEE_PCT` | `0.20` (20%) | Performance fee on returns above HWM. |
| `PERFORMANCE_FEE_HWM` | `True` | Enforce high-water mark (peak NAV since inception). |
| `PERFORMANCE_FEE_HURDLE_ANN` | `0.00` | Unhurdled. Set e.g. `0.05` for SPY-comparable hurdle. |
| `PERFORMANCE_FEE_CRYSTALLISE_FREQ` | `"Q"` | Quarterly crystallisation. Options: `"M"`/`"Q"`/`"A"`. |

See [project_fund_identity](memory/project_fund_identity.md) — Guina Family Managed Investments, wholesale-only, AFSL pending.

---

## Tilt / factor

| Constant | Default | What it controls |
|---|---|---|
| `TILT_FACTORS` | `["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM"]` | Standard 5-factor + momentum. |
| `MARGINAL_TAX_RATE`, `CAPITAL_LOSS_CARRY_FWD`, `LOT_MATCH_METHOD` | from `CFG` | User-config overrides loaded from `config.json` at startup. |

---

## FF5 data + universe filter

| Constant | Default | What it controls |
|---|---|---|
| `FF5_REGION_URLS` | dict of region → (CSV URL, MOM URL) | Sources for US / AP-ex-Japan / Japan / Europe FF5. |
| `EUROPEAN_EXCHANGE_SUFFIXES` | tuple | Suffixes that route a ticker to the European FF5 region. |
| `FF5_DAILY_ZIP` | `FF5_REGION_URLS["US"][0]` | Fallback when region detection fails. |
| `regions.json` | runtime override | User ticker → region overrides. `regions.json` is copied to `dist/` by `build_helper.py`. |

See [reference_multi_region_factors](memory/reference_multi_region_factors.md).

---

## Where the engine reads from at startup

| File | Purpose |
|---|---|
| `Stock Analysis.xlsm` Holdings sheet | Seed units for current portfolio. |
| `Stock Analysis.xlsm` Tilts sheet | User tilt targets + bands. |
| `Stock Analysis.xlsm` Lots sheet | CGT lot book (acquisition date + cost base per lot). |
| `config.json` | Optional `CFG` overrides. |
| `tlh_pairs.json` | TLH substitute pairs. |
| `regions.json` | Optional ticker → FF5 region overrides. |
| `portfolio_state.json` | Last-run NAV snapshot. |
| `trade_recommendation_log.jsonl` | History of recommended trades (drift tracker + IBKR scripts read latest). |
| `live_nav_history.jsonl` | Daily NAV snapshots. |
| `cash_ledger.jsonl` | Per-run cumulative cost tracking. |
| `metrics_history.jsonl` | Run-by-run Sharpe / α / MaxDD for regression detection. |
| `ibkr_fills_log.jsonl` | Phase 3 paper-exec fill log (Excel sheet `Actual_Fills` reads this). |

---

## Constants table at a glance

```
Build / paths:        APP_DIR, CONFIG_PATH, EXPORT_DIR
Broker:               ACTIVE_BROKER_PROFILE, BROKER_CONFIG
CGT:                  ACTIVE_CGT_PROFILE, CGT_CONFIG, MARGINAL_TAX_RATE,
                      CAPITAL_LOSS_CARRY_FWD, LOT_MATCH_METHOD
Drift:                LIVE_TRADING_START_DATE, DRIFT_*, TARGET_PORTFOLIO_VALUE_AUD
IBKR:                 USE_IBKR_LIVE_PRICES, IBKR_HOST/PORT/CLIENT_ID, etc.
Rebal:                REBALANCE_FREQ, SKIP_REBAL_DELTA, EARLY_TRIGGER_*,
                      PER_ASSET_WEIGHT_CAPS
TLH:                  TLH_ENABLED, TLH_MIN_LOSS_*, TLH_COOLDOWN_DAYS, TLH_PAIRS
Crash hedge:          CRASH_HEDGE_ENABLED/_DD_TRIGGER/_DD_RELEASE/_LOOKBACK_DAYS,
                      CRASH_HEDGE_BASKET
Production:           PRODUCTION_SLOT_OVERRIDE, PRODUCTION_CRASH_HEDGE
Fund fees:            FUND_FEES_ACTIVE, MANAGEMENT_FEE_PCT_ANN,
                      PERFORMANCE_FEE_PCT, PERFORMANCE_FEE_HWM,
                      PERFORMANCE_FEE_HURDLE_ANN, PERFORMANCE_FEE_CRYSTALLISE_FREQ
Tilt:                 TILT_FACTORS
FF5:                  FF5_REGION_URLS, EUROPEAN_EXCHANGE_SUFFIXES, FF5_DAILY_ZIP
```

If a constant is documented here, the engine treats it as part of the supported
config surface — changing it should not break anything, but **must** go through
`--dev-validation` for any change that affects return distribution.

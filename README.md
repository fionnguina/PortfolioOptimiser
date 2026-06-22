# Portfolio Optimiser

A self-hosted quant portfolio engine for an Australian investor running ~$1M across global ETFs.

Each run pulls live data, fits a Fama-French 5-factor model across 4 regions, solves a regime-adaptive ensemble of 5 candidate portfolios, and generates a rebalance trade plan with broker + CGT costs already netted. Output lands in an Excel workbook + a PowerPoint roadshow deck.

The engine has been stress-tested through the 2008 GFC (68% of SPY's drawdown on a stripped 7-ticker universe, see [AUDIT.md](AUDIT.md)) and is now wired to IBKR for paper-account execution via the `ib_insync` API.

---

## Quick start

```powershell
# From the project root, with the venv active
& ".\.venv\Scripts\python.exe" Portfolio_Optimiser.py

# Or run the bundled .exe (PyInstaller build)
& ".\dist\Portfolio Optimiser.exe"
```

Each run:
1. Downloads 5y of prices for ~46 tickers via yfinance
2. Optionally overrides last-prices with IBKR delayed quotes (TWS must be open in paper)
3. Builds the FF5 universe, fits regional betas
4. Solves the 5-candidate ensemble (Modest/Aggressive/Bold/Maximum/Stretch)
5. Generates a trade plan (delta units per ticker) at current prices
6. Writes everything to `Stock Analysis.xlsm` + a PPT report

Outputs appear in `Stock Analysis.xlsm`, `Reports/`, and three JSONL logs (`trade_recommendation_log.jsonl`, `live_nav_history.jsonl`, `cash_ledger.jsonl`).

---

## Requirements

- **Windows 10/11** (uses `xlwings` + `win32com` for live Excel + PowerPoint)
- **Python 3.12** in `.\.venv\`
- **Microsoft Excel** installed and **closed** when the optimiser runs (it needs to open the workbook)
- **Interactive Brokers TWS** (Trader Workstation), optional — only needed for live pricing override and the paper-trading scripts

### Python dependencies

Core: `pandas`, `numpy`, `scipy`, `cvxpy`, `statsmodels`, `matplotlib`, `yfinance`, `openpyxl`, `xlwings`, `python-pptx`, `customtkinter`, `requests`, `pywin32`

IBKR integration: `ib_insync`

Run from `requirements.txt` (not currently in repo — install via the imports listed in `build_helper.py:EXTRA_RUNTIME_DEPS`).

---

## The 5-second architecture

```
yfinance (5y daily) ──┐
IBKR delayed quotes ──┤──► FF5 regression × 4 regions ──► Sigma + mu
^FF/MOM/FX data ──────┘                                       │
                                                              ▼
                                            5-candidate frontier solver
                                            (Modest, Aggressive, Bold,
                                             Maximum, Stretch)
                                                              │
                                                              ▼
                                        Softmax ensemble (rolling 12M Sortino
                                        scored against backward IR vs SPY +
                                        forward 20/50 SMA cross signal)
                                                              │
                                                              ▼
                                        Trade plan (delta units per ticker)
                                          + broker costs + AU CGT model
                                          (FY-netted, 12mo discount, FIFO/HIFO)
                                                              │
                                                              ▼
                              Stock Analysis.xlsm     +     Reports/Portfolio_Report.pptx
                                       │
                                       ▼
                       Drift tracker (JSONL state)
                       Cash ledger (JSONL state)
                       Actual_Fills sheet (user enters live trades)
```

Full breakdown in [ARCHITECTURE.md](ARCHITECTURE.md).

---

## File layout

```
Portfolio_Optimiser/
├── Portfolio_Optimiser.py          # The engine. ~9k lines, one monolithic file.
├── Main.py                         # Entry point that sets up logging then exec's the engine
├── build_helper.py                 # PyInstaller wrapper. Writes _version.py with git SHA.
├── _version.py                     # AUTO-GENERATED at build time. Gitignored.
├── Stock Analysis.xlsm             # The workbook. Holdings + Tilts + Lots input;
│                                   # OPT, OOS_Validation, CGT_Audit, Drift_Fills,
│                                   # Drift_NAV, Actual_Fills, Cash_Ledger output.
├── regions.json                    # User region overrides (e.g. NDQ.AX → US)
├── portfolio_state.json            # Last-run NAV snapshot (auto-rewritten)
├── trade_recommendation_log.jsonl  # JSONL: one entry per run, all recommendations
├── live_nav_history.jsonl          # JSONL: daily NAV snapshot
├── cash_ledger.jsonl               # JSONL: per-run cumulative cost tracking
├── Reports/
│   └── Portfolio_Report.pptx       # The roadshow deck
├── ibkr_paper_test.py              # IBKR Phase 0: read-only connection test
├── ibkr_price_check.py             # PoC: delayed quotes vs yfinance comparison
├── ibkr_seed_paper.py              # IBKR Phase 1.5: pretend-trade engine units → paper
├── ibkr_dry_run.py                 # IBKR Phase 2: format engine's rebalance as IBKR
│                                   # Order objects, print, NO submit
├── ibkr_paper_exec.py              # IBKR Phase 3: paper-account execution with
│                                   # typed-YES gate, fill tracking, slippage log
├── dist/
│   ├── Portfolio Optimiser.exe     # PyInstaller-built .exe (~165 MB)
│   └── run.log + run.log.1..7      # Rotated run logs
├── AUDIT.md                        # Pre-paper-live audit + readiness checklist
├── ARCHITECTURE.md                 # Data flow + key data structures + module guide
└── tests/                          # Pytest suite (minimal coverage)
```

---

## Configuration knobs (top of Portfolio_Optimiser.py)

| Knob | Default | What it does |
|---|---|---|
| `ACTIVE_BROKER_PROFILE` | `"ibkr_pro_au"` | Broker cost model used by both backtest and live trade-plan |
| `ACTIVE_CGT_PROFILE` | `"personal_30pc"` | 30% MTR + 50% LT discount, Medicare on |
| `REBALANCE_FREQ` | `"6W"` | Backtest + recommendation cadence. Tested empirically. |
| `PER_ASSET_WEIGHT_CAPS` | SOXL/TQQQ/SVIX=5%, UVIX/VXX=0% | Leveraged + vol products. Without these caps, 10Y Sharpe drops from 0.97 to 0.72. |
| `EARLY_TRIGGER_DD_DEEPEN` | `0.05` | Off-cycle rebalance when SPY DD deepens >5% |
| `SKIP_REBAL_DELTA` | `0.03` | Skip rebalance when summed \|Δw\| < 3% |
| `TRADE_PLAN_MODE` | `"auto"` | `auto` picks highest-Sharpe among ensemble/with-tilts/no-tilts |
| `USE_IBKR_LIVE_PRICES` | `True` | Replace yfinance last-price with IBKR delayed (free in paper) |
| `LIVE_TRADING_START_DATE` | `None` | Set to `"2026-08-01"`-style string once you start real trades. Activates drift tracker monthly NAV comparison. |
| `TARGET_PORTFOLIO_VALUE_AUD` | `1_000_000` | Anchor for "drift vs target" in cash ledger |
| `DRIFT_*_THRESH` | various | Warn thresholds for monthly/cumulative drift, MaxDD, slippage, fee multiplier |

---

## IBKR integration phases

| Phase | Script | What it does | Risk |
|---|---|---|---|
| **0** | `ibkr_paper_test.py` | Connect + print account summary + positions | None — read only |
| **PoC** | `ibkr_price_check.py` | Pull 10 delayed quotes, compare to yfinance | None — read only |
| **Live pricing** | (built into engine) | Override yfinance last-price with IBKR delayed | None — read only |
| **1.5** | `ibkr_seed_paper.py --execute` | Submit MARKET BUY orders to load engine's current units into paper | Paper money only. Requires Read-Only API OFF + TWS restart. |
| **2** | `ibkr_dry_run.py` | Format engine's rebalance as IBKR Contract+Order objects. **Print, NEVER submit.** | None |
| **3** | TBD | Paper execution path with fill capture into Actual_Fills sheet | Paper only |
| **4** | TBD | Live execution. User explicit sign-off, partial allocation first. | Real money |

Currently shipped: Phase 0, PoC, Live pricing, Phase 1.5 (script ready, not executed), Phase 2 (dry-run).

---

## Common run output

```
[build] version: GIT_SHA=be9923e  BUILD_TIME=2026-06-18T14:00:00
================================================================================
[config] === SNAPSHOT ===
[config] BUILD                 ...
[config] BROKER                profile=ibkr_pro_au (Interactive Brokers (Pro AU))
[config] CGT                   profile=personal_30pc  MTR=30%  ...
... (full config dump)
================================================================================
... (yfinance download, FF5 fit, ensemble walk-forward, ~3 min)
[ibkr-price] applied to 45 tickers in 5.1s (max divergence: 14 bps on VLUE.AX; 0 >100bps warned)
[drift] logged recommendation → trade_recommendation_log.jsonl (22 trades, NAV AUD 999,773)
[drift] tracker: NAV samples=1, current DD +0.00%, fills 0/0 adherent, warnings=0
[cash] ledger: 4 run(s) recorded. Drift vs $1,000,000: $-227 | Cum. brokerage $4,382 | Cum. CGT $1 | Unexplained Δ $1,523
... (Excel write, PPT export)
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `[ibkr-price] connection skipped (ConnectionRefusedError)` | TWS not running or paper not logged in | Open TWS, log into paper. Engine continues with yfinance. |
| `[fx][CRITICAL] USD/AUD fetch failed` | yfinance outage | Wait + retry. ALL USD valuations are distorted while this is firing. |
| `Workbook locked / Read-only` | Excel is open with `Stock Analysis.xlsm` | Close Excel, re-run. |
| `[cash] ledger: 1 run(s) recorded` after multiple runs | You're running the .exe from `dist\` and the build wiped state | Already fixed in `be9923e` — state lives at project root now. Older builds had this bug. |
| Trade plan suggests very different weights run-to-run | Daily price drift moves the ensemble | Expected at intraday cadence. **Only act on trade plans at the 6W rebalance schedule.** Intraday is diagnostic. |
| `Error 321 ... Read-Only mode` from IBKR | Read-Only API checkbox is on in TWS | Engine reads are unaffected. Order submission is blocked (this is the safety belt). Turn off only when running `ibkr_seed_paper.py --execute`. |

---

## Build

```powershell
& ".\.venv\Scripts\python.exe" build_helper.py
```

Builds a ~165 MB onefile .exe to `dist\Portfolio Optimiser.exe`. Stamps `_version.py` with git SHA + build time before PyInstaller runs, so the .exe knows its own version at runtime.

---

## License + caveat

This is a personal tool. No license. Not financial advice. Use at your own risk; the author bears no responsibility for losses sustained by running this code on a real brokerage account.

The OOS walk-forward backtest is honest (CGT + brokerage netted, no look-ahead) but the future is not the past.

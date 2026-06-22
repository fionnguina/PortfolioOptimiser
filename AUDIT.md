# Portfolio Optimiser — pre-paper-live audit

Snapshot of the codebase before flipping the live IBKR switch (currently at commit `7cbd63f`, ~9,050 lines in `Portfolio_Optimiser.py`).

Findings are triaged by **action**: 🔴 must-fix before live, 🟡 should-fix soon, 🟢 nice-to-have. Each item has a file:line where applicable.

---

## 1. REDUNDANT (delete)

| # | Item | Where | Action | Pri |
|---|---|---|---|---|
| R1 | CMC broker profile fully unused since IBKR Pro AU switch | `Portfolio_Optimiser.py:236-256` | Delete `cmc_markets` block + `BROKER_PROFILES` dict reduction | 🟢 |
| R2 | Old Jupyter notebook checked-in but stale (296KB) | `Portfolio_Optimiser1903 AI intergrated.ipynb` | `git rm`; the canonical source is the .py | 🟡 |
| R3 | `build_log.txt` 224KB output artefact not gitignored | root | Add to `.gitignore` | 🟢 |
| R4 | `.jupyterlab_startup.sh.txt` (22 bytes orphan) | root | Delete | 🟢 |
| R5 | `portfolio_state.json` tracked in git but auto-rewritten every run | root | `git rm --cached portfolio_state.json` + add to `.gitignore` (noise on every `git status`) | 🟡 |
| R6 | Legacy region-name aliases | `Portfolio_Optimiser.py:953-990` | Audit which "legacy" callers still exist; if none, remove | 🟢 |
| R7 | Legacy PowerPoint template fallback path | `Portfolio_Optimiser.py:8947-8950` | Likely safe to remove given dialog-first ship | 🟢 |
| R8 | One explicit `# TODO` re: carry-forward losses | `Portfolio_Optimiser.py:286` | Was already implemented in CGT model — comment is stale | 🟢 |

---

## 2. FRAGILE (bullet-proof before live)

### 2a — Error handling

| # | Item | Where | Why it matters | Pri |
|---|---|---|---|---|
| F1 | **84 `except` blocks**, ~10+ silent `except Exception: pass` | Throughout | Real bugs get swallowed; you find out months later. Need to distinguish "expected fallback" from "unexpected" by logging stack trace at minimum | 🔴 |
| F2 | `try/except: pass` swallows in: load_config, dialog event handlers, openpyxl number-format calls | `:165, :210, :856, :868, :1189` | Most are minor BUT some hide config corruption | 🟡 |
| F3 | No `traceback.format_exc()` printed even in noisy except blocks | Throughout | When something fails silently the message says "skipped: NameError" but not WHERE | 🔴 |

### 2b — State management

| # | Item | Where | Why | Pri |
|---|---|---|---|---|
| F4 | **44 `globals()[...]` mutations** | Throughout | Module-level globals as a primary communication channel. Race-prone, hard to test. Worst offenders: `_run_ff5_and_frontier_setup` writes ~20 globals | 🟡 (defer to refactor session) |
| F5 | `last_px_hold` defined in three places (dialog cancel branch, save branch, after FF5) | `~4691, ~4695, ~4708` | If a refactor changes one, the others can quietly drift | 🟡 |
| F6 | State files have no schema version | All `*.jsonl` writers | If we add a field, old entries silently miss it. Reads work but downstream consumers can crash | 🟡 |

### 2c — Build and execution

| # | Item | Where | Why | Pri |
|---|---|---|---|---|
| F7 | **No git SHA stamped into the .exe** | `build_helper.py` | After 3+ rebuilds you can't tell which code version ran | 🔴 |
| F8 | `dist/run.log` overwritten each run — no run history | `Main.py` `_setup_logging` | If yesterday's bug isn't reproducible today, you've lost the evidence | 🔴 |
| F9 | `build_helper.py` wipes `dist/` (we just fixed the state file location — but still drops `run.log` per-build) | `build_helper.py:75-82` | Acceptable now; consider rotating logs to project root | 🟢 |
| F10 | xlwings requires Excel installed + workbook closed — no graceful failure mode | Throughout | If you forget to close Excel the engine fails halfway through and may corrupt state | 🟡 |

### 2d — Data + network

| # | Item | Where | Why | Pri |
|---|---|---|---|---|
| F11 | yfinance bulk download has no timeout | `:1245`, OOS download `:6193` | Could hang the engine indefinitely if yfinance has an outage | 🔴 |
| F12 | No retry on yfinance transient failures | Throughout | Single bad fetch breaks the whole run | 🟡 |
| F13 | IBKR connection: 8s timeout, no retry | `IBKR_CONNECT_TIMEOUT=8` | If TWS is slow on startup, engine just falls back to yfinance silently | 🟡 |
| F14 | FX rate fallback to hardcoded 1.50 if all FX fetches fail | `:1171 onwards` | Catastrophically wrong if used. Should be `raise`, not silent fallback | 🔴 |
| F15 | Outlier filter at >30% return drops data silently. Logs which days BUT doesn't explain *why* (corp action vs data error vs real move) | `:1402` | Hard to tell signal from noise when reviewing | 🟢 |

### 2e — IBKR-specific (new this week)

| # | Item | Where | Why | Pri |
|---|---|---|---|---|
| F16 | IBKR connection on every engine run adds ~5s coupling | `:7396 area` | If TWS dies mid-run, fallback is fine, but no log line "fell back". Should print "[ibkr-price] fell back to yfinance" explicitly | 🟡 |
| F17 | `ibkr_seed_paper.py` has no kill-switch once `--execute` is sent | `ibkr_seed_paper.py` | Submitting 14 orders in series with no abort. Worth adding ctrl-c handler + partial-rollback report | 🔴 |
| F18 | Ticker → contract mapping is a flat heuristic (`.AX` → ASX, else SMART/USD). No primaryExchange override per ticker | `ibkr_seed_paper.py:_ticker_to_contract` + engine | EU tickers (IEU.AX is mapped to ASX — fine, but VEA, VEU could route oddly via SMART) | 🟡 |
| F19 | No FX conversion for "available cash" check before submitting US-currency orders | `ibkr_seed_paper.py` | If you have $1M AUD but try to buy $5M USD worth, IBKR margins it (probably fails). No pre-flight check | 🟡 |

---

## 3. LOOSE ENDS (finish before live)

### 3a — IBKR rollout

| # | Item | State | Pri |
|---|---|---|---|
| L1 | Phase 1.5 — seed paper account | Script ready, dry-run not executed, `--execute` not run | 🔴 |
| L2 | Phase 2 — dry-run trade preview through IBKR | `ibkr_dry_run.py` shipped (commit `efdfd99`) | 🟢 |
| L3 | Phase 3 — paper execution + fill tracking | `ibkr_paper_exec.py` shipped 2026-06-22: typed-YES gate, fill log to `ibkr_fills_log.jsonl`, reconciliation summary. Fill capture into Excel `Actual_Fills` sheet still pending. | 🟡 |
| L4 | `LIVE_TRADING_START_DATE = None` — drift v3 monthly NAV comparison is dormant | Config flip pending | 🟡 (flip on first real fill) |

### 3b — Coverage + observability

| # | Item | State | Pri |
|---|---|---|---|
| L5 | **Zero automated tests** — smoke tests we wrote got deleted post-verification | None | 🔴 (need a minimal `test_drift.py`, `test_cgt.py` at least) |
| L6 | No assertions on `mu`, `Sigma`, returns shapes before frontier solve | None | 🟡 (one bad column blows the solver, hard to debug) |
| L7 | `Cash_Ledger` Unexplained Δ never gets explained in PPT — just sits in the workbook | Excel only | 🟢 (add to roadshow when live) |
| L8 | No CI: pre-commit, syntax check, anything | None | 🟢 |

### 3c — Documentation (the systems around the code)

| # | Item | State | Pri |
|---|---|---|---|
| L9 | **README.md** | Doesn't exist | 🔴 |
| L10 | **ARCHITECTURE.md** — data flow, the 6 main components | Doesn't exist | 🔴 |
| L11 | **RUNBOOK.md** — daily procedure, "what to do when X" | Doesn't exist | 🟡 |
| L12 | **CHANGELOG.md** — what shipped in each commit | Doesn't exist (memory holds it but isn't shareable) | 🟢 |
| L13 | **CONFIG.md** — every knob, its default, when to change it | Doesn't exist | 🟡 |
| L14 | Inline docstrings on the 12-15 critical functions (FF5 setup, ensemble walk-forward, CGT model) | Some exist, many minimal | 🟡 |

### 3d — Log debuggability gaps

| # | Item | State | Pri |
|---|---|---|---|
| L15 | Build stamp at top of run.log (git SHA + build time) | Missing | 🔴 |
| L16 | Config snapshot block at top (broker, MTR, slot config, rebal freq, weight caps) | Missing | 🔴 |
| L17 | Per-phase timings (data, FF5, frontier, OOS, dialog, trade plan, Excel write, PPT) | Only OOS timed | 🟡 |
| L18 | Stack traces on swallowed exceptions | Missing | 🔴 (paired with F3) |
| L19 | Input data signature: prices.shape, first/last date, FX last value, sheet hash | Partial — `prices` shape printed but not signature | 🟡 |
| L20 | Δ vs prior run: which tickers changed weight, which rebalances triggered why | Missing | 🟡 |
| L21 | Per-ticker FF5 R² (just the universe list right now) | Missing | 🟢 |

---

## 4. Pre-live "ducks in a row" checklist

The minimum set to feel comfortable hitting `--execute` for the first time on paper:

### Must-do (🔴)

- [ ] **F1/F3/L18** — replace silent `except: pass` with `print(traceback.format_exc())` in at least the 10 noisy ones. Even before refactor, just SURFACE the failures.
- [ ] **F7/L15** — bake git SHA + build timestamp into the .exe (build_helper writes to a `VERSION` constant; engine logs it on startup)
- [ ] **L16** — config snapshot block at top of every run.log
- [ ] **F8** — preserve N run.log files (rotation), not overwrite. 7-day history minimum.
- [ ] **F11** — timeout on all yfinance/IBKR network calls
- [ ] **F14** — replace silent FX fallback with explicit log + abort
- [ ] **F17** — ctrl-c handler in ibkr_seed_paper.py
- [ ] **L1** — execute Phase 1.5 (seed paper account). Verify positions match engine.
- [ ] **L2/L3** — Phase 2 + 3 implemented: dry-run preview, then paper execution path
- [ ] **L5** — minimal test suite (`pytest test_drift.py test_cgt.py`)
- [ ] **L9/L10** — README + ARCHITECTURE docs exist and are accurate

### Should-do (🟡) — first month of paper

- [ ] R5 — unstage `portfolio_state.json` from git
- [ ] R2 — delete the orphan notebook
- [ ] F4 — start migrating `globals()[...]` to explicit return values (slow refactor, do gradually)
- [ ] F12 — yfinance retry on transient failure (2 retries, 1s back-off)
- [ ] F13 — IBKR connection retry
- [ ] L4 — flip `LIVE_TRADING_START_DATE` on first real fill
- [ ] L11/L13 — RUNBOOK + CONFIG docs
- [ ] L17 — per-phase timings in run.log

### Nice-to-have (🟢) — post-stabilisation

- [ ] R1 — delete CMC broker profile
- [ ] R6/R7 — strip remaining legacy paths
- [ ] L7 — surface Unexplained Δ on PPT roadshow slide
- [ ] L21 — per-ticker FF5 R² in log

---

## 5. Recommended sequence

1. **Today/this session**: 🔴 items F1+F3+L18 (surface swallowed errors), L15+L16 (build stamp + config snapshot), F8 (log rotation), F14 (FX abort). All small. Maybe ~2 hours.
2. **Next session**: 🔴 docs (L9, L10). Then 🔴 tests (L5).
3. **Then**: L1 → L2 → L3 (IBKR Phase 1.5 → 2 → 3 in order).
4. **Only then**: consider flipping to live with a $10k-$50k partial allocation, not the full $1M.

---

## 6. Closing meta-observation

Codebase is in good shape for what it is — 9k lines of quant + live trading + reporting in one file is unusual but it works. The biggest risks aren't algorithmic (that's well-tested via the GFC stress test + OOS walk-forward) — they're **operational**: silent failures, no audit trail, no recovery procedure if a live trade goes wrong.

The audit above prioritises closing operational gaps over code beautification. The engine's logic survived 2008; the question is whether the surrounding system survives a Tuesday-morning yfinance outage during a rebalance window.

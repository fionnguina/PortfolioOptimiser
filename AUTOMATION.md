# AUTOMATION — semi-autonomous daily runs

This describes the **semi-autonomous** mode: the engine runs on schedule
without you, decides whether anything actually needs to happen, and pings
you with a notification. The Phase 3 paper-exec step **stays manual** —
you eyeball the preview, type YES, then orders submit. That's deliberate;
see the bottom of this file for why.

For the full validation discipline + cadence, see [LOCKBOX.md](LOCKBOX.md).

---

## The two new pieces

1. **`--auto-pipeline` engine flag** — non-interactive run. Bypasses the
   trade-plan dialog (forces `TRADE_PLAN_MODE="ensemble"`) and writes a
   `[rebal-trigger]` verdict line to `dist/run.log`. Without this flag the
   engine still opens the dialog as before, so double-clicking the exe
   for an ad-hoc run still works.
2. **`daily_auto.ps1`** — PowerShell wrapper that runs the engine,
   parses the verdict, fires a Windows toast notification, and (when
   verdict=RUN) opens `dist/Reports/Portfolio_Report.pptx` so the
   review is one click away.

---

## How it works

```
   ┌──────────────────────────────────────────────────────────────┐
   │ Windows Task Scheduler — 09:30 AEST weekdays                 │
   └──────────────────────────────────────────────────────────────┘
                            │
                            ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ daily_auto.ps1                                               │
   │   1. Runs dist/Portfolio Optimiser.exe --auto-pipeline       │
   │   2. Parses dist/run.log for [rebal-trigger] verdict         │
   │   3. Sends Windows toast notification                        │
   │   4. On RUN: opens dist/Reports/Portfolio_Report.pptx        │
   └──────────────────────────────────────────────────────────────┘
                            │
                            ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ You see a toast on your taskbar:                             │
   │                                                              │
   │   "Portfolio Optimiser — no action"   (most days)           │
   │   "Portfolio Optimiser — REBALANCE READY"  (~9×/year)       │
   └──────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┴───────────────────┐
        ▼                                       ▼
   "no action"                          "REBALANCE READY"
   you do nothing                       you:
                                          1. Open the PPT (it auto-opened)
                                          2. Eyeball Slide 2 (Fund Perf)
                                          3. Eyeball Slide 3 (Trade Plan)
                                          4. Start TWS paper if not running
                                          5. & ".\.venv\Scripts\python.exe" `
                                                ibkr_paper_exec.py --execute
                                          6. Type YES
                                          7. Wait for fills
                                          8. & ibkr_paper_exec.py --check-fills
                                                to verify broker truth
```

---

## How the verdict gets decided

After every engine run the engine writes one line to `dist/run.log`:

```
[rebal-trigger] summed_|Δw|=0.0247  threshold=0.0300  verdict=SKIP  mode=ensemble  portfolio_aud=1,000,123
```

- `summed_|Δw|` = sum of absolute weight changes from current portfolio
  to recommended target.
- `threshold` = `SKIP_REBAL_DELTA` from [Portfolio_Optimiser.py](Portfolio_Optimiser.py)
  (default 3%).
- `verdict` = `SKIP` if delta below threshold (nothing material has changed),
  `RUN` if above (the plan is materially different from your current book).
- `mode` = which plan was generated (`ensemble` under `--auto-pipeline`).
- `portfolio_aud` = current portfolio value at the run.

The wrapper parses this line and decides what kind of toast to show.

---

## Setup — one-time, ~5 minutes

### 1. Confirm the engine has the new build

The `--auto-pipeline` flag was added in the build dated 2026-06-22.

```powershell
& "$PSScriptRoot\dist\Portfolio Optimiser.exe" --auto-pipeline 2>$null
Get-Content "$PSScriptRoot\dist\run.log" | Select-String "rebal-trigger"
```

You should see a single `[rebal-trigger] ...` line. If you get a usage error or
no line, rebuild via `& ".\.venv\Scripts\python.exe" build_helper.py`.

### 2. Test the wrapper manually

```powershell
& ".\daily_auto.ps1"
```

You should:
- See engine output streaming
- Get a Windows toast notification at the end
- See a new `dist\daily_auto.log` row

If the toast doesn't appear, the wrapper falls back to a message box
(check for a `[Show-Toast]` line in `daily_auto.log`). The fallback is fine
for unattended runs; the message box is purely visual.

### 3. Schedule daily via Task Scheduler

Open PowerShell **as admin** (required to register the task), then:

```powershell
schtasks /Create /SC WEEKLY /D MON,TUE,WED,THU,FRI `
  /TN "Portfolio Optimiser Daily" `
  /TR "powershell -ExecutionPolicy Bypass -File `"C:\Users\Fionn Guina\Portfolio_Optimiser\daily_auto.ps1`"" `
  /ST 09:30
```

Verify:

```powershell
schtasks /Query /TN "Portfolio Optimiser Daily"
```

Remove (if you ever want to):

```powershell
schtasks /Delete /TN "Portfolio Optimiser Daily" /F
```

The 09:30 time is local Windows time. ASX opens at 10:00 AEST so 09:30 gives
the engine 30 minutes of pre-market price flow + IBKR live-price snapshot.

---

## What gets persisted

| Artifact | Refreshed every run | Refreshed on RUN only |
|---|---|---|
| `dist/run.log` | yes | yes |
| `dist/daily_auto.log` | yes (one row per scheduler invocation) | yes |
| `dist/Reports/Portfolio_Report.pptx` | yes | yes |
| `Stock Analysis.xlsm` workbook | yes | yes |
| `trade_recommendation_log.jsonl` | yes (the verdict is the line) | yes (with full trade list) |
| `metrics_history.jsonl` | yes | yes |
| `tlh_cooldown_state.json` | yes (no-op if no swaps) | yes (when swaps fire) |

---

## Failure modes + recovery

| Symptom | Cause | Fix |
|---|---|---|
| No toast at all | PS1 didn't run, or Windows toast API blocked | Check `dist\daily_auto.log`. Try `& ".\daily_auto.ps1"` manually. |
| Toast says "review log" | Engine ran but no `[rebal-trigger]` line | Engine crashed during the live pipeline. Read `dist\run.log` end. |
| Toast says "ENGINE ERROR" | Engine binary failed to launch | Path wrong, or `dist\Portfolio Optimiser.exe` deleted. Rebuild via build_helper. |
| Engine runs but TWS not connected | TWS / IB Gateway not running | Engine falls back to yfinance prices silently. Trade plan still emitted. For Phase 3 exec you'll need to start TWS manually before running `ibkr_paper_exec.py --execute`. |
| Verdict always RUN | Threshold too low, OR engine drift big | Check `SKIP_REBAL_DELTA` in CONFIG.md. Default 3% — should be SKIP most days. |
| Verdict always SKIP | Threshold too high, OR engine never updating | Sanity check by reading `trade_recommendation_log.jsonl` — should append a line per run. |

---

## Why Phase 3 stays manual

I deliberately did NOT wire `daily_auto.ps1` to auto-execute via
`ibkr_paper_exec.py --execute --auto-confirm`. Reasons:

1. **We literally fixed a fills_log state-capture bug today** — the
   script has been in active development for ~24 hours. That's not
   "battle-tested" enough for unattended execution.
2. **Complex/leveraged ETP permissions** — first paper run, 3 orders
   got permission-rejected. Auto-exec wouldn't have caught it; the
   3 lots would just have silently failed.
3. **US overnight orders** — 3 US tickers stayed PreSubmitted overnight.
   Auto-exec without monitoring would have left them open without
   anyone watching.
4. **AFSL not yet issued.** Unattended trading without an AFSL is
   regulatory ambiguity — even on paper. Better to keep a human in
   the loop until the licence is real.
5. **Engine ceiling is Sharpe ~1.0.** The marginal alpha from
   "execute 2 minutes faster" is well below the marginal risk from
   "wrong order silently submitted at 4 AM."

After ~3 months of clean operation with the manual loop, and once
AFSL has issued, we can revisit. By then we'll have weeks of
fills_log data, a robust slippage report, and enough live evidence
to know what to monitor for.

The plan: **wire auto-execute as a one-line config change** when the
time is right. The `--auto-confirm` flag scaffolding is already half
there in the YES gate — adding it later is a 30-minute job, not a
re-architecture.

---

## Operational discipline

Even with the scheduler running:

- **Open `dist\daily_auto.log` once a week** to confirm no silent
  failures. The log has one row per invocation; scan for runs that
  exited without a verdict.
- **Run `--show-metrics-history` monthly** to look for regression
  drift across schedule-driven runs.
- **Re-read [LOCKBOX.md](LOCKBOX.md) quarterly** to confirm the
  validation cadence still matches reality.

This semi-autonomous setup is meant to **reduce the friction of
running the engine daily**, not to remove your eyes from it. The
toast IS the design — it makes you aware without making you act.

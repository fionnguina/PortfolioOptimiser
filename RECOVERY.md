# RECOVERY — rebuild the fund on a fresh machine

Disaster-recovery + portability runbook. Follow top to bottom to get the
Portfolio Optimiser running on a new **Windows** machine.

## What is / isn't portable

- **Portable (in the git repo):** all source code (`.py`), the workbook
  (`Stock Analysis.xlsm` = ticker universe + holdings), `portfolio_state.json`,
  `lots_seed.json`, `tlh_pairs.json`. The Python is path-clean (resolves via
  `APP_DIR`); no user paths hardcoded in source.
- **NOT portable — rebuilt per machine (this doc):** the Python venv, the
  PyInstaller exe, IB Gateway + IBC + their credentials, the mail config, the
  scheduled tasks, and the local-only evidence files (restored from OneDrive).
- **Windows-only.** Excel COM (xlwings/win32com), the `.exe`, PowerShell
  wrappers, and Task Scheduler do **not** run on Mac/Linux. A new machine must
  be Windows.

## Prerequisites

- Windows 10/11, Python 3.12 (the venv targets 3.12).
- An IBKR **paper** account + credentials.
- Access to the OneDrive `GuinaFund_Backup` folder (evidence restore).
- The GuinaCapital bot Gmail app password (email alerts) — kept by you, not
  in any repo or cloud backup.

## Steps

### 1. Clone + Python environment
```
git clone <repo-url> Portfolio_Optimiser
cd Portfolio_Optimiser
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```
`requirements.txt` is a full `pip freeze` (87 pinned packages) — reproduces
the exact environment.

### 2. Build the exe
```
.\.venv\Scripts\python.exe build_helper.py
```
Produces `dist\Portfolio Optimiser.exe`. Confirm the `[build]` stamp.

### 3. Restore the evidence / track record
Copy from OneDrive `GuinaFund_Backup\` back into the repo root:
`metrics_history.jsonl`, `ibkr_nav_log.jsonl`, `ibkr_fills_log.jsonl`,
`trade_recommendation_log.jsonl`, `lots_seed.json`, `portfolio_state.json`,
`tlh_cooldown_state.json`, `sanity_alerts.jsonl`, `Stock Analysis.xlsm`.
(The workbook + lots_seed + portfolio_state also come from git, but the
OneDrive copy is the freshest live state — prefer it.)

### 4. IB Gateway + IBC (unattended broker login)
- Install offline **IB Gateway (stable)**:
  https://www.interactivebrokers.com/en/trading/ibgateway-stable.php
- Deploy **IBC** (IbcAlpha) to `C:\IBC`.
- Put paper credentials in `%USERPROFILE%\Documents\IBC\config.ini`
  (IBC's `/Config` default), with `TradingMode=paper`,
  `OverrideTwsApiPort=7497`, `AcceptIncomingConnectionAction=accept`,
  `ExistingSessionDetectedAction=primary`. Lock its ACLs to your user.
- Run `setup_ibc_gateway.ps1` — detects the Gateway version, patches
  `StartGateway.bat` to paper mode.
- **Machine-specific fixups in `C:\IBC\StartGateway.bat`:**
  `TWS_MAJOR_VRSN` (Gateway version), and `JAVA_PATH` pinned to the
  install's bundled JRE `...\<jre>_64\bin` (IBC mis-resolves the folder
  without the `_64` suffix). Also bypass the java-version parse in
  `C:\IBC\StartIBC.bat` if it errors ("set was unexpected") — set
  `java_version=17.0` literal.
- Test: `C:\IBC\StartGateway.bat` then
  `.\.venv\Scripts\python.exe probe_ibkr_api.py` (exit 0 = API alive).

### 5. Email alerts
Create `%USERPROFILE%\.portfolio_optimiser_mail.json`:
```
{ "smtp_host": "smtp.gmail.com", "smtp_port": 587,
  "sender_email": "<bot>@gmail.com",
  "sender_app_password": "<16-char app password>",
  "recipient_email": "fionn.guina@gmail.com" }
```
Lock ACLs. Test: `.\.venv\Scripts\python.exe send_alert.py --test`.

### 6. Scheduled tasks (paths are machine-specific — use the real path)
```
$root = "C:\Users\<USER>\Portfolio_Optimiser"
$fast = New-ScheduledTaskAction -Execute "powershell.exe" `
  -Argument "-ExecutionPolicy Bypass -NonInteractive -File `"$root\daily_auto.ps1`""
Register-ScheduledTask -TaskName "Portfolio Optimiser Daily" -Action $fast `
  -Trigger (New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 9:30AM) -Force
$evi = New-ScheduledTaskAction -Execute "powershell.exe" `
  -Argument "-ExecutionPolicy Bypass -NonInteractive -File `"$root\evidence_run.ps1`""
Register-ScheduledTask -TaskName "Portfolio Optimiser Evidence" -Action $evi `
  -Trigger (New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 6:00PM) -Force
```
(Use `Register-ScheduledTask`, **not** `schtasks` — the latter's `/TR`
quoting breaks on the space in the path.)

### 7. Verify end-to-end
```
.\.venv\Scripts\python.exe "Portfolio_Optimiser.py" --preflight    # 10 checks
.\.venv\Scripts\python.exe probe_ibkr_api.py                        # API alive
.\.venv\Scripts\python.exe send_alert.py --test                    # email lands
```
Then let the 9:30 task fire once and read `daily_auto.log`.

## Backups (ongoing)

`backup_evidence.ps1` (called by `daily_auto.ps1`) mirrors the local-only
evidence files to OneDrive `GuinaFund_Backup\` every morning. OneDrive's
version history is the point-in-time recovery. **Credentials are NOT backed
up to cloud** — the IBC paper login and the mail app password are re-entered
per steps 4-5.

<#
.SYNOPSIS
    Daily semi-autonomous Portfolio Optimiser wrapper.

.DESCRIPTION
    Runs the engine in non-interactive mode (--auto-pipeline), parses the
    [rebal-trigger] verdict from run.log, and surfaces a Windows toast +
    log line summarising the outcome.

    Schedule via Task Scheduler to run weekdays 09:30 AEST (just after ASX
    open). Most days the verdict will be SKIP and you do nothing; ~9 times
    a year it'll be RUN and the toast will prompt you to open the PPT,
    review, and execute via ibkr_paper_exec.py.

    Does NOT auto-execute orders. Phase 3 stays manual until you've built
    enough live-paper confidence (see LOCKBOX.md cadence).

.PARAMETER OpenPptOnRun
    Open dist/Portfolio_Report.pptx automatically when verdict=RUN.
    Default true. Set $false for headless environments.

.PARAMETER LogPath
    Path to dist/run.log to parse for [rebal-trigger]. Defaults to the
    standard location relative to the script.

.EXAMPLE
    & ".\daily_auto.ps1"
    Run with defaults: engine + parse + toast + open PPT on RUN.

.EXAMPLE
    & ".\daily_auto.ps1" -OpenPptOnRun:$false
    Run without opening PPT (useful when testing).

.NOTES
    To schedule (one-time setup):
      schtasks /Create /SC WEEKLY /D MON,TUE,WED,THU,FRI /TN "Portfolio Optimiser Daily" `
        /TR "powershell -ExecutionPolicy Bypass -File C:\Users\Fionn Guina\Portfolio_Optimiser\daily_auto.ps1" `
        /ST 09:30

    Check status:           schtasks /Query /TN "Portfolio Optimiser Daily"
    Remove:                 schtasks /Delete /TN "Portfolio Optimiser Daily" /F
#>

[CmdletBinding()]
param(
    [bool]$OpenPptOnRun = $true,
    [string]$LogPath = $null
)

$ErrorActionPreference = "Continue"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
# The scheduled tasks have no WorkingDirectory set, so the CWD defaults to
# C:\Windows\System32. Anchor to the repo so relative-path writes by child
# scripts (e.g. ibkr_paper_exec.py --snapshot-nav) don't hit PermissionError.
Set-Location -LiteralPath $ScriptDir
$ExePath = Join-Path $ScriptDir "dist\Portfolio Optimiser.exe"
# Repo root, NOT dist\ — the engine's APP_DIR resolves to the repo root even when
# frozen (_DEV_BASE at Portfolio_Optimiser.py:534 always exists on this machine),
# so EXPORT_DIR is <repo>\Reports. dist\Reports has never existed; this path
# silently failed Test-Path, so the deck never auto-opened on a RUN.
$PptPath = Join-Path $ScriptDir "Reports\Portfolio_Report.pptx"
if (-not $LogPath) {
    $LogPath = Join-Path $ScriptDir "dist\run.log"
}
# Repo root, NOT dist\ — build_helper wipes dist on every rebuild, which was
# silently destroying the wrapper's evidence trail (discovered 2026-07-06).
$DailyLogPath = Join-Path $ScriptDir "daily_auto.log"
# TWO sentinel writers disagree on location (discovered 2026-07-17), so we must
# check BOTH or we go blind to one outcome:
#   - clean finish -> Portfolio_Optimiser.py:8878 writes APP_DIR\engine_done.flag.
#     APP_DIR resolves to the REPO ROOT even when frozen, because _DEV_BASE
#     (Portfolio_Optimiser.py:534 = ~\Portfolio_Optimiser) always exists here and
#     short-circuits the frozen branch.
#   - sanity halt -> Main.py:162-165 writes dist\engine_done.flag (frozen-aware).
# This file previously pointed at dist\ ONLY: HALT was detected, but a clean run
# never was — so every good run logged "engine may have crashed", which made a
# real crash indistinguishable from success. Checking only the repo root would
# invert the bug and lose HALT detection, which is worse.
$FlagPaths = @(
    (Join-Path $ScriptDir "engine_done.flag"),
    (Join-Path $ScriptDir "dist\engine_done.flag")
)
# 1200s: the full pipeline with SCALE_SENSITIVITY (multi-scale OOS) exceeds
# 10min, which timed out + killed the 2026-07-09 graduation run mid-backtest.
$EngineTimeoutSec = 1200

# --- IBKR / TWS autostart settings ----------------------------------------
# Port 7497 = paper TWS (7496 is live, never used here).
$IbkrPort = 7497
# Where TWS is installed. Allow override via env var so different machines
# don't need to edit the script. Falls back to the standard Windows
# install location. If autodetection fails, the wrapper just skips
# autostart and the engine falls back to yfinance prices.
$TwsPath = $env:IBKR_TWS_PATH
if (-not $TwsPath) {
    $candidates = @(
        "$env:USERPROFILE\Jts\latest\tws.exe",
        "$env:USERPROFILE\Jts\tws.exe",
        "C:\Jts\tws.exe",
        "C:\Program Files\Trader Workstation\tws.exe"
    )
    foreach ($c in $candidates) {
        if (Test-Path $c) { $TwsPath = $c; break }
    }
}
$TwsReadyTimeoutSec = 60

function Write-Log {
    param([string]$Msg)
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$stamp] $Msg"
    Write-Host $line
    try {
        Add-Content -Path $DailyLogPath -Value $line -Encoding utf8 -ErrorAction Stop
    } catch {
        # Best-effort; don't crash the wrapper on log-write failure.
    }
}

function Show-Toast {
    param(
        [string]$Title,
        [string]$Body,
        [string]$Verdict
    )
    # Native Windows 10/11 toast via the Windows Runtime. No extra modules
    # required. If the WinRT assemblies fail to load (e.g. older Windows or
    # Server SKU), fall back to a quiet message box.
    try {
        [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
        $template = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent(
            [Windows.UI.Notifications.ToastTemplateType]::ToastText02)
        $xml = $template.GetXml()
        $textNodes = $template.GetElementsByTagName("text")
        $textNodes.Item(0).AppendChild($template.CreateTextNode($Title)) | Out-Null
        $textNodes.Item(1).AppendChild($template.CreateTextNode($Body)) | Out-Null
        $toast = [Windows.UI.Notifications.ToastNotification]::new($template)
        $notifier = [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("Portfolio Optimiser")
        $notifier.Show($toast)
        return $true
    } catch {
        Write-Log "Toast API failed ($($_.Exception.Message)); falling back to MessageBox."
        try {
            Add-Type -AssemblyName PresentationFramework
            $icon = if ($Verdict -eq "RUN") { 'Exclamation' } else { 'Information' }
            [System.Windows.MessageBox]::Show($Body, $Title, 'OK', $icon) | Out-Null
            return $true
        } catch {
            Write-Log "MessageBox fallback also failed: $($_.Exception.Message)"
            return $false
        }
    }
}

function Test-IbkrPort {
    param([int]$Port = 7497)
    $tcp = New-Object System.Net.Sockets.TcpClient
    try {
        $tcp.Connect("127.0.0.1", $Port)
        return $true
    } catch {
        return $false
    } finally {
        $tcp.Close()
    }
}

function Test-IbkrApiAlive {
    # "Port open" is NOT "API alive" (2026-07-06: TWS accepted sockets while
    # every data request timed out). probe_ibkr_api.py does a full ib_insync
    # handshake with a 10s timeout — exit 0 = genuinely usable API.
    $probe = Join-Path $ScriptDir "probe_ibkr_api.py"
    $py = Join-Path $ScriptDir ".venv\Scripts\python.exe"
    if (-not (Test-Path $probe) -or -not (Test-Path $py)) { return $false }
    & $py $probe *> $null
    return ($LASTEXITCODE -eq 0)
}

function Start-TwsIfNeeded {
    # Tier 2 automation (2026-07-06): prefer IBC-driven IB Gateway with
    # automated login. Falls back to the legacy bare-TWS launch (login
    # screen and all) until Gateway + credentials are set up — see
    # setup_ibc_gateway.ps1.
    if (Test-IbkrApiAlive) {
        Write-Log "IBKR API alive on 127.0.0.1:$IbkrPort."
        return $true
    }
    if (Test-IbkrPort -Port $IbkrPort) {
        Write-Log "WARN: port $IbkrPort is open but the API is NOT responding (wedged session or modal dialog). Not launching a second instance - engine will fall back to yfinance. Restart TWS/Gateway manually."
        return $false
    }

    $ibcStart = "C:\IBC\StartGateway.bat"
    # IBC reads its config from Documents\IBC\config.ini (its /Config default),
    # where it was moved 2026-07-08. The old C:\IBC path check false-negatived
    # and made the 2026-07-09 graduation run fall back to bare TWS.
    $ibcIni = Join-Path $env:USERPROFILE "Documents\IBC\config.ini"
    $ibcReady = (Test-Path $ibcStart) -and (Test-Path $ibcIni) -and
                -not (Select-String -Path $ibcIni -Pattern "YOUR_PAPER_USERNAME_HERE" -Quiet)
    if ($ibcReady) {
        Write-Log "API down; launching IB Gateway via IBC ($ibcStart)..."
        try {
            Start-Process "cmd.exe" -ArgumentList "/c", "`"$ibcStart`"" -WindowStyle Minimized | Out-Null
        } catch {
            Write-Log "IBC launch failed: $($_.Exception.Message). Engine will fall back to yfinance."
            return $false
        }
        # Gateway boot + IBC auto-login takes ~30-90s; poll the REAL probe.
        $deadline = (Get-Date).AddSeconds(150)
        while ((Get-Date) -lt $deadline) {
            Start-Sleep -Seconds 10
            if (Test-IbkrApiAlive) {
                Write-Log "IB Gateway API alive (IBC auto-login OK)."
                return $true
            }
        }
        Write-Log "IBC/Gateway did not become API-alive within 150s - check C:\IBC logs. Engine will fall back to yfinance."
        return $false
    }

    # Legacy fallback: bare TWS launch (will sit at the login screen when
    # unattended - kept only until IBC + Gateway setup is completed).
    if (-not $TwsPath -or -not (Test-Path $TwsPath)) {
        Write-Log "IBC not configured and tws.exe not found. Engine will fall back to yfinance prices."
        return $false
    }
    Write-Log "IBC not configured; legacy TWS launch of $TwsPath (may sit at login screen)..."
    try {
        Start-Process -FilePath $TwsPath -WindowStyle Minimized | Out-Null
    } catch {
        Write-Log "TWS launch failed: $($_.Exception.Message). Engine will fall back to yfinance."
        return $false
    }
    $deadline = (Get-Date).AddSeconds($TwsReadyTimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (Test-IbkrApiAlive) {
            Write-Log "TWS API alive."
            return $true
        }
        Start-Sleep -Seconds 5
    }
    Write-Log "TWS launch timed out after ${TwsReadyTimeoutSec}s without a live API (probably the login screen). Engine will fall back to yfinance."
    return $false
}

# --- Sanity checks ---
if (-not (Test-Path $ExePath)) {
    Write-Log "FATAL: $ExePath not found. Rebuild via build_helper.py."
    exit 1
}

Write-Log "Starting daily auto run."
Write-Log "  Engine: $ExePath"
Write-Log "  Log:    $LogPath"

# --- Ensure TWS is up before running the engine ---
[void](Start-TwsIfNeeded)

# --- Truncate the engine's run.log so this run's lines are easy to parse ---
# The engine ALSO rotates its log, but we want a clean per-invocation read.
try {
    if (Test-Path $LogPath) {
        Clear-Content -Path $LogPath -ErrorAction Stop
    }
} catch {
    Write-Log "Could not truncate $LogPath ($($_.Exception.Message)); continuing."
}

# --- Run engine with --auto-pipeline ---
# Two-channel completion detection because PowerShell's Start-Process -Wait
# can hang waiting for child processes that PyInstaller --noconsole exes
# spawn (Tk, matplotlib, Excel COM workers) even after the engine PID has
# exited — observed 2026-06-22 and again 2026-06-26 under scheduled-task
# execution. The previous Start-Process -Wait fix proved insufficient on
# its own.
#
# Channel 1 (primary): drop a sentinel file `dist\engine_done.flag` at the
#   very end of the engine. Wrapper polls for that file's mtime exceeding
#   $EngineStart — definitive "engine completed cleanly" signal.
# Channel 2 (fallback): direct .NET Process.WaitForExit(timeoutMs). Bounded
#   so we never block past $EngineTimeoutSec regardless of child-process
#   behavior.
#
# If either fires we proceed to parse run.log. If neither fires within the
# timeout we still proceed — partial-log verdict is more useful than a
# silently-hung wrapper.

# Pre-emptively clear any stale sentinel from a prior run — both writers'
# locations, or a leftover halt flag would masquerade as this run's outcome.
foreach ($fp in $FlagPaths) {
    if (Test-Path $fp) { Remove-Item $fp -Force -ErrorAction SilentlyContinue }
}

$EngineStart = Get-Date
try {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $ExePath
    $psi.Arguments = "--auto-pipeline"
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.WindowStyle = "Hidden"
    # SCALE_SENSITIVITY intentionally NOT set here (2026-07-09): the morning
    # run's job is the fast ~90s trade-trigger verdict + broker NAV snapshot
    # + notify. The multi-scale evidence sweep (~25min) moved to a separate
    # EVENING task (evidence_run.ps1) so it can't delay/timeout-kill the
    # decision or clobber a trade plan mid-execution. Daily-evolving evidence
    # (production 3Y/5Y/10Y metrics, regime mix, TLH, real broker NAV) still
    # accrues on THIS fast run; only the near-static per-NAV scale block is
    # deferred to evening.
    $engineProc = [System.Diagnostics.Process]::Start($psi)
} catch {
    Write-Log "Engine launch threw: $($_.Exception.Message)"
    Show-Toast `
        -Title "Portfolio Optimiser — ENGINE ERROR" `
        -Body "Engine failed to launch. See daily_auto.log." `
        -Verdict "ERROR" | Out-Null
    exit 2
}

Write-Log "Engine launched (PID=$($engineProc.Id)); awaiting sentinel or process exit (timeout ${EngineTimeoutSec}s)."

$pollIntervalMs = 1000
$deadline = $EngineStart.AddSeconds($EngineTimeoutSec)
$engineExit = $null
$sentinelHit = $false
while ((Get-Date) -lt $deadline) {
    # Channel 1: sentinel (either writer's location — see $FlagPaths)
    foreach ($fp in $FlagPaths) {
        if (Test-Path $fp) {
            if ((Get-Item $fp).LastWriteTime -ge $EngineStart) {
                $sentinelHit = $true
                break
            }
        }
    }
    if ($sentinelHit) { break }
    # Channel 2: process exit
    if ($engineProc.HasExited) {
        $engineExit = $engineProc.ExitCode
        break
    }
    Start-Sleep -Milliseconds $pollIntervalMs
}

# After breaking out, give the process up to 5 more seconds to settle so
# we get its exit code if it just finished (sentinel fires before exit).
if (-not $engineProc.HasExited) {
    [void]$engineProc.WaitForExit(5000)
}
if ($engineProc.HasExited) {
    $engineExit = $engineProc.ExitCode
}

$EngineDuration = (Get-Date) - $EngineStart
if ($sentinelHit -and $engineExit -ne $null) {
    Write-Log "Engine exited (code=$engineExit, sentinel hit) after $($EngineDuration.TotalSeconds.ToString('F1'))s."
} elseif ($sentinelHit) {
    Write-Log "Engine sentinel hit after $($EngineDuration.TotalSeconds.ToString('F1'))s (process still wrapping up child handles)."
} elseif ($engineExit -ne $null) {
    Write-Log "Engine exited (code=$engineExit) after $($EngineDuration.TotalSeconds.ToString('F1'))s (no sentinel — engine may have crashed before completing health summary)."
} else {
    # Kill the whole process tree (/T catches multiprocessing workers, which
    # under a frozen exe are additional "Portfolio Optimiser.exe" processes).
    # Leaving them alive is the orphan factory that locked dist/ against
    # rebuilds on 2026-07-03 and 2026-07-06.
    Write-Log "Engine TIMEOUT after $($EngineDuration.TotalSeconds.ToString('F1'))s — killing engine process tree (PID=$($engineProc.Id)) and proceeding with partial log."
    try {
        & taskkill /PID $engineProc.Id /T /F 2>$null | Out-Null
        Write-Log "Engine process tree killed."
    } catch {
        Write-Log "taskkill failed: $($_.Exception.Message) — orphans may hold the dist lock (build_helper now clears them pre-build)."
    }
}

# --- Check engine_done.flag for status (2026-06-28) ---
# Main.py writes a status-aware sentinel. If the engine halted due to
# a sanity violation, the flag content has status=halted_by_sanity_violation
# and we surface that prominently instead of treating "UNKNOWN verdict"
# as just a log-parse failure.
$engineHalted = $false
$haltReason = ""
# Pick the freshest sentinel from THIS run across both writer locations. The
# mtime guard matters: without it a stale halt flag from an earlier run would
# pin every subsequent run to HALTED forever.
$FreshFlag = $null
foreach ($fp in $FlagPaths) {
    if (Test-Path $fp) {
        $mt = (Get-Item $fp).LastWriteTime
        if ($mt -ge $EngineStart) {
            if ((-not $FreshFlag) -or ($mt -gt (Get-Item $FreshFlag).LastWriteTime)) {
                $FreshFlag = $fp
            }
        }
    }
}
if ($FreshFlag) {
    try {
        $flagJson = Get-Content -Path $FreshFlag -Raw -ErrorAction Stop | ConvertFrom-Json
        if ($flagJson.status -eq "halted_by_sanity_violation") {
            $engineHalted = $true
            $haltReason = $flagJson.reason
            Write-Log "Engine reported HALTED_BY_SANITY_VIOLATION: $haltReason"
        }
    } catch {
        # Flag is non-JSON (older version) or partial — ignore, fall back
        # to verdict parsing below.
    }
}

# --- Post-run fault scan (2026-07-17) ---
# The engine's own health summary CANNOT see these: `Windows fatal exception:`
# lines are emitted by faulthandler during interpreter/COM teardown, i.e. AFTER
# the health block has already printed. On 2026-07-17 a clean-looking run
# reported "Errors in log: 0" while the log ended with 26 of them (0x800706ba =
# RPC server unavailable, the Excel COM teardown that also strands an orphaned
# EXCEL.EXE). The wrapper reads the log after the process has exited, so this is
# the only place that sees the whole file. Informational, NOT a verdict change:
# the teardown faults are currently benign and turning them into a HALT would
# cry wolf. We report the count so a NEW class of fault can't hide behind them.
# MUST read the TIMESTAMPED log, not run.log. faulthandler needs a real file
# descriptor and the tee wrapper has none, so Main.py:116-118 points it at
# run_<ts>.log directly — run.log is only the tee copy and can therefore NEVER
# contain a fatal exception or a hard-crash traceback. Verified 2026-07-17:
# run_..._15-35-23.log = 451 lines / 26 faults; run.log = 347 lines / 0.
# So for CRASHES the timestamped log is authoritative, even though run.log is
# what this wrapper parses the verdict from (the verdict precedes teardown, so
# that stays fine).
$faultCount = 0
$errCount = 0
$scanPath = $null
try {
    $newest = Get-ChildItem -Path (Join-Path $ScriptDir "dist") -Filter "run_*.log" -ErrorAction Stop |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    # Staleness guard (mirrors the sentinel poll's -ge $EngineStart check): if the
    # engine crashed before _setup_logging() created THIS run's timestamped log,
    # the newest file is a PRIOR run's, and attributing its faults/errors to the
    # current run's alert would be wrong. Require the log to be from this run.
    if ($newest -and $newest.LastWriteTime -ge $EngineStart) { $scanPath = $newest.FullName }
} catch { }
if (-not $scanPath -and (Test-Path $LogPath)) { $scanPath = $LogPath }
if ($scanPath) {
    try {
        $logLines = Get-Content -Path $scanPath -ErrorAction Stop
        $faultCount = @($logLines | Where-Object { $_ -match '(?i)fatal exception' }).Count
        $errCount = @($logLines | Where-Object { $_ -match '\[ERROR' -or $_ -match 'Traceback' }).Count
        if ($faultCount -gt 0 -or $errCount -gt 0) {
            Write-Log "Post-run log scan ($([System.IO.Path]::GetFileName($scanPath))): $errCount error line(s), $faultCount fatal-exception line(s). Faults are emitted at COM teardown, AFTER the engine's health summary — the engine cannot count them, and they never reach run.log."
        }
    } catch {
        Write-Log "Post-run log scan failed: $($_.Exception.Message)"
    }
}

# --- Parse [rebal-trigger] verdict from run.log ---
$verdict = "UNKNOWN"
$summedDw = "?"
$portfolioAud = "?"
$mode = "?"
if ($engineHalted) {
    $verdict = "HALTED"
} elseif (Test-Path $LogPath) {
    $verdictLine = Select-String -Path $LogPath -Pattern "\[rebal-trigger\]" -ErrorAction SilentlyContinue |
        Select-Object -Last 1
    if ($verdictLine) {
        $line = $verdictLine.Line
        if ($line -match "verdict=(\w+)")          { $verdict = $Matches[1] }
        if ($line -match "summed_\|.w\|=([\d\.]+)")  { $summedDw = $Matches[1] }
        if ($line -match "portfolio_aud=([\d,]+)") { $portfolioAud = $Matches[1] }
        if ($line -match "mode=(\w+)")              { $mode = $Matches[1] }
    } else {
        Write-Log "No [rebal-trigger] line found in $LogPath."
    }
}
Write-Log "Verdict: $verdict (summed_|dw|=$summedDw, portfolio=$portfolioAud AUD, mode=$mode)"

# --- Run paper simulator forward-walk (Phase 2d, 2026-06-28) ---
# After the engine completes, replay the entire post-lockbox rec_log
# through the simulator. This provides:
#   1. A daily "what would have happened" trace
#   2. Sanity-layer second opinion on every rec_log entry
#   3. Visual chart for forensic review
#   4. Toast warning if sanity layer rejected >0 batches
# Failure here is non-fatal — the engine's verdict is what matters
# for the daily ops decision; simulator is diagnostic.
$simRejected = "?"
$simFills = "?"
$simChartPath = $null
if ($engineHalted) {
    Write-Log "Simulator: skipped (engine halted by sanity layer — no new rec_log entry to replay)."
} else {
try {
    $venvPython = Join-Path $ScriptDir ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        $simExe = $venvPython
    } else {
        $simExe = "python"
    }
    $simScript = Join-Path $ScriptDir "paper_simulator.py"
    if (Test-Path $simScript) {
        # Window: from lockbox date (2026-06-30) to today.
        # If lockbox is in the future, use today minus 7 days as fallback.
        $simEnd = (Get-Date).ToString("yyyy-MM-dd")
        $simStart = "2026-06-30"
        if ((Get-Date $simStart) -gt (Get-Date)) {
            $simStart = (Get-Date).AddDays(-7).ToString("yyyy-MM-dd")
        }
        Write-Log "Simulator: replaying rec_log from $simStart to $simEnd."
        $simOut = & $simExe $simScript `
            --from $simStart --to $simEnd --reset --chart 2>&1
        # Parse the simulator's terminal output for the summary line:
        # "[sim:default] done — N fills, M batches rejected, ..."
        $simOut | Where-Object { $_ -match "batches rejected" } | ForEach-Object {
            if ($_ -match "(\d+) fills, (\d+) batches rejected") {
                $simFills = $Matches[1]
                $simRejected = $Matches[2]
            }
        }
        $candidateChart = Join-Path $ScriptDir "simulator_nav_chart.png"
        if (Test-Path $candidateChart) {
            $simChartPath = $candidateChart
        }
        Write-Log "Simulator: $simFills fills, $simRejected batches rejected."
        if ($simRejected -ne "?" -and [int]$simRejected -gt 0) {
            Write-Log "Simulator FLAG: $simRejected batches were rejected by sanity layer."
        }
    } else {
        Write-Log "Simulator: paper_simulator.py not found at $simScript."
    }
} catch {
    Write-Log "Simulator step threw: $($_.Exception.Message)"
}
}  # end if -not $engineHalted

# --- Broker-truth NAV snapshot (2026-07-08, user directive) ---
# Read-only: appends NetLiquidation/cash/marks to ibkr_nav_log.jsonl so the
# fund's performance record is the BROKER's number, not a yfinance
# reconstruction. Never builds a plan or touches orders — the engine's
# [rebal-trigger] flags remain the only rebalance driver.
try {
    $navPy = Join-Path $ScriptDir ".venv\Scripts\python.exe"
    $navScript = Join-Path $ScriptDir "ibkr_paper_exec.py"
    if ((Test-Path $navPy) -and (Test-Path $navScript)) {
        $navOut = & $navPy $navScript --snapshot-nav 2>&1 | Select-Object -Last 1
        Write-Log "NAV snapshot: $navOut"
    }
} catch {
    Write-Log "NAV snapshot failed (non-fatal): $($_.Exception.Message)"
}

# --- Notification + optional PPT open ---
# Build a simulator suffix that appears on every toast — if the
# simulator's sanity layer rejected anything, the user needs to see
# that before opening the PPT or executing trades.
$simSuffix = ""
if ($simRejected -ne "?" -and [int]$simRejected -gt 0) {
    $simSuffix = "  ⚠ Simulator rejected $simRejected batches — review simulator_sanity.jsonl BEFORE executing."
} elseif ($simFills -ne "?") {
    $simSuffix = "  ✓ Simulator: $simFills fills, 0 rejected."
}

switch ($verdict) {
    "RUN" {
        $body = "Rebalance ready. summed|Δw|=$summedDw (>= $('{0:F2}' -f (0.03))). Portfolio $portfolioAud AUD. Open PPT to review, then run ibkr_paper_exec.py --execute.$simSuffix"
        Show-Toast -Title "Portfolio Optimiser — REBALANCE READY" -Body $body -Verdict "RUN" | Out-Null
        if ($OpenPptOnRun) {
            # Open the deck THIS run wrote, not the fixed name. When PowerPoint
            # holds Portfolio_Report.pptx open, ppt_export.py saves to a
            # timestamped sibling instead (WinError 5 fallback) and logs
            # "Deck saved instead to: <path>". Blindly opening the fixed name
            # then pops the STALE previous deck on a RUN — the user reviews last
            # run's weights believing they're today's. Resolve the real path
            # from the engine's own log line; fall back to the fixed name.
            $pptToOpen = $null
            if ($scanPath -and (Test-Path $scanPath)) {
                $savedLine = Select-String -Path $scanPath -Pattern 'saved (?:instead )?to:\s*(.+\.pptx)\s*$' -ErrorAction SilentlyContinue | Select-Object -Last 1
                if ($savedLine) { $pptToOpen = $savedLine.Matches[0].Groups[1].Value.Trim() }
            }
            if ((-not $pptToOpen) -or (-not (Test-Path $pptToOpen))) { $pptToOpen = $PptPath }
            if (Test-Path $pptToOpen) {
                Start-Process $pptToOpen
                Write-Log "Opened $pptToOpen."
            } else {
                Write-Log "PPT to open not found ($pptToOpen) — skipping."
            }
        }
    }
    "SKIP" {
        $body = "No action needed. summed|Δw|=$summedDw (< 0.03 threshold). Portfolio $portfolioAud AUD.$simSuffix"
        Show-Toast -Title "Portfolio Optimiser — no action" -Body $body -Verdict "SKIP" | Out-Null
    }
    "HALTED" {
        # Sanity layer fired — engine refused to ship a trade plan. The
        # user MUST triage state before re-running. Likely cause: stale
        # Holdings (run triage_reset_*.py), lot-book corruption, or a
        # genuine engine bug. DO NOT execute any pending trade plan.
        $body = "ENGINE HALTED BY SANITY LAYER. Reason: $haltReason. See sanity_alerts.jsonl. DO NOT execute trades — investigate state before re-running."
        Show-Toast -Title "Portfolio Optimiser — ⚠ HALTED" -Body $body -Verdict "ERROR" | Out-Null
    }
    default {
        $body = "Engine ran but verdict is $verdict. See dist\daily_auto.log + dist\run.log.$simSuffix"
        Show-Toast -Title "Portfolio Optimiser — review log" -Body $body -Verdict "UNKNOWN" | Out-Null
    }
}

# --- Email exception alert (2026-07-08, user directive) ---
# Reaches the phone when the user is away from the desk (a toast can't).
# Exception-only: RUN (action needed) and HALTED (must investigate). SKIP
# stays silent by design — a daily email on the ~9-in-10 no-op case would
# train the recipient to ignore the channel. Non-fatal; silently no-ops
# if the mailer isn't configured (see send_alert.py). Uses a dedicated
# throwaway bot sender so the user's personal Gmail password stays off
# this machine.
$mailSubject = $null
$mailBody = $null
switch ($verdict) {
    "RUN" {
        $mailSubject = "[Portfolio Optimiser] RUN - rebalance ready"
        $mailBody = "Verdict: RUN`nsummed|dw| = $summedDw  (>= 0.03 threshold)`nPortfolio: $portfolioAud AUD`nMode: $mode`n`nReview the PPT, then execute via ibkr_paper_exec.py.$simSuffix"
    }
    "HALTED" {
        $mailSubject = "[Portfolio Optimiser] HALTED - sanity violation"
        $mailBody = "ENGINE HALTED BY SANITY LAYER.`nReason: $haltReason`n`nDO NOT execute any pending trade plan. Investigate state (sanity_alerts.jsonl) before re-running."
    }
    "UNKNOWN" {
        # No [rebal-trigger] line reached the log = the engine died before it
        # could form a verdict (e.g. 2026-07-13, exit=1 after 5.1s). Previously
        # this fired a desk toast ONLY, so an unattended crash was silent. SKIP
        # is still deliberately excluded from mail — it is the ~9-in-10 no-op.
        $mailSubject = "[Portfolio Optimiser] NO VERDICT - engine may have failed"
        $mailBody = "The engine produced no [rebal-trigger] verdict, so it likely crashed before completing.`nExit code: $engineExit`nSentinel seen: $sentinelHit`nRuntime: $($EngineDuration.TotalSeconds.ToString('F1'))s`n`nNo trade plan should be considered current. Check dist\run.log + daily_auto.log.$simSuffix"
    }
}
if ($mailSubject) {
    # Fold in real error lines. Fatal-exception (faulthandler) lines are
    # DELIBERATELY not mentioned unless there are errors too: ~25 benign COM
    # teardown faults fire on every run, so reporting them every time would
    # train the reader to ignore the channel — the same failure the whole
    # 2026-07-17 audit was about. They ARE in daily_auto.log for triage.
    if ($errCount -gt 0) {
        $mailBody = "$mailBody`n`nERRORS: $errCount error line(s) in run.log"
        if ($faultCount -gt 0) { $mailBody = "$mailBody (+$faultCount fatal-exception line(s))" }
        $mailBody = "$mailBody — check dist\run.log."
    }
    # Fold in the engine-vs-broker mark drift warning if this run emitted one.
    try {
        if (Test-Path $LogPath) {
            $driftLine = Select-String -Path $LogPath -Pattern "\[drift\]\[WARN\].*broker NetLiq" -ErrorAction SilentlyContinue | Select-Object -Last 1
            if ($driftLine) { $mailBody = "$mailBody`n`nDRIFT: $($driftLine.Line.Trim())" }
        }
    } catch { }
    try {
        $bodyFile = Join-Path $env:TEMP "po_alert_body.txt"
        Set-Content -Path $bodyFile -Value $mailBody -Encoding utf8
        $mailPy = Join-Path $ScriptDir ".venv\Scripts\python.exe"
        $mailScript = Join-Path $ScriptDir "send_alert.py"
        if ((Test-Path $mailPy) -and (Test-Path $mailScript)) {
            $mailOut = & $mailPy $mailScript --subject $mailSubject --body-file $bodyFile 2>&1 | Select-Object -Last 1
            $mailExit = $LASTEXITCODE
            # send_alert.py returns 1 on SMTP failure (0 if unconfigured). This
            # is the ONLY channel that reaches the user away from the desk, so a
            # silent failure on the one day a RUN/HALTED verdict fires is the
            # worst case. Surface a loud toast so a dead mailer can't hide in
            # daily_auto.log (which nobody watches proactively).
            if ($mailExit -ne 0) {
                Write-Log "Email alert ($verdict) FAILED (exit=$mailExit): $mailOut"
                Show-Toast -Title "Portfolio Optimiser — EMAIL FAILED" -Body "The $verdict alert email did NOT send (exit=$mailExit). Check dist\daily_auto.log; verdict still in run.log." -Verdict "ERROR" | Out-Null
            } else {
                Write-Log "Email alert ($verdict): $mailOut"
            }
        }
    } catch {
        Write-Log "Email alert failed (non-fatal): $($_.Exception.Message)"
    }
}

# --- Evidence backup to OneDrive (2026-07-09) ---
# The real-money track record + lot book live in gitignored local-only
# files; mirror them to OneDrive each run so a disk failure can't erase the
# fund's most valuable asset. Non-fatal.
try {
    $backupScript = Join-Path $ScriptDir "backup_evidence.ps1"
    if (Test-Path $backupScript) {
        $bkOut = & powershell -ExecutionPolicy Bypass -NonInteractive -File $backupScript 2>&1 | Select-Object -Last 1
        Write-Log "Evidence backup: $bkOut"
    }
} catch {
    Write-Log "Evidence backup failed (non-fatal): $($_.Exception.Message)"
}

Write-Log "Done."
exit 0

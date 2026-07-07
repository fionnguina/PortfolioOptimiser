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
$ExePath = Join-Path $ScriptDir "dist\Portfolio Optimiser.exe"
$PptPath = Join-Path $ScriptDir "dist\Reports\Portfolio_Report.pptx"
if (-not $LogPath) {
    $LogPath = Join-Path $ScriptDir "dist\run.log"
}
# Repo root, NOT dist\ — build_helper wipes dist on every rebuild, which was
# silently destroying the wrapper's evidence trail (discovered 2026-07-06).
$DailyLogPath = Join-Path $ScriptDir "daily_auto.log"
$FlagPath = Join-Path $ScriptDir "dist\engine_done.flag"
$EngineTimeoutSec = 600

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
    $ibcIni = "C:\IBC\config.ini"
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

# Pre-emptively clear any stale sentinel from a prior run.
if (Test-Path $FlagPath) { Remove-Item $FlagPath -Force -ErrorAction SilentlyContinue }

$EngineStart = Get-Date
try {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $ExePath
    $psi.Arguments = "--auto-pipeline"
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.WindowStyle = "Hidden"
    # Hybrid evidence config (2026-06-27): daily scheduled runs auto-
    # enable SCALE_SENSITIVITY so metrics_history.jsonl accumulates a
    # continuous track at every NAV ($100k / $250k / $500k / $1M) in
    # parallel. User decided to skip QuantConnect — the existing engine
    # at multiple scales is the evidence pipeline for the wholesale-
    # fund pitch. Interactive runs (without this wrapper) stay fast by
    # leaving SCALE_SENSITIVITY off unless explicitly enabled.
    $psi.EnvironmentVariables["SCALE_SENSITIVITY"] = "1"
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
    # Channel 1: sentinel
    if (Test-Path $FlagPath) {
        $flagMtime = (Get-Item $FlagPath).LastWriteTime
        if ($flagMtime -ge $EngineStart) {
            $sentinelHit = $true
            break
        }
    }
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
if (Test-Path $FlagPath) {
    try {
        $flagJson = Get-Content -Path $FlagPath -Raw -ErrorAction Stop | ConvertFrom-Json
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
        if ($OpenPptOnRun -and (Test-Path $PptPath)) {
            Start-Process $PptPath
            Write-Log "Opened $PptPath."
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

Write-Log "Done."
exit 0

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
$DailyLogPath = Join-Path $ScriptDir "dist\daily_auto.log"
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

function Start-TwsIfNeeded {
    if (Test-IbkrPort -Port $IbkrPort) {
        Write-Log "TWS already listening on 127.0.0.1:$IbkrPort."
        return $true
    }
    if (-not $TwsPath -or -not (Test-Path $TwsPath)) {
        Write-Log "TWS not running and tws.exe not found (looked in `$env:IBKR_TWS_PATH and standard install paths). Engine will fall back to yfinance prices."
        return $false
    }
    Write-Log "TWS not listening; launching $TwsPath..."
    try {
        Start-Process -FilePath $TwsPath -WindowStyle Minimized | Out-Null
    } catch {
        Write-Log "TWS launch failed: $($_.Exception.Message). Engine will fall back to yfinance."
        return $false
    }
    $deadline = (Get-Date).AddSeconds($TwsReadyTimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (Test-IbkrPort -Port $IbkrPort) {
            Write-Log "TWS port ${IbkrPort} ready."
            return $true
        }
        Start-Sleep -Seconds 2
    }
    Write-Log "TWS launch timed out after ${TwsReadyTimeoutSec}s waiting for port ${IbkrPort}. May still be on the login screen — engine will fall back to yfinance."
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
    Write-Log "Engine TIMEOUT after $($EngineDuration.TotalSeconds.ToString('F1'))s — proceeding with partial log."
}

# --- Parse [rebal-trigger] verdict from run.log ---
$verdict = "UNKNOWN"
$summedDw = "?"
$portfolioAud = "?"
$mode = "?"
if (Test-Path $LogPath) {
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

# --- Notification + optional PPT open ---
switch ($verdict) {
    "RUN" {
        $body = "Rebalance ready. summed|Δw|=$summedDw (>= $('{0:F2}' -f (0.03))). Portfolio $portfolioAud AUD. Open PPT to review, then run ibkr_paper_exec.py --execute."
        Show-Toast -Title "Portfolio Optimiser — REBALANCE READY" -Body $body -Verdict "RUN" | Out-Null
        if ($OpenPptOnRun -and (Test-Path $PptPath)) {
            Start-Process $PptPath
            Write-Log "Opened $PptPath."
        }
    }
    "SKIP" {
        $body = "No action needed. summed|Δw|=$summedDw (< 0.03 threshold). Portfolio $portfolioAud AUD."
        Show-Toast -Title "Portfolio Optimiser — no action" -Body $body -Verdict "SKIP" | Out-Null
    }
    default {
        $body = "Engine ran but verdict is $verdict. See dist\daily_auto.log + dist\run.log."
        Show-Toast -Title "Portfolio Optimiser — review log" -Body $body -Verdict "UNKNOWN" | Out-Null
    }
}

Write-Log "Done."
exit 0

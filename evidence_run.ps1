<#
.SYNOPSIS
    Evening multi-scale evidence run (SCALE_SENSITIVITY sweep).

.DESCRIPTION
    Runs the engine with SCALE_SENSITIVITY=1 so metrics_history.jsonl accrues
    the per-NAV ($100k/$250k/$500k/$1M) evidence track for the wholesale-fund
    pitch. Split out of the 9:30 monitoring run on 2026-07-09: the sweep takes
    ~25min, which timed out + killed the fast trade verdict when they shared a
    run. Scheduled for the EVENING so it runs AFTER any morning execution —
    it regenerates the rec log / Excel (same verdict) with no one waiting on
    it and no plan to clobber.

    No toast / email / simulator / Gateway launch: this is a backtest
    (yfinance history), not a trade decision. Nothing here places orders.

    Schedule (weekdays 18:00):
      schtasks /Create /SC WEEKLY /D MON,TUE,WED,THU,FRI /TN "Portfolio Optimiser Evidence" `
        /TR "powershell -ExecutionPolicy Bypass -File C:\Users\Fionn Guina\Portfolio_Optimiser\evidence_run.ps1" `
        /ST 18:00
#>

$ErrorActionPreference = "Continue"
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$ExePath    = Join-Path $ScriptDir "dist\Portfolio Optimiser.exe"
$FlagPath   = Join-Path $ScriptDir "dist\engine_done.flag"
$LogPath    = Join-Path $ScriptDir "evidence_run.log"
$TimeoutSec = 2400   # 40min — generous; the sweep runs ~25min and nothing waits

function Write-Log {
    param([string]$Msg)
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Msg"
    Write-Host $line
    try { Add-Content -Path $LogPath -Value $line -Encoding utf8 -ErrorAction Stop } catch { }
}

if (-not (Test-Path $ExePath)) {
    Write-Log "FATAL: $ExePath not found. Rebuild via build_helper.py."
    exit 1
}

Write-Log "Starting evening evidence run (SCALE_SENSITIVITY=1)."
if (Test-Path $FlagPath) { Remove-Item $FlagPath -Force -ErrorAction SilentlyContinue }

$start = Get-Date
try {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $ExePath
    $psi.Arguments = "--auto-pipeline"
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.WindowStyle = "Hidden"
    $psi.EnvironmentVariables["SCALE_SENSITIVITY"] = "1"
    $proc = [System.Diagnostics.Process]::Start($psi)
} catch {
    Write-Log "Engine launch threw: $($_.Exception.Message)"
    exit 2
}
Write-Log "Engine launched (PID=$($proc.Id)); awaiting sentinel or exit (timeout ${TimeoutSec}s)."

$deadline = $start.AddSeconds($TimeoutSec)
$sentinel = $false
while ((Get-Date) -lt $deadline) {
    if ((Test-Path $FlagPath) -and ((Get-Item $FlagPath).LastWriteTime -ge $start)) { $sentinel = $true; break }
    if ($proc.HasExited) { break }
    Start-Sleep -Seconds 5
}
if (-not $proc.HasExited) { [void]$proc.WaitForExit(5000) }

$elapsed = [int]((Get-Date) - $start).TotalSeconds
if ($proc.HasExited) {
    Write-Log "Evidence run finished (exit=$($proc.ExitCode), sentinel=$sentinel) after ${elapsed}s."
} else {
    # Kill the whole tree (multiprocessing workers) so it can't orphan and
    # lock dist/ against the next build — the recurring 2026-07 gotcha.
    Write-Log "Evidence run TIMEOUT after ${elapsed}s — killing process tree (PID=$($proc.Id))."
    try { & taskkill /PID $proc.Id /T /F 2>$null | Out-Null; Write-Log "Process tree killed." }
    catch { Write-Log "taskkill failed: $($_.Exception.Message)" }
}
Write-Log "Done."
exit 0

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
# Both sentinel locations, mirroring daily_auto.ps1's $FlagPaths (2026-07-18):
# a CLEAN finish writes APP_DIR\engine_done.flag = the REPO ROOT (APP_DIR's
# _DEV_BASE short-circuits the frozen branch), while a sanity HALT writes
# dist\engine_done.flag. Polling dist\ only meant this wrapper's sentinel never
# fired on a clean run and it silently fell back to WaitForExit alone (documented
# hang-prone under scheduled-task conditions), and its log always read
# sentinel=False even on success.
$FlagPaths  = @(
    (Join-Path $ScriptDir "engine_done.flag"),
    (Join-Path $ScriptDir "dist\engine_done.flag")
)
$LogPath    = Join-Path $ScriptDir "evidence_run.log"
# The budget is AWAKE seconds, not wall clock. Start-Sleep is suspended along
# with the machine, so a laptop that sleeps at 18:03 and wakes at 09:59 used to
# come back to "TIMEOUT after 57559s" and kill an engine that had barely run
# (observed 2026-08-07 — that evening's evidence sample was lost). Sleep is now
# measured and excluded.
$TimeoutSec = 2400   # 40min AWAKE — generous; the sweep runs ~25min
# But sleep is forgiven only so far: a resumed run must never still be going at
# 09:30 when daily_auto starts its own engine — two of them contend for Excel
# COM and the dist/ lock. 18:00 + 11h = 05:00 leaves clear air.
$WallCeilingSec   = 39600
$SuspendGapSec    = 60    # an inter-tick gap this far past the 5s we asked for
                          # is the machine sleeping, not the engine working
$SentinelGraceSec = 120   # post-sentinel the engine has DONE its work and is
                          # only releasing child handles; the old 5s expired
                          # first and logged every clean run as a TIMEOUT

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

# Block idle sleep for the run. The awake-budget logic below DIAGNOSES a slept
# machine; this is what PREVENTS it. Non-fatal — a run that can't hold the
# machine up is still worth attempting, it just risks the ABANDONED path.
$PowerHelper = Join-Path $ScriptDir "ops_power.ps1"
$SleepHeld = $false
if (Test-Path $PowerHelper) {
    . $PowerHelper
    $SleepHeld = Suspend-IdleSleep
    if ($SleepHeld) { Write-Log "Idle sleep blocked for the duration of this run." }
    else { Write-Log "WARN: could not block idle sleep; a nap may cost this sample." }
} else {
    Write-Log "WARN: ops_power.ps1 not found; idle sleep NOT blocked."
}

foreach ($fp in $FlagPaths) {
    if (Test-Path $fp) { Remove-Item $fp -Force -ErrorAction SilentlyContinue }
}

$start = Get-Date
try {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $ExePath
    $psi.Arguments = "--auto-pipeline"
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.WindowStyle = "Hidden"
    $psi.EnvironmentVariables["SCALE_SENSITIVITY"] = "1"
    # This is a research sweep, not a trading decision. Without this it wrote a
    # rec-log entry at 18:00 that superseded the morning's — and the 02:00 US
    # pass loads the LATEST entry, so the US legs would chase the evening plan
    # instead of the approved morning one. Harmless so far only because every
    # day has been a cadence-gated SKIP; on a RUN day the morning fills the ASX
    # legs, which moves the cadence anchor to today, so the 18:00 run returns
    # SKIP and the US legs are then refused at 02:00 — half a rebalance, with
    # the anchor claiming it just rebalanced.
    $psi.EnvironmentVariables["PORTOPT_NO_REC_LOG"] = "1"
    $proc = [System.Diagnostics.Process]::Start($psi)
} catch {
    Write-Log "Engine launch threw: $($_.Exception.Message)"
    exit 2
}
Write-Log ("Engine launched (PID=$($proc.Id)); awaiting sentinel or exit " +
           "(${TimeoutSec}s awake budget, ${WallCeilingSec}s wall ceiling).")

$sentinel = $false
$awake    = 0.0
$slept    = 0.0
$lastTick = Get-Date
while ($true) {
    foreach ($fp in $FlagPaths) {
        if ((Test-Path $fp) -and ((Get-Item $fp).LastWriteTime -ge $start)) { $sentinel = $true; break }
    }
    if ($sentinel) { break }
    if ($proc.HasExited) { break }
    if ($awake -ge $TimeoutSec) { break }
    if (((Get-Date) - $start).TotalSeconds -ge $WallCeilingSec) { break }
    Start-Sleep -Seconds 5
    $now = Get-Date
    $gap = ($now - $lastTick).TotalSeconds
    $lastTick = $now
    if ($gap -gt $SuspendGapSec) { $slept += $gap } else { $awake += $gap }
}

# A fired sentinel means the engine finished its work and is only tearing down
# child handles — give it real time. No sentinel means it is genuinely stuck and
# there is nothing to wait for.
if (-not $proc.HasExited) {
    if ($sentinel) { $graceMs = $SentinelGraceSec * 1000 } else { $graceMs = 5000 }
    [void]$proc.WaitForExit($graceMs)
}

$wall      = [int]((Get-Date) - $start).TotalSeconds
$awakeSec  = [int]$awake
$sleptSec  = [int]$slept
$sleptNote = ""
if ($sleptSec -gt 0) { $sleptNote = " (+${sleptSec}s machine sleep, excluded)" }

if ($proc.HasExited) {
    $why = "finished (exit=$($proc.ExitCode), sentinel=$sentinel)"
    $outcome = "ok"
    Write-Log ("Evidence run $why after ${awakeSec}s awake / ${wall}s wall$sleptNote.")
} else {
    # Kill the whole tree (multiprocessing workers) so it can't orphan and
    # lock dist/ against the next build — the recurring 2026-07 gotcha. The
    # kill is the same in every branch; only the diagnosis differs, and it has
    # to, because "finished but slow to reap" and "never ran" both used to read
    # as TIMEOUT and were indistinguishable when triaging.
    if ($sentinel) {
        # The evidence WAS written — this is a success that reaped slowly, and
        # the ledger must not record it as a failure or the heartbeat cries
        # wolf on a run that did its job.
        $outcome = "ok"
        $why = ("COMPLETE (sentinel fired, evidence written) but still holding " +
                "child handles after ${SentinelGraceSec}s grace — reaping tree")
    } elseif ($wall -ge $WallCeilingSec) {
        $outcome = "fail"
        $why = ("ABANDONED — machine slept past the ${WallCeilingSec}s wall ceiling " +
                "(${awakeSec}s awake / ${wall}s wall$sleptNote). NO evidence sample " +
                "this run — reaping tree")
    } else {
        $outcome = "fail"
        $why = ("HUNG — no sentinel after ${awakeSec}s awake$sleptNote. NO evidence " +
                "sample this run — reaping tree")
    }
    Write-Log "Evidence run $why (PID=$($proc.Id))."
    try { & taskkill /PID $proc.Id /T /F 2>$null | Out-Null; Write-Log "Process tree killed." }
    catch { Write-Log "taskkill failed: $($_.Exception.Message)" }
}
# Stamp the run ledger so a missing evening run becomes visible tomorrow
# morning instead of being indistinguishable from a healthy quiet night.
# No --check here: the 09:30/10:20 run owns the reporting.
try {
    $opsPy = Join-Path $ScriptDir ".venv\Scripts\python.exe"
    $opsScript = Join-Path $ScriptDir "ops_assertions.py"
    if ((Test-Path $opsPy) -and (Test-Path $opsScript)) {
        & $opsPy $opsScript --record evidence_run --outcome $outcome --detail $why | Out-Null
    }
} catch { Write-Log "Ledger stamp failed (non-fatal): $($_.Exception.Message)" }

if ($SleepHeld) { Resume-IdleSleep }
Write-Log "Done."
exit 0

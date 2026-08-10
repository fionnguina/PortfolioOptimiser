<#
.SYNOPSIS
    Execute the US legs of the morning's APPROVED plan, inside the US session.

.DESCRIPTION
    The 10:20 AEST run fires 4h20m AFTER the US close, so every US leg used to go
    in as an overnight DAY order priced off a ~13h-stale close, with no retry.
    That is the root of the SMH non-fills, the frozen qty_filled=0 rows, and the
    false "DID NOT FILL" reports.

    This pass runs while the US market is actually OPEN and executes the SAME
    approved plan against live prices.

    IT DOES NOT RUN THE ENGINE. No re-optimisation, no new verdict, no new
    recommendation. It loads the rec-log entry the morning run already wrote and
    produced a verdict for, keeps only the non-.AX legs, and re-solves their unit
    counts from the plan's frozen TARGET WEIGHTS at live prices
    (--reprice-to-targets). A gap up therefore buys proportionally fewer units
    rather than overshooting the target and the cash budget.

.NOTES
    START TIME IS LOAD-BEARING, and not for the obvious reason. The US session in
    Australian local time MOVES with two independent DST switches:

        AEST (UTC+10) + EDT : 23:30 - 06:00 local
        AEDT (UTC+11) + EDT : 00:30 - 07:00 local
        AEDT (UTC+11) + EST : 01:30 - 08:00 local

    The only clock time inside RTH in EVERY combination is roughly 01:30-06:00,
    so this is scheduled at 02:00 and never needs seasonal adjustment.

    DAYS ARE ALSO NOT WHAT YOU EXPECT. A plan built Monday 10:20 AEST trades in
    the US session that opens Monday night and is still open at 02:00 TUESDAY.
    Friday's plan therefore executes at 02:00 SATURDAY. The task runs TUE-SAT,
    not MON-FRI.

    Register (needs an ELEVATED prompt):
      schtasks /Create /SC WEEKLY /D TUE,WED,THU,FRI,SAT ^
        /TN "Portfolio Optimiser US Session" ^
        /TR "powershell -ExecutionPolicy Bypass -File C:\Users\Fionn Guina\Portfolio_Optimiser\us_session_run.ps1" ^
        /ST 02:00
    Then enable wake: Task Scheduler > task > Conditions > "Wake the computer".
#>

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PyPath    = Join-Path $ScriptDir ".venv\Scripts\python.exe"
$ExecPath  = Join-Path $ScriptDir "ibkr_paper_exec.py"
$LogPath   = Join-Path $ScriptDir "us_session_run.log"

function Write-Log {
    param([string]$Msg)
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Msg"
    Write-Host $line
    try { Add-Content -Path $LogPath -Value $line -Encoding utf8 -ErrorAction Stop } catch { }
}

Write-Log "Starting US-session execution pass."

if (-not (Test-Path $PyPath))   { Write-Log "FATAL: venv python not found at $PyPath."; exit 1 }
if (-not (Test-Path $ExecPath)) { Write-Log "FATAL: ibkr_paper_exec.py not found.";     exit 1 }

# Hold the machine up for the pass. At 02:00 an idle sleep is far more likely
# than it is at 10:20, and this path places ORDERS — suspending between
# submitting and reconciling is the worst possible moment.
$PowerHelper = Join-Path $ScriptDir "ops_power.ps1"
$SleepHeld = $false
if (Test-Path $PowerHelper) {
    . $PowerHelper
    $SleepHeld = Suspend-IdleSleep
    if (-not $SleepHeld) { Write-Log "WARN: could not block idle sleep." }
} else {
    Write-Log "WARN: ops_power.ps1 not found; idle sleep NOT blocked."
}

$outcome = "fail"
$detail  = "did not complete"
try {
    # --venue US              only the legs that could not sensibly trade at 10:20
    # --reprice-to-targets    re-solve units from the approved weights at live prices
    # --auto-execute          headless; the pre-trade validation gate IS the approval
    # --wait-for-funds        the morning's ASX sells have had a full session to
    #                         settle, so poll rather than defer on a timing race
    $out = & $PyPath $ExecPath --auto-execute --venue US --reprice-to-targets `
                               --wait-for-funds 900 --email 2>&1
    $code = $LASTEXITCODE
    $tail = ($out | Select-Object -Last 12) -join " | "
    Write-Log "Executor exit=$code. $tail"

    # 0 = submitted or nothing to do. 3 = refused by the verdict gate, which is a
    # CORRECT outcome on a SKIP day, not a failure of this job.
    if ($code -eq 0 -or $code -eq 3) { $outcome = "ok" } else { $outcome = "fail" }
    $detail = "exit=$code"
} catch {
    Write-Log "Executor threw: $($_.Exception.Message)"
    $detail = "threw: $($_.Exception.Message)"
}

try {
    $opsScript = Join-Path $ScriptDir "ops_assertions.py"
    if (Test-Path $opsScript) {
        & $PyPath $opsScript --record us_session --outcome $outcome --detail $detail | Out-Null
    }
} catch { Write-Log "Ledger stamp failed (non-fatal): $($_.Exception.Message)" }

if ($SleepHeld) { Resume-IdleSleep }
Write-Log "Done ($outcome)."
exit 0

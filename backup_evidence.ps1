<#
.SYNOPSIS
    Back up the irreplaceable local evidence + state files to OneDrive.

.DESCRIPTION
    The fund's real-money track record and lot-book state live in gitignored,
    local-only files - a disk failure would erase them (they are the fund's
    most valuable asset). This mirrors them to a OneDrive folder each run;
    OneDrive's own version history provides point-in-time recovery, so a flat
    mirror is enough.

    Credentials (IBC config, mail config) are deliberately NOT copied to
    cloud - see RECOVERY.md for how they are re-created on a new machine.

    Called from daily_auto.ps1 after the NAV snapshot. Non-fatal.
#>
$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Backup target: OneDrive if present, else a local dir (documented fallback).
$oneDrive = $env:OneDrive
if (-not $oneDrive) { $oneDrive = Join-Path $env:USERPROFILE "OneDrive" }
if (Test-Path $oneDrive) {
    $dest = Join-Path $oneDrive "GuinaFund_Backup"
} else {
    $dest = Join-Path $ScriptDir "_local_backup"   # fallback; point at cloud manually
}
New-Item -ItemType Directory -Force $dest | Out-Null

# Irreplaceable local state - the real-money track record + lot book + universe.
$files = @(
    "metrics_history.jsonl",           # strategy metrics track
    "ibkr_nav_log.jsonl",              # REAL broker NAV record
    "ibkr_fills_log.jsonl",            # executed fills since seed
    "trade_recommendation_log.jsonl",  # every engine recommendation
    "lots_seed.json",                  # lot book seed
    "portfolio_state.json",            # NAV / net-invested state
    "tlh_cooldown_state.json",         # TLH wash-swap protection
    "sanity_alerts.jsonl",             # halt/violation audit
    "cash_ledger.jsonl",               # per-run brokerage/CGT/loss-carry = TAX record
    "live_nav_history.jsonl",          # daily live NAV series (drift tracker input)
    "Stock Analysis.xlsm"              # holdings sheet = ticker universe
)

$ok = 0; $miss = 0
foreach ($f in $files) {
    $src = Join-Path $ScriptDir $f
    if (Test-Path $src) {
        try { Copy-Item $src (Join-Path $dest $f) -Force -ErrorAction Stop; $ok++ }
        catch { Write-Host "[backup] FAILED $f : $($_.Exception.Message)" }
    } else { $miss++ }
}
$stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"[$stamp] backed up $ok file(s) ($miss absent) -> $dest" | Tee-Object -Append (Join-Path $ScriptDir "backup_evidence.log")

<#
.SYNOPSIS
    One-time IBC + IB Gateway wiring. Run AFTER installing offline IB Gateway
    (stable) and AFTER putting your paper credentials in C:\IBC\config.ini.

.DESCRIPTION
    1. Detects the installed Gateway version under C:\Jts\ibgateway\<ver>.
    2. Patches C:\IBC\StartGateway.bat (TWS_MAJOR_VRSN + TRADING_MODE=paper).
    3. Locks C:\IBC\config.ini ACLs to the current user (it holds paper
       credentials in plaintext — IBC requirement).
    4. Warns if the credentials are still placeholders.

    Gateway offline installer: https://www.interactivebrokers.com/en/trading/ibgateway-stable.php
    (choose "IB Gateway - Stable", offline installer, default install path.)
#>

$ErrorActionPreference = "Stop"

# 1. Detect Gateway version
$gwRoot = "C:\Jts\ibgateway"
if (-not (Test-Path $gwRoot)) {
    Write-Host "[setup][ERR] $gwRoot not found - install offline IB Gateway (stable) first." -ForegroundColor Red
    Write-Host "             https://www.interactivebrokers.com/en/trading/ibgateway-stable.php"
    exit 1
}
$ver = Get-ChildItem $gwRoot -Directory | Where-Object { $_.Name -match "^\d+$" } |
       Sort-Object { [int]$_.Name } -Descending | Select-Object -First 1 -ExpandProperty Name
if (-not $ver) {
    Write-Host "[setup][ERR] no numeric version folder under $gwRoot" -ForegroundColor Red
    exit 1
}
Write-Host "[setup] detected IB Gateway version: $ver"

# 2. Patch StartGateway.bat
$bat = "C:\IBC\StartGateway.bat"
$c = Get-Content $bat -Raw
$c = $c -replace "(?m)^set TWS_MAJOR_VRSN=\d+", "set TWS_MAJOR_VRSN=$ver"
$c = $c -replace "(?m)^set TRADING_MODE=\r?$", "set TRADING_MODE=paper"
Set-Content -Path $bat -Value $c -Encoding ascii
Write-Host "[setup] StartGateway.bat patched (version $ver, paper mode)"

# 3. Lock config.ini to current user (holds credentials)
$ini = "C:\IBC\config.ini"
icacls $ini /inheritance:r /grant:r "${env:USERNAME}:(R,W)" | Out-Null
Write-Host "[setup] config.ini ACLs restricted to $env:USERNAME"

# 4. Credential sanity
if (Select-String -Path $ini -Pattern "YOUR_PAPER_USERNAME_HERE" -Quiet) {
    Write-Host "[setup][WARN] config.ini still has PLACEHOLDER credentials -" -ForegroundColor Yellow
    Write-Host "              edit C:\IBC\config.ini -> IbLoginId= / IbPassword= (paper account)." -ForegroundColor Yellow
} else {
    Write-Host "[setup] credentials present (not validated - first launch will tell)."
}

Write-Host ""
Write-Host "[setup] Done. Test with:  C:\IBC\StartGateway.bat"
Write-Host "[setup] then verify:      .\.venv\Scripts\python.exe probe_ibkr_api.py"

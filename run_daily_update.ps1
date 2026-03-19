param(
    [switch]$Force,
    [switch]$SymbolsOnly,
    [switch]$FeaturesOnly,
    [int]$BatchSize = 700
)

$ErrorActionPreference = "Stop"
$ProjectRoot = $PSScriptRoot

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " AI Trading System - Daily EOD Update" -ForegroundColor Cyan
Write-Host (" " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss")) -ForegroundColor Gray
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$PythonExe = Join-Path $ProjectRoot "venv\Scripts\python.exe"
$Script = Join-Path $ProjectRoot "collectors\daily_update_runner.py"

if (-not (Test-Path $PythonExe)) {
    Write-Host "[ERROR] venv python not found at: $PythonExe" -ForegroundColor Red
    exit 1
}

if ($Force) {
    Write-Host "[MODE] Force update - overwriting existing rows" -ForegroundColor Yellow
}

if ($SymbolsOnly) {
    Write-Host "[MODE] OHLCV only - skipping feature recomputation" -ForegroundColor Yellow
}

if ($FeaturesOnly) {
    Write-Host "[MODE] Features only - skipping OHLCV fetch" -ForegroundColor Yellow
}

$ArgStr = "--batch-size $BatchSize"
if ($Force) { $ArgStr = "$ArgStr --force" }
if ($SymbolsOnly) { $ArgStr = "$ArgStr --symbols-only" }
if ($FeaturesOnly) { $ArgStr = "$ArgStr --features-only" }

Write-Host "[RUN] $PythonExe $Script $ArgStr" -ForegroundColor Gray
Write-Host ""

try {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $PythonExe
    $psi.Arguments = "`"$Script`" $ArgStr"
    $psi.RedirectStandardOutput = $false
    $psi.RedirectStandardError = $false
    $psi.UseShellExecute = $true
    $proc = Start-Process -FilePath $PythonExe -ArgumentList "`"$Script`" $ArgStr" -Wait -PassThru -NoNewWindow
    $exitCode = $proc.ExitCode
} catch {
    Write-Host "[ERROR] $_" -ForegroundColor Red
    $exitCode = 1
}

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "[DONE] Daily update completed successfully" -ForegroundColor Green
} else {
    Write-Host "[FAIL] Daily update exited with code $exitCode" -ForegroundColor Red
}

exit $exitCode

# Setup Windows Task Scheduler for Daily EOD Update
# Run as admin: .\setup_daily_task.ps1
# Schedule: Mon-Fri at 3:45 PM IST (market closes 3:30 PM)

$TaskName = "AI-Trading-Daily-Update"
$ScriptPath = Join-Path $PSScriptRoot "run_daily_update.ps1"
$LogPath = Join-Path $PSScriptRoot "logs\daily_update.log"

if (-not (Test-Path (Split-Path $LogPath))) {
    New-Item -ItemType Directory -Force -Path (Split-Path $LogPath) | Out-Null
}

$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -File `"$ScriptPath`" 2>&1 | Tee-Object -Append `"$LogPath`""

$Trigger = New-ScheduledTaskTrigger `
    -Weekly -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday `
    -At "15:45"

$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable:$false

$Principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Limited

Write-Host "Setting up scheduled task: $TaskName" -ForegroundColor Cyan
Write-Host "Schedule: Mon-Fri at 3:45 PM IST" -ForegroundColor Cyan
Write-Host "Log file: $LogPath" -ForegroundColor Cyan

try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $Action `
        -Trigger $Trigger `
        -Settings $Settings `
        -Principal $Principal `
        -Description "AI Trading System — Daily EOD OHLCV + Feature Update (after market close)" `
        | Out-Null

    Write-Host "[OK] Task '$TaskName' created successfully" -ForegroundColor Green
    Write-Host ""
    Write-Host "To run manually:" -ForegroundColor Yellow
    Write-Host "  .\run_daily_update.ps1" -ForegroundColor Gray
    Write-Host ""
    Write-Host "To check status:" -ForegroundColor Yellow
    Write-Host "  Get-ScheduledTask -TaskName '$TaskName' | Get-ScheduledTaskInfo" -ForegroundColor Gray

} catch {
    Write-Host "[ERROR] Failed to create task: $_" -ForegroundColor Red
}

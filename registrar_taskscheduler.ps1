# Registra os bots no Task Scheduler do Windows pra subir automaticamente no login.
# Execute em PowerShell COMO ADMINISTRADOR:
#   powershell -ExecutionPolicy Bypass -File C:\robo-trade\registrar_taskscheduler.ps1

$ErrorActionPreference = "Stop"

Write-Host "Registrando Nunes Bot e Nunes Bot Lisa no Task Scheduler..."
Write-Host ""

# --- Nunes Bot principal ---
$NomePrincipal = "NunesBot"
$BatPrincipal = "C:\robo-trade\iniciar_bot.bat"

if (-not (Test-Path $BatPrincipal)) {
    Write-Error "Arquivo nao encontrado: $BatPrincipal"
    exit 1
}

# Remove task anterior se existir
Unregister-ScheduledTask -TaskName $NomePrincipal -Confirm:$false -ErrorAction SilentlyContinue

$Action = New-ScheduledTaskAction -Execute $BatPrincipal
$Trigger = New-ScheduledTaskTrigger -AtLogOn
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Days 365)
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $NomePrincipal -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Description "Nunes Bot: inicia nunes.py + auditor_continuo.py no login"

Write-Host "OK: $NomePrincipal registrado (C:\robo-trade)"
Write-Host ""

# --- Nunes Bot Lisa ---
$NomeLisa = "NunesBotLisa"
$BatLisa = "C:\robo-trade-lisa\iniciar_bot.bat"

if (Test-Path $BatLisa) {
    Unregister-ScheduledTask -TaskName $NomeLisa -Confirm:$false -ErrorAction SilentlyContinue

    $ActionLisa = New-ScheduledTaskAction -Execute $BatLisa
    $TriggerLisa = New-ScheduledTaskTrigger -AtLogOn
    $SettingsLisa = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Days 365)
    $PrincipalLisa = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

    Register-ScheduledTask -TaskName $NomeLisa -Action $ActionLisa -Trigger $TriggerLisa -Settings $SettingsLisa -Principal $PrincipalLisa -Description "Nunes Bot Lisa: inicia nunes.py + auditor_continuo.py no login"

    Write-Host "OK: $NomeLisa registrado (C:\robo-trade-lisa)"
} else {
    Write-Host "robo-trade-lisa nao encontrado, pulando."
}

Write-Host ""
Write-Host "=== Concluido ==="
Write-Host "Tasks criadas:"
Get-ScheduledTask -TaskName "NunesBot*" | Select-Object TaskName, State | Format-Table -AutoSize

Write-Host ""
Write-Host "Pra testar sem reiniciar: Start-ScheduledTask -TaskName NunesBot"
Write-Host "Pra ver no painel:       taskschd.msc"

param(
  [string]$ConfigPath = ".\\signer_config.json",
  [switch]$Quiet
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $scriptDir
try {
  if (-not (Test-Path $ConfigPath)) {
    if (-not $Quiet) { Write-Host "Config not found: $ConfigPath" }
    exit 0
  }
  $configResolved = (Resolve-Path $ConfigPath).Path

  $cfgRegex = [regex]::Escape($configResolved)
  $targets = @(Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -and
    $_.CommandLine -match "signer_service\.py" -and
    $_.CommandLine -match $cfgRegex
  })

  if ($targets.Count -eq 0) {
    if (-not $Quiet) { Write-Host "[signer] no process found" }
    exit 0
  }

  foreach ($p in $targets) {
    $id = [int]$p.ProcessId
    try {
      Stop-Process -Id $id -Force -ErrorAction Stop
      if (-not $Quiet) { Write-Host "[signer] stopped PID=$id" }
    } catch {
      if (-not $Quiet) { Write-Host "[signer] failed stopping PID=$id : $($_.Exception.Message)" }
    }
  }
}
finally {
  Pop-Location
}

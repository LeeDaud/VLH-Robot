param(
  [string]$ConfigPath = ".\config.json",
  [string]$PythonExe = "python",
  [switch]$ForceRestart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $scriptDir
try {
  if (-not (Test-Path $ConfigPath)) {
    throw "Config file not found: $ConfigPath"
  }

  $configResolved = (Resolve-Path $ConfigPath).Path
  $dataDir = Join-Path $scriptDir "data"
  $logDir = Join-Path $dataDir "logs"
  $pidFile = Join-Path $dataDir "role_pids.json"

  New-Item -ItemType Directory -Path $dataDir -Force | Out-Null
  New-Item -ItemType Directory -Path $logDir -Force | Out-Null

  if ($ForceRestart) {
    & (Join-Path $scriptDir "stop_3roles.ps1") -ConfigPath $configResolved -Quiet
  }

  function Get-RoleProcesses {
    param(
      [string]$Role,
      [string]$Cfg
    )
    $cfgRegex = [regex]::Escape($Cfg)
    $roleRegex = [regex]::Escape("--role $Role")
    Get-CimInstance Win32_Process | Where-Object {
      $_.CommandLine -and
      $_.CommandLine -match "virtuals_bot\.py" -and
      $_.CommandLine -match $roleRegex -and
      $_.CommandLine -match $cfgRegex
    }
  }

  $roles = @("writer", "realtime", "backfill")
  $records = @()

  foreach ($role in $roles) {
    $existing = @(Get-RoleProcesses -Role $role -Cfg $configResolved)
    if ($existing.Count -gt 0) {
      $procId = [int]$existing[0].ProcessId
      Write-Host "[$role] already running (PID=$procId)"
      $records += [pscustomobject]@{
        role      = $role
        pid       = $procId
        config    = $configResolved
        startedAt = [int][DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
        status    = "existing"
      }
      continue
    }

    $stdoutPath = Join-Path $logDir "$role.out.log"
    $stderrPath = Join-Path $logDir "$role.err.log"
    $proc = Start-Process `
      -FilePath $PythonExe `
      -ArgumentList @("virtuals_bot.py", "--config", $configResolved, "--role", $role) `
      -WorkingDirectory $scriptDir `
      -RedirectStandardOutput $stdoutPath `
      -RedirectStandardError $stderrPath `
      -PassThru

    Start-Sleep -Milliseconds 350
    Write-Host "[$role] started (PID=$($proc.Id))"
    $records += [pscustomobject]@{
      role      = $role
      pid       = [int]$proc.Id
      config    = $configResolved
      startedAt = [int][DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
      status    = "started"
    }
  }

  $json = $records | ConvertTo-Json -Depth 4
  Set-Content -Path $pidFile -Value $json -Encoding UTF8
  Write-Host "PID file updated: $pidFile"
}
finally {
  Pop-Location
}

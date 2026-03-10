param(
  [string]$ConfigPath = ".\\signer_config.json",
  [string]$PythonExe = "python",
  [switch]$ForceRestart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $scriptDir
try {
  if (-not (Test-Path $ConfigPath)) {
    throw "Signer config file not found: $ConfigPath"
  }

  $configResolved = (Resolve-Path $ConfigPath).Path
  $runtimeDir = Join-Path $scriptDir "runtime"
  $logDir = Join-Path $runtimeDir "logs"
  $pidFile = Join-Path $runtimeDir "signer_pid.json"
  New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
  New-Item -ItemType Directory -Path $logDir -Force | Out-Null

  function Get-SignerProcesses {
    param([string]$Cfg)
    $cfgRegex = [regex]::Escape($Cfg)
    Get-CimInstance Win32_Process | Where-Object {
      $_.CommandLine -and
      $_.CommandLine -match "signer_service\.py" -and
      $_.CommandLine -match $cfgRegex
    }
  }

  if ($ForceRestart) {
    $old = @(Get-SignerProcesses -Cfg $configResolved)
    foreach ($p in $old) {
      try { Stop-Process -Id ([int]$p.ProcessId) -Force -ErrorAction Stop } catch {}
    }
    Start-Sleep -Milliseconds 300
  }

  $existing = @(Get-SignerProcesses -Cfg $configResolved)
  if ($existing.Count -gt 0) {
    $pid = [int]$existing[0].ProcessId
    Write-Host "[signer] already running (PID=$pid)"
    $record = [pscustomobject]@{
      role      = "signer"
      pid       = $pid
      config    = $configResolved
      startedAt = [int][DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
      status    = "existing"
    }
    $record | ConvertTo-Json -Depth 4 | Set-Content -Path $pidFile -Encoding UTF8
    exit 0
  }

  $stdoutPath = Join-Path $logDir "signer.out.log"
  $stderrPath = Join-Path $logDir "signer.err.log"
  $proc = Start-Process `
    -FilePath $PythonExe `
    -ArgumentList @("signer_service.py", "--config", $configResolved) `
    -WorkingDirectory $scriptDir `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath `
    -PassThru

  Start-Sleep -Milliseconds 500
  Write-Host "[signer] started (PID=$($proc.Id))"
  $record = [pscustomobject]@{
    role      = "signer"
    pid       = [int]$proc.Id
    config    = $configResolved
    startedAt = [int][DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    status    = "started"
  }
  $record | ConvertTo-Json -Depth 4 | Set-Content -Path $pidFile -Encoding UTF8
  Write-Host "PID file updated: $pidFile"
}
finally {
  Pop-Location
}

param(
  [string]$ConfigPath = ".\config.json",
  [switch]$Quiet
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $scriptDir
try {
  $configResolved = $null
  if (Test-Path $ConfigPath) {
    $configResolved = (Resolve-Path $ConfigPath).Path
  }

  $pidFile = Join-Path $scriptDir "data\role_pids.json"
  $stopped = @()

  function Stop-PidIfRunning {
    param([int]$ProcessId)
    try {
      $p = Get-Process -Id $ProcessId -ErrorAction Stop
      Stop-Process -Id $ProcessId -Force -ErrorAction Stop
      return $true
    }
    catch {
      return $false
    }
  }

  function Find-RoleProcesses {
    param(
      [string]$Role,
      [string]$Cfg
    )
    $roleRegex = [regex]::Escape("--role $Role")
    Get-CimInstance Win32_Process | Where-Object {
      if (-not $_.CommandLine) { return $false }
      if (-not ($_.CommandLine -match "virtuals_bot\.py")) { return $false }
      if (-not ($_.CommandLine -match $roleRegex)) { return $false }
      if ([string]::IsNullOrWhiteSpace($Cfg)) { return $true }
      $cfgRegex = [regex]::Escape($Cfg)
      return ($_.CommandLine -match $cfgRegex)
    }
  }

  if (Test-Path $pidFile) {
    try {
      $raw = Get-Content -Path $pidFile -Raw -Encoding UTF8
      if (-not [string]::IsNullOrWhiteSpace($raw)) {
        $obj = $raw | ConvertFrom-Json
        $items = @()
        if ($obj -is [System.Array]) {
          $items = $obj
        }
        else {
          $items = @($obj)
        }
        foreach ($it in $items) {
          $procId = [int]$it.pid
          if ($procId -le 0) { continue }
          $ok = Stop-PidIfRunning -ProcessId $procId
          if ($ok) { $stopped += $procId }
        }
      }
    }
    catch {
      if (-not $Quiet) {
        Write-Host "Failed to read PID file, fallback to process scan."
      }
    }
  }

  $roles = @("writer", "realtime", "backfill")
  foreach ($role in $roles) {
    $ps = @(Find-RoleProcesses -Role $role -Cfg $configResolved)
    foreach ($p in $ps) {
      $procId = [int]$p.ProcessId
      $ok = Stop-PidIfRunning -ProcessId $procId
      if ($ok) { $stopped += $procId }
    }
  }

  $stopped = $stopped | Sort-Object -Unique
  if (-not $Quiet) {
    if ($stopped.Count -gt 0) {
      Write-Host "Stopped PIDs: $($stopped -join ', ')"
    }
    else {
      Write-Host "No running 3-role process found."
    }
  }

  if (Test-Path $pidFile) {
    Remove-Item -Path $pidFile -Force -ErrorAction SilentlyContinue
  }
}
finally {
  Pop-Location
}

param(
  [string]$Dsn,
  [string]$MigrationsDir,
  [switch]$Force,
  [switch]$UseDockerPsql
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-PythonLauncher {
  if (Get-Command python3 -ErrorAction SilentlyContinue) {
    return @("python3")
  }

  if (Get-Command python -ErrorAction SilentlyContinue) {
    return @("python")
  }

  if (Get-Command py -ErrorAction SilentlyContinue) {
    return @("py", "-3")
  }

  throw "Python 3 is required to run reset_postgres.py."
}

$pythonLauncher = Resolve-PythonLauncher
$scriptPath = Join-Path $PSScriptRoot "reset_postgres.py"
if (-not (Test-Path $scriptPath)) {
  throw "Reset helper not found at $scriptPath"
}

if ($UseDockerPsql) {
  Write-Warning "-UseDockerPsql is ignored for reset_postgres_from_scratch.ps1 because reset_postgres.py connects via psycopg."
}

$scriptArgs = @($scriptPath)
if ($Force) {
  $scriptArgs += "--force"
}
if ($Dsn) {
  $scriptArgs += @("--dsn", $Dsn)
}
if ($MigrationsDir) {
  $scriptArgs += @("--migrations-dir", $MigrationsDir)
}

if ($pythonLauncher.Count -gt 1) {
  & $pythonLauncher[0] $pythonLauncher[1] @scriptArgs
}
else {
  & $pythonLauncher[0] @scriptArgs
}

if (-not $?) {
  throw "reset_postgres.py failed."
}

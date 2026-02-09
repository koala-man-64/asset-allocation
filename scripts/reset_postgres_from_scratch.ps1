param(
  [string]$Dsn = "",

  [string]$MigrationsDir = "deploy/sql/postgres/migrations",
  [string]$EnvFile = "",
  [switch]$UseDockerPsql,
  [switch]$Force
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if (-not $Force) {
  $confirmation = Read-Host "WARNING: This operation will DESTROY all data in the target database. Are you sure you want to continue? (y/N)"
  if ($confirmation -ne "y") {
    Write-Error "Operation aborted by user."
    exit 1
  }
}

function Resolve-EnvFilePath {
  param([string]$RequestedPath)

  if (-not [string]::IsNullOrWhiteSpace($RequestedPath)) {
    $resolved = Resolve-Path $RequestedPath -ErrorAction Stop
    return $resolved.Path
  }

  $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..") -ErrorAction Stop).Path
  $candidate = Join-Path $repoRoot ".env"
  if (Test-Path $candidate) {
    return (Resolve-Path $candidate -ErrorAction Stop).Path
  }
  return $null
}

function Get-EnvLines {
  param([string]$EnvPath)
  if ([string]::IsNullOrWhiteSpace($EnvPath) -or (-not (Test-Path $EnvPath))) {
    return @()
  }
  return Get-Content $EnvPath
}

function Get-EnvValue {
  param(
    [Parameter(Mandatory = $true)][string]$Key,
    [string[]]$Lines = @()
  )
  foreach ($line in $Lines) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) { continue }
    if ($trimmed -match ("^" + [regex]::Escape($Key) + "=(.*)$")) {
      $value = $matches[1].Trim()
      if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
        ($value.StartsWith("'") -and $value.EndsWith("'"))) {
        $value = $value.Substring(1, $value.Length - 2)
      }
      return $value
    }
  }
  return $null
}

$envPath = Resolve-EnvFilePath -RequestedPath $EnvFile
$envLines = Get-EnvLines -EnvPath $envPath

if ([string]::IsNullOrWhiteSpace($Dsn)) {
  $Dsn = $env:POSTGRES_DSN
}
if ([string]::IsNullOrWhiteSpace($Dsn)) {
  $Dsn = Get-EnvValue -Key "POSTGRES_DSN" -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($Dsn)) {
  throw "Dsn is required. Provide -Dsn, set POSTGRES_DSN env var, or add POSTGRES_DSN to .env."
}

function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command '$Name'. Install it and retry."
  }
}

function Invoke-Psql {
  param(
    [Parameter(Mandatory = $true)][string[]]$Args
  )

  if ($UseDockerPsql) {
    Assert-CommandExists -Name "docker"
    $cmd = @("run", "--rm", "-i", "postgres:16-alpine", "psql") + $Args
    & docker @cmd
    if (-not $?) { throw "psql (docker) failed." }
    return
  }

  Assert-CommandExists -Name "psql"
  & psql @Args
  if (-not $?) { throw "psql failed." }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..") -ErrorAction SilentlyContinue).Path
if (-not (Test-Path $MigrationsDir)) {
  $candidate = Join-Path $repoRoot $MigrationsDir
  if (Test-Path $candidate) {
    $MigrationsDir = $candidate
  }
}

$resolvedMigrationsDir = (Resolve-Path $MigrationsDir -ErrorAction Stop).Path

Write-Host "Resetting database objects (destructive) for DSN target..."

$dropSql = @'
DO $$
DECLARE
  s RECORD;
BEGIN
  FOR s IN
    SELECT n.nspname AS schema_name
    FROM pg_namespace n
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'public')
      AND n.nspname NOT LIKE 'pg_toast%'
      AND n.nspname NOT LIKE 'pg_temp_%'
  LOOP
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', s.schema_name);
  END LOOP;
END $$;

DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO CURRENT_USER;
GRANT USAGE ON SCHEMA public TO PUBLIC;
'@

Invoke-Psql -Args @("$Dsn", "-v", "ON_ERROR_STOP=1", "-c", $dropSql)

Write-Host "Reapplying migrations..."
& "$PSScriptRoot/apply_postgres_migrations.ps1" -Dsn $Dsn -MigrationsDir $resolvedMigrationsDir -UseDockerPsql:$UseDockerPsql
if (-not $?) {
  throw "Migration apply failed after reset."
}

Write-Host "Database reset + migration apply complete."

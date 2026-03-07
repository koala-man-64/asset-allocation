param(
  [string]$Dsn,
  [string]$MigrationsDir,
  [switch]$UseDockerPsql
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..") -ErrorAction Stop).Path
if (-not $MigrationsDir) {
  $MigrationsDir = Join-Path $RepoRoot "deploy/sql/postgres/migrations"
}

function Is-CompleteDsn {
  param([string]$Value)
  return ($Value -match "^[a-zA-Z][a-zA-Z0-9+.-]*://")
}

function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command '$Name'. Install it and retry."
  }
}

function Get-EnvValue {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][string]$Key
  )

  if (-not (Test-Path $Path)) {
    return ""
  }

  $keyPattern = "^{0}\s*=" -f [regex]::Escape($Key)
  foreach ($line in Get-Content $Path) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) {
      continue
    }

    if ($trimmed -notmatch $keyPattern) {
      continue
    }

    $parts = $trimmed -split "=", 2
    if ($parts.Count -ne 2) {
      continue
    }

    $value = $parts[1].Trim()
    if ($value.StartsWith('"') -and $value.EndsWith('"') -and $value.Length -ge 2) {
      $value = $value.Substring(1, $value.Length - 2)
    }
    return $value
  }

  return ""
}

function Invoke-Psql {
  param(
    [Parameter(Mandatory = $true)][string[]]$Args
  )

  $preferDocker = $UseDockerPsql.IsPresent

  if ($preferDocker) {
    Assert-CommandExists -Name "docker"
    $cmd = @("run", "--rm", "postgres:16-alpine", "psql") + $Args
    & docker @cmd
    if (-not $?) { throw "psql (docker) failed." }
    return
  }

  Assert-CommandExists -Name "psql"
  & psql @Args
  if (-not $?) { throw "psql failed." }
}

if (-not $Dsn) {
  $DsnFromEnv = Get-EnvValue -Path (Join-Path $RepoRoot ".env") -Key "POSTGRES_DSN"
  if (-not $DsnFromEnv) {
    $DsnFromEnv = $env:POSTGRES_DSN
    if (-not $DsnFromEnv) {
      $DsnFromEnv = Get-EnvValue -Path (Join-Path (Split-Path $PSScriptRoot) ".env") -Key "POSTGRES_DSN"
    }
  }
  if ($DsnFromEnv) {
    $Dsn = $DsnFromEnv
  }
}

if (-not $Dsn) {
  throw "POSTGRES_DSN is not configured. Set POSTGRES_DSN in `.env` or pass -Dsn."
}

if (-not (Is-CompleteDsn -Value $Dsn)) {
  throw "Invalid or incomplete POSTGRES_DSN: '$Dsn'. Expected full DSN format, e.g. postgresql://user:pass@host:5432/db?sslmode=require"
}

$resolvedDir = (Resolve-Path $MigrationsDir -ErrorAction Stop).Path
Write-Host "Applying migrations from: $resolvedDir"

$files = Get-ChildItem -Path $resolvedDir -File -Filter "*.sql" | Sort-Object Name
if (-not $files) {
  throw "No migration files found in $resolvedDir"
}

foreach ($file in $files) {
  Write-Host "Applying: $($file.Name)"
  Invoke-Psql -Args @("$Dsn", "-v", "ON_ERROR_STOP=1", "-f", $file.FullName)
}

Write-Host "Migrations applied successfully."

param(
  [Parameter(Mandatory = $true)]
  [string]$Dsn,

  [string]$MigrationsDir = "deploy/sql/postgres/migrations",
  [switch]$UseDockerPsql
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

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
    $cmd = @("run", "--rm", "postgres:16-alpine", "psql") + $Args
    & docker @cmd
    if (-not $?) { throw "psql (docker) failed." }
    return
  }

  Assert-CommandExists -Name "psql"
  & psql @Args
  if (-not $?) { throw "psql failed." }
}

$resolvedDir = (Resolve-Path $MigrationsDir -ErrorAction Stop).Path
Write-Host "Applying migrations from: $resolvedDir"

$files = Get-ChildItem -Path $resolvedDir -File -Filter "*.sql" | Sort-Object Name
if (-not $files) {
  throw "No migration files found in $resolvedDir"
}

# Ensure schema_migrations exists (migration 0001 should create it, but we need it to track application state).
Invoke-Psql -Args @("$Dsn", "-v", "ON_ERROR_STOP=1", "-c", "CREATE TABLE IF NOT EXISTS public.schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now());")

foreach ($file in $files) {
  $version = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
  $applied = Invoke-Psql -Args @("$Dsn", "-tA", "-c", "SELECT 1 FROM public.schema_migrations WHERE version='${version}' LIMIT 1;") 2>$null
  if ($applied -match "1") {
    Write-Host "Already applied: $version"
    continue
  }

  Write-Host "Applying: $version"
  Invoke-Psql -Args @("$Dsn", "-v", "ON_ERROR_STOP=1", "-f", $file.FullName)
  Invoke-Psql -Args @("$Dsn", "-v", "ON_ERROR_STOP=1", "-c", "INSERT INTO public.schema_migrations(version) VALUES ('${version}') ON CONFLICT DO NOTHING;")
}

Write-Host "Migrations applied successfully."

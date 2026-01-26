param(
  # Note: some subscriptions are restricted from provisioning Postgres Flexible Server in certain regions (e.g., eastus).
  [string]$SubscriptionId = "",
  [string]$DotEnvPath = "",
  [string]$Location = "eastus",
  # If provisioning fails due to a restricted region, retry creation in these locations (in order).
  # Example: -Location "eastus" -LocationFallback @("eastus2","centralus","westus2")
  [string[]]$LocationFallback = @("eastus2", "centralus", "westus2"),
  [string]$ResourceGroup = "AssetAllocationRG",
  [string]$ServerName = "pg-asset-allocation",

  [string]$DatabaseName = "asset_allocation",
  [string]$AdminUser = "assetallocadmin",
  [string]$AdminPassword = "mysupersecretpassword1234$",

  [switch]$ApplyMigrations,
  [switch]$UseDockerPsql,
  [switch]$CreateAppUsers,
  [string]$RankingWriterUser = "ranking_writer",
  [string]$RankingWriterPassword = $AdminPassword,
  [string]$ApiServiceUser = "api_service",
  [string]$ApiServicePassword = $AdminPassword,

  # Burstable SKUs (standard_b*) require `--tier Burstable`.
  [string]$SkuName = "standard_b1ms",
  [ValidateSet("", "Burstable", "GeneralPurpose", "MemoryOptimized")]
  [string]$Tier = "",
  [ValidateRange(32, 16384)]
  [int]$StorageSizeGiB = 32,
  [ValidateSet("14", "15", "16")]
  [string]$PostgresVersion = "16",

  # Cost-minimizing baseline: public endpoint enabled, restricted by firewall rules.
  # - "None" keeps public access but does not create any default firewall rule.
  # - Add firewall rules explicitly via -AllowAzureServices / -AllowIpRangeStart/-End.
  [ValidateSet("Disabled", "Enabled", "All", "None")]
  [string]$PublicAccess = "Enabled",

  [bool]$AllowAzureServices = $true,
  [string]$AllowIpRangeStart = "",
  [string]$AllowIpRangeEnd = "",
  [bool]$AllowCurrentClientIp = $true,

  [switch]$EmitSecrets
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Get-PublicIp {
  try {
    $ip = (Invoke-WebRequest -Uri "https://api.ipify.org" -UseBasicParsing).Content.Trim()
    Write-Host "Detected public IP: $ip"
    return $ip
  }
  catch {
    Write-Warning "Failed to detect public IP: $_"
    return $null
  }
}


function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command '$Name'. Install it and retry."
  }
}

function Read-DotEnvValue {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][string]$Key
  )

  if (-not (Test-Path -Path $Path)) {
    return ""
  }

  foreach ($line in (Get-Content -Path $Path -ErrorAction SilentlyContinue)) {
    if (-not $line) { continue }
    $t = $line.Trim()
    if (-not $t) { continue }
    if ($t.StartsWith("#")) { continue }
    if ($t.StartsWith("export ")) { $t = $t.Substring(7).Trim() }

    $idx = $t.IndexOf("=")
    if ($idx -lt 1) { continue }

    $k = $t.Substring(0, $idx).Trim()
    if ($k -ne $Key) { continue }

    $v = $t.Substring($idx + 1).Trim()
    if (-not $v) { return "" }

    $isSingleQuoted = $v.StartsWith("'") -and $v.EndsWith("'")
    $isDoubleQuoted = $v.StartsWith('"') -and $v.EndsWith('"')

    if (-not ($isSingleQuoted -or $isDoubleQuoted)) {
      # Treat leading '#' as an inline comment (common in .env templates: KEY= # comment).
      if ($v.StartsWith("#")) { return "" }

      # Strip inline comments for unquoted values: KEY=value # comment
      $m = [regex]::Match($v, '^(.*?)\s+#')
      if ($m.Success) { $v = $m.Groups[1].Value.TrimEnd() }
    }
    elseif ($v.Length -ge 2) {
      $v = $v.Substring(1, $v.Length - 2)
    }

    return $v.Trim()
  }

  return ""
}

function Invoke-Az {
  param(
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $true)][string[]]$Args
  )
  & az @Args
  if (-not $?) {
    throw "Azure CLI command failed: $Label"
  }
}

function Invoke-AzCapture {
  param(
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $true)][string[]]$Args
  )
  $output = & az @Args 2>&1
  return [pscustomobject]@{
    Label   = $Label
    Success = [bool]$?
    Output  = ($output | Out-String)
  }
}

function Test-AzRegionRestrictedError {
  param([string]$Text)
  if (-not $Text) { return $false }
  return (
    ($Text -match "location is restricted for provisioning of flexible servers") -or
    ($Text -match "Postgres Flexible Server provisioning is restricted") -or
    ($Text -match "Please try using another region")
  )
}

function Test-AzAlreadyExistsError {
  param([string]$Text)
  if (-not $Text) { return $false }
  return (
    ($Text -match "(?i)already exists") -or
    ($Text -match "(?i)resourcealreadyexists") -or
    ($Text -match "(?i)conflict")
  )
}

function New-RandomPassword {
  param([int]$Length = 32)
  $bytes = New-Object byte[] ($Length)
  [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
  # Base64 can include '+' and '/', which are allowed by Postgres password rules but can be awkward in shells.
  # Keep it URL-safe-ish and ensure we have multiple character classes.
  $raw = [Convert]::ToBase64String($bytes)
  $raw = $raw.Replace("+", "A").Replace("/", "b").Replace("=", "c")
  return $raw.Substring(0, [Math]::Min($raw.Length, $Length))
}

function Assert-PgIdentifier {
  param(
    [Parameter(Mandatory = $true)][string]$Value,
    [Parameter(Mandatory = $true)][string]$Label
  )
  $text = $Value
  if ($null -eq $text) { $text = "" }
  $text = $text.Trim()
  if (-not $text) { throw "$Label must be non-empty." }
  if ($text -notmatch '^[a-z][a-z0-9_]{0,62}$') {
    throw "$Label must match ^[a-z][a-z0-9_]{0,62}$ (got '$Value'). Use lowercase letters, digits, and underscores only."
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

function Resolve-PostgresTier {
  param(
    [Parameter(Mandatory = $true)][string]$SkuName,
    [string]$TierOverride
  )
  if ($TierOverride) { return $TierOverride }

  $sku = $SkuName.ToLowerInvariant().Trim()
  if ($sku.StartsWith("standard_b")) { return "Burstable" }
  if ($sku.StartsWith("standard_d")) { return "GeneralPurpose" }
  if ($sku.StartsWith("standard_e")) { return "MemoryOptimized" }
  return "GeneralPurpose"
}

Assert-CommandExists -Name "az"

Assert-PgIdentifier -Value $DatabaseName -Label "DatabaseName"
Assert-PgIdentifier -Value $RankingWriterUser -Label "RankingWriterUser"
Assert-PgIdentifier -Value $ApiServiceUser -Label "ApiServiceUser"

$SkuName = $SkuName.ToLowerInvariant().Trim()
$selectedLocation = $Location

if (-not $SubscriptionId) {
  $effectiveDotEnvPath = if ($DotEnvPath) { $DotEnvPath } else { Join-Path (Join-Path $PSScriptRoot "..") ".env" }
  $SubscriptionId = Read-DotEnvValue -Path $effectiveDotEnvPath -Key "SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID"
  if (-not $SubscriptionId) { $SubscriptionId = Read-DotEnvValue -Path $effectiveDotEnvPath -Key "AZURE_SUBSCRIPTION_ID" }
  if (-not $SubscriptionId) { $SubscriptionId = Read-DotEnvValue -Path $effectiveDotEnvPath -Key "SUBSCRIPTION_ID" }
}

if (-not $SubscriptionId) {
  throw "Missing SubscriptionId. Pass -SubscriptionId or set SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID (or AZURE_SUBSCRIPTION_ID or SUBSCRIPTION_ID) in .env."
}
Write-Host "Using subscription: $SubscriptionId"
Invoke-Az -Label "account set" -Args @("account", "set", "--subscription", $SubscriptionId, "--only-show-errors")

Write-Host "Ensuring required Azure resource providers are registered..."
$providers = @(
  "Microsoft.DBforPostgreSQL",
  "Microsoft.Network"
)
foreach ($p in $providers) {
  Invoke-Az -Label "provider register $p" -Args @("provider", "register", "--namespace", $p, "--only-show-errors", "-o", "none")
}

Write-Host "Ensuring resource group exists: $ResourceGroup ($Location)"
Invoke-Az -Label "group create" -Args @("group", "create", "--name", $ResourceGroup, "--location", $Location, "--only-show-errors", "-o", "none")

Write-Host "Ensuring Postgres Flexible Server exists: $ServerName"
$serverExists = $false
$serverShow = Invoke-AzCapture -Label "postgres flexible-server show" -Args @(
  "postgres", "flexible-server", "show",
  "--name", $ServerName,
  "--resource-group", $ResourceGroup,
  "--only-show-errors",
  "-o", "none"
)
if ($serverShow.Success) { $serverExists = $true }

if (-not $serverExists) {
  if (-not $AdminPassword) {
    $AdminPassword = New-RandomPassword -Length 32
  }

  $effectiveTier = Resolve-PostgresTier -SkuName $SkuName -TierOverride $Tier
  Write-Host "Resolved Postgres compute: sku=$SkuName tier=$effectiveTier (tierOverride='$Tier')"

  if ($SkuName.StartsWith("standard_b") -and ($effectiveTier -ne "Burstable")) {
    throw "Invalid tier '$effectiveTier' for Burstable SKU '$SkuName'. Use -Tier Burstable (or omit -Tier)."
  }

  $candidateLocations = @($Location) + $LocationFallback
  $candidateLocations = $candidateLocations |
  ForEach-Object { if ($null -eq $_) { "" } else { $_.Trim() } } |
  Where-Object { $_ } |
  Select-Object -Unique

  $created = $false
  foreach ($loc in $candidateLocations) {
    Write-Host "Attempting Postgres Flexible Server create in region: $loc (sku=$SkuName, tier=$effectiveTier)"
    $result = Invoke-AzCapture -Label "postgres flexible-server create ($loc)" -Args @(
      "postgres", "flexible-server", "create",
      "--name", $ServerName,
      "--resource-group", $ResourceGroup,
      "--location", $loc,
      "--version", $PostgresVersion,
      "--tier", $effectiveTier,
      "--sku-name", $SkuName,
      "--storage-size", "$StorageSizeGiB",
      "--admin-user", $AdminUser,
      "--admin-password", $AdminPassword,
      "--public-access", $PublicAccess,
      "--high-availability", "Disabled",
      "--backup-retention", "7",
      "--yes",
      "--only-show-errors",
      "-o", "none"
    )

    if ($result.Success) {
      $created = $true
      $selectedLocation = $loc
      break
    }

    if ((Test-AzRegionRestrictedError -Text $result.Output) -and ($loc -ne $candidateLocations[-1])) {
      Write-Host "Region '$loc' appears restricted for Postgres provisioning; retrying in next fallback region..."
      continue
    }

    throw ("Azure CLI command failed: $($result.Label)`n$($result.Output)")
  }

  if (-not $created) {
    throw "Failed to create Postgres Flexible Server '$ServerName' in any candidate region: $($candidateLocations -join ', ')"
  }
}
else {
  Write-Host "Server already exists; skipping create."
}

$requiresDbAdminAuth = [bool]($ApplyMigrations -or $CreateAppUsers)
if ($requiresDbAdminAuth -and (-not $AdminPassword)) {
  throw (
    "Server '$ServerName' already exists, but -AdminPassword was not provided. " +
    "Provide the existing admin password (or reset it via Azure) to run -ApplyMigrations/-CreateAppUsers."
  )
}

Write-Host "Ensuring database exists: $DatabaseName"
$dbShow = Invoke-AzCapture -Label "postgres flexible-server db show" -Args @(
  "postgres", "flexible-server", "db", "show",
  "--resource-group", $ResourceGroup,
  "--server-name", $ServerName,
  "--database-name", $DatabaseName,
  "--only-show-errors",
  "-o", "none"
)
if ($dbShow.Success) {
  Write-Host "Database already exists; skipping create."
}
else {
  $dbCreate = Invoke-AzCapture -Label "postgres flexible-server db create" -Args @(
    "postgres", "flexible-server", "db", "create",
    "--resource-group", $ResourceGroup,
    "--server-name", $ServerName,
    "--database-name", $DatabaseName,
    "--only-show-errors",
    "-o", "none"
  )
  if (-not $dbCreate.Success) {
    if (Test-AzAlreadyExistsError -Text $dbCreate.Output) {
      Write-Host "Database already exists; skipping create."
    }
    else {
      throw ("Azure CLI command failed: $($dbCreate.Label)`n$($dbCreate.Output)")
    }
  }
}

if ($AllowAzureServices) {
  Write-Host "Ensuring firewall rule allows Azure services (0.0.0.0)..."
  Invoke-Az -Label "postgres flexible-server firewall-rule create allow-azure-services" -Args @(
      "postgres", "flexible-server", "firewall-rule", "create",
      "--resource-group", $ResourceGroup,
      "--name", $ServerName,
      "--rule-name", "allow-azure-services",
      "--start-ip-address", "0.0.0.0",
      "--end-ip-address", "0.0.0.0",
      "--only-show-errors",
      "-o", "none"
    )
}

if ($AllowIpRangeStart) {
  $end = if ($AllowIpRangeEnd) { $AllowIpRangeEnd } else { $AllowIpRangeStart }
  Write-Host "Ensuring firewall rule allows IP range: $AllowIpRangeStart - $end"
  $fwShow = Invoke-AzCapture -Label "postgres flexible-server firewall-rule show allow-custom-ip-range" -Args @(
    "postgres", "flexible-server", "firewall-rule", "show",
    "--resource-group", $ResourceGroup,
    "--name", $ServerName,
    "--rule-name", "allow-custom-ip-range",
    "--only-show-errors",
    "-o", "none"
  )
  if (-not $fwShow.Success) {
    Invoke-Az -Label "postgres flexible-server firewall-rule create allow-custom-ip-range" -Args @(
      "postgres", "flexible-server", "firewall-rule", "create",
      "--resource-group", $ResourceGroup,
      "--name", $ServerName,
      "--rule-name", "allow-custom-ip-range",
      "--start-ip-address", $AllowIpRangeStart,
      "--end-ip-address", $end,
      "--only-show-errors",
      "-o", "none"
    )
  }
  else {
    Write-Host "Firewall rule already exists; skipping."
  }
}

if ($AllowCurrentClientIp) {
  $myIp = Get-PublicIp
  if ($myIp) {
    Write-Host "Ensuring firewall rule allows current client IP ($myIp)..."
    Invoke-Az -Label "postgres flexible-server firewall-rule create allow-current-client-ip" -Args @(
      "postgres", "flexible-server", "firewall-rule", "create",
      "--resource-group", $ResourceGroup,
      "--name", $ServerName,
      "--rule-name", "allow-current-client-ip",
      "--start-ip-address", $myIp,
      "--end-ip-address", $myIp,
      "--only-show-errors",
      "-o", "none"
    )
  }
}

$fqdn = & az postgres flexible-server show --name $ServerName --resource-group $ResourceGroup --only-show-errors --query fullyQualifiedDomainName -o tsv 2>$null
if ($null -eq $fqdn) { $fqdn = "" }
$fqdn = $fqdn.Trim()
if (-not $fqdn) {
  # Fallback that doesn't depend on `az` query output.
  $fqdn = "$ServerName.postgres.database.azure.com"
}

$adminDsn = ""
if ($AdminPassword) {
  $adminDsn = "postgresql://$AdminUser`:$AdminPassword@$fqdn`:5432/${DatabaseName}?sslmode=require"
}

if ($ApplyMigrations) {
  Write-Host "Applying repo-owned migrations..."
  & "$PSScriptRoot/apply_postgres_migrations.ps1" -Dsn $adminDsn -UseDockerPsql:$UseDockerPsql
}

if ($CreateAppUsers) {
  if (-not $RankingWriterPassword) { $RankingWriterPassword = New-RandomPassword -Length 32 }
  if (-not $ApiServicePassword) { $ApiServicePassword = New-RandomPassword -Length 32 }

  Write-Host "Creating least-privileged application roles..."

  $sql = @"
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '$RankingWriterUser') THEN
    CREATE ROLE $RankingWriterUser LOGIN PASSWORD '$RankingWriterPassword';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '$ApiServiceUser') THEN
    CREATE ROLE $ApiServiceUser LOGIN PASSWORD '$ApiServicePassword';
  END IF;
END
\$\$;

ALTER ROLE $RankingWriterUser WITH PASSWORD '$RankingWriterPassword';
ALTER ROLE $ApiServiceUser WITH PASSWORD '$ApiServicePassword';

GRANT CONNECT ON DATABASE $DatabaseName TO $RankingWriterUser, $ApiServiceUser;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS ranking;
CREATE SCHEMA IF NOT EXISTS backtest;
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS platinum;

GRANT USAGE, CREATE ON SCHEMA ranking TO $RankingWriterUser;
GRANT USAGE, CREATE ON SCHEMA gold TO $RankingWriterUser;
GRANT USAGE, CREATE ON SCHEMA platinum TO $RankingWriterUser;

GRANT USAGE ON SCHEMA core TO $RankingWriterUser;
GRANT USAGE ON SCHEMA monitoring TO $RankingWriterUser;

GRANT USAGE ON SCHEMA core TO $ApiServiceUser;
GRANT USAGE ON SCHEMA ranking TO $ApiServiceUser;
GRANT USAGE ON SCHEMA backtest TO $ApiServiceUser;
GRANT USAGE ON SCHEMA monitoring TO $ApiServiceUser;
GRANT USAGE ON SCHEMA gold TO $ApiServiceUser;
GRANT USAGE ON SCHEMA platinum TO $ApiServiceUser;

-- Ensure grants work regardless of whether roles existed when migrations ran.
GRANT SELECT ON ALL TABLES IN SCHEMA core TO $RankingWriterUser, $ApiServiceUser;
GRANT SELECT ON ALL TABLES IN SCHEMA monitoring TO $RankingWriterUser;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ranking TO $RankingWriterUser;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gold TO $RankingWriterUser;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA platinum TO $RankingWriterUser;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA ranking TO $RankingWriterUser;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA gold TO $RankingWriterUser;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA platinum TO $RankingWriterUser;

GRANT SELECT ON ALL TABLES IN SCHEMA ranking TO $ApiServiceUser;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO $ApiServiceUser;
GRANT SELECT ON ALL TABLES IN SCHEMA platinum TO $ApiServiceUser;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA backtest TO $ApiServiceUser;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA monitoring TO $ApiServiceUser;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA gold TO $ApiServiceUser;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA platinum TO $ApiServiceUser;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA backtest TO $ApiServiceUser;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA monitoring TO $ApiServiceUser;

ALTER DEFAULT PRIVILEGES IN SCHEMA ranking GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $RankingWriterUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA ranking GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO $RankingWriterUser;

ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO $RankingWriterUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO $ApiServiceUser;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $RankingWriterUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO $RankingWriterUser;

ALTER DEFAULT PRIVILEGES IN SCHEMA platinum GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $RankingWriterUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA platinum GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO $RankingWriterUser;

ALTER DEFAULT PRIVILEGES IN SCHEMA ranking GRANT SELECT ON TABLES TO $ApiServiceUser;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO $ApiServiceUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO $ApiServiceUser;

ALTER DEFAULT PRIVILEGES IN SCHEMA platinum GRANT SELECT ON TABLES TO $ApiServiceUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA platinum GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO $ApiServiceUser;

ALTER DEFAULT PRIVILEGES IN SCHEMA backtest GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $ApiServiceUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA backtest GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO $ApiServiceUser;

ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring GRANT SELECT ON TABLES TO $RankingWriterUser;

ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $ApiServiceUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO $ApiServiceUser;
"@

  Invoke-Psql -Args @($adminDsn, "-v", "ON_ERROR_STOP=1", "-c", $sql)
}

$rankingWriterDsn = ""
$apiServiceDsn = ""
if ($CreateAppUsers) {
  $rankingWriterDsn = "postgresql://$RankingWriterUser`:$RankingWriterPassword@$fqdn`:5432/${DatabaseName}?sslmode=require"
  $apiServiceDsn = "postgresql://$ApiServiceUser`:$ApiServicePassword@$fqdn`:5432/${DatabaseName}?sslmode=require"
}

$outputs = [ordered]@{
  subscriptionId    = $SubscriptionId
  location          = $selectedLocation
  resourceGroup     = $ResourceGroup
  serverName        = $ServerName
  serverFqdn        = $fqdn
  databaseName      = $DatabaseName
  adminUser         = $AdminUser
  adminPassword     = if ($EmitSecrets) { $AdminPassword } else { "<redacted>" }
  appUsers          = if ($CreateAppUsers) {
    [ordered]@{
      rankingWriterUser       = $RankingWriterUser
      rankingWriterPassword   = if ($EmitSecrets) { $RankingWriterPassword } else { "<redacted>" }
      apiServiceUser          = $ApiServiceUser
      apiServicePassword      = if ($EmitSecrets) { $ApiServicePassword } else { "<redacted>" }
    }
  }
  else {
    "<not_created>"
  }
  connectionStrings = if ($EmitSecrets) {
    [ordered]@{
      adminDsn           = if ($adminDsn) { $adminDsn } else { "<unavailable>" }
      rankingWriterDsn   = if ($rankingWriterDsn) { $rankingWriterDsn } else { "<not_created>" }
      apiServiceDsn      = if ($apiServiceDsn) { $apiServiceDsn } else { "<not_created>" }
    }
  }
  else {
    "<redacted>"
  }
}

Write-Host ""
Write-Host "Provisioning complete. Outputs:"
$outputs | ConvertTo-Json -Depth 6

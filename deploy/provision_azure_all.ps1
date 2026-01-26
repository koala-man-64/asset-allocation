param(
  [string]$SubscriptionId = "",
  [string]$DotEnvPath = "",

  [switch]$SkipInfra,
  [switch]$SkipPostgres,

  # Infra (matches deploy/provision_azure.ps1)
  [string]$Location = "eastus",
  [string]$ResourceGroup = "AssetAllocationRG",
  [string]$StorageAccountName = "assetallocstorage001",
  [string[]]$StorageContainers = @(
    "bronze",
    "silver",
    "gold",
    "platinum",
    "common"
  ),
  [string]$AcrName = "assetallocationacr",
  [switch]$EnableAcrAdmin,
  [switch]$EmitSecrets,
  [switch]$GrantAcrPullToAcaResources,
  [string]$LogAnalyticsWorkspaceName = "asset-allocation-law",
  [string]$ContainerAppsEnvironmentName = "asset-allocation-env",

  # Postgres (matches deploy/provision_azure_postgres.ps1)
  [string[]]$LocationFallback = @("eastus2", "centralus", "westus2"),
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

  [string]$SkuName = "standard_b1ms",
  [ValidateSet("", "Burstable", "GeneralPurpose", "MemoryOptimized")]
  [string]$Tier = "",
  [ValidateRange(32, 16384)]
  [int]$StorageSizeGiB = 32,
  [ValidateSet("14", "15", "16")]
  [string]$PostgresVersion = "16",

  [ValidateSet("Disabled", "Enabled", "All", "None")]
  [string]$PublicAccess = "Enabled",
  [bool]$AllowAzureServices = $true,
  [string]$AllowIpRangeStart = "",
  [string]$AllowIpRangeEnd = "",
  [bool]$AllowCurrentClientIp = $true
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

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
      if ($v.StartsWith("#")) { return "" }
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

Assert-CommandExists -Name "az"

$effectiveDotEnvPath = if ($DotEnvPath) { $DotEnvPath } else { Join-Path (Join-Path $PSScriptRoot "..") ".env" }

if (-not $SubscriptionId) {
  $SubscriptionId = Read-DotEnvValue -Path $effectiveDotEnvPath -Key "SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID"
  if (-not $SubscriptionId) { $SubscriptionId = Read-DotEnvValue -Path $effectiveDotEnvPath -Key "AZURE_SUBSCRIPTION_ID" }
  if (-not $SubscriptionId) { $SubscriptionId = Read-DotEnvValue -Path $effectiveDotEnvPath -Key "SUBSCRIPTION_ID" }
}

if (-not $SubscriptionId) {
  throw "Missing SubscriptionId. Pass -SubscriptionId or set SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID (or AZURE_SUBSCRIPTION_ID or SUBSCRIPTION_ID) in .env."
}

if (-not $SkipInfra) {
  Write-Host ""
  Write-Host "== Provisioning Azure core infrastructure (RG/Storage/ACR/LAW/ACA Env) =="
  & (Join-Path $PSScriptRoot "provision_azure.ps1") `
    -SubscriptionId $SubscriptionId `
    -DotEnvPath $effectiveDotEnvPath `
    -Location $Location `
    -ResourceGroup $ResourceGroup `
    -StorageAccountName $StorageAccountName `
    -StorageContainers $StorageContainers `
    -AcrName $AcrName `
    -EnableAcrAdmin:$EnableAcrAdmin `
    -EmitSecrets:$EmitSecrets `
    -GrantAcrPullToAcaResources:$GrantAcrPullToAcaResources `
    -LogAnalyticsWorkspaceName $LogAnalyticsWorkspaceName `
    -ContainerAppsEnvironmentName $ContainerAppsEnvironmentName
}

if (-not $SkipPostgres) {
  Write-Host ""
  Write-Host "== Provisioning Azure Postgres Flexible Server =="
  & (Join-Path $PSScriptRoot "provision_azure_postgres.ps1") `
    -SubscriptionId $SubscriptionId `
    -DotEnvPath $effectiveDotEnvPath `
    -Location $Location `
    -LocationFallback $LocationFallback `
    -ResourceGroup $ResourceGroup `
    -ServerName $ServerName `
    -DatabaseName $DatabaseName `
    -AdminUser $AdminUser `
    -AdminPassword $AdminPassword `
    -ApplyMigrations:$ApplyMigrations `
    -UseDockerPsql:$UseDockerPsql `
    -CreateAppUsers:$CreateAppUsers `
    -RankingWriterUser $RankingWriterUser `
    -RankingWriterPassword $RankingWriterPassword `
    -ApiServiceUser $ApiServiceUser `
    -ApiServicePassword $ApiServicePassword `
    -SkuName $SkuName `
    -Tier $Tier `
    -StorageSizeGiB $StorageSizeGiB `
    -PostgresVersion $PostgresVersion `
    -PublicAccess $PublicAccess `
    -AllowAzureServices:$AllowAzureServices `
    -AllowIpRangeStart $AllowIpRangeStart `
    -AllowIpRangeEnd $AllowIpRangeEnd `
    -AllowCurrentClientIp:$AllowCurrentClientIp `
    -EmitSecrets:$EmitSecrets
}

if ($SkipInfra -and $SkipPostgres) {
  Write-Warning "Nothing to do: both -SkipInfra and -SkipPostgres were set."
}

param(
  [string]$SubscriptionId = "",
  [string]$DotEnvPath = "",

  [string]$Location = "eastus",
  [string]$ResourceGroup = "AssetAllocationRG",

  [string]$StorageAccountName = "assetallocstorage001",

  [string[]]$StorageContainers = @(),
  [string]$AcrName = "assetallocationacr",
  [switch]$EnableAcrAdmin,
  [switch]$EmitSecrets,
  [switch]$GrantAcrPullToAcaResources,

  [string]$LogAnalyticsWorkspaceName = "asset-allocation-law",
  [string]$ContainerAppsEnvironmentName = "asset-allocation-env",
  [string]$AzureClientId = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# Load containers from .env if not specified
if ($StorageContainers.Count -eq 0) {
    $envPath = "$PSScriptRoot\..\.env"
    if (Test-Path $envPath) {
        Write-Host "Reading container names from .env..."
        $envLines = Get-Content $envPath
        $containers = @()
        foreach ($line in $envLines) {
            if ($line -match "^AZURE_CONTAINER_[^=]+=(.*)$") {
                $val = $matches[1].Trim('"').Trim("'")
                Write-Host "Found container: $val" -ForegroundColor Cyan
                $containers += $val
            }
        }
        if ($containers.Count -gt 0) {
            $StorageContainers = $containers | Select-Object -Unique
        }
    }
}

# If still empty, fall back to defaults (or error? original script had defaults)
if ($StorageContainers.Count -eq 0) {
    Write-Warning "No containers found in .env and none provided. Using defaults."
    $StorageContainers = @("bronze", "silver", "gold", "platinum", "common")
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

Assert-CommandExists -Name "az"

if (-not $SubscriptionId) {
  $effectiveDotEnvPath = if ($DotEnvPath) { $DotEnvPath } else { Join-Path (Join-Path $PSScriptRoot "..") ".env" }
  $SubscriptionId = Read-DotEnvValue -Path $effectiveDotEnvPath -Key "SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID"
  if (-not $SubscriptionId) { $SubscriptionId = Read-DotEnvValue -Path $effectiveDotEnvPath -Key "AZURE_SUBSCRIPTION_ID" }
  if (-not $SubscriptionId) { $SubscriptionId = Read-DotEnvValue -Path $effectiveDotEnvPath -Key "SUBSCRIPTION_ID" }
}

if (-not $SubscriptionId) {
  throw "Missing SubscriptionId. Pass -SubscriptionId or set SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID (or AZURE_SUBSCRIPTION_ID or SUBSCRIPTION_ID) in .env."
}

if ($AzureClientId) {
    Write-Host "Checking for existing Federated Credential 'github-actions-production'..."
    $paramsFile = "credential.json"
    $subject = "repo:koala-man-64/asset-allocation:environment:production"
    
    # Check if exists
    $creds = az ad app federated-credential list --id $AzureClientId --query "[?name=='github-actions-production']" -o json | ConvertFrom-Json
    
    if (-not $creds) {
        Write-Host "Creating Federated Credential 'github-actions-production'..."
        $json = @{
            name = "github-actions-production"
            issuer = "https://token.actions.githubusercontent.com"
            subject = $subject
            description = "GitHub Actions Production Environment"
            audiences = @("api://AzureADTokenExchange")
        } | ConvertTo-Json -Compress

        Set-Content -Path $paramsFile -Value $json
        
        try {
            az ad app federated-credential create --id $AzureClientId --parameters $paramsFile 2>&1
            Write-Host "Successfully created federated credential."
        }
        catch {
            Write-Error "Failed to create federated credential: $_"
            if (Test-Path $paramsFile) { Remove-Item $paramsFile }
            throw
        }
        
        if (Test-Path $paramsFile) { Remove-Item $paramsFile }
    } else {
        Write-Host "Federated Credential 'github-actions-production' already exists."
    }
}

Write-Host "Using subscription: $SubscriptionId"
az account set --subscription $SubscriptionId | Out-Null

Write-Host "Ensuring required Azure resource providers are registered..."
$providers = @(
  "Microsoft.Storage",
  "Microsoft.ContainerRegistry",
  "Microsoft.ManagedIdentity",
  "Microsoft.OperationalInsights",
  "Microsoft.App"
)
foreach ($p in $providers) {
  az provider register --namespace $p | Out-Null
}

Write-Host "Ensuring Azure CLI extensions are installed..."
az extension add --name containerapp --upgrade --only-show-errors | Out-Null

Write-Host "Ensuring resource group exists: $ResourceGroup ($Location)"
az group create --name $ResourceGroup --location $Location --only-show-errors | Out-Null

Write-Host "Ensuring storage account exists: $StorageAccountName"
$existingStorage = $null
try {
  $existingStorage = az storage account show `
    --name $StorageAccountName `
    --resource-group $ResourceGroup `
    --only-show-errors -o json 2>$null | ConvertFrom-Json
}
catch {
  $existingStorage = $null
}

if ($null -eq $existingStorage) {
  $foundInSubscription = $null
  try {
    $foundInSubscription = az storage account show `
      --name $StorageAccountName `
      --only-show-errors -o json 2>$null | ConvertFrom-Json
  }
  catch {
    $foundInSubscription = $null
  }

  if ($null -ne $foundInSubscription) {
    throw "Storage account '$StorageAccountName' already exists in resource group '$($foundInSubscription.resourceGroup)'. Set -ResourceGroup to that value or choose a new -StorageAccountName."
  }

  $nameAvailable = az storage account check-name --name $StorageAccountName --query nameAvailable -o tsv --only-show-errors
  if ($nameAvailable -ne "true") {
    throw "Storage account name '$StorageAccountName' is not available. Choose a different -StorageAccountName."
  }

  az storage account create `
    --name $StorageAccountName `
    --resource-group $ResourceGroup `
    --location $Location `
    --sku Standard_LRS `
    --kind StorageV2 `
    --https-only true `
    --min-tls-version TLS1_2 `
    --allow-blob-public-access false `
    --hns true `
    --only-show-errors | Out-Null
}
else {
  if (-not [bool]$existingStorage.isHnsEnabled) {
    Write-Warning "Storage account '$StorageAccountName' exists but Hierarchical Namespace (HNS) is disabled. This cannot be enabled after creation; continuing without updating HNS. To use ADLS Gen2, create a new storage account (or delete & recreate) with --hns true."
  }

  az storage account update `
    --name $StorageAccountName `
    --resource-group $ResourceGroup `
    --https-only true `
    --min-tls-version TLS1_2 `
    --allow-blob-public-access false `
    --only-show-errors | Out-Null
}

Write-Host "Creating blob containers (auth-mode=login)..."
foreach ($c in $StorageContainers) {
  if (-not $c) { continue }
  az storage container create --name $c --account-name $StorageAccountName --auth-mode login --only-show-errors | Out-Null
}

Write-Host "Ensuring ACR exists: $AcrName"
$acrAdmin = if ($EnableAcrAdmin) { "true" } else { "false" }
az acr create `
  --name $AcrName `
  --resource-group $ResourceGroup `
  --location $Location `
  --sku Basic `
  --admin-enabled $acrAdmin `
  --only-show-errors | Out-Null

Write-Host "Ensuring Log Analytics workspace exists: $LogAnalyticsWorkspaceName"
az monitor log-analytics workspace create `
  --resource-group $ResourceGroup `
  --workspace-name $LogAnalyticsWorkspaceName `
  --location $Location `
  --only-show-errors | Out-Null

$lawCustomerId = az monitor log-analytics workspace show `
  --resource-group $ResourceGroup `
  --workspace-name $LogAnalyticsWorkspaceName `
  --query customerId -o tsv

$lawSharedKey = az monitor log-analytics workspace get-shared-keys `
  --resource-group $ResourceGroup `
  --workspace-name $LogAnalyticsWorkspaceName `
  --query primarySharedKey -o tsv

Write-Host "Ensuring Container Apps environment exists: $ContainerAppsEnvironmentName"
az containerapp env create `
  --name $ContainerAppsEnvironmentName `
  --resource-group $ResourceGroup `
  --location $Location `
  --logs-workspace-id $lawCustomerId `
  --logs-workspace-key $lawSharedKey `
  --only-show-errors | Out-Null

$storageConnectionString = ""
if ($EmitSecrets) {
  $storageConnectionString = az storage account show-connection-string `
    --name $StorageAccountName `
    --resource-group $ResourceGroup `
    --query connectionString -o tsv
}

$acrLoginServer = az acr show --name $AcrName --resource-group $ResourceGroup --query loginServer -o tsv
$acrId = az acr show --name $AcrName --resource-group $ResourceGroup --query id -o tsv --only-show-errors

function Ensure-AcrPullRoleAssignment {
  param(
    [Parameter(Mandatory = $true)][string]$PrincipalId,
    [Parameter(Mandatory = $true)][string]$Scope
  )

  if (-not $PrincipalId -or $PrincipalId -eq "None") {
    return $false
  }

  $existing = "0"
  try {
    $existing = az role assignment list `
      --assignee $PrincipalId `
      --scope $Scope `
      --query "[?roleDefinitionName=='AcrPull'] | length(@)" -o tsv --only-show-errors 2>$null
    if (-not $existing) { $existing = "0" }
  }
  catch {
    $existing = "0"
  }

  if ([int]$existing -gt 0) {
    return $false
  }

  az role assignment create --assignee $PrincipalId --role "AcrPull" --scope $Scope --only-show-errors | Out-Null
  return $true
}

function Get-AcaPrincipalId {
  param(
    [Parameter(Mandatory = $true)][ValidateSet("app", "job")][string]$Kind,
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][string]$ResourceGroup,
    [int]$Retries = 10,
    [int]$DelaySeconds = 3
  )

  for ($i = 0; $i -lt $Retries; $i++) {
    $principalId = if ($Kind -eq "app") {
      az containerapp show --name $Name --resource-group $ResourceGroup --query identity.principalId -o tsv --only-show-errors 2>$null
    }
    else {
      az containerapp job show --name $Name --resource-group $ResourceGroup --query identity.principalId -o tsv --only-show-errors 2>$null
    }

    if ($principalId) { $principalId = $principalId.Trim() }
    if ($principalId -and $principalId -ne "None") {
      return $principalId
    }

    if ($i -lt ($Retries - 1)) {
      Start-Sleep -Seconds $DelaySeconds
    }
  }

  return ""
}

$acrPullAssignmentsCreated = 0
$acrPullAssignmentsSkipped = 0

if ($GrantAcrPullToAcaResources) {
  Write-Host ""
  Write-Host "Granting AcrPull on ACR to existing Container Apps + Jobs (best-effort)..."
  Write-Host "  ACR: $AcrName"
  Write-Host "  Scope: $acrId"

  $appNames = @()
  $jobNames = @()

  try {
    $appNames = @(az containerapp list --resource-group $ResourceGroup --query "[].name" -o tsv --only-show-errors)
  }
  catch {
    Write-Warning "Could not list Container Apps in RG '$ResourceGroup'."
  }

  foreach ($name in $appNames) {
    if (-not $name) { continue }
    try {
      $principalId = Get-AcaPrincipalId -Kind "app" -Name $name -ResourceGroup $ResourceGroup
      if (-not $principalId) {
        Write-Warning "No system-assigned identity principalId found (app '$name')."
        continue
      }
      if (Ensure-AcrPullRoleAssignment -PrincipalId $principalId -Scope $acrId) {
        $acrPullAssignmentsCreated += 1
        Write-Host "  AcrPull granted (app): $name"
      }
      else {
        $acrPullAssignmentsSkipped += 1
      }
    }
    catch {
      Write-Warning "Failed to grant AcrPull (app '$name'): $($_.Exception.Message)"
    }
  }

  try {
    $jobNames = @(az containerapp job list --resource-group $ResourceGroup --query "[].name" -o tsv --only-show-errors)
  }
  catch {
    Write-Warning "Could not list Container App Jobs in RG '$ResourceGroup'."
  }

  foreach ($name in $jobNames) {
    if (-not $name) { continue }
    try {
      $principalId = Get-AcaPrincipalId -Kind "job" -Name $name -ResourceGroup $ResourceGroup
      if (-not $principalId) {
        Write-Warning "No system-assigned identity principalId found (job '$name')."
        continue
      }
      if (Ensure-AcrPullRoleAssignment -PrincipalId $principalId -Scope $acrId) {
        $acrPullAssignmentsCreated += 1
        Write-Host "  AcrPull granted (job): $name"
      }
      else {
        $acrPullAssignmentsSkipped += 1
      }
    }
    catch {
      Write-Warning "Failed to grant AcrPull (job '$name'): $($_.Exception.Message)"
    }
  }

  Write-Host "AcrPull role assignment summary: created=$acrPullAssignmentsCreated skipped=$acrPullAssignmentsSkipped"
}
else {
  Write-Host ""
  Write-Host "NOTE: This repo's Container Apps/Jobs are configured to pull ACR images via managed identity."
  Write-Host "To grant pull permissions, re-run this script after deployment with -GrantAcrPullToAcaResources (requires RBAC permissions to create role assignments)."
}

$outputs = [ordered]@{
  subscriptionId               = $SubscriptionId
  location                     = $Location
  resourceGroup                = $ResourceGroup
  storageAccountName           = $StorageAccountName
  storageConnectionString      = if ($EmitSecrets) { $storageConnectionString } else { "<redacted>" }
  storageContainers            = $StorageContainers
  acrName                      = $AcrName
  acrId                        = $acrId
  acrLoginServer               = $acrLoginServer
  acrAdminEnabled              = [bool]$EnableAcrAdmin
  acrPullAuthMode              = "managedIdentity"
  acrPullAssignmentsCreated    = $acrPullAssignmentsCreated
  acrPullAssignmentsSkipped    = $acrPullAssignmentsSkipped
  logAnalyticsWorkspaceName    = $LogAnalyticsWorkspaceName
  logAnalyticsCustomerId       = $lawCustomerId
  containerAppsEnvironmentName = $ContainerAppsEnvironmentName
}

Write-Host ""
Write-Host "Provisioning complete. Outputs:"
$outputs | ConvertTo-Json -Depth 4

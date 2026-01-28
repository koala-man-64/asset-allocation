param(
  [string]$SubscriptionId = "",

  [string]$Location = "eastus",
  [string]$ResourceGroup = "AssetAllocationRG",

  [string]$StorageAccountName = "assetallocstorage001",

  [string[]]$StorageContainers = @(),
  [string]$AcrName = "assetallocationacr",
  # User-assigned managed identity used by Container Apps/Jobs to pull from ACR on first create.
  [string]$AcrPullIdentityName = "asset-allocation-acr-pull-mi",
  [switch]$EnableAcrAdmin,
  [switch]$EmitSecrets,
  [switch]$GrantAcrPullToAcaResources,

  [string]$LogAnalyticsWorkspaceName = "asset-allocation-law",
  [string]$ContainerAppsEnvironmentName = "asset-allocation-env",
  [string]$AzureClientId = "",
  [string]$AksClusterName = "",
  [string]$KubernetesNamespace = "k8s-apps",
  [string]$ServiceAccountName = "asset-allocation-sa"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$envPath = "$PSScriptRoot\..\.env"
$envLines = @()
if (Test-Path $envPath) {
    $envLines = Get-Content $envPath
}

function Get-EnvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Key,
        [string[]]$Lines = $envLines
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

function Get-EnvValueFirst {
    param(
        [Parameter(Mandatory = $true)][string[]]$Keys
    )
    foreach ($key in $Keys) {
        $value = Get-EnvValue -Key $key
        if ($value) {
            return $value
        }
    }
    return $null
}

# Load containers from .env if not specified
if ($StorageContainers.Count -eq 0 -and $envLines.Count -gt 0) {
    Write-Host "Reading container names from .env..."
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

if ((-not $PSBoundParameters.ContainsKey("SubscriptionId")) -or [string]::IsNullOrWhiteSpace($SubscriptionId)) {
    $subscriptionFromEnv = Get-EnvValueFirst -Keys @("AZURE_SUBSCRIPTION_ID", "SUBSCRIPTION_ID")
    if ($subscriptionFromEnv) {
        Write-Host "Using AZURE_SUBSCRIPTION_ID from .env: $subscriptionFromEnv"
        $SubscriptionId = $subscriptionFromEnv
    }
}

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
    throw "SubscriptionId is required. Provide -SubscriptionId or set AZURE_SUBSCRIPTION_ID in .env."
}

if (-not $PSBoundParameters.ContainsKey("ResourceGroup")) {
    $resourceGroupFromEnv = Get-EnvValueFirst -Keys @("RESOURCE_GROUP", "AZURE_RESOURCE_GROUP", "SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
    if ($resourceGroupFromEnv) {
        Write-Host "Using RESOURCE_GROUP from .env: $resourceGroupFromEnv"
        $ResourceGroup = $resourceGroupFromEnv
    }
}

if (-not $PSBoundParameters.ContainsKey("Location")) {
    $locationFromEnv = Get-EnvValueFirst -Keys @("AZURE_LOCATION", "AZURE_REGION", "LOCATION")
    if ($locationFromEnv) {
        Write-Host "Using AZURE_LOCATION from .env: $locationFromEnv"
        $Location = $locationFromEnv
    }
}

if (-not $PSBoundParameters.ContainsKey("StorageAccountName")) {
    $storageFromEnv = Get-EnvValueFirst -Keys @("AZURE_STORAGE_ACCOUNT_NAME")
    if ($storageFromEnv) {
        Write-Host "Using AZURE_STORAGE_ACCOUNT_NAME from .env: $storageFromEnv"
        $StorageAccountName = $storageFromEnv
    }
}

if (-not $PSBoundParameters.ContainsKey("AcrName")) {
    $acrFromEnv = Get-EnvValueFirst -Keys @("ACR_NAME", "AZURE_ACR_NAME")
    if ($acrFromEnv) {
        Write-Host "Using ACR_NAME from .env: $acrFromEnv"
        $AcrName = $acrFromEnv
    }
}

if (-not $PSBoundParameters.ContainsKey("LogAnalyticsWorkspaceName")) {
    $lawFromEnv = Get-EnvValueFirst -Keys @("LOG_ANALYTICS_WORKSPACE_NAME", "LOG_ANALYTICS_WORKSPACE")
    if ($lawFromEnv) {
        Write-Host "Using LOG_ANALYTICS_WORKSPACE_NAME from .env: $lawFromEnv"
        $LogAnalyticsWorkspaceName = $lawFromEnv
    }
}

if (-not $PSBoundParameters.ContainsKey("ContainerAppsEnvironmentName")) {
    $envFromEnv = Get-EnvValueFirst -Keys @("CONTAINER_APPS_ENVIRONMENT_NAME", "CONTAINERAPPS_ENVIRONMENT_NAME", "ACA_ENVIRONMENT_NAME")
    if ($envFromEnv) {
        Write-Host "Using CONTAINER_APPS_ENVIRONMENT_NAME from .env: $envFromEnv"
        $ContainerAppsEnvironmentName = $envFromEnv
    }
}

if (-not $PSBoundParameters.ContainsKey("ServiceAccountName")) {
    $serviceAccountFromEnv = Get-EnvValue -Key "SERVICE_ACCOUNT_NAME"
    if ($serviceAccountFromEnv) {
        Write-Host "Using SERVICE_ACCOUNT_NAME from .env: $serviceAccountFromEnv"
        $ServiceAccountName = $serviceAccountFromEnv
    }
}

if (-not $PSBoundParameters.ContainsKey("KubernetesNamespace")) {
    $namespaceFromEnv = Get-EnvValue -Key "KUBERNETES_NAMESPACE"
    if ($namespaceFromEnv) {
        Write-Host "Using KUBERNETES_NAMESPACE from .env: $namespaceFromEnv"
        $KubernetesNamespace = $namespaceFromEnv
    }
}

if (-not $PSBoundParameters.ContainsKey("AksClusterName")) {
    $aksFromEnv = Get-EnvValue -Key "AKS_CLUSTER_NAME"
    if ($aksFromEnv) {
        Write-Host "Using AKS_CLUSTER_NAME from .env: $aksFromEnv"
        $AksClusterName = $aksFromEnv
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

Assert-CommandExists -Name "az"
if ($AksClusterName) {
    Assert-CommandExists -Name "kubectl"
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
az account set --subscription $SubscriptionId 1>$null

Write-Host "Ensuring required Azure resource providers are registered..."
$providers = @(
  "Microsoft.Storage",
  "Microsoft.ContainerRegistry",
  "Microsoft.ManagedIdentity",
  "Microsoft.OperationalInsights",
  "Microsoft.App"
)
foreach ($p in $providers) {
  az provider register --namespace $p 1>$null
}

Write-Host "Ensuring Azure CLI extensions are installed..."
az extension add --name containerapp --upgrade --only-show-errors 1>$null

Write-Host "Ensuring resource group exists: $ResourceGroup ($Location)"
az group create --name $ResourceGroup --location $Location --only-show-errors 1>$null

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
    --only-show-errors 1>$null
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
    --only-show-errors 1>$null
}

Write-Host "Creating blob containers (auth-mode=login)..."
foreach ($c in $StorageContainers) {
  if (-not $c) { continue }
  az storage container create --name $c --account-name $StorageAccountName --auth-mode login --only-show-errors 1>$null
}

Write-Host "Ensuring ACR exists: $AcrName"
$acrAdmin = if ($EnableAcrAdmin) { "true" } else { "false" }
az acr create `
  --name $AcrName `
  --resource-group $ResourceGroup `
  --location $Location `
  --sku Basic `
  --admin-enabled $acrAdmin `
  --only-show-errors 1>$null

Write-Host "Ensuring Log Analytics workspace exists: $LogAnalyticsWorkspaceName"
az monitor log-analytics workspace create `
  --resource-group $ResourceGroup `
  --workspace-name $LogAnalyticsWorkspaceName `
  --location $Location `
  --only-show-errors 1>$null

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
  --only-show-errors 1>$null

if ($AksClusterName) {
  Write-Host "Ensuring Kubernetes service account exists: $ServiceAccountName (namespace: $KubernetesNamespace)"
  az aks get-credentials --resource-group $ResourceGroup --name $AksClusterName --overwrite-existing --only-show-errors 1>$null
  kubectl get namespace $KubernetesNamespace 1>$null 2>$null
  if ($LASTEXITCODE -ne 0) {
    kubectl create namespace $KubernetesNamespace | Out-Null
  }
  $serviceAccountYaml = @"
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $ServiceAccountName
  namespace: $KubernetesNamespace
"@
  $serviceAccountYaml | kubectl apply -f - | Out-Null
}

$storageConnectionString = ""
if ($EmitSecrets) {
  $storageConnectionString = az storage account show-connection-string `
    --name $StorageAccountName `
    --resource-group $ResourceGroup `
    --query connectionString -o tsv
}

$acrLoginServer = az acr show --name $AcrName --resource-group $ResourceGroup --query loginServer -o tsv
$acrId = az acr show --name $AcrName --resource-group $ResourceGroup --query id -o tsv --only-show-errors

Write-Host "Ensuring user-assigned managed identity exists (for ACR pull): $AcrPullIdentityName"
$acrPullIdentity = $null
try {
  $acrPullIdentity = az identity show --name $AcrPullIdentityName --resource-group $ResourceGroup --only-show-errors -o json 2>$null | ConvertFrom-Json
}
catch {
  $acrPullIdentity = $null
}

if ($null -eq $acrPullIdentity) {
  $acrPullIdentity = az identity create --name $AcrPullIdentityName --resource-group $ResourceGroup --location $Location --only-show-errors -o json | ConvertFrom-Json
}

$acrPullIdentityId = $acrPullIdentity.id
$acrPullIdentityClientId = $acrPullIdentity.clientId
$acrPullIdentityPrincipalId = $acrPullIdentity.principalId

if (-not $acrPullIdentityId -or -not $acrPullIdentityPrincipalId) {
  throw "Failed to resolve AcrPull identity details for '$AcrPullIdentityName'."
}

Write-Host "Ensuring AcrPull role assignment exists for identity on ACR..."
$acrPullExisting = "0"
try {
  $acrPullExisting = az role assignment list `
    --assignee-object-id $acrPullIdentityPrincipalId `
    --scope $acrId `
    --query "[?roleDefinitionName=='AcrPull'] | length(@)" -o tsv --only-show-errors 2>$null
  if (-not $acrPullExisting) { $acrPullExisting = "0" }
}
catch {
  $acrPullExisting = "0"
}

if ([int]$acrPullExisting -eq 0) {
  az role assignment create `
    --assignee-object-id $acrPullIdentityPrincipalId `
    --assignee-principal-type ServicePrincipal `
    --role "AcrPull" `
    --scope $acrId `
    --only-show-errors 1>$null
  Write-Host "  AcrPull granted to $AcrPullIdentityName ($acrPullIdentityPrincipalId)"
}
else {
  Write-Host "  AcrPull already present for $AcrPullIdentityName ($acrPullIdentityPrincipalId)"
}

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

  az role assignment create --assignee $PrincipalId --role "AcrPull" --scope $Scope --only-show-errors 1>$null
  return $true
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
      $principalId = az containerapp show --name $name --resource-group $ResourceGroup --query identity.principalId -o tsv --only-show-errors
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
      $principalId = az containerapp job show --name $name --resource-group $ResourceGroup --query identity.principalId -o tsv --only-show-errors
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
  acrPullUserAssignedIdentityName       = $AcrPullIdentityName
  acrPullUserAssignedIdentityId         = $acrPullIdentityId
  acrPullUserAssignedIdentityClientId   = $acrPullIdentityClientId
  acrPullUserAssignedIdentityPrincipalId = $acrPullIdentityPrincipalId
  acrPullAssignmentsCreated    = $acrPullAssignmentsCreated
  acrPullAssignmentsSkipped    = $acrPullAssignmentsSkipped
  logAnalyticsWorkspaceName    = $LogAnalyticsWorkspaceName
  logAnalyticsCustomerId       = $lawCustomerId
  containerAppsEnvironmentName = $ContainerAppsEnvironmentName
  kubernetesServiceAccountName = $ServiceAccountName
  kubernetesNamespace          = $KubernetesNamespace
}

Write-Host ""
Write-Host "Provisioning complete. Outputs:"
$outputs | ConvertTo-Json -Depth 4

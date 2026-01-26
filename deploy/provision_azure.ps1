param(
  [Parameter(Mandatory = $true)]
  [string]$SubscriptionId,

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

  [string]$LogAnalyticsWorkspaceName = "asset-allocation-law",
  [string]$ContainerAppsEnvironmentName = "asset-allocation-env",
  [string]$AzureClientId = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command '$Name'. Install it and retry."
  }
}

Assert-CommandExists -Name "az"

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

$acrPassword = ""
if ($EnableAcrAdmin) {
  if ($EmitSecrets) {
    $acrPassword = az acr credential show --name $AcrName --resource-group $ResourceGroup --query "passwords[0].value" -o tsv
  }
}

$outputs = [ordered]@{
  subscriptionId               = $SubscriptionId
  location                     = $Location
  resourceGroup                = $ResourceGroup
  storageAccountName           = $StorageAccountName
  storageConnectionString      = if ($EmitSecrets) { $storageConnectionString } else { "<redacted>" }
  storageContainers            = $StorageContainers
  acrName                      = $AcrName
  acrLoginServer               = $acrLoginServer
  acrAdminEnabled              = [bool]$EnableAcrAdmin
  acrPassword                  = if ($EmitSecrets) { $acrPassword } else { "<redacted>" }
  logAnalyticsWorkspaceName    = $LogAnalyticsWorkspaceName
  logAnalyticsCustomerId       = $lawCustomerId
  containerAppsEnvironmentName = $ContainerAppsEnvironmentName
}

Write-Host ""
Write-Host "Provisioning complete. Outputs:"
$outputs | ConvertTo-Json -Depth 4

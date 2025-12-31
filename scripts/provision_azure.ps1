
# Script to provision Azure Identity Resources
# Usage: ./provision_azure.ps1

$ErrorActionPreference = "Stop"

# 1. Get Storage Account Name from Env or Prompt
$accName = $env:AZURE_STORAGE_ACCOUNT_NAME
if (-not $accName) {
    Write-Host "AZURE_STORAGE_ACCOUNT_NAME is not set in environment." -ForegroundColor Red
    $accName = Read-Host "Please enter your Storage Account Name"
}

Write-Host "Target Storage Account: $accName" -ForegroundColor Cyan

# 2. Get Azure Context
Write-Host "Getting Subscription Context..."
try {
    $subId = az account show --query id -o tsv
    $userId = az ad signed-in-user show --query id -o tsv
} catch {
    Write-Host "Error: Could not get Azure context. Please run 'az login' first." -ForegroundColor Red
    exit 1
}

# 3. Find Resource Group
Write-Host "Finding Resource Group..."
try {
    $rg = az storage account show --name $accName --query resourceGroup -o tsv
} catch {
    Write-Host "Error: Could not find storage account '$accName'. Check name and permissions." -ForegroundColor Red
    exit 1
}
Write-Host "Found Resource Group: $rg" -ForegroundColor Green

# 4. Create Managed Identity
$idName = "id-asset-allocation"
Write-Host "Creating User Assigned Managed Identity: $idName ..."
$idPrincipalId = az identity create --name $idName --resource-group $rg --query principalId -o tsv
Write-Host "Identity Created. Principal ID: $idPrincipalId" -ForegroundColor Green

# 5. Assign Roles
$scope = "/subscriptions/$subId/resourceGroups/$rg/providers/Microsoft.Storage/storageAccounts/$accName"

Write-Host "Assigning 'Storage Blob Data Contributor' to Identity ($idName)..."
az role assignment create --role "Storage Blob Data Contributor" --assignee-object-id $idPrincipalId --assignee-principal-type ServicePrincipal --scope $scope

Write-Host "Assigning 'Storage Blob Data Contributor' to User (You)..."
az role assignment create --role "Storage Blob Data Contributor" --assignee-object-id $userId --assignee-principal-type User --scope $scope

Write-Host "`n--- Deployment Complete ---" -ForegroundColor Cyan
Write-Host "1. Identity '$idName' created."
Write-Host "2. RBAC roles assigned to Identity and You."

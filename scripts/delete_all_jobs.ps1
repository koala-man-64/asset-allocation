param(
  [string]$ResourceGroup = "AssetAllocationRG",
  [string]$ApiAppName = "",
  [string]$UiAppName = ""
)

$jobs = @(
  "bronze-market-job",
  "bronze-finance-job",
  "bronze-price-target-job",
  "bronze-earnings-job",
  "silver-market-job",
  "silver-finance-job",
  "silver-price-target-job",
  "silver-earnings-job",
  "gold-market-job",
  "gold-finance-job",
  "gold-price-target-job",
  "gold-earnings-job"
)

$resolvedApiAppName = $ApiAppName
if (-not $resolvedApiAppName) {
  $resolvedApiAppName = $env:API_APP_NAME
}
if (-not $resolvedApiAppName) {
  $resolvedApiAppName = "asset-allocation-api"
}

$resolvedUiAppName = $UiAppName
if (-not $resolvedUiAppName) {
  $resolvedUiAppName = $env:UI_APP_NAME
}
if (-not $resolvedUiAppName) {
  $resolvedUiAppName = "asset-allocation-ui"
}

$containerApps = @($resolvedApiAppName, $resolvedUiAppName)

Write-Host "Deleting jobs in Resource Group: $ResourceGroup"

foreach ($job in $jobs) {
  Write-Host "Deleting $job..."
  az containerapp job delete --name $job --resource-group $ResourceGroup --yes --only-show-errors
  if ($LASTEXITCODE -eq 0) {
    Write-Host "Successfully deleted $job" -ForegroundColor Green
  } else {
    Write-Host "Failed to delete $job (it may not exist)" -ForegroundColor Yellow
  }
}

Write-Host "Deleting container apps in Resource Group: $ResourceGroup"

foreach ($app in $containerApps) {
  Write-Host "Deleting $app..."
  az containerapp delete --name $app --resource-group $ResourceGroup --yes --only-show-errors
  if ($LASTEXITCODE -eq 0) {
    Write-Host "Successfully deleted $app" -ForegroundColor Green
  } else {
    Write-Host "Failed to delete $app (it may not exist)" -ForegroundColor Yellow
  }
}

Write-Host "Done."

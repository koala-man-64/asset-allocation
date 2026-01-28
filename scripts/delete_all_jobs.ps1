param(
  [string]$ResourceGroup = "AssetAllocationRG"
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
  "gold-earnings-job",
  "platinum-ranking-job"
)

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

Write-Host "Done."

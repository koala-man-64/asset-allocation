#!/bin/bash
jobs=(
  "bronze-market-job"
  "bronze-finance-job"
  "bronze-price-target-job"
  "bronze-earnings-job"
  "silver-market-job"
  "silver-finance-job"
  "silver-price-target-job"
  "silver-earnings-job"
  "gold-market-job"
  "gold-finance-job"
  "gold-price-target-job"
  "gold-earnings-job"
  "platinum-ranking-job"
)

"/mnt/c/Users/rdpro/Projects/AssetAllocation - AG/asset-allocation/scripts/ensure_acr_pull.sh" AssetAllocationRG assetallocationacr app backtest-api

for job in "${jobs[@]}"; do
  echo "Processing $job..."
  "/mnt/c/Users/rdpro/Projects/AssetAllocation - AG/asset-allocation/scripts/ensure_acr_pull.sh" AssetAllocationRG assetallocationacr job "$job"
done

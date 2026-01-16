<#
.SYNOPSIS
  For each Azure Container Apps Job in a resource group:
  - find the latest execution
  - fetch and print ONLY the last 20 log lines for that execution

.EXAMPLE
  .\get-latest-containerapp-job-runs.ps1 -ResourceGroupName "AssetAllocationRG"
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$ResourceGroupName,

  [Parameter(Mandatory = $false)]
  [string]$SubscriptionIdOrName
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-AzCli {
  if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI (az) not found in PATH."
  }
}

function Ensure-ContainerAppExtension {
  $ext = & az extension list --only-show-errors --query "[?name=='containerapp'] | length(@)" -o tsv 2>$null
  if (-not $ext -or [int]$ext -eq 0) {
    Write-Host "Installing Azure CLI extension: containerapp ..."
    & az extension add --name containerapp --only-show-errors | Out-Null
  }
}

function Get-ExecTimestamp([object]$exec) {
  $t = $null
  if ($exec -and $exec.properties) {
    if ($exec.properties.startTime) { $t = $exec.properties.startTime }
    elseif ($exec.properties.createdTime) { $t = $exec.properties.createdTime }
  }
  if (-not $t) { return $null }
  try { return [DateTimeOffset]::Parse($t) } catch { return $null }
}

function Get-FirstContainerNameFromJob([object]$job) {
  # Try to read from the job object (job list often includes properties.template.containers)
  try {
    $containers = $job.properties.template.containers
    if ($containers -and $containers.Count -gt 0 -and $containers[0].name) {
      return [string]$containers[0].name
    }
  } catch { }

  return $null
}

function Get-ContainerNameViaJobShow([string]$rg, [string]$jobName) {
  $jobJson = & az containerapp job show -g $rg -n $jobName -o json --only-show-errors 2>$null
  if (-not $jobJson) { return $null }
  $jobObj = $jobJson | ConvertFrom-Json
  return Get-FirstContainerNameFromJob $jobObj
}

Ensure-AzCli
Ensure-ContainerAppExtension

if ($SubscriptionIdOrName) {
  Write-Host "Setting subscription: $SubscriptionIdOrName"
  & az account set --subscription $SubscriptionIdOrName --only-show-errors | Out-Null
}

Write-Host "Listing Container Apps Jobs in resource group: $ResourceGroupName ..."

$jobsJson = & az containerapp job list -g $ResourceGroupName -o json --only-show-errors 2>$null
if (-not $jobsJson) {
  Write-Host "No jobs found (empty response)."
  exit 0
}

$jobs = $jobsJson | ConvertFrom-Json
if (-not $jobs -or $jobs.Count -eq 0) {
  Write-Host "No jobs found in resource group '$ResourceGroupName'."
  exit 0
}

Write-Host "Found $($jobs.Count) job(s). Fetching latest execution + last 20 log lines for each ..."
Write-Host ""

# Quick capability check: does your CLI have `az containerapp job logs show`?
$hasJobLogsShow = $true
try {
  & az containerapp job logs show -h --only-show-errors 1>$null 2>$null
} catch {
  $hasJobLogsShow = $false
}

if (-not $hasJobLogsShow) {
  Write-Host "ERROR: Your Azure CLI/containerapp extension does not have 'az containerapp job logs show'."
  Write-Host "Fix: Update Azure CLI + the containerapp extension, then re-run:"
  Write-Host "  az upgrade"
  Write-Host "  az extension update --name containerapp"
  Write-Host ""
  Write-Host "Microsoft docs for the command: az containerapp job logs show"
  exit 1
}

foreach ($job in $jobs) {
  $jobName = $job.name
  $jobId   = $job.id
  $jobLoc  = $job.location

  Write-Host "=============================="
  Write-Host "Job: $jobName"
  Write-Host "ID : $jobId"
  Write-Host "Loc: $jobLoc"
  Write-Host "------------------------------"

  # Find latest execution (PowerShell selection, no JMESPath functions)
  $execJson = & az containerapp job execution list -g $ResourceGroupName -n $jobName -o json --only-show-errors 2>$null
  if (-not $execJson) {
    Write-Host "(No executions found for this job.)"
    Write-Host ""
    continue
  }

  $execs = $execJson | ConvertFrom-Json
  if (-not $execs -or $execs.Count -eq 0) {
    Write-Host "(No executions found for this job.)"
    Write-Host ""
    continue
  }

  $latest = $null
  $latestTs = $null
  foreach ($e in $execs) {
    $ts = Get-ExecTimestamp $e
    if (-not $ts) { continue }
    if (-not $latestTs -or $ts -gt $latestTs) {
      $latestTs = $ts
      $latest = $e
    }
  }

  if (-not $latest) {
    Write-Host "(Executions exist but none had parseable startTime/createdTime.)"
    Write-Host ""
    continue
  }

  $execName = $latest.name
  Write-Host "Latest Execution: $execName  ($latestTs)"
  Write-Host ""

  # Need container name (required by `az containerapp job logs show`) :contentReference[oaicite:1]{index=1}
  $containerName = Get-FirstContainerNameFromJob $job
  if (-not $containerName) {
    $containerName = Get-ContainerNameViaJobShow $ResourceGroupName $jobName
  }

  if (-not $containerName) {
    Write-Host "(Could not determine container name for this job. Try: az containerapp job show -g $ResourceGroupName -n $jobName)"
    Write-Host ""
    continue
  }

  # Fetch ONLY the last 20 lines from that specific execution (`--tail 20`) :contentReference[oaicite:2]{index=2}
  $logText = & az containerapp job logs show `
      -g $ResourceGroupName `
      -n $jobName `
      --execution $execName `
      --container $containerName `
      --tail 20 `
      --format text `
      --only-show-errors `
      2>&1

  Write-Host $logText
  Write-Host ""
}

Write-Host "=============================="
Write-Host "Done."

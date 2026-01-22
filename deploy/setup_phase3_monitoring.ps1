param(
  [string]$Repo = "asset-allocation",
  [string]$ResourceGroup = "AssetAllocationRG",
  [string]$AcrName = "assetallocationacr",
  [string]$StorageAccountName = "assetallocstorage001",
  [string]$BacktestAppName = "backtest-api",
  [string]$ContainerAppsEnvironmentName = "asset-allocation-env",
  [switch]$SkipGitHub,
  [switch]$SkipAzure
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command '$Name'. Install it and retry."
  }
}

function Read-YesNo {
  param(
    [Parameter(Mandatory = $true)][string]$Prompt,
    [bool]$Default = $true
  )
  $suffix = if ($Default) { " [Y/n]" } else { " [y/N]" }
  while ($true) {
    $raw = (Read-Host ($Prompt + $suffix)).Trim()
    if (-not $raw) { return $Default }
    $v = $raw.ToLowerInvariant()
    if ($v -in @("y", "yes")) { return $true }
    if ($v -in @("n", "no")) { return $false }
    Write-Host "Please enter 'y' or 'n'."
  }
}

function Read-Value {
  param(
    [Parameter(Mandatory = $true)][string]$Prompt,
    [string]$Default = "",
    [switch]$AllowEmpty
  )
  while ($true) {
    $suffix = if ($Default) { " [$Default]" } else { "" }
    $raw = Read-Host ($Prompt + $suffix)
    $raw = $raw.Trim()
    if ($raw) { return $raw }
    if ($Default) { return $Default }
    if ($AllowEmpty) { return "" }
    Write-Host "A value is required."
  }
}

function Read-SecretPlain {
  param([Parameter(Mandatory = $true)][string]$Prompt)
  $secure = Read-Host -AsSecureString $Prompt
  $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
  try {
    return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
  }
  finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
  }
}

function Invoke-Cli {
  param(
    [Parameter(Mandatory = $true)][string]$Exe,
    [Parameter(Mandatory = $true)][string[]]$Args,
    [switch]$AllowFail
  )
  $output = & $Exe @Args 2>&1
  if ($LASTEXITCODE -ne 0 -and -not $AllowFail) {
    throw "$Exe $($Args -join ' ') failed: $output"
  }
  return $output
}

function Try-AssignRole {
  param(
    [Parameter(Mandatory = $true)][string]$AssigneeObjectId,
    [Parameter(Mandatory = $true)][string]$Role,
    [Parameter(Mandatory = $true)][string]$Scope
  )
  $out = Invoke-Cli -Exe "az" -Args @(
    "role", "assignment", "create",
    "--assignee-object-id", $AssigneeObjectId,
    "--assignee-principal-type", "ServicePrincipal",
    "--role", $Role,
    "--scope", $Scope,
    "--only-show-errors"
  ) -AllowFail
  if ($LASTEXITCODE -ne 0) {
    if ($out -match "RoleAssignmentExists") {
      Write-Host "Role already assigned: $Role @ $Scope"
    }
    else {
      Write-Warning "Role assignment may have failed ($Role @ $Scope): $out"
    }
  }
  else {
    Write-Host "Assigned role: $Role @ $Scope"
  }
}

function Get-RepoFromGitRemote {
  $remote = ""
  try {
    $remote = (Invoke-Cli -Exe "git" -Args @("config", "--get", "remote.origin.url") -AllowFail).Trim()
  }
  catch {
    return ""
  }
  if (-not $remote) { return "" }

  # https://github.com/org/repo(.git)
  if ($remote -match "^https://github\.com/([^/]+)/([^/]+?)(\.git)?$") {
    return "$($Matches[1])/$($Matches[2])"
  }

  # git@github.com:org/repo(.git)
  if ($remote -match "^git@github\.com:([^/]+)/([^/]+?)(\.git)?$") {
    return "$($Matches[1])/$($Matches[2])"
  }
  return ""
}

function Select-FromList {
  param(
    [Parameter(Mandatory = $true)][string]$Title,
    [Parameter(Mandatory = $true)][string[]]$Items
  )
  if (-not $Items -or $Items.Count -eq 0) {
    Write-Host "Title: none found."
    return @()
  }

  Write-Host ""
  Write-Host $Title
  for ($i = 0; $i -lt $Items.Count; $i++) {
    Write-Host ("[{0}] {1}" -f ($i + 1), $Items[$i])
  }
  Write-Host "Enter comma-separated numbers (e.g. 1,3) or leave blank for none."
  $raw = (Read-Host "Selection").Trim()
  if (-not $raw) { return @() }

  $picked = @()
  foreach ($part in $raw.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }) {
    $n = 0
    if ([int]::TryParse($part, [ref]$n)) {
      if ($n -ge 1 -and $n -le $Items.Count) {
        $picked += $Items[$n - 1]
      }
    }
    else {
      if ($Items -contains $part) {
        $picked += $part
      }
    }
  }
  return $picked | Select-Object -Unique
}

Write-Host "Phase 3 Monitoring Setup (GitHub + Azure)"
Write-Host "This script configures:"
Write-Host "- GitHub Actions OIDC + repo secrets (optional)"
Write-Host "- Azure RBAC + backtest-api env vars for system health probes (Phase 2/3A/3B)"
Write-Host ""

Assert-CommandExists -Name "az"

if (-not $SkipGitHub) { Assert-CommandExists -Name "gh" }

if (-not $Repo) {
  $Repo = Get-RepoFromGitRemote
}
if (-not $Repo -and -not $SkipGitHub) {
  $Repo = Read-Value -Prompt "GitHub repo (ORG/REPO)" -Default "" 
}

Write-Host ""
Write-Host "Azure login check..."
$accountJson = Invoke-Cli -Exe "az" -Args @("account", "show", "-o", "json") -AllowFail
if ($LASTEXITCODE -ne 0) {
  Write-Host "Not logged in to Azure CLI. Launching 'az login'..."
  Invoke-Cli -Exe "az" -Args @("login") | Out-Null
  $accountJson = Invoke-Cli -Exe "az" -Args @("account", "show", "-o", "json")
}
$account = $accountJson | ConvertFrom-Json
$subscriptionId = [string]$account.id
$tenantId = [string]$account.tenantId
Write-Host "Azure subscription: $subscriptionId"
Write-Host "Azure tenant:       $tenantId"

if (-not $SkipGitHub) {
  Write-Host ""
  Write-Host "GitHub login check..."
  $ghStatus = Invoke-Cli -Exe "gh" -Args @("auth", "status") -AllowFail
  if ($LASTEXITCODE -ne 0) {
    Write-Host "Not logged in to GitHub CLI. Launching 'gh auth login'..."
    Invoke-Cli -Exe "gh" -Args @("auth", "login") | Out-Null
  }

  if (Read-YesNo -Prompt "Configure GitHub Actions Azure OIDC + secrets for ${Repo}?" -Default $true) {
    $clientId = ""
    $spObjectId = ""
    $appObjectId = ""

    if (Read-YesNo -Prompt "Create a new Entra App Registration for GitHub OIDC?" -Default $false) {
      $displayName = Read-Value -Prompt "App display name" -Default "asset-allocation-gh-actions"

      Write-Host "Creating app registration..."
      $clientId = (Invoke-Cli -Exe "az" -Args @(
          "ad", "app", "create",
          "--display-name", $displayName,
          "--sign-in-audience", "AzureADMyOrg",
          "--query", "appId",
          "-o", "tsv"
        )).Trim()

      $appObjectId = (Invoke-Cli -Exe "az" -Args @(
          "ad", "app", "show",
          "--id", $clientId,
          "--query", "id",
          "-o", "tsv"
        )).Trim()

      Write-Host "Creating service principal..."
      $spObjectId = (Invoke-Cli -Exe "az" -Args @(
          "ad", "sp", "create",
          "--id", $clientId,
          "--query", "id",
          "-o", "tsv"
        )).Trim()

      Write-Host "Creating federated credential for repo/main..."
      $fed = @{
        name      = "github-main"
        issuer    = "https://token.actions.githubusercontent.com"
        subject   = "repo:$Repo:ref:refs/heads/main"
        audiences = @("api://AzureADTokenExchange")
      } | ConvertTo-Json -Depth 5
      $tmp = New-TemporaryFile
      Set-Content -Path $tmp -Value $fed -Encoding utf8

      $fcOut = Invoke-Cli -Exe "az" -Args @(
        "ad", "app", "federated-credential", "create",
        "--id", $appObjectId,
        "--parameters", $tmp
      ) -AllowFail
      Remove-Item $tmp -Force
      if ($LASTEXITCODE -ne 0) {
        Write-Warning "Federated credential creation failed. You may need to create it manually in Entra ID. Details: $fcOut"
      }
      else {
        Write-Host "Federated credential created."
      }

      Write-Host "New OIDC app created:"
      Write-Host "  AZURE_CLIENT_ID  = $clientId"
    }
    else {
      $clientId = Read-Value -Prompt "Existing AZURE_CLIENT_ID (Entra app 'Application (client) ID')" -Default ""
      $spObjectId = (Invoke-Cli -Exe "az" -Args @(
          "ad", "sp", "show",
          "--id", $clientId,
          "--query", "id",
          "-o", "tsv"
        ) -AllowFail).Trim()
      if ($LASTEXITCODE -ne 0) {
        Write-Warning "Could not resolve service principal for client ID. Role assignments may need to be handled manually."
        $spObjectId = ""
      }
    }

    if ($spObjectId -and (Read-YesNo -Prompt "Grant GitHub Actions identity deploy permissions (RG Contributor + ACR Contributor)?" -Default $true)) {
      $rgId = (Invoke-Cli -Exe "az" -Args @("group", "show", "--name", $ResourceGroup, "--query", "id", "-o", "tsv")).Trim()
      $acrId = (Invoke-Cli -Exe "az" -Args @("acr", "show", "--name", $AcrName, "--resource-group", $ResourceGroup, "--query", "id", "-o", "tsv") -AllowFail).Trim()
      Try-AssignRole -AssigneeObjectId $spObjectId -Role "Contributor" -Scope $rgId
      if ($acrId) {
        Try-AssignRole -AssigneeObjectId $spObjectId -Role "Contributor" -Scope $acrId
      }
      else {
        Write-Warning "ACR '$AcrName' not found in RG '$ResourceGroup'. Skipping ACR role assignment."
      }
    }

    if (Read-YesNo -Prompt "Set required GitHub secrets (AZURE_CLIENT_ID/TENANT_ID/SUBSCRIPTION_ID)?" -Default $true) {
      $existing = @()
      $listOut = Invoke-Cli -Exe "gh" -Args @("secret", "list", "-R", $Repo, "--json", "name") -AllowFail
      if ($LASTEXITCODE -eq 0 -and $listOut) {
        $existing = ($listOut | ConvertFrom-Json | ForEach-Object { $_.name })
      }

      function Set-GHSecretFromValue {
        param([string]$Name, [string]$Value)
        if (-not $Value) { return }
        if ($existing -contains $Name) {
          if (-not (Read-YesNo -Prompt "Secret '$Name' exists. Overwrite?" -Default $false)) { return }
        }
        $tmp = New-TemporaryFile
        Set-Content -Path $tmp -Value $Value -NoNewline -Encoding utf8
        Invoke-Cli -Exe "gh" -Args @("secret", "set", $Name, "-R", $Repo, "--body-file", $tmp) | Out-Null
        Remove-Item $tmp -Force
        Write-Host "Set GitHub secret: $Name"
      }

      Set-GHSecretFromValue -Name "AZURE_CLIENT_ID" -Value $clientId
      Set-GHSecretFromValue -Name "AZURE_TENANT_ID" -Value $tenantId
      Set-GHSecretFromValue -Name "AZURE_SUBSCRIPTION_ID" -Value $subscriptionId

      if (Read-YesNo -Prompt "Set CI storage secrets (AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_CONNECTION_STRING)?" -Default $false) {
        Set-GHSecretFromValue -Name "AZURE_STORAGE_ACCOUNT_NAME" -Value $StorageAccountName
        $cs = Read-SecretPlain -Prompt "Enter AZURE_STORAGE_CONNECTION_STRING (will be stored as a GitHub secret)"
        Set-GHSecretFromValue -Name "AZURE_STORAGE_CONNECTION_STRING" -Value $cs
      }
    }
  }
}

if (-not $SkipAzure) {
  Write-Host ""
  if (-not (Read-YesNo -Prompt "Configure Azure RBAC + backtest-api env vars for monitoring?" -Default $true)) {
    Write-Host "Skipping Azure setup."
    exit 0
  }

  Write-Host "Ensuring Azure CLI containerapp extension..."
  Invoke-Cli -Exe "az" -Args @("extension", "add", "--name", "containerapp", "--upgrade", "--only-show-errors") | Out-Null

  Write-Host "Registering providers (best-effort)..."
  foreach ($ns in @("Microsoft.App", "Microsoft.Insights", "Microsoft.OperationalInsights", "Microsoft.ResourceHealth")) {
    Invoke-Cli -Exe "az" -Args @("provider", "register", "--namespace", $ns, "--only-show-errors") -AllowFail | Out-Null
  }

  $ResourceGroup = Read-Value -Prompt "Azure Resource Group" -Default $ResourceGroup
  $BacktestAppName = Read-Value -Prompt "Backtest API Container App name" -Default $BacktestAppName
  $StorageAccountName = Read-Value -Prompt "Storage Account name (ADLS/Blob)" -Default $StorageAccountName

  $rgId = (Invoke-Cli -Exe "az" -Args @("group", "show", "--name", $ResourceGroup, "--query", "id", "-o", "tsv")).Trim()
  $storageId = (Invoke-Cli -Exe "az" -Args @("storage", "account", "show", "--name", $StorageAccountName, "--resource-group", $ResourceGroup, "--query", "id", "-o", "tsv") -AllowFail).Trim()

  Write-Host "Checking backtest-api managed identity..."
  $principalId = (Invoke-Cli -Exe "az" -Args @(
      "containerapp", "show",
      "--name", $BacktestAppName,
      "--resource-group", $ResourceGroup,
      "--query", "identity.principalId",
      "-o", "tsv"
    ) -AllowFail).Trim()

  if (-not $principalId) {
    if (Read-YesNo -Prompt "Container App has no system-assigned identity. Assign one now?" -Default $true) {
      Invoke-Cli -Exe "az" -Args @("containerapp", "identity", "assign", "--name", $BacktestAppName, "--resource-group", $ResourceGroup, "--system-assigned") | Out-Null
      $principalId = (Invoke-Cli -Exe "az" -Args @(
          "containerapp", "show",
          "--name", $BacktestAppName,
          "--resource-group", $ResourceGroup,
          "--query", "identity.principalId",
          "-o", "tsv"
        )).Trim()
    }
  }

  if (-not $principalId) {
    throw "Could not resolve managed identity principalId for '$BacktestAppName'."
  }
  Write-Host "Managed identity principalId: $principalId"

  Write-Host ""
  Write-Host "Assigning runtime RBAC (best-effort)..."
  Try-AssignRole -AssigneeObjectId $principalId -Role "Reader" -Scope $rgId
  Try-AssignRole -AssigneeObjectId $principalId -Role "Monitoring Reader" -Scope $rgId
  if ($storageId) {
    Try-AssignRole -AssigneeObjectId $principalId -Role "Storage Blob Data Reader" -Scope $storageId
  }
  else {
    Write-Warning "Storage account '$StorageAccountName' not found. Skipping Storage Blob Data Reader assignment."
  }

  $workspaceId = ""
  $workspaceResourceId = ""
  if (Read-YesNo -Prompt "Enable Log Analytics probing (requires workspace + RBAC)?" -Default $false) {
    $workspacesJson = Invoke-Cli -Exe "az" -Args @(
      "monitor", "log-analytics", "workspace", "list",
      "--resource-group", $ResourceGroup,
      "-o", "json"
    ) -AllowFail
    $workspaceNames = @()
    if ($LASTEXITCODE -eq 0 -and $workspacesJson) {
      $workspaceNames = ($workspacesJson | ConvertFrom-Json | ForEach-Object { $_.name })
    }

    $workspaceName = ""
    if ($workspaceNames.Count -gt 0) {
      $pickedWs = Select-FromList -Title "Select Log Analytics workspace" -Items $workspaceNames
      if ($pickedWs.Count -gt 0) { $workspaceName = $pickedWs[0] }
    }
    if (-not $workspaceName) {
      $workspaceName = Read-Value -Prompt "Log Analytics workspace name" -Default "asset-allocation-law"
    }

    $workspaceId = (Invoke-Cli -Exe "az" -Args @(
        "monitor", "log-analytics", "workspace", "show",
        "--resource-group", $ResourceGroup,
        "--workspace-name", $workspaceName,
        "--query", "customerId",
        "-o", "tsv"
      )).Trim()

    $workspaceResourceId = (Invoke-Cli -Exe "az" -Args @(
        "monitor", "log-analytics", "workspace", "show",
        "--resource-group", $ResourceGroup,
        "--workspace-name", $workspaceName,
        "--query", "id",
        "-o", "tsv"
      )).Trim()

    Try-AssignRole -AssigneeObjectId $principalId -Role "Log Analytics Reader" -Scope $workspaceResourceId
  }

  Write-Host ""
  Write-Host "Discovering Container Apps and Jobs in resource group..."
  $apps = @()
  $jobs = @()
  $appsOut = Invoke-Cli -Exe "az" -Args @("containerapp", "list", "--resource-group", $ResourceGroup, "--query", "[].name", "-o", "tsv") -AllowFail
  if ($LASTEXITCODE -eq 0 -and $appsOut) { $apps = $appsOut -split "\\s+" | Where-Object { $_ } }
  $jobsOut = Invoke-Cli -Exe "az" -Args @("containerapp", "job", "list", "--resource-group", $ResourceGroup, "--query", "[].name", "-o", "tsv") -AllowFail
  if ($LASTEXITCODE -eq 0 -and $jobsOut) { $jobs = $jobsOut -split "\\s+" | Where-Object { $_ } }

  $selectedApps = Select-FromList -Title "Select Container Apps to monitor (ARM)" -Items $apps
  $selectedJobs = Select-FromList -Title "Select Container App Jobs to monitor (ARM)" -Items $jobs

  $enableResourceHealth = Read-YesNo -Prompt "Enable Azure Resource Health (availabilityStatuses/current)?" -Default $true
  $enableMetrics = Read-YesNo -Prompt "Enable Azure Monitor Metrics probing?" -Default $false

  $containerAppMetrics = @()
  $jobMetrics = @()
  $thresholdsJson = ""
  if ($enableMetrics) {
    if ($selectedApps.Count -gt 0 -and (Read-YesNo -Prompt "List available metric names for the first selected Container App?" -Default $true)) {
      $rid = "/subscriptions/$subscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.App/containerApps/$($selectedApps[0])"
      $defs = Invoke-Cli -Exe "az" -Args @("monitor", "metrics", "list-definitions", "--resource", $rid, "--query", "[].name.value", "-o", "tsv") -AllowFail
      if ($LASTEXITCODE -eq 0 -and $defs) {
        Write-Host ""
        Write-Host "Available metrics for $($selectedApps[0]):"
        Write-Host $defs
      }
    }

    $containerAppMetrics = (Read-Value -Prompt "Container App metric names (comma-separated)" -Default "" -AllowEmpty).Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    $jobMetrics = (Read-Value -Prompt "Job metric names (comma-separated)" -Default "" -AllowEmpty).Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    $thresholdsJson = Read-Value -Prompt "Metric thresholds JSON (or leave blank)" -Default "" -AllowEmpty
  }

  $enableLogs = $false
  $queriesJson = ""
  if ($workspaceId) {
    $enableLogs = Read-YesNo -Prompt "Enable Log Analytics aggregate probing (requires KQL JSON)?" -Default $false
    if ($enableLogs) {
      Write-Host "Paste SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON (JSON array). Use {resourceName} and/or {resourceId} placeholders."
      $queriesJson = Read-Value -Prompt "Queries JSON" -Default "" 
    }
  }

  $envVars = @()
  $envVars += "SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID=$subscriptionId"
  $envVars += "SYSTEM_HEALTH_ARM_RESOURCE_GROUP=$ResourceGroup"
  if ($selectedApps.Count -gt 0) { $envVars += ("SYSTEM_HEALTH_ARM_CONTAINERAPPS=" + ($selectedApps -join ",")) }
  if ($selectedJobs.Count -gt 0) { $envVars += ("SYSTEM_HEALTH_ARM_JOBS=" + ($selectedJobs -join ",")) }

  $envVars += ("SYSTEM_HEALTH_RESOURCE_HEALTH_ENABLED=" + ($(if ($enableResourceHealth) { "true" } else { "false" })))

  $envVars += ("SYSTEM_HEALTH_MONITOR_METRICS_ENABLED=" + ($(if ($enableMetrics) { "true" } else { "false" })))
  if ($enableMetrics) {
    if ($containerAppMetrics.Count -gt 0) { $envVars += ("SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS=" + ($containerAppMetrics -join ",")) }
    if ($jobMetrics.Count -gt 0) { $envVars += ("SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS=" + ($jobMetrics -join ",")) }
    if ($thresholdsJson) { $envVars += ("SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON=" + $thresholdsJson) }
  }

  $envVars += ("SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED=" + ($(if ($enableLogs) { "true" } else { "false" })))
  if ($enableLogs) {
    $envVars += "SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID=$workspaceId"
    $envVars += ("SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON=" + $queriesJson)
  }

  Write-Host ""
  Write-Host "Updating Container App env vars..."
  Invoke-Cli -Exe "az" -Args @(
    "containerapp", "update",
    "--name", $BacktestAppName,
    "--resource-group", $ResourceGroup,
    "--set-env-vars"
  ) + $envVars | Out-Null

  Write-Host "Done. Monitoring env vars applied to $BacktestAppName."
}

Write-Host ""
Write-Host "Setup complete."


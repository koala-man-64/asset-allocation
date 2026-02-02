param(
    [string]$EnvFilePath = "",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# Determine the repo root (prefer parent of scripts/ when executed from this folder).
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = if ((Split-Path -Leaf $scriptDir) -eq "scripts") { Split-Path -Parent $scriptDir } else { (Get-Location).Path }

if ([string]::IsNullOrWhiteSpace($EnvFilePath)) {
    $EnvFilePath = Join-Path $rootDir ".env"
}

# Load existing variables (if any) to use as defaults.
$ExistingVars = @{}
if (Test-Path $EnvFilePath) {
    Write-Host "Loading existing values from $EnvFilePath as defaults..." -ForegroundColor Gray
    Get-Content $EnvFilePath | ForEach-Object {
        if ($_ -match "^\s*([^#\s]+)\s*=\s*(.*)$") {
            $ExistingVars[$Matches[1]] = $Matches[2].Trim()
        }
    }
}

Write-Host "`n--- AssetAllocation Environment Setup ---" -ForegroundColor Cyan
Write-Host "This writes plaintext values to .env. Do NOT commit .env to git." -ForegroundColor Yellow
Write-Host "Press [Enter] to accept the suggestion in [brackets].`n"

function ConvertFrom-SecureStringPlain {
    param([Parameter(Mandatory = $true)][System.Security.SecureString]$Secure)
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

function Prompt-Var {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string]$Suggestion = "",
        [string]$Description = "",
        [switch]$Secret
    )

    if ($ExistingVars.ContainsKey($Name)) {
        $Suggestion = $ExistingVars[$Name]
    }

    if ($Description) { Write-Host "# $Description" -ForegroundColor Gray }

    if ($Secret) {
        $hasDefault = -not [string]::IsNullOrWhiteSpace($Suggestion)
        $hint = if ($hasDefault) { "[stored]" } else { "" }
        $secure = Read-Host "$Name $hint" -AsSecureString
        $input = ConvertFrom-SecureStringPlain -Secure $secure
        if ([string]::IsNullOrWhiteSpace($input)) { return $Suggestion }
        return $input
    }

    $input = Read-Host "$Name [$Suggestion]"
    if ([string]::IsNullOrWhiteSpace($input)) { return $Suggestion }
    return $input
}

$Config = @()

# -------------------------------------------------------------------------
# Local Development & Logging
# -------------------------------------------------------------------------
$Config += "# =========================================="
$Config += "# Local Development & Logging"
$Config += "# =========================================="
$Config += "DISABLE_DOTENV=" + (Prompt-Var "DISABLE_DOTENV" "false" "Set true to prevent python-dotenv from loading .env automatically.")
$Config += "LOG_FORMAT=" + (Prompt-Var "LOG_FORMAT" "JSON" "Options: JSON | TEXT")
$Config += "LOG_LEVEL=" + (Prompt-Var "LOG_LEVEL" "INFO" "Options: DEBUG | INFO | WARNING | ERROR")
$Config += "TEST_MODE=" + (Prompt-Var "TEST_MODE" "false" "Set true to disable network calls during some code paths.")
$Config += "ENABLE_ENV_DIAGNOSTICS=" + (Prompt-Var "ENABLE_ENV_DIAGNOSTICS" "false" "Set true to log additional (allowlisted) environment diagnostics.")
$Config += "DEBUG_SYMBOLS=" + (Prompt-Var "DEBUG_SYMBOLS" "" "Optional: comma-separated symbols for debug runs (e.g., AAPL,MSFT).")
$Config += "FEATURE_ENGINEERING_MAX_WORKERS=" + (Prompt-Var "FEATURE_ENGINEERING_MAX_WORKERS" "" "Optional: max workers for feature engineering fan-out.")
$Config += "DOMAIN_METADATA_MAX_SCANNED_BLOBS=" + (Prompt-Var "DOMAIN_METADATA_MAX_SCANNED_BLOBS" "200000" "Optional: upper bound for domain metadata scans (monitoring).")
$Config += "ASSET_ALLOCATION_REQUIRE_AZURE_STORAGE=" + (Prompt-Var "ASSET_ALLOCATION_REQUIRE_AZURE_STORAGE" "" "Optional: set true to require Azure storage config at startup.")

# -------------------------------------------------------------------------
# Azure Identity (GitHub deploy + optional local auth)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# Azure Identity (GitHub deploy + optional local auth)"
$Config += "# =========================================="
$Config += "AZURE_CLIENT_ID=" + (Prompt-Var "AZURE_CLIENT_ID" "" "GitHub Actions OIDC client/app ID (required for deploy)." -Secret)
$Config += "AZURE_TENANT_ID=" + (Prompt-Var "AZURE_TENANT_ID" "" "Azure tenant ID (required for deploy)." -Secret)
$Config += "AZURE_SUBSCRIPTION_ID=" + (Prompt-Var "AZURE_SUBSCRIPTION_ID" "" "Azure subscription ID (required for deploy)." -Secret)
$Config += "AZURE_CLIENT_SECRET=" + (Prompt-Var "AZURE_CLIENT_SECRET" "" "Optional: Service Principal client secret (not required for GitHub OIDC)." -Secret)

# -------------------------------------------------------------------------
# Azure Storage (required for pipelines)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# Azure Storage (required for pipelines)"
$Config += "# =========================================="
$Config += "AZURE_STORAGE_ACCOUNT_NAME=" + (Prompt-Var "AZURE_STORAGE_ACCOUNT_NAME" "" "Storage account name (also used by CI)." -Secret)
$Config += "AZURE_STORAGE_CONNECTION_STRING=" + (Prompt-Var "AZURE_STORAGE_CONNECTION_STRING" "" "Storage connection string (recommended for local dev; required by CI)." -Secret)
$Config += "AZURE_STORAGE_ACCOUNT_KEY=" + (Prompt-Var "AZURE_STORAGE_ACCOUNT_KEY" "" "Optional: storage account key (alternative to connection string)." -Secret)
$Config += "AZURE_STORAGE_ACCESS_KEY=" + (Prompt-Var "AZURE_STORAGE_ACCESS_KEY" "" "Optional: storage access key (alias of account key)." -Secret)
$Config += "AZURE_STORAGE_SAS_TOKEN=" + (Prompt-Var "AZURE_STORAGE_SAS_TOKEN" "" "Optional: SAS token (alternative auth)." -Secret)

# -------------------------------------------------------------------------
# Storage Containers & Folders (canonical names)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# Storage Containers & Folders (canonical names)"
$Config += "# =========================================="
$Config += "AZURE_CONTAINER_COMMON=" + (Prompt-Var "AZURE_CONTAINER_COMMON" "common" "Blob container for shared artifacts.")
$Config += "AZURE_CONTAINER_BRONZE=" + (Prompt-Var "AZURE_CONTAINER_BRONZE" "bronze" "Blob container for bronze layer.")
$Config += "AZURE_CONTAINER_SILVER=" + (Prompt-Var "AZURE_CONTAINER_SILVER" "silver" "Blob container for silver layer.")
$Config += "AZURE_CONTAINER_GOLD=" + (Prompt-Var "AZURE_CONTAINER_GOLD" "gold" "Blob container for gold layer.")
$Config += "AZURE_CONTAINER_PLATINUM=" + (Prompt-Var "AZURE_CONTAINER_PLATINUM" "platinum" "Blob container for platinum layer.")
$Config += "AZURE_FOLDER_MARKET=" + (Prompt-Var "AZURE_FOLDER_MARKET" "market-data" "Folder/prefix for market data.")
$Config += "AZURE_FOLDER_FINANCE=" + (Prompt-Var "AZURE_FOLDER_FINANCE" "finance-data" "Folder/prefix for finance data.")
$Config += "AZURE_FOLDER_EARNINGS=" + (Prompt-Var "AZURE_FOLDER_EARNINGS" "earnings-data" "Folder/prefix for earnings data.")
$Config += "AZURE_FOLDER_TARGETS=" + (Prompt-Var "AZURE_FOLDER_TARGETS" "price-target-data" "Folder/prefix for price target data.")

# -------------------------------------------------------------------------
# External Data APIs
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# External Data APIs"
$Config += "# =========================================="
$Config += "ALPHA_VANTAGE_API_KEY=" + (Prompt-Var "ALPHA_VANTAGE_API_KEY" "" "Alpha Vantage API key (required)." -Secret)
$Config += "ALPHA_VANTAGE_RATE_LIMIT_PER_MIN=" + (Prompt-Var "ALPHA_VANTAGE_RATE_LIMIT_PER_MIN" "300" "Requests per minute allowed for your tier.")
$Config += "ALPHA_VANTAGE_TIMEOUT_SECONDS=" + (Prompt-Var "ALPHA_VANTAGE_TIMEOUT_SECONDS" "15" "HTTP timeout per request (seconds).")
$Config += "ALPHA_VANTAGE_MAX_WORKERS=" + (Prompt-Var "ALPHA_VANTAGE_MAX_WORKERS" "32" "Max concurrent fetch workers.")
$Config += "ALPHA_VANTAGE_EARNINGS_FRESH_DAYS=" + (Prompt-Var "ALPHA_VANTAGE_EARNINGS_FRESH_DAYS" "7" "Skip re-fetching earnings newer than this many days.")
$Config += "ALPHA_VANTAGE_FINANCE_FRESH_DAYS=" + (Prompt-Var "ALPHA_VANTAGE_FINANCE_FRESH_DAYS" "28" "Skip re-fetching fundamentals newer than this many days.")
$Config += "NASDAQ_API_KEY=" + (Prompt-Var "NASDAQ_API_KEY" "" "Nasdaq Data Link API key (required for price targets)." -Secret)

# -------------------------------------------------------------------------
# Postgres (optional locally; used by API and some tasks)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# Postgres"
$Config += "# =========================================="
$Config += "POSTGRES_DSN=" + (Prompt-Var "POSTGRES_DSN" "" "Postgres DSN (postgresql://user:pass@host:5432/db)." -Secret)

# -------------------------------------------------------------------------
# API Service
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# API Service"
$Config += "# =========================================="
$Config += "API_AUTH_MODE=" + (Prompt-Var "API_AUTH_MODE" "none" "Options: none | api_key | oidc | api_key_or_oidc")
$Config += "API_KEY=" + (Prompt-Var "API_KEY" "" "API key (required if API_AUTH_MODE=api_key or api_key_or_oidc)." -Secret)
$Config += "API_KEY_HEADER=" + (Prompt-Var "API_KEY_HEADER" "X-API-Key" "Header name for API keys.")
$Config += "API_ROOT_PREFIX=" + (Prompt-Var "API_ROOT_PREFIX" "" "Optional: mount API under /{API_ROOT_PREFIX}/api/* (e.g. asset-allocation).")
$Config += "API_PORT=" + (Prompt-Var "API_PORT" "8000" "Local API port (used by core/config.py).")
$Config += "API_CSP=" + (Prompt-Var "API_CSP" "" "Optional: Content-Security-Policy header value.")
$Config += "API_CORS_ALLOW_ORIGINS=" + (Prompt-Var "API_CORS_ALLOW_ORIGINS" "" "Optional: comma-separated or JSON list of allowed origins.")

# OIDC (optional)
$Config += "API_OIDC_ISSUER=" + (Prompt-Var "API_OIDC_ISSUER" "" "Optional: OIDC issuer URL (required for API_AUTH_MODE=oidc).")
$Config += "API_OIDC_AUDIENCE=" + (Prompt-Var "API_OIDC_AUDIENCE" "" "Optional: comma-separated audiences (required for API_AUTH_MODE=oidc).")
$Config += "API_OIDC_JWKS_URL=" + (Prompt-Var "API_OIDC_JWKS_URL" "" "Optional: JWKS URL (if not discoverable).")
$Config += "API_OIDC_REQUIRED_SCOPES=" + (Prompt-Var "API_OIDC_REQUIRED_SCOPES" "" "Optional: comma-separated required scopes.")
$Config += "API_OIDC_REQUIRED_ROLES=" + (Prompt-Var "API_OIDC_REQUIRED_ROLES" "" "Optional: comma-separated required roles.")

# UI auth config served by API (optional)
$Config += "UI_AUTH_MODE=" + (Prompt-Var "UI_AUTH_MODE" "" "Optional: UI auth mode (none|api_key|oidc).")
$Config += "UI_OIDC_CLIENT_ID=" + (Prompt-Var "UI_OIDC_CLIENT_ID" "" "Optional: UI OIDC client ID.")
$Config += "UI_OIDC_AUTHORITY=" + (Prompt-Var "UI_OIDC_AUTHORITY" "" "Optional: UI OIDC authority (defaults to API_OIDC_ISSUER).")
$Config += "UI_OIDC_SCOPES=" + (Prompt-Var "UI_OIDC_SCOPES" "" "Optional: UI OIDC scopes.")
$Config += "UI_OIDC_REDIRECT_URI=" + (Prompt-Var "UI_OIDC_REDIRECT_URI" "" "Optional: UI redirect URI.")
$Config += "UI_API_BASE_URL=" + (Prompt-Var "UI_API_BASE_URL" "" "Optional: base URL where the UI reaches this API.")
$Config += "UI_DIST_DIR=" + (Prompt-Var "UI_DIST_DIR" "" "Optional: local path to UI dist for serving static UI.")

# -------------------------------------------------------------------------
# System Health Monitoring (FastAPI: GET /api/system/health)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# System Health Monitoring (FastAPI: GET /api/system/health)"
$Config += "# =========================================="
$Config += "SYSTEM_HEALTH_TTL_SECONDS=" + (Prompt-Var "SYSTEM_HEALTH_TTL_SECONDS" "10" "Cache TTL for /api/system/health.")
$Config += "SYSTEM_HEALTH_MAX_AGE_SECONDS=" + (Prompt-Var "SYSTEM_HEALTH_MAX_AGE_SECONDS" "129600" "Max staleness before reporting stale.")
$Config += "SYSTEM_HEALTH_VERBOSE_IDS=" + (Prompt-Var "SYSTEM_HEALTH_VERBOSE_IDS" "" "Optional: include Azure resource IDs in response.")

# Optional: Azure control-plane probes (ARM) for Container Apps + Jobs
$Config += "SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID=" + (Prompt-Var "SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID" "" "Optional: ARM subscription ID for probes / job start allowlist.")
$Config += "SYSTEM_HEALTH_ARM_RESOURCE_GROUP=" + (Prompt-Var "SYSTEM_HEALTH_ARM_RESOURCE_GROUP" "" "Optional: ARM resource group for probes / job start allowlist.")
$Config += "SYSTEM_HEALTH_ARM_CONTAINERAPPS=" + (Prompt-Var "SYSTEM_HEALTH_ARM_CONTAINERAPPS" "" "Optional: comma-separated Container Apps names.")
$Config += "SYSTEM_HEALTH_ARM_JOBS=" + (Prompt-Var "SYSTEM_HEALTH_ARM_JOBS" "" "Optional: comma-separated Job names (also used as job-start allowlist).")
$Config += "SYSTEM_HEALTH_ARM_API_VERSION=" + (Prompt-Var "SYSTEM_HEALTH_ARM_API_VERSION" "" "Optional: ARM API version (required if ARM probes enabled).")
$Config += "SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS=" + (Prompt-Var "SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS" "" "Optional: ARM timeout seconds.")
$Config += "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB=" + (Prompt-Var "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB" "" "Optional: how many executions to return per job.")

# Optional: Azure Resource Health (runtime availability)
$Config += "SYSTEM_HEALTH_RESOURCE_HEALTH_ENABLED=" + (Prompt-Var "SYSTEM_HEALTH_RESOURCE_HEALTH_ENABLED" "" "Optional: enable Azure Resource Health probes (true/false).")
$Config += "SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION=" + (Prompt-Var "SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION" "" "Optional: Resource Health API version.")

# Optional: Azure Monitor Metrics (runtime telemetry)
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_ENABLED=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_ENABLED" "" "Optional: enable metrics probes (true/false).")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION" "" "Optional: Metrics API version.")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES" "" "Optional: timespan minutes (e.g., 15).")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL" "" "Optional: interval (e.g., PT1M).")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION" "" "Optional: aggregation (e.g., Average).")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS" "" "Optional: comma-separated metric names.")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS" "" "Optional: comma-separated metric names.")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON" "" "Optional: JSON thresholds object.")

# Optional: Azure Log Analytics (KQL aggregates + job execution log tails)
$Config += "SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED=" + (Prompt-Var "SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED" "" "Optional: enable Log Analytics probes (true/false).")
$Config += "SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID=" + (Prompt-Var "SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID" "" "Optional: Log Analytics workspace ID.")
$Config += "SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS=" + (Prompt-Var "SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS" "" "Optional: Log Analytics timeout seconds.")
$Config += "SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES=" + (Prompt-Var "SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES" "" "Optional: timespan minutes (e.g., 15).")
$Config += "SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON=" + (Prompt-Var "SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON" "" "Optional: JSON array of query specs.")

# -------------------------------------------------------------------------
# Pipeline Controls
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# Pipeline Controls"
$Config += "# =========================================="
$Config += "SILVER_LATEST_ONLY=" + (Prompt-Var "SILVER_LATEST_ONLY" "" "Optional: default latest-only flag for silver pipelines.")
$Config += "SILVER_MARKET_LATEST_ONLY=" + (Prompt-Var "SILVER_MARKET_LATEST_ONLY" "" "Optional: override latest-only for market.")
$Config += "SILVER_FINANCE_LATEST_ONLY=" + (Prompt-Var "SILVER_FINANCE_LATEST_ONLY" "" "Optional: override latest-only for finance.")
$Config += "SILVER_EARNINGS_LATEST_ONLY=" + (Prompt-Var "SILVER_EARNINGS_LATEST_ONLY" "" "Optional: override latest-only for earnings.")
$Config += "SILVER_PRICE_TARGET_LATEST_ONLY=" + (Prompt-Var "SILVER_PRICE_TARGET_LATEST_ONLY" "" "Optional: override latest-only for price targets.")
$Config += "BACKFILL_START_DATE=" + (Prompt-Var "BACKFILL_START_DATE" "" "Optional: YYYY-MM-DD (backfill start).")
$Config += "BACKFILL_END_DATE=" + (Prompt-Var "BACKFILL_END_DATE" "" "Optional: YYYY-MM-DD (backfill end).")
$Config += "MATERIALIZE_YEAR_MONTH=" + (Prompt-Var "MATERIALIZE_YEAR_MONTH" "" "Optional: force materialization month (YYYY-MM).")
$Config += "MATERIALIZE_WINDOW_MONTHS=" + (Prompt-Var "MATERIALIZE_WINDOW_MONTHS" "" "Optional: rolling window months for materialization (default 1).")
$Config += "MATERIALIZE_BY_DATE_RUN_AT_UTC_HOUR=" + (Prompt-Var "MATERIALIZE_BY_DATE_RUN_AT_UTC_HOUR" "" "Optional: only run materialization at this UTC hour.")
$Config += "TRIGGER_NEXT_JOB_NAME=" + (Prompt-Var "TRIGGER_NEXT_JOB_NAME" "" "Optional: if set, trigger next job when current finishes.")
$Config += "TRIGGER_NEXT_JOB_REQUIRED=" + (Prompt-Var "TRIGGER_NEXT_JOB_REQUIRED" "" "Optional: whether triggering the next job is required (true/false).")
$Config += "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS=" + (Prompt-Var "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS" "" "Optional: retries when triggering next job (default 3).")
$Config += "TRIGGER_NEXT_JOB_RETRY_BASE_SECONDS=" + (Prompt-Var "TRIGGER_NEXT_JOB_RETRY_BASE_SECONDS" "" "Optional: base backoff seconds (default 1.0).")

# -------------------------------------------------------------------------
# UI / CI Variables (GitHub Variables)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# UI / CI Variables (GitHub Variables)"
$Config += "# =========================================="
$Config += "VITE_PORT=" + (Prompt-Var "VITE_PORT" "5174" "Vite dev server port (required by CI UI build).")
$Config += "SERVICE_ACCOUNT_NAME=" + (Prompt-Var "SERVICE_ACCOUNT_NAME" "asset-allocation-sa" "Service account name (used by deploy manifests).")
$Config += "KUBERNETES_NAMESPACE=" + (Prompt-Var "KUBERNETES_NAMESPACE" "" "Optional: Kubernetes namespace (used by provision_azure.ps1 when AKS is enabled).")
$Config += "AKS_CLUSTER_NAME=" + (Prompt-Var "AKS_CLUSTER_NAME" "" "Optional: AKS cluster name (used by provision_azure.ps1 when AKS is enabled).")

if ($DryRun) {
    Write-Host "`n[DRY RUN] Would write the following to ${EnvFilePath}:" -ForegroundColor Yellow
    $Config | ForEach-Object { Write-Host $_ }
    exit 0
}

$Config | Out-File -FilePath $EnvFilePath -Encoding utf8
Write-Host "`n[SUCCESS] Environment saved to $EnvFilePath" -ForegroundColor Green

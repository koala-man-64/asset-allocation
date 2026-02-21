param (
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$repoRoot = Join-Path $PSScriptRoot ".."
$envPath = Join-Path $repoRoot ".env.web"
$localEnvPath = Join-Path $repoRoot ".env"

if (-not (Test-Path $envPath)) {
    Write-Error "Error: env file not found at $envPath (create .env.web)."
    exit 1
}

function Parse-EnvFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $map = @{}
    foreach ($rawLine in (Get-Content $Path)) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) { continue }
        if ($line -notmatch "^([^=]+)=(.*)$") { continue }

        $key = $matches[1].Trim()
        $value = $matches[2].Trim()
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        $map[$key] = $value
    }
    return $map
}

function Test-LocalAddressValue {
    param(
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $false
    }

    try {
        $uri = [System.Uri]$Value
        if ($uri.IsAbsoluteUri -and ($uri.Host -in @("localhost", "127.0.0.1", "::1"))) {
            return $true
        }
    } catch {
        # Fall back to substring check for non-URI values.
    }

    return $Value -match "(?i)\blocalhost\b|127\.0\.0\.1|::1"
}

# Check if gh CLI is installed
if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    Write-Error "Error: GitHub CLI (gh) is not installed or not in PATH."
    exit 1
}

Write-Host "Reading local env from: $envPath"
if ($DryRun) { Write-Host "Running in DRY RUN mode (no changes will be made)..." -ForegroundColor Yellow }

$envMap = Parse-EnvFile -Path $envPath
$lines = Get-Content $envPath

if (Test-Path $localEnvPath) {
    $localMap = Parse-EnvFile -Path $localEnvPath
    $missingInWeb = @()
    foreach ($key in $localMap.Keys) {
        $localValue = $localMap[$key]
        if ([string]::IsNullOrWhiteSpace($localValue)) { continue }
        if (-not $envMap.ContainsKey($key) -or [string]::IsNullOrWhiteSpace($envMap[$key])) {
            $missingInWeb += $key
        }
    }

    if ($missingInWeb.Count -gt 0) {
        $sortedMissing = $missingInWeb | Sort-Object -Unique
        Write-Error ("Error: .env.web is missing populated keys from .env: {0}" -f ($sortedMissing -join ", "))
        exit 1
    }
} else {
    Write-Warning "No local .env found; skipping key parity check against .env."
}

$webUrlKeys = @("ASSET_ALLOCATION_API_BASE_URL", "VITE_API_PROXY_TARGET")
$localEndpointViolations = @()
foreach ($key in $webUrlKeys) {
    if (-not $envMap.ContainsKey($key)) { continue }
    $value = $envMap[$key]
    if (Test-LocalAddressValue -Value $value) {
        $localEndpointViolations += "$key=$value"
    }
}
if ($localEndpointViolations.Count -gt 0) {
    Write-Error ("Error: .env.web contains local endpoints for web sync: {0}" -f ($localEndpointViolations -join ", "))
    exit 1
}
$ExpectedSecrets = @()
$ExpectedVars = @()

# These patterns identify CONFIGURATION variables (GitHub Variables).
# Anything NOT matching these patterns is treated as a SECRET.
$ConfigPatterns = @(
    "^AZURE_CONTAINER_",
    "^AZURE_FOLDER_",
    "^[A-Z]+_(MARKET|FINANCE|EARNINGS|PRICE_TARGET)_JOB$",
    "^DEBUG_SYMBOLS$",
    "^SYMBOLS_REFRESH_INTERVAL_HOURS$",
    "^FEATURE_ENGINEERING_MAX_WORKERS$",
    "^DOMAIN_METADATA_MAX_SCANNED_BLOBS$",
    "^ASSET_ALLOCATION_REQUIRE_AZURE_STORAGE$",
    "^ASSET_ALLOCATION_API_(?!KEY$)",
    "^ALPHA_VANTAGE_(?!API_KEY$)",
    # Massive: keep runtime tuning/location values as Variables; keep credentials as Secrets.
    "^MASSIVE_BASE_URL$",
    "^MASSIVE_FINANCE_FRESH_DAYS$",
    "^MASSIVE_FLATFILES_BUCKET$",
    "^MASSIVE_FLATFILES_ENDPOINT_URL$",
    "^MASSIVE_MAX_WORKERS$",
    "^MASSIVE_PREFER_OFFICIAL_SDK$",
    "^MASSIVE_TIMEOUT_SECONDS$",
    "^MASSIVE_WS_SUBSCRIPTIONS$",
    "^SYSTEM_HEALTH_(?!LINK_TOKEN_SECRET$)",
    "^HEADLESS_MODE$",
    "^DISABLE_DOTENV$",
    "^LOG_",
    "^TEST_MODE$",
    "^VITE_PORT$",
    "^VITE_PROXY_CONFIG_JS$",
    "^VITE_API_PROXY_TARGET$",
    "^SERVICE_ACCOUNT_NAME$",
    "^KUBERNETES_NAMESPACE$",
    "^AKS_CLUSTER_NAME$",
    "^API_AUTH_MODE$",
    "^API_KEY_HEADER$",
    "^API_ROOT_PREFIX$",
    "^API_INGRESS_EXTERNAL$",
    "^API_PORT$",
    "^API_CSP$",
    "^API_CORS_ALLOW_ORIGINS$",
    "^API_OIDC_",
    "^UI_"
)

# -------------------------------------------------------------------------
# 1. PARSE .ENV
# -------------------------------------------------------------------------
foreach ($line in $lines) {
    $line = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) { continue }

    if ($line -match "^([^=]+)=(.*)$") {
        $key = $matches[1].Trim()
        $value = $matches[2].Trim()

        # Remove surrounding quotes for value
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or 
            ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        # Classify as Config (Var) or Secret
        $isConfig = $false
        foreach ($pattern in $ConfigPatterns) {
            if ($key -match $pattern) {
                $isConfig = $true
                break
            }
        }

	        if ($isConfig) {
	            # It's a Variable
	            if ([string]::IsNullOrWhiteSpace($value)) {
	                if ($DryRun) {
	                    Write-Host "[DRY RUN] Would SKIP empty VARIABLE: $key" -ForegroundColor Yellow
	                } else {
	                    Write-Host "Skipping VARIABLE with empty value: $key" -ForegroundColor Yellow
	                }
	            } else {
	                if ($DryRun) {
	                    Write-Host "[DRY RUN] Would set VARIABLE: $key" -ForegroundColor Cyan
	                } else {
	                    Write-Host "Setting VARIABLE: $key" -NoNewline
	                    try {
	                        $value | gh variable set "$key" 
	                        Write-Host " [OK]" -ForegroundColor Green
	                    } catch {
	                        Write-Host " [FAILED]" -ForegroundColor Red
	                        Write-Error $_
	                    }
	                }
	            }
	            $ExpectedVars += $key
	        } else {
	            # It's a Secret
	            if ([string]::IsNullOrWhiteSpace($value)) {
	                if ($DryRun) {
	                    Write-Host "[DRY RUN] Would SKIP empty SECRET:   $key" -ForegroundColor Yellow
	                } else {
	                    Write-Host "Skipping SECRET with empty value:   $key" -ForegroundColor Yellow
	                }
	            } else {
	                if ($DryRun) {
	                    Write-Host "[DRY RUN] Would set SECRET:   $key" -ForegroundColor Magenta
	                } else {
	                    Write-Host "Setting SECRET:   $key" -NoNewline
	                    try {
	                        $value | gh secret set "$key" 
	                        Write-Host " [OK]" -ForegroundColor Green
	                    } catch {
	                        Write-Host " [FAILED]" -ForegroundColor Red
	                        Write-Error $_
	                    }
	                }
	            }
	            $ExpectedSecrets += $key
	        }
	    }
	}

# -------------------------------------------------------------------------
# 2. PRUNE SECRETS
# -------------------------------------------------------------------------
Write-Host "`n----------------------------------------"
Write-Host "Checking for unexpected SECRETS in GitHub..."
$remoteSecrets = gh secret list --json name --jq ".[].name" 2>$null
if (-not $remoteSecrets) { $remoteSecrets = @() }

$secretsToDelete = @()
foreach ($s in $remoteSecrets) {
    if ($ExpectedSecrets -notcontains $s) {
        $secretsToDelete += $s
    }
}

if ($secretsToDelete.Count -gt 0) {
    $secretsToDelete | ForEach-Object { Write-Host " - [UNEXPECTED SECRET] $_" -ForegroundColor Red }
    if ($DryRun) {
        Write-Host "[DRY RUN] Would delete these secrets." -ForegroundColor Cyan
    } else {
        $confirm = Read-Host "Delete these secrets? Type 'yes' to confirm"
        if ($confirm -eq "yes") {
            foreach ($s in $secretsToDelete) {
                Write-Host "Deleting secret: $s..." -NoNewline
                gh secret delete "$s"
                Write-Host " [OK]" -ForegroundColor Green
            }
        } else {
            Write-Host "Skipping secret deletions." -ForegroundColor Yellow
        }
    }
} else {
    Write-Host "No unexpected secrets found." -ForegroundColor Green
}

# -------------------------------------------------------------------------
# 3. PRUNE VARIABLES
# -------------------------------------------------------------------------
Write-Host "`n----------------------------------------"
Write-Host "Checking for unexpected VARIABLES in GitHub..."
$remoteVars = gh variable list --json name --jq ".[].name" 2>$null
if (-not $remoteVars) { $remoteVars = @() }

$varsToDelete = @()
foreach ($v in $remoteVars) {
    if ($ExpectedVars -notcontains $v) {
        $varsToDelete += $v
    }
}

if ($varsToDelete.Count -gt 0) {
    $varsToDelete | ForEach-Object { Write-Host " - [UNEXPECTED VARIABLE] $_" -ForegroundColor Red }
    if ($DryRun) {
        Write-Host "[DRY RUN] Would delete these variables." -ForegroundColor Cyan
    } else {
        $confirm = Read-Host "Delete these variables? Type 'yes' to confirm"
        if ($confirm -eq "yes") {
            foreach ($v in $varsToDelete) {
                Write-Host "Deleting variable: $v..." -NoNewline
                gh variable delete "$v"
                Write-Host " [OK]" -ForegroundColor Green
            }
        } else {
            Write-Host "Skipping variable deletions." -ForegroundColor Yellow
        }
    }
} else {
    Write-Host "No unexpected variables found." -ForegroundColor Green
}

Write-Host "`nSync complete." -ForegroundColor Green

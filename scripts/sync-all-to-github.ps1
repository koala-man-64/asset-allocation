param (
    [string]$EnvFilePath = "$PSScriptRoot\..\.env",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $EnvFilePath)) {
    Write-Error "Error: .env file not found at $EnvFilePath"
    exit 1
}

# Check if gh CLI is installed
if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    Write-Error "Error: GitHub CLI (gh) is not installed or not in PATH."
    exit 1
}

Write-Host "Reading local .env from: $EnvFilePath"
if ($DryRun) { Write-Host "Running in DRY RUN mode (no changes will be made)..." -ForegroundColor Yellow }

$lines = Get-Content $EnvFilePath
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
	    "^SYSTEM_HEALTH_",
	    "^HEADLESS_MODE$",
	    "^DISABLE_DOTENV$",
	    "^LOG_",
	    "^TEST_MODE$",
	    "^VITE_PORT$",
	    "^SERVICE_ACCOUNT_NAME$",
	    "^KUBERNETES_NAMESPACE$",
	    "^AKS_CLUSTER_NAME$",
	    "^API_AUTH_MODE$",
	    "^API_KEY_HEADER$",
	    "^API_ROOT_PREFIX$",
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

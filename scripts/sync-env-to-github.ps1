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
try {
    Get-Command gh -ErrorAction Stop | Out-Null
} catch {
    Write-Error "Error: GitHub CLI (gh) is not installed or not in PATH."
    exit 1
}

Write-Host "Reading secrets from: $EnvFilePath"
if ($DryRun) { Write-Host "Checking secrets in DRY RUN mode (no changes will be made)..." -ForegroundColor Yellow }

$lines = Get-Content $EnvFilePath
$count = 0

foreach ($line in $lines) {
    # Trim whitespace
    $line = $line.Trim()

    # Skip comments and empty lines
    if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) {
        continue
    }

    # Split by first '=' only
    if ($line -match "^([^=]+)=(.*)$") {
        $key = $matches[1].Trim()
        $value = $matches[2].Trim()

        # Remove surrounding quotes if present
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or 
            ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        if ($DryRun) {
            Write-Host "[DRY RUN] Would set secret: $key" -ForegroundColor Cyan
            # Validate gh auth status quickly in dry run without setting
        } else {
            Write-Host "Setting secret: $key" -NoNewline
            try {
                # Pipe value to gh secret set to avoid shell escaping issues
                $value | gh secret set "$key" 
                Write-Host " [OK]" -ForegroundColor Green
            } catch {
                Write-Host " [FAILED]" -ForegroundColor Red
                Write-Error $_
            }
        }
        $count++
    }
}

Write-Host "----------------------------------------"
if ($DryRun) {
    Write-Host "Dry run complete. Found $count secrets." -ForegroundColor Yellow
} else {
    Write-Host "Sync complete. Updated $count secrets." -ForegroundColor Green
}

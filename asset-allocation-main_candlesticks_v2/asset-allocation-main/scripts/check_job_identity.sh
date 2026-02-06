#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: check_job_identity.sh <resource-group> (--all | <job-name> [job-name...])

Checks that Container App Jobs have a system-assigned identity.

Arguments:
  resource-group  Azure resource group containing the jobs
  --all           Check all jobs in the resource group
  job-name(s)     One or more job names to check
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ] || [ $# -lt 2 ]; then
  usage
  exit 2
fi

resource_group="$1"
shift

jobs=()
if [ "${1:-}" = "--all" ]; then
  mapfile -t jobs < <(az containerapp job list --resource-group "$resource_group" --query "[].name" -o tsv)
else
  jobs=("$@")
fi

if [ "${#jobs[@]}" -eq 0 ]; then
  echo "ERROR: no jobs found to check." >&2
  exit 1
fi

failed=0

for job_name in "${jobs[@]}"; do
  job_name=$(echo "$job_name" | tr -d '\r')
  if ! az containerapp job show --name "$job_name" --resource-group "$resource_group" > /dev/null 2>&1; then
    echo "WARN: job $job_name not found; skipping."
    continue
  fi

  { read -r identity_type; read -r principal_id; } < <(az containerapp job show --name "$job_name" --resource-group "$resource_group" --query "[identity.type || '', identity.principalId || '']" -o tsv)
  identity_type=$(echo "$identity_type" | tr -d '\r')
  principal_id=$(echo "$principal_id" | tr -d '\r')

  if [[ "$identity_type" != *SystemAssigned* ]] || [ -z "$principal_id" ] || [ "$principal_id" = "None" ]; then
    echo "ERROR: job $job_name missing SystemAssigned identity (type='$identity_type', principalId='$principal_id')." >&2
    failed=$((failed + 1))
  else
    echo "OK: job $job_name has SystemAssigned identity ($principal_id)."
  fi
done

if [ "$failed" -gt 0 ]; then
  exit 1
fi

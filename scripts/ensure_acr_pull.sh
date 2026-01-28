#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ensure_acr_pull.sh <resource-group> <acr-name> <kind> <resource-name>

Ensures the target Azure Container App / Job system-assigned identity has AcrPull
role assignment on the given Azure Container Registry.

Arguments:
  resource-group  Azure resource group containing the resources
  acr-name        Azure Container Registry name (e.g., assetallocationacr)
  kind            "app" or "job"
  resource-name   Container App name or Container App Job name
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ] || [ $# -ne 4 ]; then
  usage
  exit 2
fi

resource_group="$1"
acr_name="$2"
kind="$3"
resource_name="$4"

acr_id="$(az acr show --name "$acr_name" --resource-group "$resource_group" --query id -o tsv | tr -d '\r')"

principal_id=""
for _ in $(seq 1 10); do
  case "$kind" in
    app)
      principal_id="$(az containerapp show --name "$resource_name" --resource-group "$resource_group" --query identity.principalId -o tsv 2>/dev/null | tr -d '\r' || true)"
      ;;
    job)
      principal_id="$(az containerapp job show --name "$resource_name" --resource-group "$resource_group" --query identity.principalId -o tsv 2>/dev/null | tr -d '\r' || true)"
      ;;
    *)
      echo "ERROR: unknown kind '$kind' (expected 'app' or 'job')" >&2
      exit 2
      ;;
  esac

  if [ -n "$principal_id" ] && [ "$principal_id" != "None" ]; then
    break
  fi
  sleep 3
done

if [ -z "$principal_id" ] || [ "$principal_id" = "None" ]; then
  echo "ERROR: no system-assigned identity principalId found for $kind '$resource_name' in RG '$resource_group'." >&2
  echo "Ensure the resource has identity.type=SystemAssigned enabled." >&2
  exit 1
fi


existing="$(az role assignment list --assignee-object-id "$principal_id" --scope "$acr_id" --query "[?roleDefinitionName=='AcrPull'] | length(@)" -o tsv --only-show-errors || true)"
existing="${existing:-0}"

if [ "$existing" = "0" ]; then
  az role assignment create --assignee-object-id "$principal_id" --assignee-principal-type ServicePrincipal --role "AcrPull" --scope "$acr_id" --only-show-errors 1>/dev/null
  echo "Granted AcrPull on '$acr_name' to $kind '$resource_name' ($principal_id)."
else
  echo "AcrPull already present for $kind '$resource_name' ($principal_id)."
fi


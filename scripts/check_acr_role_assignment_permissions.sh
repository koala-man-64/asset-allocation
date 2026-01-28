#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: check_acr_role_assignment_permissions.sh <resource-group> <acr-name> [assignee]

Checks whether the assignee has permission to create role assignments (e.g., AcrPull)
on the target ACR scope (or parent scopes).

Arguments:
  resource-group  Azure resource group containing the ACR
  acr-name        Azure Container Registry name (e.g., assetallocationacr)
  assignee        Service principal appId/clientId or objectId. Defaults to AZURE_CLIENT_ID.
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ] || [ $# -lt 2 ] || [ $# -gt 3 ]; then
  usage
  exit 2
fi

resource_group="$1"
acr_name="$2"
assignee="${3:-${AZURE_CLIENT_ID:-}}"

load_env_var() {
  local key="$1"
  local env_file="$2"
  local line=""
  if [ ! -f "$env_file" ]; then
    return 0
  fi
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      ""|\#*) continue ;;
    esac
    if [[ "$line" == "$key="* ]]; then
      local value="${line#${key}=}"
      value="${value%$'\r'}"
      value="${value#\"}"
      value="${value%\"}"
      value="${value#\'}"
      value="${value%\'}"
      printf '%s' "$value"
      return 0
    fi
  done < "$env_file"
}

if [ -z "$assignee" ]; then
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  env_path="$repo_root/.env"
  env_value="$(load_env_var "AZURE_CLIENT_ID" "$env_path")"
  if [ -n "$env_value" ]; then
    assignee="$env_value"
  fi
fi

if [ -z "$assignee" ]; then
  echo "ERROR: assignee not provided and AZURE_CLIENT_ID is empty." >&2
  exit 2
fi

acr_id="$(az acr show --name "$acr_name" --resource-group "$resource_group" --query id -o tsv)"

assignments="$(az role assignment list --assignee "$assignee" --all \
  --query "[?starts_with('${acr_id}', scope)]" -o json)"

ASSIGNMENTS_JSON="$assignments" ACR_ID="$acr_id" ASSIGNEE="$assignee" python3 - <<'PY'
import json
import os
import subprocess
import sys
from fnmatch import fnmatch

assignments = json.loads(os.environ["ASSIGNMENTS_JSON"])
acr_id = os.environ["ACR_ID"]
assignee = os.environ["ASSIGNEE"]

target_action = "Microsoft.Authorization/roleAssignments/write"

def allows_role_assignments(role_def):
    perms = role_def.get("permissions", []) or []
    for perm in perms:
        actions = perm.get("actions", []) or []
        not_actions = perm.get("notActions", []) or []
        if any(fnmatch(target_action, pat) for pat in not_actions):
            continue
        if any(fnmatch(target_action, pat) for pat in actions):
            return True
    return False

def role_allows(role_id):
    try:
        raw = subprocess.check_output(
            ["az", "role", "definition", "show", "--id", role_id, "-o", "json"],
            text=True,
        )
        role_def = json.loads(raw)
    except subprocess.CalledProcessError as exc:
        print(f"ERROR: failed to fetch role definition {role_id}: {exc}", file=sys.stderr)
        return False
    return allows_role_assignments(role_def)

if not assignments:
    print(
        f"ERROR: no role assignments found for assignee {assignee} at or above ACR scope {acr_id}.",
        file=sys.stderr,
    )
    sys.exit(1)

for assignment in assignments:
    role_id = assignment.get("roleDefinitionId")
    role_name = assignment.get("roleDefinitionName", "<unknown>")
    scope = assignment.get("scope", "<unknown>")
    if not role_id:
        continue
    if role_allows(role_id):
        print(
            "OK: assignee has role-assignment write permission "
            f"(role={role_name}, scope={scope})."
        )
        sys.exit(0)

print(
    "ERROR: assignee does not appear to have role-assignment write permission "
    f"at or above ACR scope {acr_id}.",
    file=sys.stderr,
)
print("Hint: assign Owner or User Access Administrator at the RG or ACR scope.", file=sys.stderr)
sys.exit(1)
PY

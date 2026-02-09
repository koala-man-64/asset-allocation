#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: reset_postgres_from_scratch.sh --force [--dsn <postgres-dsn>] [--migrations-dir <dir>] [--env-file <path>] [--use-docker-psql]

Destructive operation:
- Drops all non-system schemas in the target database (including public) with CASCADE.
- Recreates public schema.
- Reapplies SQL migrations in lexical order from deploy/sql/postgres/migrations.

Arguments:
  --force                 Required safety flag.
  --dsn <postgres-dsn>    Postgres DSN. Defaults to POSTGRES_DSN env var.
  --migrations-dir <dir>  Migration directory. Default: deploy/sql/postgres/migrations
  --env-file <path>       Optional env file path (default: <repo>/.env if present).
  --use-docker-psql       Run psql via `docker run --rm -i postgres:16-alpine psql`.
  -h, --help              Show this help message.
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $cmd" >&2
    exit 1
  fi
}

force="false"
dsn="${POSTGRES_DSN:-}"
migrations_dir="deploy/sql/postgres/migrations"
use_docker_psql="false"
env_file=""

load_env_var() {
  local key="$1"
  local file_path="$2"
  local line=""

  if [[ ! -f "$file_path" ]]; then
    return 0
  fi

  while IFS= read -r line || [[ -n "$line" ]]; do
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
  done < "$file_path"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      force="true"
      shift
      ;;
    --dsn)
      dsn="${2:-}"
      shift 2
      ;;
    --migrations-dir)
      migrations_dir="${2:-}"
      shift 2
      ;;
    --env-file)
      env_file="${2:-}"
      shift 2
      ;;
    --use-docker-psql)
      use_docker_psql="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$env_file" ]]; then
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  candidate_env="${repo_root}/.env"
  if [[ -f "$candidate_env" ]]; then
    env_file="$candidate_env"
  fi
fi

if [[ -n "$env_file" && ! -f "$env_file" ]]; then
  echo "ERROR: env file not found: $env_file" >&2
  exit 2
fi

if [[ -z "$dsn" && -n "$env_file" && -f "$env_file" ]]; then
  dsn="$(load_env_var "POSTGRES_DSN" "$env_file")"
fi

if [[ "$force" != "true" ]]; then
  echo "ERROR: --force is required for destructive reset." >&2
  usage
  exit 2
fi

if [[ -z "${dsn}" ]]; then
  echo "ERROR: Postgres DSN not set. Pass --dsn or set POSTGRES_DSN." >&2
  exit 2
fi

if [[ ! -d "${migrations_dir}" ]]; then
  echo "ERROR: migrations directory not found: ${migrations_dir}" >&2
  exit 2
fi

run_psql() {
  if [[ "${use_docker_psql}" == "true" ]]; then
    docker run --rm -i postgres:16-alpine psql "$dsn" "$@"
  else
    psql "$dsn" "$@"
  fi
}

if [[ "${use_docker_psql}" == "true" ]]; then
  require_cmd docker
else
  require_cmd psql
fi

echo "Resetting database objects in DSN target (destructive)..."

run_psql -v ON_ERROR_STOP=1 <<'SQL'
DO $$
DECLARE
  s RECORD;
BEGIN
  FOR s IN
    SELECT n.nspname AS schema_name
    FROM pg_namespace n
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'public')
      AND n.nspname NOT LIKE 'pg_toast%'
      AND n.nspname NOT LIKE 'pg_temp_%'
  LOOP
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', s.schema_name);
  END LOOP;
END $$;

DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO CURRENT_USER;
GRANT USAGE ON SCHEMA public TO PUBLIC;
SQL

echo "Applying migrations from: ${migrations_dir}"

mapfile -t migration_files < <(find "${migrations_dir}" -maxdepth 1 -type f -name '*.sql' | sort)
if [[ ${#migration_files[@]} -eq 0 ]]; then
  echo "ERROR: no migration files found in ${migrations_dir}" >&2
  exit 1
fi

run_psql -v ON_ERROR_STOP=1 -c \
  "CREATE TABLE IF NOT EXISTS public.schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now());"

for file in "${migration_files[@]}"; do
  version="$(basename "$file" .sql)"
  applied="$(run_psql -tA -c "SELECT 1 FROM public.schema_migrations WHERE version='${version}' LIMIT 1;" | tr -d '\r')"
  if [[ "$applied" == "1" ]]; then
    echo "Already applied: ${version}"
    continue
  fi

  echo "Applying: ${version}"
  run_psql -v ON_ERROR_STOP=1 -f "$file"
  run_psql -v ON_ERROR_STOP=1 -c \
    "INSERT INTO public.schema_migrations(version) VALUES ('${version}') ON CONFLICT DO NOTHING;"
done

echo "Postgres reset complete."

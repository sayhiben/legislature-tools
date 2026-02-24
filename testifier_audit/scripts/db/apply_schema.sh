#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# shellcheck disable=SC1091
source "${PROJECT_ROOT}/scripts/lib/env.sh"
load_project_env "${PROJECT_ROOT}"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/db/apply_schema.sh [db_url] [schema_path]

Defaults:
  db_url      TESTIFIER_AUDIT_DB_URL, DATABASE_URL, or postgresql://legislature:legislature@localhost:55432/legislature
  schema_path ./sql/schema.sql

Also loads .env/.env.local from testifier_audit/ before resolving defaults.
EOF
}

if [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

DB_URL="${1:-${TESTIFIER_AUDIT_DB_URL:-${DATABASE_URL:-postgresql://legislature:legislature@localhost:55432/legislature}}}"
SCHEMA_PATH="${2:-${SCHEMA_PATH:-${PROJECT_ROOT}/sql/schema.sql}}"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required to apply schema. Install PostgreSQL client tools first." >&2
  exit 1
fi

if [[ ! -f "${SCHEMA_PATH}" ]]; then
  echo "Schema file not found: ${SCHEMA_PATH}" >&2
  exit 1
fi

echo "Applying schema from: ${SCHEMA_PATH}"
echo "Target database URL: ${DB_URL}"
psql "${DB_URL}" -v ON_ERROR_STOP=1 -f "${SCHEMA_PATH}"
echo "Schema apply complete."

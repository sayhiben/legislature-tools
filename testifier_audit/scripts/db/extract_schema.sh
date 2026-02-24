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
  ./scripts/db/extract_schema.sh [db_url] [out_path]

Defaults:
  db_url   TESTIFIER_AUDIT_DB_URL, DATABASE_URL, or postgresql://legislature:legislature@localhost:55432/legislature
  out_path ./sql/schema.sql

Also loads .env/.env.local from testifier_audit/ before resolving defaults.
EOF
}

if [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

DB_URL="${1:-${TESTIFIER_AUDIT_DB_URL:-${DATABASE_URL:-postgresql://legislature:legislature@localhost:55432/legislature}}}"
OUT_PATH="${2:-${PROJECT_ROOT}/sql/schema.sql}"

if ! command -v pg_dump >/dev/null 2>&1; then
  echo "pg_dump is required to extract schema. Install PostgreSQL client tools first." >&2
  exit 1
fi

mkdir -p "$(dirname "${OUT_PATH}")"

echo "Extracting schema from: ${DB_URL}"
echo "Writing schema file: ${OUT_PATH}"
pg_dump "${DB_URL}" \
  --schema-only \
  --no-owner \
  --no-privileges \
  --table=public_submissions \
  --table=voter_registry \
  --table=data_imports \
  >"${OUT_PATH}"

echo "Schema extract complete."

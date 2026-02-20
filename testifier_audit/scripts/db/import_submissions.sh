#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${PROJECT_ROOT}"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <submissions_csv_path> [additional import-submissions args]" >&2
  echo "Example: $0 /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv" >&2
  exit 1
fi

CSV_PATH="$1"
shift

DB_URL="${TESTIFIER_AUDIT_DB_URL:-${DATABASE_URL:-}}"
if [[ -z "${DB_URL}" ]]; then
  echo "Set TESTIFIER_AUDIT_DB_URL (or DATABASE_URL) before running this script." >&2
  exit 1
fi

CONFIG_PATH="${CONFIG_PATH:-configs/default.yaml}"
TABLE_NAME="${SUBMISSIONS_TABLE_NAME:-public_submissions}"
CHUNK_SIZE="${SUBMISSIONS_CHUNK_SIZE:-50000}"

if [[ "${CI_SKIP_INSTALL:-0}" != "1" ]]; then
  python -m pip install -e ".[dev]"
fi

python -m testifier_audit.cli import-submissions \
  --csv "${CSV_PATH}" \
  --config "${CONFIG_PATH}" \
  --db-url "${DB_URL}" \
  --table-name "${TABLE_NAME}" \
  --chunk-size "${CHUNK_SIZE}" \
  "$@"

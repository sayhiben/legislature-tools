#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CALLER_CWD="$(pwd)"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <vrdb_extract_path> [additional testifier-audit import-vrdb args]" >&2
  echo "Example: $0 ./data/raw/20260202_VRDB_Extract.txt" >&2
  exit 1
fi

EXTRACT_PATH="$1"
shift
if [[ "${EXTRACT_PATH}" != /* ]]; then
  EXTRACT_PATH="${CALLER_CWD}/${EXTRACT_PATH#./}"
fi

cd "${PROJECT_ROOT}"

DB_URL="${TESTIFIER_AUDIT_DB_URL:-${DATABASE_URL:-}}"
if [[ -z "${DB_URL}" ]]; then
  echo "Set TESTIFIER_AUDIT_DB_URL (or DATABASE_URL) before running this script." >&2
  exit 1
fi

CONFIG_PATH="${CONFIG_PATH:-configs/default.yaml}"
TABLE_NAME="${VRDB_TABLE_NAME:-voter_registry}"
CHUNK_SIZE="${VRDB_CHUNK_SIZE:-50000}"

if [[ "${CI_SKIP_INSTALL:-0}" != "1" ]]; then
  python -m pip install -e ".[dev]"
fi

FORCE_REQUESTED=0
for arg in "$@"; do
  if [[ "${arg}" == "--force" ]]; then
    FORCE_REQUESTED=1
    break
  fi
done

if [[ "${FORCE_REQUESTED}" != "1" ]]; then
  PRECHECK_RESULT="$(
    python - "${EXTRACT_PATH}" "${DB_URL}" "${TABLE_NAME}" <<'PY'
from __future__ import annotations

import hashlib
from pathlib import Path
import sys


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(1 << 20)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _safe_error(prefix: str, exc: Exception) -> str:
    message = str(exc).replace("\n", " ").replace("|", "/")
    return f"ERROR|{prefix}|{exc.__class__.__name__}|{message}"


extract_path = Path(sys.argv[1]).resolve()
db_url = sys.argv[2]
table_name = sys.argv[3]
file_hash = _sha256(extract_path)

try:
    from testifier_audit.io.vrdb_postgres import IMPORT_KIND_VRDB, VRDB_IMPORTER_VERSION
except Exception as exc:  # pragma: no cover
    print(_safe_error("constants_import_failed", exc))
    raise SystemExit(0)

try:
    import psycopg
except Exception as exc:  # pragma: no cover
    print(_safe_error("psycopg_import_failed", exc))
    raise SystemExit(0)

try:
    with psycopg.connect(db_url, connect_timeout=5) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT import_id, rows_upserted
                FROM data_imports
                WHERE
                  import_kind = %s
                  AND target_table = %s
                  AND file_hash = %s
                  AND importer_version = %s
                  AND status = 'completed'
                ORDER BY created_at DESC, import_id DESC
                LIMIT 1
                """,
                (
                    IMPORT_KIND_VRDB,
                    table_name,
                    file_hash,
                    VRDB_IMPORTER_VERSION,
                ),
            )
            row = cursor.fetchone()
except Exception as exc:
    print(_safe_error("query_failed", exc))
    raise SystemExit(0)

if row is not None:
    import_id = int(row[0]) if row[0] is not None else 0
    rows_upserted = int(row[1]) if row[1] is not None else 0
    print(
        f"SKIP|{file_hash}|{VRDB_IMPORTER_VERSION}|{import_id}|{rows_upserted}"
    )
else:
    print(f"RUN|{file_hash}|{VRDB_IMPORTER_VERSION}")
PY
  )"

  if [[ "${PRECHECK_RESULT}" == SKIP\|* ]]; then
    IFS='|' read -r _ file_hash importer_version import_id rows_upserted <<< "${PRECHECK_RESULT}"
    echo "VRDB import shell precheck: checksum already imported (import_id=${import_id}, rows_upserted=${rows_upserted}, importer_version=${importer_version}, file_hash=${file_hash})."
    echo "Skipping VRDB import."
    exit 0
  fi

  if [[ "${PRECHECK_RESULT}" == RUN\|* ]]; then
    IFS='|' read -r _ file_hash importer_version <<< "${PRECHECK_RESULT}"
    echo "VRDB import shell precheck: no completed import found for file_hash=${file_hash}, importer_version=${importer_version}."
    echo "Proceeding with VRDB import."
  else
    echo "VRDB import shell precheck unavailable: ${PRECHECK_RESULT}"
    echo "Proceeding with VRDB import."
  fi
fi

HEARTBEAT_SECONDS="${VRDB_IMPORT_HEARTBEAT_SECONDS:-20}"
if ! [[ "${HEARTBEAT_SECONDS}" =~ ^[0-9]+$ ]] || [[ "${HEARTBEAT_SECONDS}" -lt 5 ]]; then
  HEARTBEAT_SECONDS=20
fi

echo "Launching VRDB importer (heartbeat every ${HEARTBEAT_SECONDS}s)..."
IMPORT_STARTED_AT="$(date +%s)"
python -m testifier_audit.cli import-vrdb \
  --extract "${EXTRACT_PATH}" \
  --config "${CONFIG_PATH}" \
  --db-url "${DB_URL}" \
  --table-name "${TABLE_NAME}" \
  --chunk-size "${CHUNK_SIZE}" \
  "$@" &
IMPORT_PID=$!

while kill -0 "${IMPORT_PID}" >/dev/null 2>&1; do
  sleep "${HEARTBEAT_SECONDS}"
  if kill -0 "${IMPORT_PID}" >/dev/null 2>&1; then
    NOW="$(date +%s)"
    ELAPSED_SECONDS=$((NOW - IMPORT_STARTED_AT))
    echo "VRDB import in progress... elapsed=${ELAPSED_SECONDS}s"
  fi
done

set +e
wait "${IMPORT_PID}"
IMPORT_STATUS=$?
set -e

TOTAL_ELAPSED_SECONDS=$(( $(date +%s) - IMPORT_STARTED_AT ))
if [[ "${IMPORT_STATUS}" -ne 0 ]]; then
  echo "VRDB import failed after ${TOTAL_ELAPSED_SECONDS}s (exit=${IMPORT_STATUS})." >&2
  exit "${IMPORT_STATUS}"
fi

echo "VRDB import command completed in ${TOTAL_ELAPSED_SECONDS}s."

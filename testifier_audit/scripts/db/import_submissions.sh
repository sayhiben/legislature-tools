#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CALLER_CWD="$(pwd)"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <submissions_csv_path> [additional import-submissions args]" >&2
  echo "Example: $0 ./data/raw/SB6346-20260206-1330.csv" >&2
  exit 1
fi

CSV_PATH="$1"
shift
if [[ "${CSV_PATH}" != /* ]]; then
  CSV_PATH="${CALLER_CWD}/${CSV_PATH#./}"
fi

cd "${PROJECT_ROOT}"

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

FORCE_REQUESTED=0
for arg in "$@"; do
  if [[ "${arg}" == "--force" ]]; then
    FORCE_REQUESTED=1
    break
  fi
done

if [[ "${FORCE_REQUESTED}" != "1" ]]; then
  PRECHECK_RESULT="$(
    python - "${CSV_PATH}" "${DB_URL}" "${TABLE_NAME}" "${PROJECT_ROOT}" <<'PY'
from __future__ import annotations

import hashlib
from pathlib import Path
import re
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


def _resolve_importer_version(project_root: Path) -> str:
    source_path = project_root / "src" / "testifier_audit" / "io" / "submissions_postgres.py"
    text = source_path.read_text(encoding="utf-8")
    match = re.search(
        r'^SUBMISSIONS_IMPORTER_VERSION\s*=\s*"([^"]+)"',
        text,
        flags=re.MULTILINE,
    )
    if match is None:
        return "submissions_csv_v1"
    return match.group(1).strip() or "submissions_csv_v1"


csv_path = Path(sys.argv[1]).resolve()
db_url = sys.argv[2]
table_name = sys.argv[3]
project_root = Path(sys.argv[4]).resolve()
file_hash = _sha256(csv_path)
import_kind = "submissions_csv"
importer_version = _resolve_importer_version(project_root)

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
                    import_kind,
                    table_name,
                    file_hash,
                    importer_version,
                ),
            )
            row = cursor.fetchone()
except Exception as exc:
    print(_safe_error("query_failed", exc))
    raise SystemExit(0)

if row is not None:
    import_id = int(row[0]) if row[0] is not None else 0
    rows_upserted = int(row[1]) if row[1] is not None else 0
    print(f"SKIP|{file_hash}|{importer_version}|{import_id}|{rows_upserted}")
else:
    print(f"RUN|{file_hash}|{importer_version}")
PY
  )"

  if [[ "${PRECHECK_RESULT}" == SKIP\|* ]]; then
    IFS='|' read -r _ file_hash importer_version import_id rows_upserted <<< "${PRECHECK_RESULT}"
    echo "Submission import shell precheck: checksum already imported (import_id=${import_id}, rows_upserted=${rows_upserted}, importer_version=${importer_version}, file_hash=${file_hash})."
    echo "Skipping submissions import."
    exit 0
  fi

  if [[ "${PRECHECK_RESULT}" == RUN\|* ]]; then
    IFS='|' read -r _ file_hash importer_version <<< "${PRECHECK_RESULT}"
    echo "Submission import shell precheck: no completed import found for file_hash=${file_hash}, importer_version=${importer_version}."
    echo "Proceeding with submissions import."
  else
    echo "Submission import shell precheck unavailable: ${PRECHECK_RESULT}"
    echo "Proceeding with submissions import."
  fi
fi

python -m testifier_audit.cli import-submissions \
  --csv "${CSV_PATH}" \
  --config "${CONFIG_PATH}" \
  --db-url "${DB_URL}" \
  --table-name "${TABLE_NAME}" \
  --chunk-size "${CHUNK_SIZE}" \
  "$@"

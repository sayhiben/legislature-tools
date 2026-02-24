#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"

# shellcheck disable=SC1091
source "${PROJECT_ROOT}/scripts/lib/env.sh"
load_project_env "${PROJECT_ROOT}"

cd "${PROJECT_ROOT}"

DEFAULT_SUBMISSIONS_CSV="${REPO_ROOT}/data/raw/SB6346-20260206-1330.csv"
DEFAULT_VRDB_EXTRACT="${REPO_ROOT}/data/raw/20260202_VRDB_Extract.txt"

SUBMISSIONS_CSV="${1:-${SUBMISSIONS_CSV:-${DEFAULT_SUBMISSIONS_CSV}}}"
VRDB_EXTRACT="${2:-${VRDB_EXTRACT:-${DEFAULT_VRDB_EXTRACT}}}"
HEARING_METADATA_PATH_INPUT="${3:-${HEARING_METADATA_PATH:-}}"
REPORTS_ROOT="${REPORTS_ROOT:-${REPO_ROOT}/reports}"
CSV_BASENAME="$(basename "${SUBMISSIONS_CSV}")"
CSV_STEM="${CSV_BASENAME%.*}"
DEFAULT_OUT_DIR="${REPORTS_ROOT}/${CSV_STEM}"
OUT_DIR="${OUT_DIR:-${DEFAULT_OUT_DIR}}"
DEFAULT_HEARING_METADATA_PATH="${REPO_ROOT}/data/metadata/${CSV_STEM}.hearing.yaml"
HEARING_METADATA_PATH="${HEARING_METADATA_PATH_INPUT}"
AUTO_DETECTED_HEARING_METADATA=0
if [[ -z "${HEARING_METADATA_PATH}" ]] && [[ -f "${DEFAULT_HEARING_METADATA_PATH}" ]]; then
  HEARING_METADATA_PATH="${DEFAULT_HEARING_METADATA_PATH}"
  AUTO_DETECTED_HEARING_METADATA=1
fi
CONFIG_PATH="${CONFIG_PATH:-${PROJECT_ROOT}/configs/voter_registry_enabled.yaml}"
DB_URL="${TESTIFIER_AUDIT_DB_URL:-${DATABASE_URL:-postgresql://legislature:legislature@localhost:55432/legislature}}"
DEDUP_MODE="${DEDUP_MODE:-}"
SKIP_DOCKER_POSTGRES="${TESTIFIER_AUDIT_SKIP_DOCKER_POSTGRES:-0}"

db_tcp_reachable() {
  python - "$1" <<'PY'
import socket
import sys
from urllib.parse import urlparse

url = sys.argv[1]
parsed = urlparse(url)
host = parsed.hostname or "localhost"
port = parsed.port or 5432
try:
    with socket.create_connection((host, port), timeout=1.5):
        pass
except OSError:
    raise SystemExit(1)
raise SystemExit(0)
PY
}

if [[ ! -f "${SUBMISSIONS_CSV}" ]]; then
  echo "Submissions CSV not found: ${SUBMISSIONS_CSV}" >&2
  exit 1
fi

if [[ ! -f "${VRDB_EXTRACT}" ]]; then
  echo "VRDB extract not found: ${VRDB_EXTRACT}" >&2
  exit 1
fi

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Config not found: ${CONFIG_PATH}" >&2
  exit 1
fi

if [[ -n "${HEARING_METADATA_PATH}" ]] && [[ ! -f "${HEARING_METADATA_PATH}" ]]; then
  echo "Hearing metadata sidecar not found: ${HEARING_METADATA_PATH}" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"

export TESTIFIER_AUDIT_DB_URL="${DB_URL}"

echo "Using TESTIFIER_AUDIT_DB_URL=${TESTIFIER_AUDIT_DB_URL}"
echo "Using submissions CSV: ${SUBMISSIONS_CSV}"
echo "Using VRDB extract: ${VRDB_EXTRACT}"
echo "Using config: ${CONFIG_PATH}"
echo "Output directory: ${OUT_DIR}"
echo "Scoped postgres source_file: ${CSV_BASENAME}"
if [[ -n "${DEDUP_MODE}" ]]; then
  echo "Using dedup mode override: ${DEDUP_MODE}"
fi
if [[ -n "${HEARING_METADATA_PATH}" ]]; then
  if [[ "${AUTO_DETECTED_HEARING_METADATA}" == "1" ]]; then
    echo "Auto-detected hearing metadata sidecar: ${HEARING_METADATA_PATH}"
  fi
  echo "Using hearing metadata sidecar: ${HEARING_METADATA_PATH}"
fi

if [[ "${SKIP_DOCKER_POSTGRES}" == "1" ]]; then
  echo "Skipping docker postgres startup (TESTIFIER_AUDIT_SKIP_DOCKER_POSTGRES=1)."
elif db_tcp_reachable "${TESTIFIER_AUDIT_DB_URL}"; then
  echo "Postgres is already reachable at TESTIFIER_AUDIT_DB_URL; skipping docker postgres startup."
else
  docker compose up -d postgres
fi

if [[ "${CI_SKIP_INSTALL:-0}" != "1" ]]; then
  python -m pip install -e ".[dev]"
fi

CI_SKIP_INSTALL=1 "${PROJECT_ROOT}/scripts/db/import_submissions.sh" "${SUBMISSIONS_CSV}"
CI_SKIP_INSTALL=1 "${PROJECT_ROOT}/scripts/vrdb/import_vrdb.sh" "${VRDB_EXTRACT}"

CLI_ARGS=(run-all --out "${OUT_DIR}" --config "${CONFIG_PATH}" --source-file "${CSV_BASENAME}")
if [[ -n "${DEDUP_MODE}" ]]; then
  CLI_ARGS+=(--dedup-mode "${DEDUP_MODE}")
fi
if [[ -n "${HEARING_METADATA_PATH}" ]]; then
  CLI_ARGS+=(--hearing-metadata "${HEARING_METADATA_PATH}")
fi
python -m testifier_audit.cli "${CLI_ARGS[@]}"

echo "Unified report written to: ${OUT_DIR}/report.html"

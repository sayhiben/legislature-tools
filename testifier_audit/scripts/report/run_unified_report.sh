#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"

cd "${PROJECT_ROOT}"

DEFAULT_SUBMISSIONS_CSV="${REPO_ROOT}/data/raw/SB6346-20260206-1330.csv"
DEFAULT_VRDB_EXTRACT="${REPO_ROOT}/data/raw/20260202_VRDB_Extract.txt"

SUBMISSIONS_CSV="${1:-${SUBMISSIONS_CSV:-${DEFAULT_SUBMISSIONS_CSV}}}"
VRDB_EXTRACT="${2:-${VRDB_EXTRACT:-${DEFAULT_VRDB_EXTRACT}}}"
HEARING_METADATA_PATH="${3:-${HEARING_METADATA_PATH:-}}"
REPORTS_ROOT="${REPORTS_ROOT:-${REPO_ROOT}/reports}"
CSV_BASENAME="$(basename "${SUBMISSIONS_CSV}")"
CSV_STEM="${CSV_BASENAME%.*}"
DEFAULT_OUT_DIR="${REPORTS_ROOT}/${CSV_STEM}"
OUT_DIR="${OUT_DIR:-${DEFAULT_OUT_DIR}}"
CONFIG_PATH="${CONFIG_PATH:-${PROJECT_ROOT}/configs/voter_registry_enabled.yaml}"
DB_URL="${TESTIFIER_AUDIT_DB_URL:-${DATABASE_URL:-postgresql://legislature:legislature@localhost:55432/legislature}}"
DEDUP_MODE="${DEDUP_MODE:-}"

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
if [[ -n "${DEDUP_MODE}" ]]; then
  echo "Using dedup mode override: ${DEDUP_MODE}"
fi
if [[ -n "${HEARING_METADATA_PATH}" ]]; then
  echo "Using hearing metadata sidecar: ${HEARING_METADATA_PATH}"
fi

docker compose up -d postgres

if [[ "${CI_SKIP_INSTALL:-0}" != "1" ]]; then
  python -m pip install -e ".[dev]"
fi

CI_SKIP_INSTALL=1 "${PROJECT_ROOT}/scripts/db/import_submissions.sh" "${SUBMISSIONS_CSV}"
CI_SKIP_INSTALL=1 "${PROJECT_ROOT}/scripts/vrdb/import_vrdb.sh" "${VRDB_EXTRACT}"

CLI_ARGS=(run-all --out "${OUT_DIR}" --config "${CONFIG_PATH}")
if [[ -n "${DEDUP_MODE}" ]]; then
  CLI_ARGS+=(--dedup-mode "${DEDUP_MODE}")
fi
if [[ -n "${HEARING_METADATA_PATH}" ]]; then
  CLI_ARGS+=(--hearing-metadata "${HEARING_METADATA_PATH}")
fi
python -m testifier_audit.cli "${CLI_ARGS[@]}"

echo "Unified report written to: ${OUT_DIR}/report.html"

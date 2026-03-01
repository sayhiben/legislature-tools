#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"
CALLER_CWD="$(pwd)"

# shellcheck disable=SC1091
source "${PROJECT_ROOT}/scripts/lib/env.sh"
load_project_env "${PROJECT_ROOT}"

log_ts() {
  date +"%Y-%m-%d %H:%M:%S"
}

log_info() {
  printf '%s [unified][info] %s\n' "$(log_ts)" "$*"
}

log_warn() {
  printf '%s [unified][warn] %s\n' "$(log_ts)" "$*"
}

log_error() {
  printf '%s [unified][error] %s\n' "$(log_ts)" "$*" >&2
}

on_error() {
  local exit_code="$?"
  local line="${BASH_LINENO[0]:-unknown}"
  local cmd="${BASH_COMMAND:-unknown}"
  log_error "run_unified_report.sh failed at line ${line}: ${cmd}"
  exit "${exit_code}"
}
trap on_error ERR

cd "${PROJECT_ROOT}"

resolve_path_from_caller() {
  local path_value="$1"
  if [[ -z "${path_value}" ]]; then
    printf '%s\n' "${path_value}"
    return
  fi
  if [[ "${path_value}" = /* ]]; then
    printf '%s\n' "${path_value}"
    return
  fi
  printf '%s\n' "${CALLER_CWD}/${path_value#./}"
}

DEFAULT_SUBMISSIONS_CSV="${REPO_ROOT}/data/raw/SB6346-20260206-1330.csv"
DEFAULT_VRDB_EXTRACT="${REPO_ROOT}/data/raw/20260202_VRDB_Extract.txt"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/report/run_unified_report.sh [options] [submissions_csv] [vrdb_extract] [hearing_metadata]

Options:
  --skip-imports              Skip both submissions and VRDB imports.
  --skip-submissions-import   Skip submissions import only.
  --skip-vrdb-import          Skip VRDB import only.
  -h, --help                  Show this help text.

Positional defaults:
  submissions_csv   ./data/raw/SB6346-20260206-1330.csv
  vrdb_extract      ./data/raw/20260202_VRDB_Extract.txt
  hearing_metadata  auto-detected from data/metadata/<csv-stem>.hearing.yaml when present
EOF
}

SKIP_IMPORTS="${TESTIFIER_AUDIT_SKIP_IMPORTS:-0}"
SKIP_SUBMISSIONS_IMPORT="${TESTIFIER_AUDIT_SKIP_SUBMISSIONS_IMPORT:-0}"
SKIP_VRDB_IMPORT="${TESTIFIER_AUDIT_SKIP_VRDB_IMPORT:-0}"

declare -a POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-imports)
      SKIP_IMPORTS=1
      shift
      ;;
    --skip-submissions-import)
      SKIP_SUBMISSIONS_IMPORT=1
      shift
      ;;
    --skip-vrdb-import)
      SKIP_VRDB_IMPORT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      POSITIONAL_ARGS+=("$@")
      break
      ;;
    -*)
      log_error "Unknown argument: $1"
      usage >&2
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done

SUBMISSIONS_CSV="${POSITIONAL_ARGS[0]:-${SUBMISSIONS_CSV:-${DEFAULT_SUBMISSIONS_CSV}}}"
VRDB_EXTRACT="${POSITIONAL_ARGS[1]:-${VRDB_EXTRACT:-${DEFAULT_VRDB_EXTRACT}}}"
HEARING_METADATA_PATH_INPUT="${POSITIONAL_ARGS[2]:-${HEARING_METADATA_PATH:-}}"
SUBMISSIONS_CSV="$(resolve_path_from_caller "${SUBMISSIONS_CSV}")"
VRDB_EXTRACT="$(resolve_path_from_caller "${VRDB_EXTRACT}")"
HEARING_METADATA_PATH_INPUT="$(resolve_path_from_caller "${HEARING_METADATA_PATH_INPUT}")"
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
CONFIG_PATH="$(resolve_path_from_caller "${CONFIG_PATH}")"
DB_URL="${TESTIFIER_AUDIT_DB_URL:-${DATABASE_URL:-postgresql://legislature:legislature@localhost:55432/legislature}}"
DEDUP_MODE="${DEDUP_MODE:-}"
SKIP_DOCKER_POSTGRES="${TESTIFIER_AUDIT_SKIP_DOCKER_POSTGRES:-0}"

if [[ "${SKIP_IMPORTS}" == "1" ]]; then
  SKIP_SUBMISSIONS_IMPORT=1
  SKIP_VRDB_IMPORT=1
fi

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
  log_error "Submissions CSV not found: ${SUBMISSIONS_CSV}"
  exit 1
fi

if [[ "${SKIP_VRDB_IMPORT}" != "1" ]] && [[ ! -f "${VRDB_EXTRACT}" ]]; then
  log_error "VRDB extract not found: ${VRDB_EXTRACT}"
  exit 1
fi

if [[ ! -f "${CONFIG_PATH}" ]]; then
  log_error "Config not found: ${CONFIG_PATH}"
  exit 1
fi

if [[ -n "${HEARING_METADATA_PATH}" ]] && [[ ! -f "${HEARING_METADATA_PATH}" ]]; then
  log_error "Hearing metadata sidecar not found: ${HEARING_METADATA_PATH}"
  exit 1
fi

mkdir -p "${OUT_DIR}"

export TESTIFIER_AUDIT_DB_URL="${DB_URL}"

log_info "Using TESTIFIER_AUDIT_DB_URL=${TESTIFIER_AUDIT_DB_URL}"
log_info "Using submissions CSV: ${SUBMISSIONS_CSV}"
log_info "Using VRDB extract: ${VRDB_EXTRACT}"
log_info "Using config: ${CONFIG_PATH}"
log_info "Output directory: ${OUT_DIR}"
log_info "Scoped postgres source_file: ${CSV_BASENAME}"
log_info "Skip imports: ${SKIP_IMPORTS} (submissions=${SKIP_SUBMISSIONS_IMPORT}, vrdb=${SKIP_VRDB_IMPORT})"
if [[ -n "${DEDUP_MODE}" ]]; then
  log_info "Using dedup mode override: ${DEDUP_MODE}"
fi
if [[ -n "${HEARING_METADATA_PATH}" ]]; then
  if [[ "${AUTO_DETECTED_HEARING_METADATA}" == "1" ]]; then
    log_info "Auto-detected hearing metadata sidecar: ${HEARING_METADATA_PATH}"
  fi
  log_info "Using hearing metadata sidecar: ${HEARING_METADATA_PATH}"
fi

if [[ "${SKIP_DOCKER_POSTGRES}" == "1" ]]; then
  log_warn "Skipping docker postgres startup (TESTIFIER_AUDIT_SKIP_DOCKER_POSTGRES=1)."
elif db_tcp_reachable "${TESTIFIER_AUDIT_DB_URL}"; then
  log_info "Postgres is already reachable at TESTIFIER_AUDIT_DB_URL; skipping docker postgres startup."
else
  log_info "Starting docker postgres service..."
  docker compose up -d postgres
fi

if [[ "${CI_SKIP_INSTALL:-0}" != "1" ]]; then
  python -m pip install -e ".[dev]"
fi

if [[ "${SKIP_SUBMISSIONS_IMPORT}" == "1" ]]; then
  log_info "Skipping submissions import (--skip-submissions-import or --skip-imports)."
else
  log_info "Importing submissions CSV..."
  CI_SKIP_INSTALL=1 "${PROJECT_ROOT}/scripts/db/import_submissions.sh" "${SUBMISSIONS_CSV}"
fi

if [[ "${SKIP_VRDB_IMPORT}" == "1" ]]; then
  log_info "Skipping VRDB import (--skip-vrdb-import or --skip-imports)."
else
  log_info "Importing VRDB extract..."
  CI_SKIP_INSTALL=1 "${PROJECT_ROOT}/scripts/vrdb/import_vrdb.sh" "${VRDB_EXTRACT}"
fi

CLI_ARGS=(run-all --out "${OUT_DIR}" --config "${CONFIG_PATH}" --source-file "${CSV_BASENAME}")
if [[ -n "${DEDUP_MODE}" ]]; then
  CLI_ARGS+=(--dedup-mode "${DEDUP_MODE}")
fi
if [[ -n "${HEARING_METADATA_PATH}" ]]; then
  CLI_ARGS+=(--hearing-metadata "${HEARING_METADATA_PATH}")
fi
python -m testifier_audit.cli "${CLI_ARGS[@]}"

log_info "Unified report written to: ${OUT_DIR}/report.html"

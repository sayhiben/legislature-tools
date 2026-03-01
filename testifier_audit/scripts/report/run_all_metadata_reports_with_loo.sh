#!/usr/bin/env bash
set -euo pipefail

log_ts() {
  date +"%Y-%m-%d %H:%M:%S"
}

log_info() {
  printf '%s [run-all][info] %s\n' "$(log_ts)" "$*"
}

log_warn() {
  printf '%s [run-all][warn] %s\n' "$(log_ts)" "$*"
}

log_error() {
  printf '%s [run-all][error] %s\n' "$(log_ts)" "$*" >&2
}

on_error() {
  local exit_code="$?"
  local line="${BASH_LINENO[0]:-unknown}"
  local cmd="${BASH_COMMAND:-unknown}"
  log_error "run_all_metadata_reports_with_loo.sh failed at line ${line}: ${cmd}"
  exit "${exit_code}"
}
trap on_error ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"
CALLER_CWD="$(pwd)"

# shellcheck disable=SC1091
source "${PROJECT_ROOT}/scripts/lib/env.sh"
load_project_env "${PROJECT_ROOT}"

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

usage() {
  cat <<'EOF'
Usage:
  ./scripts/report/run_all_metadata_reports_with_loo.sh [options]

Description:
  1. Finds all metadata sidecars with matching CSV files (<stem>.hearing.yaml + <stem>.csv).
  2. Imports VRDB once and prepares shared lookup cache config.
  3. Builds contextual duplicate baseline from matched datasets.
  4. Runs report generation in either:
     - legacy two-pass mode (non-contextual pass, then contextual rerun), or
     - fast contextual mode (single full contextual pass after validation).
  5. Rebuilds reports index + global baselines.
  6. Builds leave-one-out baseline payloads for each successful report.

Options:
  --vrdb <path>             VRDB extract path (default: ./data/raw/20260202_VRDB_Extract.txt)
  --base-config <path>      Base report config path (default: ./testifier_audit/configs/voter_registry_enabled.yaml)
  --fast-contextual         Enable fast contextual mode (single full contextual report pass).
  --skip-fast-failure-rerun In fast mode, skip failure-mitigation rerun that rebuilds baseline from successful reports.
  --skip-contextual-rerun   Skip the second pass that reruns reports with contextual baseline enabled.
                            (legacy mode only; ignored in fast contextual mode)
  --loo-mode <mode>         Leave-one-out mode: batch (default) or subprocess.
  -h, --help                Show this help text.

Environment overrides:
  METADATA_DIR    (default: ./data/metadata)
  RAW_DIR         (default: ./data/raw)
  REPORTS_ROOT    (default: ./reports)
  CONTEXTUAL_JSON (default: ./data/metadata/contextual_duplicate_baseline.json)
  CONTEXTUAL_CSV  (default: ./data/metadata/contextual_duplicate_baseline.csv)
  VR_LOOKUP_CACHE_DIR (default: ./output/voter_registry_lookup_cache)
  VR_LOOKUP_CACHE_DB_SNAPSHOT (default: auto from VRDB file stat)
EOF
}

DEFAULT_VRDB="${REPO_ROOT}/data/raw/20260202_VRDB_Extract.txt"
DEFAULT_BASE_CONFIG="${PROJECT_ROOT}/configs/voter_registry_enabled.yaml"

VRDB_EXTRACT="${DEFAULT_VRDB}"
BASE_CONFIG_PATH="${DEFAULT_BASE_CONFIG}"
FAST_CONTEXTUAL=0
FAST_FAILURE_RERUN=1
SKIP_CONTEXTUAL_RERUN=0
LOO_MODE="${LOO_MODE:-batch}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vrdb)
      VRDB_EXTRACT="$2"
      shift 2
      ;;
    --base-config)
      BASE_CONFIG_PATH="$2"
      shift 2
      ;;
    --fast-contextual)
      FAST_CONTEXTUAL=1
      shift
      ;;
    --skip-fast-failure-rerun)
      FAST_FAILURE_RERUN=0
      shift
      ;;
    --skip-contextual-rerun)
      SKIP_CONTEXTUAL_RERUN=1
      shift
      ;;
    --loo-mode)
      LOO_MODE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      log_error "Unknown argument: $1"
      usage >&2
      exit 1
      ;;
  esac
done

VRDB_EXTRACT="$(resolve_path_from_caller "${VRDB_EXTRACT}")"
BASE_CONFIG_PATH="$(resolve_path_from_caller "${BASE_CONFIG_PATH}")"

if [[ "${LOO_MODE}" != "batch" && "${LOO_MODE}" != "subprocess" ]]; then
  log_error "Invalid --loo-mode value: ${LOO_MODE}. Expected 'batch' or 'subprocess'."
  exit 1
fi

if [[ "${FAST_CONTEXTUAL}" == "1" && "${SKIP_CONTEXTUAL_RERUN}" == "1" ]]; then
  log_warn "Ignoring --skip-contextual-rerun because --fast-contextual is enabled."
fi

METADATA_DIR="${METADATA_DIR:-${REPO_ROOT}/data/metadata}"
RAW_DIR="${RAW_DIR:-${REPO_ROOT}/data/raw}"
REPORTS_ROOT="${REPORTS_ROOT:-${REPO_ROOT}/reports}"
CONTEXTUAL_JSON="${CONTEXTUAL_JSON:-${REPO_ROOT}/data/metadata/contextual_duplicate_baseline.json}"
CONTEXTUAL_CSV="${CONTEXTUAL_CSV:-${REPO_ROOT}/data/metadata/contextual_duplicate_baseline.csv}"
RUN_LOG_DIR="${RUN_LOG_DIR:-${REPO_ROOT}/output/run_logs}"
DB_URL="${TESTIFIER_AUDIT_DB_URL:-${DATABASE_URL:-postgresql://legislature:legislature@localhost:55432/legislature}}"
SKIP_DOCKER_POSTGRES="${TESTIFIER_AUDIT_SKIP_DOCKER_POSTGRES:-0}"
SUBMISSIONS_TABLE_NAME="${SUBMISSIONS_TABLE_NAME:-public_submissions}"

mkdir -p "${RUN_LOG_DIR}"
RUN_TS="$(date +"%Y%m%d-%H%M%S")"
RUN_LOG_PATH="${RUN_LOG_DIR}/run_all_metadata_reports_with_loo-${RUN_TS}.log"
RUN_FAILURES_PATH="${RUN_LOG_DIR}/run_all_metadata_reports_with_loo-${RUN_TS}.failures.txt"
exec > >(tee -a "${RUN_LOG_PATH}") 2>&1
log_info "Run log: ${RUN_LOG_PATH}"

PASS1_CONFIG_PATH=""
CONTEXTUAL_CONFIG_PATH=""
TMP_CONTEXTUAL_CSV_DIR=""
TMP_CONTEXTUAL_META_DIR=""
LOO_TARGETS_PATH=""
LOO_FAILURES_JSON_PATH=""
cleanup() {
  if [[ -n "${PASS1_CONFIG_PATH}" ]]; then
    rm -f "${PASS1_CONFIG_PATH}" || true
  fi
  if [[ -n "${CONTEXTUAL_CONFIG_PATH}" ]]; then
    rm -f "${CONTEXTUAL_CONFIG_PATH}" || true
  fi
  if [[ -n "${TMP_CONTEXTUAL_CSV_DIR}" ]]; then
    rm -rf "${TMP_CONTEXTUAL_CSV_DIR}" || true
  fi
  if [[ -n "${TMP_CONTEXTUAL_META_DIR}" ]]; then
    rm -rf "${TMP_CONTEXTUAL_META_DIR}" || true
  fi
  if [[ -n "${LOO_TARGETS_PATH}" ]]; then
    rm -f "${LOO_TARGETS_PATH}" || true
  fi
  if [[ -n "${LOO_FAILURES_JSON_PATH}" ]]; then
    rm -f "${LOO_FAILURES_JSON_PATH}" || true
  fi
}
trap cleanup EXIT

if [[ ! -f "${VRDB_EXTRACT}" ]]; then
  log_error "VRDB extract not found: ${VRDB_EXTRACT}"
  exit 1
fi
if [[ ! -f "${BASE_CONFIG_PATH}" ]]; then
  log_error "Base config not found: ${BASE_CONFIG_PATH}"
  exit 1
fi
if [[ ! -d "${METADATA_DIR}" ]]; then
  log_error "Metadata directory not found: ${METADATA_DIR}"
  exit 1
fi
if [[ ! -d "${RAW_DIR}" ]]; then
  log_error "Raw CSV directory not found: ${RAW_DIR}"
  exit 1
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

mkdir -p "${REPORTS_ROOT}" "${REPO_ROOT}/output"
export TESTIFIER_AUDIT_DB_URL="${DB_URL}"
log_info "Using TESTIFIER_AUDIT_DB_URL=${TESTIFIER_AUDIT_DB_URL}"

if [[ "${SKIP_DOCKER_POSTGRES}" == "1" ]]; then
  log_warn "Skipping docker postgres startup (TESTIFIER_AUDIT_SKIP_DOCKER_POSTGRES=1)."
elif db_tcp_reachable "${TESTIFIER_AUDIT_DB_URL}"; then
  log_info "Postgres is already reachable at TESTIFIER_AUDIT_DB_URL; skipping docker postgres startup."
else
  log_info "Starting docker postgres service..."
  (
    cd "${PROJECT_ROOT}"
    docker compose up -d postgres
  )
fi

array_contains() {
  local needle="$1"
  shift || true
  local item
  for item in "$@"; do
    if [[ "${item}" == "${needle}" ]]; then
      return 0
    fi
  done
  return 1
}

progress_label() {
  local phase="$1"
  local current="$2"
  local total="$3"
  local stem="$4"
  local percent=0
  if [[ "${total}" -gt 0 ]]; then
    percent=$(( (current * 100) / total ))
  fi
  printf '%s [run-all][progress] [%s %d/%d %d%%] %s\n' "$(log_ts)" "${phase}" "${current}" "${total}" "${percent}" "${stem}"
}

preflight_dataset_for_contextual_baseline() {
  local csv_path="$1"
  local sidecar_path="$2"
  python - "${csv_path}" "${sidecar_path}" <<'PY'
from pathlib import Path
import sys

import pandas as pd
import yaml

csv_path = Path(sys.argv[1]).resolve()
sidecar_path = Path(sys.argv[2]).resolve()
required_columns = {"Name", "Time Signed In"}

try:
    frame = pd.read_csv(csv_path, nrows=5)
except Exception as exc:
    raise SystemExit(f"csv_read_failed:{csv_path.name}:{exc}")

missing = sorted(required_columns - set(frame.columns))
if missing:
    missing_cols = ",".join(missing)
    raise SystemExit(f"csv_missing_required_columns:{csv_path.name}:{missing_cols}")

try:
    payload = yaml.safe_load(sidecar_path.read_text(encoding="utf-8")) or {}
except Exception as exc:
    raise SystemExit(f"sidecar_parse_failed:{sidecar_path.name}:{exc}")
if not isinstance(payload, dict):
    raise SystemExit(f"sidecar_not_mapping:{sidecar_path.name}")
PY
}

count_submissions_rows_for_source_file() {
  local source_file="$1"
  python - "${TESTIFIER_AUDIT_DB_URL}" "${SUBMISSIONS_TABLE_NAME}" "${source_file}" <<'PY'
import sys

import psycopg
from psycopg import sql

db_url = sys.argv[1]
table_name = sys.argv[2]
source_file = sys.argv[3]

with psycopg.connect(db_url) as conn:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {} WHERE source_file = %s").format(
                sql.Identifier(table_name)
            ),
            (source_file,),
        )
        value = cur.fetchone()
        count = int(value[0]) if value and value[0] is not None else 0
print(count)
PY
}

import_submissions_with_coverage_guard() {
  local csv_path="$1"
  local source_file
  source_file="$(basename "${csv_path}")"

  CI_SKIP_INSTALL=1 "${PROJECT_ROOT}/scripts/db/import_submissions.sh" "${csv_path}"
  local row_count
  row_count="$(count_submissions_rows_for_source_file "${source_file}")"
  if [[ "${row_count}" -gt 0 ]]; then
    log_info "[import-check] source_file=${source_file} rows=${row_count}"
    return 0
  fi

  log_warn "[import-check] source_file=${source_file} has 0 rows after import; forcing re-import."
  CI_SKIP_INSTALL=1 "${PROJECT_ROOT}/scripts/db/import_submissions.sh" "${csv_path}" --force
  row_count="$(count_submissions_rows_for_source_file "${source_file}")"
  if [[ "${row_count}" -le 0 ]]; then
    log_error "[import-check] source_file=${source_file} still has 0 rows after forced import."
    return 1
  fi
  log_info "[import-check] source_file=${source_file} rows=${row_count} (after forced import)"
}

write_contextual_config() {
  python - "${PASS1_CONFIG_PATH}" "${CONTEXTUAL_JSON}" "${CONTEXTUAL_CONFIG_PATH}" <<'PY'
import sys
from pathlib import Path

import yaml

base_config = Path(sys.argv[1])
contextual_json = Path(sys.argv[2]).resolve()
output_config = Path(sys.argv[3])

payload = yaml.safe_load(base_config.read_text(encoding="utf-8")) or {}
if not isinstance(payload, dict):
    raise SystemExit("Base config must deserialize to a mapping.")
name_analysis = payload.get("name_analysis")
if not isinstance(name_analysis, dict):
    name_analysis = {}
payload["name_analysis"] = name_analysis
name_analysis["contextual_baseline_path"] = str(contextual_json)

output_config.write_text(
    yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
    encoding="utf-8",
)
PY
}

build_contextual_baseline_from_stems() {
  local label="$1"
  shift || true
  local stems_to_include=("$@")
  if [[ "${#stems_to_include[@]}" -eq 0 ]]; then
    log_error "No datasets provided for contextual baseline build (${label})."
    return 1
  fi

  if [[ -n "${TMP_CONTEXTUAL_CSV_DIR}" ]]; then
    rm -rf "${TMP_CONTEXTUAL_CSV_DIR}" || true
  fi
  if [[ -n "${TMP_CONTEXTUAL_META_DIR}" ]]; then
    rm -rf "${TMP_CONTEXTUAL_META_DIR}" || true
  fi
  TMP_CONTEXTUAL_CSV_DIR="$(mktemp -d "${REPO_ROOT}/output/contextual-csvs.XXXXXX")"
  TMP_CONTEXTUAL_META_DIR="$(mktemp -d "${REPO_ROOT}/output/contextual-meta.XXXXXX")"

  local linked_count=0
  local i stem
  for i in "${!STEMS[@]}"; do
    stem="${STEMS[$i]}"
    if ! array_contains "${stem}" "${stems_to_include[@]}"; then
      continue
    fi
    ln -sf "${CSVS[$i]}" "${TMP_CONTEXTUAL_CSV_DIR}/${stem}.csv"
    ln -sf "${SIDECARS[$i]}" "${TMP_CONTEXTUAL_META_DIR}/${stem}.hearing.yaml"
    linked_count=$((linked_count + 1))
  done

  if [[ "${linked_count}" -eq 0 ]]; then
    log_error "Contextual baseline input set is empty (${label})."
    return 1
  fi

  log_info "Building contextual duplicate baseline from ${linked_count} dataset(s) [${label}]..."
  (
    cd "${PROJECT_ROOT}"
    python -m testifier_audit.io.contextual_duplicate_baseline \
      --csv-dir "${TMP_CONTEXTUAL_CSV_DIR}" \
      --metadata-dir "${TMP_CONTEXTUAL_META_DIR}" \
      --output-json "${CONTEXTUAL_JSON}" \
      --output-csv "${CONTEXTUAL_CSV}"
  )
}

declare -a STEMS=()
declare -a CSVS=()
declare -a SIDECARS=()
shopt -s nullglob
for sidecar in "${METADATA_DIR}"/*.hearing.yaml; do
  stem="$(basename "${sidecar}" .hearing.yaml)"
  csv_path="${RAW_DIR}/${stem}.csv"
  if [[ -f "${csv_path}" ]]; then
    STEMS+=("${stem}")
    CSVS+=("${csv_path}")
    SIDECARS+=("${sidecar}")
  fi
done
shopt -u nullglob

if [[ "${#STEMS[@]}" -eq 0 ]]; then
  log_error "No metadata-supported CSV datasets found in ${METADATA_DIR} + ${RAW_DIR}."
  exit 1
fi

log_info "Found ${#STEMS[@]} metadata-supported dataset(s)."
log_info "Using base config: ${BASE_CONFIG_PATH}"
log_info "Using VRDB extract: ${VRDB_EXTRACT}"
log_info "Contextual mode: $([[ "${FAST_CONTEXTUAL}" == "1" ]] && echo "fast" || echo "legacy")"
log_info "Fast failure rerun mitigation: ${FAST_FAILURE_RERUN}"
log_info "LOO mode: ${LOO_MODE}"

if [[ "${CI_SKIP_INSTALL:-0}" != "1" ]]; then
  (
    cd "${PROJECT_ROOT}"
    python -m pip install -e ".[dev]"
  )
fi
export CI_SKIP_INSTALL=1

log_info "Checking/importing VRDB once before report loops..."
CI_SKIP_INSTALL=1 "${PROJECT_ROOT}/scripts/vrdb/import_vrdb.sh" "${VRDB_EXTRACT}"

VR_LOOKUP_CACHE_DIR="${VR_LOOKUP_CACHE_DIR:-${REPO_ROOT}/output/voter_registry_lookup_cache}"
mkdir -p "${VR_LOOKUP_CACHE_DIR}"
if [[ -n "${VR_LOOKUP_CACHE_DB_SNAPSHOT:-}" ]]; then
  VR_LOOKUP_CACHE_DB_SNAPSHOT_VALUE="${VR_LOOKUP_CACHE_DB_SNAPSHOT}"
else
  VR_LOOKUP_CACHE_DB_SNAPSHOT_VALUE="$(python - "${VRDB_EXTRACT}" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1]).resolve()
stat = path.stat()
print(f"{path.name}:{int(stat.st_size)}:{int(stat.st_mtime)}")
PY
)"
fi
log_info "Voter lookup cache dir: ${VR_LOOKUP_CACHE_DIR}"
log_info "Voter lookup cache snapshot token: ${VR_LOOKUP_CACHE_DB_SNAPSHOT_VALUE}"

PASS1_CONFIG_PATH="$(mktemp "${REPO_ROOT}/output/voter-registry-cache-config.XXXXXX.yaml")"
python - "${BASE_CONFIG_PATH}" "${VR_LOOKUP_CACHE_DIR}" "${VR_LOOKUP_CACHE_DB_SNAPSHOT_VALUE}" "${PASS1_CONFIG_PATH}" <<'PY'
import sys
from pathlib import Path
import yaml

base_config = Path(sys.argv[1])
lookup_cache_dir = str(sys.argv[2]).strip()
lookup_cache_db_snapshot = str(sys.argv[3]).strip()
output_config = Path(sys.argv[4])

payload = yaml.safe_load(base_config.read_text(encoding="utf-8")) or {}
if not isinstance(payload, dict):
    raise SystemExit("Base config must deserialize to a mapping.")
voter_registry = payload.get("voter_registry")
if not isinstance(voter_registry, dict):
    voter_registry = {}
payload["voter_registry"] = voter_registry
voter_registry["lookup_cache_dir"] = lookup_cache_dir
voter_registry["lookup_cache_db_snapshot"] = lookup_cache_db_snapshot

output_config.write_text(
    yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
    encoding="utf-8",
)
PY

declare -a PASS1_OK=()
declare -a PASS1_FAILED=()
declare -a FINAL_OK=()
declare -a PASS2_FAILED=()
CONTEXTUAL_CONFIG_PATH="$(mktemp "${REPO_ROOT}/output/contextual-baseline-config.XXXXXX.yaml")"

if [[ "${FAST_CONTEXTUAL}" == "1" ]]; then
  PASS1_TOTAL="${#STEMS[@]}"
  log_info "Pass 1/3: validating + importing datasets for fast contextual mode - ${PASS1_TOTAL} total..."
  for i in "${!STEMS[@]}"; do
    stem="${STEMS[$i]}"
    csv_path="${CSVS[$i]}"
    sidecar_path="${SIDECARS[$i]}"
    current=$((i + 1))
    progress_label "pass1" "${current}" "${PASS1_TOTAL}" "${stem}"
    if ! preflight_dataset_for_contextual_baseline "${csv_path}" "${sidecar_path}"; then
      PASS1_FAILED+=("${stem}")
      log_error "[pass1][failed] ${stem}"
      continue
    fi
    if import_submissions_with_coverage_guard "${csv_path}"; then
      PASS1_OK+=("${stem}")
    else
      PASS1_FAILED+=("${stem}")
      log_error "[pass1][failed-import] ${stem}"
    fi
  done

  if [[ "${#PASS1_OK[@]}" -eq 0 ]]; then
    log_error "All datasets failed preflight validation for fast contextual mode."
    exit 1
  fi
  log_info "Pass 1 complete: ${#PASS1_OK[@]} validated/imported, ${#PASS1_FAILED[@]} failed validation/import."

  build_contextual_baseline_from_stems "fast-preflight" "${PASS1_OK[@]}"
  write_contextual_config

  PASS2_TOTAL="${#PASS1_OK[@]}"
  log_info "Pass 2/3: generating contextual reports (single full pass) - ${PASS2_TOTAL} total..."
  pass2_current=0
  for i in "${!STEMS[@]}"; do
    stem="${STEMS[$i]}"
    if ! array_contains "${stem}" "${PASS1_OK[@]}"; then
      continue
    fi
    pass2_current=$((pass2_current + 1))
    csv_path="${CSVS[$i]}"
    sidecar_path="${SIDECARS[$i]}"
    progress_label "pass2" "${pass2_current}" "${PASS2_TOTAL}" "${stem}"
    if CONFIG_PATH="${CONTEXTUAL_CONFIG_PATH}" REPORTS_ROOT="${REPORTS_ROOT}" \
      "${PROJECT_ROOT}/scripts/report/run_unified_report.sh" --skip-imports "${csv_path}" "${VRDB_EXTRACT}" "${sidecar_path}"; then
      FINAL_OK+=("${stem}")
    else
      PASS2_FAILED+=("${stem}")
      log_error "[pass2][failed] ${stem}"
    fi
  done
  log_info "Pass 2 complete: ${#FINAL_OK[@]} succeeded, ${#PASS2_FAILED[@]} failed."

  if [[ "${#FINAL_OK[@]}" -eq 0 ]]; then
    log_error "No reports succeeded in fast contextual mode."
    exit 1
  fi

  if [[ "${FAST_FAILURE_RERUN}" == "1" && "${#PASS2_FAILED[@]}" -gt 0 && "${#FINAL_OK[@]}" -gt 0 ]]; then
    log_warn "Fast-mode failure mitigation: rebuilding contextual baseline from successful reports only..."
    build_contextual_baseline_from_stems "fast-success-only-rerun" "${FINAL_OK[@]}"
    write_contextual_config

    declare -a RERUN_OK=()
    PASS2_RERUN_TOTAL="${#FINAL_OK[@]}"
    pass2_rerun_current=0
    log_info "Pass 2b/3: rerunning successful reports against success-only contextual baseline - ${PASS2_RERUN_TOTAL} total..."
    for i in "${!STEMS[@]}"; do
      stem="${STEMS[$i]}"
      if ! array_contains "${stem}" "${FINAL_OK[@]}"; then
        continue
      fi
      pass2_rerun_current=$((pass2_rerun_current + 1))
      csv_path="${CSVS[$i]}"
      sidecar_path="${SIDECARS[$i]}"
      progress_label "pass2b" "${pass2_rerun_current}" "${PASS2_RERUN_TOTAL}" "${stem}"
      if CONFIG_PATH="${CONTEXTUAL_CONFIG_PATH}" REPORTS_ROOT="${REPORTS_ROOT}" \
        "${PROJECT_ROOT}/scripts/report/run_unified_report.sh" --skip-imports "${csv_path}" "${VRDB_EXTRACT}" "${sidecar_path}"; then
        RERUN_OK+=("${stem}")
      else
        PASS2_FAILED+=("${stem}")
        log_error "[pass2b][failed] ${stem}"
      fi
    done
    FINAL_OK=("${RERUN_OK[@]}")
    log_info "Pass 2b complete: ${#FINAL_OK[@]} succeeded after mitigation rerun."
  fi
else
  PASS1_TOTAL="${#STEMS[@]}"
  log_info "Pass 1/3: generating reports for all matched datasets - ${PASS1_TOTAL} total..."
  for i in "${!STEMS[@]}"; do
    stem="${STEMS[$i]}"
    csv_path="${CSVS[$i]}"
    sidecar_path="${SIDECARS[$i]}"
    current=$((i + 1))
    progress_label "pass1" "${current}" "${PASS1_TOTAL}" "${stem}"
    if CONFIG_PATH="${PASS1_CONFIG_PATH}" REPORTS_ROOT="${REPORTS_ROOT}" \
      "${PROJECT_ROOT}/scripts/report/run_unified_report.sh" --skip-vrdb-import "${csv_path}" "${VRDB_EXTRACT}" "${sidecar_path}"; then
      PASS1_OK+=("${stem}")
    else
      PASS1_FAILED+=("${stem}")
      log_error "[pass1][failed] ${stem}"
    fi
  done

  if [[ "${#PASS1_OK[@]}" -eq 0 ]]; then
    log_error "All first-pass report generations failed."
    exit 1
  fi
  log_info "Pass 1 complete: ${#PASS1_OK[@]} succeeded, ${#PASS1_FAILED[@]} failed."

  build_contextual_baseline_from_stems "legacy-pass1-successes" "${PASS1_OK[@]}"
  write_contextual_config

  if [[ "${SKIP_CONTEXTUAL_RERUN}" -eq 0 ]]; then
    PASS2_TOTAL="${#PASS1_OK[@]}"
    log_info "Pass 2/3: rerunning successful pass1 reports with contextual baseline enabled - ${PASS2_TOTAL} total..."
    pass2_current=0
    for i in "${!STEMS[@]}"; do
      stem="${STEMS[$i]}"
      if ! array_contains "${stem}" "${PASS1_OK[@]}"; then
        continue
      fi
      pass2_current=$((pass2_current + 1))
      csv_path="${CSVS[$i]}"
      sidecar_path="${SIDECARS[$i]}"
      progress_label "pass2" "${pass2_current}" "${PASS2_TOTAL}" "${stem}"
      if CONFIG_PATH="${CONTEXTUAL_CONFIG_PATH}" REPORTS_ROOT="${REPORTS_ROOT}" \
        "${PROJECT_ROOT}/scripts/report/run_unified_report.sh" --skip-imports "${csv_path}" "${VRDB_EXTRACT}" "${sidecar_path}"; then
        FINAL_OK+=("${stem}")
      else
        PASS2_FAILED+=("${stem}")
        log_error "[pass2][failed] ${stem}"
      fi
    done
    log_info "Pass 2 complete: ${#FINAL_OK[@]} succeeded, ${#PASS2_FAILED[@]} failed."
  else
    log_warn "Skipping contextual rerun (--skip-contextual-rerun)."
    FINAL_OK=("${PASS1_OK[@]}")
  fi
fi

if [[ "${#FINAL_OK[@]}" -eq 0 ]]; then
  log_error "No reports available for baseline output after report generation."
  exit 1
fi

log_info "Rebuilding reports index + global baselines..."
(
  cd "${REPO_ROOT}"
  python ./testifier_audit/scripts/report/build_reports_index.py
  python ./testifier_audit/scripts/report/build_global_baselines.py
)

declare -a LOO_FAILED=()
LOO_TOTAL="${#FINAL_OK[@]}"
log_info "Pass 3/3: building leave-one-out baseline payloads for each successful report - ${LOO_TOTAL} total (mode=${LOO_MODE})..."
if [[ "${LOO_MODE}" == "batch" ]]; then
  LOO_TARGETS_PATH="$(mktemp "${REPO_ROOT}/output/loo-targets.XXXXXX.txt")"
  LOO_FAILURES_JSON_PATH="$(mktemp "${REPO_ROOT}/output/loo-failures.XXXXXX.json")"
  printf '%s\n' "${FINAL_OK[@]}" > "${LOO_TARGETS_PATH}"

  loo_batch_status=0
  (
    cd "${PROJECT_ROOT}" && \
    python ./scripts/report/build_leave_one_out_baselines_batch.py \
      --reports-dir "${REPORTS_ROOT}" \
      --report-id-file "${LOO_TARGETS_PATH}" \
      --cohort-strategy hierarchical \
      --failure-output "${LOO_FAILURES_JSON_PATH}"
  ) || loo_batch_status=$?

  if [[ -f "${LOO_FAILURES_JSON_PATH}" ]]; then
    while IFS= read -r stem; do
      if [[ -n "${stem}" ]]; then
        LOO_FAILED+=("${stem}")
      fi
    done < <(python - "${LOO_FAILURES_JSON_PATH}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(0)
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(0)
if not isinstance(payload, list):
    raise SystemExit(0)
for item in payload:
    if not isinstance(item, dict):
        continue
    report_id = str(item.get("report_id") or "").strip()
    if report_id:
        print(report_id)
PY
)
  fi

  if [[ "${loo_batch_status}" -ne 0 && "${#LOO_FAILED[@]}" -eq 0 ]]; then
    LOO_FAILED=("${FINAL_OK[@]}")
  fi
  for stem in "${LOO_FAILED[@]-}"; do
    if [[ -n "${stem}" ]]; then
      log_error "[loo][failed] ${stem}"
    fi
  done
else
  loo_current=0
  for stem in "${FINAL_OK[@]}"; do
    loo_current=$((loo_current + 1))
    progress_label "loo" "${loo_current}" "${LOO_TOTAL}" "${stem}"
    if ! (
      cd "${PROJECT_ROOT}" && \
      python ./scripts/report/build_leave_one_out_baseline.py \
        --reports-dir "${REPORTS_ROOT}" \
        --report-id "${stem}" \
        --cohort-strategy hierarchical
    ); then
      LOO_FAILED+=("${stem}")
      log_error "[loo][failed] ${stem}"
    fi
  done
fi
log_info "Pass 3 complete: ${#LOO_FAILED[@]} LOO failures."
log_info "Run complete."
log_info "matched_datasets=${#STEMS[@]} pass1_failed=${#PASS1_FAILED[@]} pass2_failed=${#PASS2_FAILED[@]} loo_failed=${#LOO_FAILED[@]} final_reports=${#FINAL_OK[@]}"
log_info "contextual_mode=$([[ "${FAST_CONTEXTUAL}" == "1" ]] && echo "fast" || echo "legacy") loo_mode=${LOO_MODE}"
log_info "reports_root=${REPORTS_ROOT}"
log_info "contextual_baseline_json=${CONTEXTUAL_JSON}"
log_info "global_baselines_json=${REPORTS_ROOT}/global_baselines.json"

if [[ "${#PASS1_FAILED[@]}" -gt 0 || "${#PASS2_FAILED[@]}" -gt 0 || "${#LOO_FAILED[@]}" -gt 0 ]]; then
  {
    echo "run_log=${RUN_LOG_PATH}"
    echo "matched_datasets=${#STEMS[@]}"
    echo "pass1_failed=${#PASS1_FAILED[@]}"
    echo "pass2_failed=${#PASS2_FAILED[@]}"
    echo "loo_failed=${#LOO_FAILED[@]}"
    echo ""
    echo "[pass1_failed_stems]"
    if [[ "${#PASS1_FAILED[@]}" -gt 0 ]]; then
      printf '%s\n' "${PASS1_FAILED[@]}"
    fi
    echo ""
    echo "[pass2_failed_stems]"
    if [[ "${#PASS2_FAILED[@]}" -gt 0 ]]; then
      printf '%s\n' "${PASS2_FAILED[@]}"
    fi
    echo ""
    echo "[loo_failed_stems]"
    if [[ "${#LOO_FAILED[@]}" -gt 0 ]]; then
      printf '%s\n' "${LOO_FAILED[@]}"
    fi
  } > "${RUN_FAILURES_PATH}"
  log_error "Failure details written: ${RUN_FAILURES_PATH}"
  log_error "One or more steps failed for some datasets."
  exit 1
fi

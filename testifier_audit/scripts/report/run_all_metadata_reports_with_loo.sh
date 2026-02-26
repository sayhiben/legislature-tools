#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"

# shellcheck disable=SC1091
source "${PROJECT_ROOT}/scripts/lib/env.sh"
load_project_env "${PROJECT_ROOT}"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/report/run_all_metadata_reports_with_loo.sh [--vrdb <path>] [--base-config <path>] [--skip-contextual-rerun]

Description:
  1. Finds all metadata sidecars with matching CSV files (<stem>.hearing.yaml + <stem>.csv).
  2. Runs unified report generation for all matched datasets.
  3. Builds contextual duplicate baselines from matched datasets only.
  4. Re-runs all successful reports with contextual baseline enabled (unless --skip-contextual-rerun).
  5. Rebuilds reports index + global baselines.
  6. Builds leave-one-out baseline payload for each successful report.

Options:
  --vrdb <path>             VRDB extract path (default: ./data/raw/20260202_VRDB_Extract.txt)
  --base-config <path>      Base report config path (default: ./testifier_audit/configs/voter_registry_enabled.yaml)
  --skip-contextual-rerun   Skip the second pass that reruns reports with contextual baseline enabled.
  -h, --help                Show this help text.

Environment overrides:
  METADATA_DIR    (default: ./data/metadata)
  RAW_DIR         (default: ./data/raw)
  REPORTS_ROOT    (default: ./reports)
  CONTEXTUAL_JSON (default: ./data/metadata/contextual_duplicate_baseline.json)
  CONTEXTUAL_CSV  (default: ./data/metadata/contextual_duplicate_baseline.csv)
EOF
}

DEFAULT_VRDB="${REPO_ROOT}/data/raw/20260202_VRDB_Extract.txt"
DEFAULT_BASE_CONFIG="${PROJECT_ROOT}/configs/voter_registry_enabled.yaml"

VRDB_EXTRACT="${DEFAULT_VRDB}"
BASE_CONFIG_PATH="${DEFAULT_BASE_CONFIG}"
SKIP_CONTEXTUAL_RERUN=0

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
    --skip-contextual-rerun)
      SKIP_CONTEXTUAL_RERUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

METADATA_DIR="${METADATA_DIR:-${REPO_ROOT}/data/metadata}"
RAW_DIR="${RAW_DIR:-${REPO_ROOT}/data/raw}"
REPORTS_ROOT="${REPORTS_ROOT:-${REPO_ROOT}/reports}"
CONTEXTUAL_JSON="${CONTEXTUAL_JSON:-${REPO_ROOT}/data/metadata/contextual_duplicate_baseline.json}"
CONTEXTUAL_CSV="${CONTEXTUAL_CSV:-${REPO_ROOT}/data/metadata/contextual_duplicate_baseline.csv}"

if [[ ! -f "${VRDB_EXTRACT}" ]]; then
  echo "VRDB extract not found: ${VRDB_EXTRACT}" >&2
  exit 1
fi
if [[ ! -f "${BASE_CONFIG_PATH}" ]]; then
  echo "Base config not found: ${BASE_CONFIG_PATH}" >&2
  exit 1
fi
if [[ ! -d "${METADATA_DIR}" ]]; then
  echo "Metadata directory not found: ${METADATA_DIR}" >&2
  exit 1
fi
if [[ ! -d "${RAW_DIR}" ]]; then
  echo "Raw CSV directory not found: ${RAW_DIR}" >&2
  exit 1
fi

mkdir -p "${REPORTS_ROOT}" "${REPO_ROOT}/output"

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
  echo "No metadata-supported CSV datasets found in ${METADATA_DIR} + ${RAW_DIR}." >&2
  exit 1
fi

echo "Found ${#STEMS[@]} metadata-supported dataset(s)."
echo "Using base config: ${BASE_CONFIG_PATH}"
echo "Using VRDB extract: ${VRDB_EXTRACT}"

if [[ "${CI_SKIP_INSTALL:-0}" != "1" ]]; then
  (
    cd "${PROJECT_ROOT}"
    python -m pip install -e ".[dev]"
  )
fi
export CI_SKIP_INSTALL=1

declare -a PASS1_OK=()
declare -a PASS1_FAILED=()

echo "Pass 1: generating reports for all matched datasets..."
for i in "${!STEMS[@]}"; do
  stem="${STEMS[$i]}"
  csv_path="${CSVS[$i]}"
  sidecar_path="${SIDECARS[$i]}"
  echo "  [pass1] ${stem}"
  if CONFIG_PATH="${BASE_CONFIG_PATH}" REPORTS_ROOT="${REPORTS_ROOT}" \
    "${PROJECT_ROOT}/scripts/report/run_unified_report.sh" "${csv_path}" "${VRDB_EXTRACT}" "${sidecar_path}"; then
    PASS1_OK+=("${stem}")
  else
    PASS1_FAILED+=("${stem}")
    echo "  [pass1][failed] ${stem}" >&2
  fi
done

if [[ "${#PASS1_OK[@]}" -eq 0 ]]; then
  echo "All first-pass report generations failed." >&2
  exit 1
fi

CONTEXTUAL_CONFIG_PATH="$(mktemp "${REPO_ROOT}/output/contextual-baseline-config.XXXXXX.yaml")"
TMP_CONTEXTUAL_CSV_DIR="$(mktemp -d "${REPO_ROOT}/output/contextual-csvs.XXXXXX")"
TMP_CONTEXTUAL_META_DIR="$(mktemp -d "${REPO_ROOT}/output/contextual-meta.XXXXXX")"
cleanup() {
  rm -f "${CONTEXTUAL_CONFIG_PATH}" || true
  rm -rf "${TMP_CONTEXTUAL_CSV_DIR}" "${TMP_CONTEXTUAL_META_DIR}" || true
}
trap cleanup EXIT

for i in "${!STEMS[@]}"; do
  stem="${STEMS[$i]}"
  if ! array_contains "${stem}" "${PASS1_OK[@]}"; then
    continue
  fi
  ln -sf "${CSVS[$i]}" "${TMP_CONTEXTUAL_CSV_DIR}/${stem}.csv"
  ln -sf "${SIDECARS[$i]}" "${TMP_CONTEXTUAL_META_DIR}/${stem}.hearing.yaml"
done

echo "Building contextual duplicate baseline from matched datasets..."
(
  cd "${PROJECT_ROOT}"
  python -m testifier_audit.io.contextual_duplicate_baseline \
    --csv-dir "${TMP_CONTEXTUAL_CSV_DIR}" \
    --metadata-dir "${TMP_CONTEXTUAL_META_DIR}" \
    --output-json "${CONTEXTUAL_JSON}" \
    --output-csv "${CONTEXTUAL_CSV}"
)

python - "${BASE_CONFIG_PATH}" "${CONTEXTUAL_JSON}" "${CONTEXTUAL_CONFIG_PATH}" <<'PY'
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

declare -a FINAL_OK=()
declare -a PASS2_FAILED=()

if [[ "${SKIP_CONTEXTUAL_RERUN}" -eq 0 ]]; then
  echo "Pass 2: rerunning reports with contextual baseline enabled..."
  for i in "${!STEMS[@]}"; do
    stem="${STEMS[$i]}"
    if ! array_contains "${stem}" "${PASS1_OK[@]}"; then
      continue
    fi
    csv_path="${CSVS[$i]}"
    sidecar_path="${SIDECARS[$i]}"
    echo "  [pass2] ${stem}"
    if CONFIG_PATH="${CONTEXTUAL_CONFIG_PATH}" REPORTS_ROOT="${REPORTS_ROOT}" \
      "${PROJECT_ROOT}/scripts/report/run_unified_report.sh" "${csv_path}" "${VRDB_EXTRACT}" "${sidecar_path}"; then
      FINAL_OK+=("${stem}")
    else
      PASS2_FAILED+=("${stem}")
      echo "  [pass2][failed] ${stem}" >&2
    fi
  done
else
  echo "Skipping contextual rerun (--skip-contextual-rerun)."
  FINAL_OK=("${PASS1_OK[@]}")
fi

if [[ "${#FINAL_OK[@]}" -eq 0 ]]; then
  echo "No reports available for baseline output after rerun." >&2
  exit 1
fi

echo "Rebuilding reports index + global baselines..."
(
  cd "${REPO_ROOT}"
  python ./testifier_audit/scripts/report/build_reports_index.py
  python ./testifier_audit/scripts/report/build_global_baselines.py
)

declare -a LOO_FAILED=()
echo "Building leave-one-out baseline payloads for each successful report..."
for stem in "${FINAL_OK[@]}"; do
  echo "  [loo] ${stem}"
  if ! (
    cd "${PROJECT_ROOT}" && \
    python ./scripts/report/build_leave_one_out_baseline.py \
      --reports-dir "${REPORTS_ROOT}" \
      --report-id "${stem}"
  ); then
    LOO_FAILED+=("${stem}")
    echo "  [loo][failed] ${stem}" >&2
  fi
done

echo ""
echo "Run complete."
echo "  matched_datasets: ${#STEMS[@]}"
echo "  pass1_failed: ${#PASS1_FAILED[@]}"
echo "  pass2_failed: ${#PASS2_FAILED[@]}"
echo "  loo_failed: ${#LOO_FAILED[@]}"
echo "  final_reports: ${#FINAL_OK[@]}"
echo "  reports_root: ${REPORTS_ROOT}"
echo "  contextual_baseline_json: ${CONTEXTUAL_JSON}"
echo "  global_baselines_json: ${REPORTS_ROOT}/global_baselines.json"

if [[ "${#PASS1_FAILED[@]}" -gt 0 || "${#PASS2_FAILED[@]}" -gt 0 || "${#LOO_FAILED[@]}" -gt 0 ]]; then
  echo "One or more steps failed for some datasets." >&2
  exit 1
fi

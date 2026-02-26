#!/usr/bin/env bash
set -euo pipefail

on_error() {
  local exit_code="$?"
  local line="${BASH_LINENO[0]:-unknown}"
  local cmd="${BASH_COMMAND:-unknown}"
  echo "ERROR: rerender_all_reports.sh failed at line ${line}: ${cmd}" >&2
  exit "${exit_code}"
}
trap on_error ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"

# shellcheck disable=SC1091
source "${PROJECT_ROOT}/scripts/lib/env.sh"
load_project_env "${PROJECT_ROOT}"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/report/rerender_all_reports.sh [--reports-root <path>] [--metadata-dir <path>] [--config <path>] [--dedup-mode <mode>] [--skip-index]

Description:
  Re-renders report HTML/assets/report_data shards for every existing report directory that
  already contains report_data/index.json. This does not rerun detector computation.

Options:
  --reports-root <path>   Reports root directory (default: ./reports)
  --metadata-dir <path>   Hearing metadata sidecars directory (default: ./data/metadata)
  --config <path>         Config used for report CLI (default: ./testifier_audit/configs/voter_registry_enabled.yaml)
  --dedup-mode <mode>     Optional dedup override: raw | exact_row_dedup | side_by_side
  --skip-index            Skip rebuilding reports/index.html
  -h, --help              Show help.
EOF
}

REPORTS_ROOT="${REPORTS_ROOT:-${REPO_ROOT}/reports}"
METADATA_DIR="${METADATA_DIR:-${REPO_ROOT}/data/metadata}"
CONFIG_PATH="${CONFIG_PATH:-${PROJECT_ROOT}/configs/voter_registry_enabled.yaml}"
DEDUP_MODE="${DEDUP_MODE:-}"
SKIP_INDEX=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reports-root)
      REPORTS_ROOT="$2"
      shift 2
      ;;
    --metadata-dir)
      METADATA_DIR="$2"
      shift 2
      ;;
    --config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --dedup-mode)
      DEDUP_MODE="$2"
      shift 2
      ;;
    --skip-index)
      SKIP_INDEX=1
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

if [[ ! -d "${REPORTS_ROOT}" ]]; then
  echo "Reports root not found: ${REPORTS_ROOT}" >&2
  exit 1
fi
if [[ ! -d "${METADATA_DIR}" ]]; then
  echo "Metadata directory not found: ${METADATA_DIR}" >&2
  exit 1
fi
if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Config not found: ${CONFIG_PATH}" >&2
  exit 1
fi

echo "Reports root: ${REPORTS_ROOT}"
echo "Metadata dir: ${METADATA_DIR}"
echo "Config: ${CONFIG_PATH}"
if [[ -n "${DEDUP_MODE}" ]]; then
  echo "Dedup mode override: ${DEDUP_MODE}"
fi

shopt -s nullglob
declare -a REPORT_DIRS=()
for report_dir in "${REPORTS_ROOT}"/*; do
  [[ -d "${report_dir}" ]] || continue
  [[ -f "${report_dir}/report_data/index.json" ]] || continue
  REPORT_DIRS+=("${report_dir}")
done
shopt -u nullglob

if [[ "${#REPORT_DIRS[@]}" -eq 0 ]]; then
  echo "No report directories with report_data/index.json found under ${REPORTS_ROOT}."
  exit 0
fi

IFS=$'\n' REPORT_DIRS=($(printf '%s\n' "${REPORT_DIRS[@]}" | sort))
unset IFS

SUCCESS=0
FAILED=0
TOTAL="${#REPORT_DIRS[@]}"

for i in "${!REPORT_DIRS[@]}"; do
  report_dir="${REPORT_DIRS[$i]}"
  stem="$(basename "${report_dir}")"
  sidecar="${METADATA_DIR}/${stem}.hearing.yaml"
  idx=$((i + 1))
  echo "[${idx}/${TOTAL}] rerender ${stem}"

  CLI_ARGS=(report --out "${report_dir}" --config "${CONFIG_PATH}")
  if [[ -f "${sidecar}" ]]; then
    CLI_ARGS+=(--hearing-metadata "${sidecar}")
  else
    echo "  warning: metadata sidecar not found for ${stem}; rendering without --hearing-metadata"
  fi
  if [[ -n "${DEDUP_MODE}" ]]; then
    CLI_ARGS+=(--dedup-mode "${DEDUP_MODE}")
  fi

  if (cd "${PROJECT_ROOT}" && python -m testifier_audit.cli "${CLI_ARGS[@]}"); then
    SUCCESS=$((SUCCESS + 1))
  else
    FAILED=$((FAILED + 1))
    echo "  failed: ${stem}" >&2
  fi
done

echo "Rerender complete: ${SUCCESS} succeeded, ${FAILED} failed."

if [[ "${SKIP_INDEX}" -eq 0 ]]; then
  echo "Rebuilding reports index..."
  (cd "${REPO_ROOT}" && python ./testifier_audit/scripts/report/build_reports_index.py)
fi

if [[ "${FAILED}" -gt 0 ]]; then
  exit 1
fi


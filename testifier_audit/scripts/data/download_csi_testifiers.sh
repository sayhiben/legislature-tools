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
  ./scripts/data/download_csi_testifiers.sh <bill-query> [additional download-csi-testifiers args]

Examples:
  ./scripts/data/download_csi_testifiers.sh "SB 6005"
  ./scripts/data/download_csi_testifiers.sh "transportation funding" --meeting-index 1 --verbose

Environment:
  CSI_TESTIFIERS_CSV_OUT_DIR=<path>      Optional CSV output directory override.
  CSI_TESTIFIERS_METADATA_OUT_DIR=<path> Optional hearing metadata output directory override.
EOF
}

if [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 1
fi

BILL_QUERY="$1"
shift

CSV_OUT_DIR="${CSI_TESTIFIERS_CSV_OUT_DIR:-${REPO_ROOT}/data/raw}"
METADATA_OUT_DIR="${CSI_TESTIFIERS_METADATA_OUT_DIR:-${REPO_ROOT}/data/metadata}"

if [[ "${CI_SKIP_INSTALL:-0}" != "1" ]]; then
  python -m pip install -e ".[dev]"
fi

cd "${PROJECT_ROOT}"
python -m testifier_audit.cli download-csi-testifiers \
  "${BILL_QUERY}" \
  --csv-out-dir "${CSV_OUT_DIR}" \
  --metadata-out-dir "${METADATA_OUT_DIR}" \
  "$@"

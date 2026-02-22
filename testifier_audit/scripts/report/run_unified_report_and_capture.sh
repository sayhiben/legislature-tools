#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/report/run_unified_report_and_capture.sh <submissions_csv> <vrdb_extract> [hearing_metadata]

Description:
  Runs the unified import+analysis+report pipeline, then captures a stitched screenshot of
  the fully loaded report page (sidebar open + all <details> expanded by default).
  A temporary localhost server is started automatically by capture_report_screenshot.py.

Environment overrides:
  OUT_DIR                     Report output directory (default: reports/<csv-stem>)
  SCREENSHOT_PATH             Final stitched PNG path (default under <out_dir>/screenshots/)
  SCREENSHOT_WIDTH            Capture viewport width (default: 1920)
  SCREENSHOT_HEIGHT           Capture viewport height (default: 1400)
  SCREENSHOT_WAIT_MS          Initial post-load wait (default: 12000)
  SCREENSHOT_SETTLE_MS        Scroll settle wait between tiles (default: 600)
  SCREENSHOT_TIMEOUT_SEC      Per command timeout for playwright-cli (default: 90)
  SCREENSHOT_MAX_TILES        Tile cap (default: 0 => no cap)
  SCREENSHOT_SESSION          Optional playwright-cli session name
  SCREENSHOT_TILES_DIR        Optional tile directory override
  SCREENSHOT_KEEP_TILES       Set to 1 to keep tile PNGs
  SCREENSHOT_KEEP_FIXED_CHROME Set to 1 to keep fixed sidebar/menu chrome
  SCREENSHOT_NO_OPEN_SIDEBAR  Set to 1 to skip forcing sidebar open
  SCREENSHOT_NO_EXPAND_DETAILS Set to 1 to skip opening all <details> blocks

Notes:
  - Report-generation options such as CONFIG_PATH, DEDUP_MODE, REPORTS_ROOT, and TESTIFIER_AUDIT_DB_URL
    are passed through to run_unified_report.sh via environment variables.
EOF
}

if [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 2 ]] || [[ $# -gt 3 ]]; then
  usage >&2
  exit 1
fi

if ! command -v playwright-cli >/dev/null 2>&1; then
  echo "playwright-cli not found in PATH. Install it first (for example: brew install playwright-cli)." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"

SUBMISSIONS_CSV="$1"
VRDB_EXTRACT="$2"
HEARING_METADATA_PATH="${3:-}"

RUN_ARGS=("${SUBMISSIONS_CSV}" "${VRDB_EXTRACT}")
if [[ -n "${HEARING_METADATA_PATH}" ]]; then
  RUN_ARGS+=("${HEARING_METADATA_PATH}")
fi

echo "[run] Building report artifacts..."
"${SCRIPT_DIR}/run_unified_report.sh" "${RUN_ARGS[@]}"

CSV_BASENAME="$(basename "${SUBMISSIONS_CSV}")"
CSV_STEM="${CSV_BASENAME%.*}"
REPORTS_ROOT="${REPORTS_ROOT:-${REPO_ROOT}/reports}"
DEFAULT_OUT_DIR="${REPORTS_ROOT}/${CSV_STEM}"
OUT_DIR="${OUT_DIR:-${DEFAULT_OUT_DIR}}"
REPORT_HTML="${OUT_DIR}/report.html"

if [[ ! -f "${REPORT_HTML}" ]]; then
  echo "Expected report not found after unified run: ${REPORT_HTML}" >&2
  exit 1
fi

SCREENSHOT_WIDTH="${SCREENSHOT_WIDTH:-1920}"
SCREENSHOT_HEIGHT="${SCREENSHOT_HEIGHT:-1400}"
SCREENSHOT_WAIT_MS="${SCREENSHOT_WAIT_MS:-12000}"
SCREENSHOT_SETTLE_MS="${SCREENSHOT_SETTLE_MS:-600}"
SCREENSHOT_TIMEOUT_SEC="${SCREENSHOT_TIMEOUT_SEC:-90}"
SCREENSHOT_MAX_TILES="${SCREENSHOT_MAX_TILES:-0}"

TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"
DEFAULT_SCREENSHOT_PATH="${OUT_DIR}/screenshots/report-full-expanded-sidebar-${TIMESTAMP}-stitched.png"
SCREENSHOT_PATH="${SCREENSHOT_PATH:-${DEFAULT_SCREENSHOT_PATH}}"
mkdir -p "$(dirname "${SCREENSHOT_PATH}")"

CAPTURE_CMD=(
  python "${SCRIPT_DIR}/capture_report_screenshot.py"
  "${REPORT_HTML}"
  "${SCREENSHOT_PATH}"
  --width "${SCREENSHOT_WIDTH}"
  --height "${SCREENSHOT_HEIGHT}"
  --wait-ms "${SCREENSHOT_WAIT_MS}"
  --settle-ms "${SCREENSHOT_SETTLE_MS}"
  --command-timeout-sec "${SCREENSHOT_TIMEOUT_SEC}"
)

if [[ -n "${SCREENSHOT_SESSION:-}" ]]; then
  CAPTURE_CMD+=(--session "${SCREENSHOT_SESSION}")
fi
if [[ "${SCREENSHOT_MAX_TILES}" != "0" ]]; then
  CAPTURE_CMD+=(--max-tiles "${SCREENSHOT_MAX_TILES}")
fi
if [[ -n "${SCREENSHOT_TILES_DIR:-}" ]]; then
  CAPTURE_CMD+=(--tiles-dir "${SCREENSHOT_TILES_DIR}")
fi
if [[ "${SCREENSHOT_KEEP_TILES:-0}" == "1" ]]; then
  CAPTURE_CMD+=(--keep-tiles)
fi
if [[ "${SCREENSHOT_KEEP_FIXED_CHROME:-0}" == "1" ]]; then
  CAPTURE_CMD+=(--keep-fixed-chrome)
fi
if [[ "${SCREENSHOT_NO_OPEN_SIDEBAR:-0}" == "1" ]]; then
  CAPTURE_CMD+=(--no-open-sidebar)
fi
if [[ "${SCREENSHOT_NO_EXPAND_DETAILS:-0}" == "1" ]]; then
  CAPTURE_CMD+=(--no-expand-details)
fi

echo "[capture] Capturing stitched report screenshot..."
"${CAPTURE_CMD[@]}"

echo "Screenshot written to: ${SCREENSHOT_PATH}"
echo "Capture diagnostics JSON written next to the PNG with a .json suffix."

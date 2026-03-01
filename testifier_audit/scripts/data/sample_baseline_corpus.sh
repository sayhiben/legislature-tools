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
  ./scripts/data/sample_baseline_corpus.sh <sample-size> [additional sample-baseline-corpus args]

Examples:
  ./scripts/data/sample_baseline_corpus.sh 25
  ./scripts/data/sample_baseline_corpus.sh 40 --seed 42 --rate-limit-seconds 1.5 --verbose

Environment:
  SAMPLE_BASELINE_INDEX_JSON=<path>         Optional GetCommitteeMeetings cache JSON override.
  SAMPLE_BASELINE_INDEX_CSV=<path>          Legacy arg retained for compatibility (unused).
  SAMPLE_BASELINE_MEETING_ITEMS_CACHE_DIR=<path>
                                           Optional directory for per-meeting items cache files.
  SAMPLE_BASELINE_MEETINGS_CACHE_MAX_AGE_HOURS=<hours>
                                           Optional max age before meetings cache refresh.
  SAMPLE_BASELINE_MAX_MEETING_ITEMS_FETCHES=<count>
                                           Optional cap for uncached meeting-items fetches/run.
  SAMPLE_BASELINE_CSV_OUT_DIR=<path>        Optional sampled CSV output directory override.
  SAMPLE_BASELINE_METADATA_OUT_DIR=<path>   Optional sampled metadata output directory override.
  SAMPLE_BASELINE_MANIFEST_OUT=<path>       Optional manifest output path override.
  SAMPLE_BASELINE_SESSION_COUNT=<count>     Optional session-year count override (default: 3).
  SAMPLE_BASELINE_RATE_LIMIT_SECONDS=<sec>  Optional request rate-limit override (default: 1.0).
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

SAMPLE_SIZE="$1"
shift

INDEX_JSON="${SAMPLE_BASELINE_INDEX_JSON:-${REPO_ROOT}/data/metadata/wa_committee_meetings_cache.json}"
INDEX_CSV="${SAMPLE_BASELINE_INDEX_CSV:-${REPO_ROOT}/data/metadata/wa_meeting_bill_index.csv}"
MEETING_ITEMS_CACHE_DIR="${SAMPLE_BASELINE_MEETING_ITEMS_CACHE_DIR:-${REPO_ROOT}/data/metadata/wa_committee_meeting_items}"
MEETINGS_CACHE_MAX_AGE_HOURS="${SAMPLE_BASELINE_MEETINGS_CACHE_MAX_AGE_HOURS:-12.0}"
MAX_MEETING_ITEMS_FETCHES="${SAMPLE_BASELINE_MAX_MEETING_ITEMS_FETCHES:-}"
CSV_OUT_DIR="${SAMPLE_BASELINE_CSV_OUT_DIR:-${REPO_ROOT}/data/raw}"
METADATA_OUT_DIR="${SAMPLE_BASELINE_METADATA_OUT_DIR:-${REPO_ROOT}/data/metadata}"
MANIFEST_OUT="${SAMPLE_BASELINE_MANIFEST_OUT:-${REPO_ROOT}/data/metadata/baseline_sample_manifest.json}"
SESSION_COUNT="${SAMPLE_BASELINE_SESSION_COUNT:-3}"
RATE_LIMIT_SECONDS="${SAMPLE_BASELINE_RATE_LIMIT_SECONDS:-1.0}"

if [[ "${CI_SKIP_INSTALL:-0}" != "1" ]]; then
  python -m pip install -e ".[dev]"
fi

cd "${PROJECT_ROOT}"
cmd=(
  python -m testifier_audit.cli sample-baseline-corpus
  --sample-size "${SAMPLE_SIZE}"
  --session-count "${SESSION_COUNT}"
  --index-json "${INDEX_JSON}"
  --index-csv "${INDEX_CSV}"
  --meeting-items-cache-dir "${MEETING_ITEMS_CACHE_DIR}"
  --meetings-cache-max-age-hours "${MEETINGS_CACHE_MAX_AGE_HOURS}"
  --csv-out-dir "${CSV_OUT_DIR}"
  --metadata-out-dir "${METADATA_OUT_DIR}"
  --manifest-out "${MANIFEST_OUT}"
  --rate-limit-seconds "${RATE_LIMIT_SECONDS}"
)

if [[ -n "${MAX_MEETING_ITEMS_FETCHES}" ]]; then
  cmd+=(--max-meeting-items-fetches "${MAX_MEETING_ITEMS_FETCHES}")
fi

cmd+=("$@")
"${cmd[@]}"

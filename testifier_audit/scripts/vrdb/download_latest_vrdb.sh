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
  ./scripts/vrdb/download_latest_vrdb.sh

Environment:
  RAW_DATA_DIR=<path>          Destination directory (default: ../data/raw)
  VRDB_URL=<url>               Override URL; if unset script tries current month then prior month
  FORCE_VRDB_DOWNLOAD=1        Download even if current-month extract already exists

Also loads .env/.env.local from testifier_audit/ before resolving defaults.
EOF
}

if [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

RAW_DATA_DIR="${RAW_DATA_DIR:-${REPO_ROOT}/data/raw}"
VRDB_URL="${VRDB_URL:-}"
FORCE_DOWNLOAD="${FORCE_VRDB_DOWNLOAD:-0}"

if ! command -v unzip >/dev/null 2>&1; then
  echo "unzip is required to extract VRDB archives." >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required to download VRDB archives." >&2
  exit 1
fi

mkdir -p "${RAW_DATA_DIR}"

CURRENT_YYYYMM="$(date '+%Y%m')"
EXISTING_CURRENT_MONTH="$(
  find "${RAW_DATA_DIR}" -maxdepth 1 -type f -name '????????_VRDB_Extract.txt' -print \
    | awk -F/ '{print $NF}' \
    | awk -v yyyymm="${CURRENT_YYYYMM}" '$0 ~ ("^" yyyymm "[0-9][0-9]_VRDB_Extract\\.txt$")' \
    | sort \
    | tail -n 1
)"

if [[ -n "${EXISTING_CURRENT_MONTH}" ]] && [[ "${FORCE_DOWNLOAD}" != "1" ]]; then
  echo "Current-month VRDB extract already exists: ${RAW_DATA_DIR}/${EXISTING_CURRENT_MONTH}"
  echo "Skipping download."
  exit 0
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

ZIP_PATH="${TMP_DIR}/vrdb.zip"
declare -a VRDB_URL_CANDIDATES=()
if [[ -n "${VRDB_URL}" ]]; then
  VRDB_URL_CANDIDATES=("${VRDB_URL}")
else
  VRDB_MONTH_TOKENS="$(
    python - <<'PY'
from datetime import date

today = date.today()
current = f"{today.month:02d}.{today.year:04d}"
prev_year = today.year
prev_month = today.month - 1
if prev_month == 0:
    prev_month = 12
    prev_year -= 1
previous = f"{prev_month:02d}.{prev_year:04d}"
print(current)
print(previous)
PY
  )"
  CURRENT_MONTH_TOKEN="$(printf '%s\n' "${VRDB_MONTH_TOKENS}" | sed -n '1p')"
  PREVIOUS_MONTH_TOKEN="$(printf '%s\n' "${VRDB_MONTH_TOKENS}" | sed -n '2p')"
  VRDB_URL_CANDIDATES=(
    "https://sos.wa.gov/_assets/elections/${CURRENT_MONTH_TOKEN}.WA.zip"
    "https://sos.wa.gov/_assets/elections/${PREVIOUS_MONTH_TOKEN}.WA.zip"
  )
fi

SELECTED_VRDB_URL=""
SELECTED_ZIP_ENTRY=""
for candidate_url in "${VRDB_URL_CANDIDATES[@]}"; do
  echo "Downloading VRDB archive: ${candidate_url}"
  if ! curl -fL --retry 3 --connect-timeout 20 --max-time 300 -o "${ZIP_PATH}" "${candidate_url}"; then
    echo "VRDB archive unavailable: ${candidate_url}"
    continue
  fi

  if ! unzip -tq "${ZIP_PATH}" >/dev/null 2>&1; then
    echo "Downloaded payload is not a valid zip archive: ${candidate_url}"
    continue
  fi

  CANDIDATE_ZIP_ENTRY="$(
    unzip -Z1 "${ZIP_PATH}" \
      | tr -d '\r' \
      | awk '/(^|\/)[0-9]{8}_VRDB_Extract\.txt$/ { print; exit }'
  )"
  if [[ -z "${CANDIDATE_ZIP_ENTRY}" ]]; then
    CANDIDATE_ZIP_ENTRY="$(
      unzip -Z1 "${ZIP_PATH}" \
        | tr -d '\r' \
        | awk '/(^|\/)VRDB_Extract\.txt$/ { print; exit }'
    )"
  fi
  if [[ -z "${CANDIDATE_ZIP_ENTRY}" ]]; then
    echo "No VRDB extract file found in archive: ${candidate_url}"
    continue
  fi

  SELECTED_VRDB_URL="${candidate_url}"
  SELECTED_ZIP_ENTRY="${CANDIDATE_ZIP_ENTRY}"
  break
done

if [[ -z "${SELECTED_VRDB_URL}" ]]; then
  echo "VRDB download failed for all candidate URLs:" >&2
  for candidate_url in "${VRDB_URL_CANDIDATES[@]}"; do
    echo "  - ${candidate_url}" >&2
  done
  exit 2
fi

ZIP_ENTRY="${SELECTED_ZIP_ENTRY}"

EXTRACT_BASENAME="$(basename "${ZIP_ENTRY}")"
unzip -j "${ZIP_PATH}" "${ZIP_ENTRY}" -d "${TMP_DIR}" >/dev/null

if [[ "${EXTRACT_BASENAME}" =~ ^[0-9]{8}_VRDB_Extract\.txt$ ]]; then
  TARGET_BASENAME="${EXTRACT_BASENAME}"
else
  TARGET_BASENAME="$(date '+%Y%m%d')_VRDB_Extract.txt"
fi

TARGET_PATH="${RAW_DATA_DIR}/${TARGET_BASENAME}"
if [[ -f "${TARGET_PATH}" ]] && [[ "${FORCE_DOWNLOAD}" != "1" ]]; then
  echo "Extract target already exists: ${TARGET_PATH}"
  echo "Skipping overwrite."
  exit 0
fi

mv -f "${TMP_DIR}/${EXTRACT_BASENAME}" "${TARGET_PATH}"
echo "VRDB extract ready: ${TARGET_PATH}"

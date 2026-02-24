#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/local/teardown_local_db.sh [--keep-data]

Options:
  --keep-data  Stop postgres but keep LOCAL_PGDATA directory.

Environment (or .env/.env.local):
  LOCAL_PGDATA=../output/postgres17-local
EOF
}

KEEP_DATA=0
if [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi
if [[ "${1:-}" == "--keep-data" ]]; then
  KEEP_DATA=1
fi
if [[ $# -gt 1 ]]; then
  usage >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"

# shellcheck disable=SC1091
source "${PROJECT_ROOT}/scripts/lib/env.sh"
load_project_env "${PROJECT_ROOT}"

log() {
  printf '[teardown] %s\n' "$*"
}

pgdata_path() {
  local configured="${LOCAL_PGDATA:-${REPO_ROOT}/output/postgres17-local}"
  if [[ "${configured}" = /* ]]; then
    printf '%s\n' "${configured}"
  else
    printf '%s\n' "${PROJECT_ROOT}/${configured}"
  fi
}

main() {
  local pgdata
  pgdata="$(pgdata_path)"

  "${PROJECT_ROOT}/scripts/local/postgres_local.sh" stop || true

  if [[ "${KEEP_DATA}" -eq 1 ]]; then
    log "Stopped local postgres. Keeping data directory: ${pgdata}"
    exit 0
  fi

  if [[ -d "${pgdata}" ]]; then
    rm -rf "${pgdata}"
    log "Removed local postgres data directory: ${pgdata}"
  else
    log "Data directory does not exist: ${pgdata}"
  fi
}

main

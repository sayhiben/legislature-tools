#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/local/postgres_local.sh <start|stop|restart|status>

Environment (or .env/.env.local):
  BREW_POSTGRES_FORMULA=postgresql@17
  LOCAL_PGDATA=../output/postgres17-local
  LOCAL_PGLOG=../output/postgres17-local/postgres.log
  DB_HOST=localhost
  DB_PORT=55432
EOF
}

if [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 1
fi

ACTION="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"

# shellcheck disable=SC1091
source "${PROJECT_ROOT}/scripts/lib/env.sh"
load_project_env "${PROJECT_ROOT}"

log() {
  printf '[postgres] %s\n' "$*"
}

die() {
  printf '[postgres][error] %s\n' "$*" >&2
  exit 1
}

maybe_add_brew_postgres_to_path() {
  if ! command -v brew >/dev/null 2>&1; then
    return 0
  fi
  local pg_formula="${BREW_POSTGRES_FORMULA:-postgresql@17}"
  local brew_prefix
  brew_prefix="$(brew --prefix)"
  export PATH="${brew_prefix}/opt/${pg_formula}/bin:${PATH}"
}

ensure_command() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    die "Required command not found: ${cmd}"
  fi
}

pgdata_path() {
  local configured="${LOCAL_PGDATA:-${REPO_ROOT}/output/postgres17-local}"
  if [[ "${configured}" = /* ]]; then
    printf '%s\n' "${configured}"
  else
    printf '%s\n' "${PROJECT_ROOT}/${configured}"
  fi
}

pglog_path() {
  local resolved_pgdata="$1"
  local configured="${LOCAL_PGLOG:-${resolved_pgdata}/postgres.log}"
  if [[ "${configured}" = /* ]]; then
    printf '%s\n' "${configured}"
  else
    printf '%s\n' "${PROJECT_ROOT}/${configured}"
  fi
}

ensure_initialized_cluster() {
  local pgdata="$1"
  if [[ ! -f "${pgdata}/PG_VERSION" ]]; then
    die "Local postgres cluster not initialized at ${pgdata}. Run: make setup-env"
  fi
}

is_running() {
  local pgdata="$1"
  pg_ctl -D "${pgdata}" status >/dev/null 2>&1
}

wait_until_ready() {
  local host="$1"
  local port="$2"
  local tries=30
  local waited=0
  until pg_isready -h "${host}" -p "${port}" >/dev/null 2>&1; do
    waited=$((waited + 1))
    if [[ "${waited}" -ge "${tries}" ]]; then
      die "Postgres did not become ready on ${host}:${port}."
    fi
    sleep 1
  done
}

start_pg() {
  local pgdata="$1"
  local pglog="$2"
  local host="$3"
  local port="$4"

  ensure_initialized_cluster "${pgdata}"
  mkdir -p "$(dirname "${pglog}")"
  if is_running "${pgdata}"; then
    log "Already running."
    return
  fi
  log "Starting local postgres using ${pgdata}"
  pg_ctl -D "${pgdata}" -l "${pglog}" start
  wait_until_ready "${host}" "${port}"
  log "Ready on ${host}:${port}"
}

stop_pg() {
  local pgdata="$1"
  if [[ ! -f "${pgdata}/PG_VERSION" ]]; then
    log "Cluster not initialized; nothing to stop."
    return
  fi
  if ! is_running "${pgdata}"; then
    log "Already stopped."
    return
  fi
  log "Stopping local postgres."
  pg_ctl -D "${pgdata}" stop -m fast
}

status_pg() {
  local pgdata="$1"
  local host="$2"
  local port="$3"
  if [[ ! -f "${pgdata}/PG_VERSION" ]]; then
    log "not-initialized (${pgdata})"
    return 1
  fi
  if is_running "${pgdata}"; then
    log "running (${host}:${port})"
    return 0
  fi
  log "stopped (${pgdata})"
  return 1
}

main() {
  maybe_add_brew_postgres_to_path
  ensure_command pg_ctl
  ensure_command pg_isready

  local db_host="${DB_HOST:-localhost}"
  local db_port="${DB_PORT:-55432}"
  local pgdata
  pgdata="$(pgdata_path)"
  local pglog
  pglog="$(pglog_path "${pgdata}")"

  case "${ACTION}" in
    start)
      start_pg "${pgdata}" "${pglog}" "${db_host}" "${db_port}"
      ;;
    stop)
      stop_pg "${pgdata}"
      ;;
    restart)
      stop_pg "${pgdata}"
      start_pg "${pgdata}" "${pglog}" "${db_host}" "${db_port}"
      ;;
    status)
      status_pg "${pgdata}" "${db_host}" "${db_port}"
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
}

main

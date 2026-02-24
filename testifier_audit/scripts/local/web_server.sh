#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/local/web_server.sh <start|stop|restart|status>

Environment (or .env/.env.local):
  LOCAL_WEB_HOST=127.0.0.1
  LOCAL_WEB_PORT=8774
  LOCAL_WEB_ROOT=../reports
  LOCAL_WEB_PID_FILE=../output/local-web-server.pid
  LOCAL_WEB_LOG_FILE=../output/local-web-server.log
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
  printf '[web] %s\n' "$*"
}

die() {
  printf '[web][error] %s\n' "$*" >&2
  exit 1
}

resolve_path() {
  local path="$1"
  if [[ "${path}" = /* ]]; then
    printf '%s\n' "${path}"
  else
    printf '%s\n' "${PROJECT_ROOT}/${path}"
  fi
}

python_cmd() {
  if command -v python3 >/dev/null 2>&1; then
    printf '%s\n' "python3"
    return
  fi
  if command -v python >/dev/null 2>&1; then
    printf '%s\n' "python"
    return
  fi
  die "Python interpreter not found."
}

read_pid() {
  local pid_file="$1"
  if [[ -f "${pid_file}" ]]; then
    tr -d '[:space:]' <"${pid_file}"
  fi
}

pid_running() {
  local pid="$1"
  [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

start_server() {
  local host="$1"
  local port="$2"
  local root="$3"
  local pid_file="$4"
  local log_file="$5"

  local existing_pid
  existing_pid="$(read_pid "${pid_file}")"
  if pid_running "${existing_pid}"; then
    log "Already running (pid=${existing_pid}, url=http://${host}:${port}/)."
    return
  fi

  mkdir -p "${root}"
  mkdir -p "$(dirname "${pid_file}")"
  mkdir -p "$(dirname "${log_file}")"

  local py
  py="$(python_cmd)"
  log "Starting server for ${root} at http://${host}:${port}/"
  nohup "${py}" -m http.server "${port}" --bind "${host}" --directory "${root}" \
    >"${log_file}" 2>&1 &
  local pid=$!
  printf '%s\n' "${pid}" >"${pid_file}"
  log "Started (pid=${pid}, log=${log_file})."
}

stop_server() {
  local pid_file="$1"

  local existing_pid
  existing_pid="$(read_pid "${pid_file}")"
  if ! pid_running "${existing_pid}"; then
    rm -f "${pid_file}"
    log "Already stopped."
    return
  fi

  log "Stopping server (pid=${existing_pid})."
  kill "${existing_pid}" >/dev/null 2>&1 || true
  for _ in $(seq 1 20); do
    if ! pid_running "${existing_pid}"; then
      break
    fi
    sleep 0.2
  done
  if pid_running "${existing_pid}"; then
    kill -9 "${existing_pid}" >/dev/null 2>&1 || true
  fi
  rm -f "${pid_file}"
}

status_server() {
  local host="$1"
  local port="$2"
  local pid_file="$3"

  local existing_pid
  existing_pid="$(read_pid "${pid_file}")"
  if pid_running "${existing_pid}"; then
    log "running (pid=${existing_pid}, url=http://${host}:${port}/)"
    return 0
  fi
  log "stopped"
  return 1
}

main() {
  local host="${LOCAL_WEB_HOST:-127.0.0.1}"
  local port="${LOCAL_WEB_PORT:-8774}"
  local root
  root="$(resolve_path "${LOCAL_WEB_ROOT:-${REPO_ROOT}/reports}")"
  local pid_file
  pid_file="$(resolve_path "${LOCAL_WEB_PID_FILE:-${REPO_ROOT}/output/local-web-server.pid}")"
  local log_file
  log_file="$(resolve_path "${LOCAL_WEB_LOG_FILE:-${REPO_ROOT}/output/local-web-server.log}")"

  case "${ACTION}" in
    start)
      start_server "${host}" "${port}" "${root}" "${pid_file}" "${log_file}"
      ;;
    stop)
      stop_server "${pid_file}"
      ;;
    restart)
      stop_server "${pid_file}"
      start_server "${host}" "${port}" "${root}" "${pid_file}" "${log_file}"
      ;;
    status)
      status_server "${host}" "${port}" "${pid_file}"
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
}

main

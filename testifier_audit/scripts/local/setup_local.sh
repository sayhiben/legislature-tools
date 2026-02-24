#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/local/setup_local.sh [options]

Options:
  --skip-deps            Skip Homebrew/system dependency installation.
  --skip-python          Skip Python virtualenv + pip install.
  --skip-db              Skip local PostgreSQL initialization/config.
  --skip-schema          Skip schema apply step.
  --skip-vrdb            Skip VRDB monthly download check.
  --force-vrdb-download  Force VRDB download even if current-month extract exists.
  -h, --help             Show this help text.

Environment overrides:
  TESTIFIER_AUDIT_ENV_FILE=.env        Optional primary dotenv path (relative to testifier_audit/).
  INSTALL_HOMEBREW=1                 Install Homebrew automatically when missing.
  BREW_PYTHON_FORMULA=python@3.12    Python formula to install via brew.
  BREW_POSTGRES_FORMULA=postgresql@17 PostgreSQL formula to install via brew.
  VENV_PATH=<path>                   Virtualenv path (default: testifier_audit/.venv).
  LOCAL_PGDATA=<path>                Local postgres data dir (default: output/postgres17-local).
  LOCAL_PGLOG=<path>                 Local postgres log path (default: <LOCAL_PGDATA>/postgres.log).
  LOCAL_PG_SUPERUSER=<name>          Superuser name for local cluster (default: current user).
  DB_HOST=localhost
  DB_PORT=55432
  DB_NAME=legislature
  DB_USER=legislature
  DB_PASSWORD=legislature
  EXPECTED_POSTGRES_MAJOR=17
  RAW_DATA_DIR=<path>                Raw data directory (default: ../data/raw).
  VRDB_URL=<url>                     Monthly VRDB zip URL.
  FORCE_VRDB_DOWNLOAD=1              Force VRDB download.

Notes:
  - macOS and Debian-family Linux are supported.
  - This script provisions a local Postgres instance on port 55432 to match docker-compose defaults.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"

# shellcheck disable=SC1091
source "${PROJECT_ROOT}/scripts/lib/env.sh"
load_project_env "${PROJECT_ROOT}"

SKIP_DEPS=0
SKIP_PYTHON=0
SKIP_DB=0
SKIP_SCHEMA=0
SKIP_VRDB=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-deps)
      SKIP_DEPS=1
      shift
      ;;
    --skip-python)
      SKIP_PYTHON=1
      shift
      ;;
    --skip-db)
      SKIP_DB=1
      shift
      ;;
    --skip-schema)
      SKIP_SCHEMA=1
      shift
      ;;
    --skip-vrdb)
      SKIP_VRDB=1
      shift
      ;;
    --force-vrdb-download)
      FORCE_VRDB_DOWNLOAD=1
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

log() {
  printf '[setup] %s\n' "$*"
}

warn() {
  printf '[setup][warn] %s\n' "$*" >&2
}

die() {
  printf '[setup][error] %s\n' "$*" >&2
  exit 1
}

ensure_command() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    die "Required command not found: ${cmd}"
  fi
}

escape_sql_literal() {
  printf "%s" "$1" | sed "s/'/''/g"
}

escape_sql_identifier() {
  printf "%s" "$1" | sed 's/"/""/g'
}

ensure_brew_in_path() {
  if command -v brew >/dev/null 2>&1; then
    return 0
  fi
  if [[ -x "/opt/homebrew/bin/brew" ]]; then
    eval "$(/opt/homebrew/bin/brew shellenv)"
    return 0
  fi
  if [[ -x "/usr/local/bin/brew" ]]; then
    eval "$(/usr/local/bin/brew shellenv)"
    return 0
  fi
  if [[ -x "/home/linuxbrew/.linuxbrew/bin/brew" ]]; then
    eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
    return 0
  fi
  return 1
}

install_homebrew_if_requested() {
  if ensure_brew_in_path; then
    return 0
  fi
  if [[ "${INSTALL_HOMEBREW:-0}" != "1" ]]; then
    die "Homebrew is required. Install from https://brew.sh/ or re-run with INSTALL_HOMEBREW=1."
  fi
  log "Installing Homebrew (INSTALL_HOMEBREW=1)"
  NONINTERACTIVE=1 /bin/bash -c \
    "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  ensure_brew_in_path || die "Homebrew installation completed but brew is still not on PATH."
}

install_system_deps() {
  local py_formula="${BREW_PYTHON_FORMULA:-python@3.12}"
  local pg_formula="${BREW_POSTGRES_FORMULA:-postgresql@17}"

  install_homebrew_if_requested
  ensure_command brew

  log "Installing Homebrew dependencies: ${py_formula}, ${pg_formula}, unzip, curl"
  brew install "${py_formula}" "${pg_formula}" unzip curl
}

ensure_python_version() {
  local python_bin="$1"
  "${python_bin}" - <<'PY'
import sys
if sys.version_info < (3, 11):
    raise SystemExit("Python 3.11+ is required")
PY
}

setup_python_env() {
  local py_formula="${BREW_PYTHON_FORMULA:-python@3.12}"
  local brew_prefix
  brew_prefix="$(brew --prefix)"
  export PATH="${brew_prefix}/opt/${py_formula}/bin:${PATH}"

  local python_bin=""
  if command -v python3.12 >/dev/null 2>&1; then
    python_bin="$(command -v python3.12)"
  elif command -v python3 >/dev/null 2>&1; then
    python_bin="$(command -v python3)"
  elif command -v python >/dev/null 2>&1; then
    python_bin="$(command -v python)"
  else
    die "No Python interpreter found after dependency installation."
  fi

  ensure_python_version "${python_bin}"

  local venv_path="${VENV_PATH:-${PROJECT_ROOT}/.venv}"
  if [[ ! -d "${venv_path}" ]]; then
    log "Creating virtualenv: ${venv_path}"
    "${python_bin}" -m venv "${venv_path}"
  else
    log "Virtualenv already exists: ${venv_path}"
  fi

  # shellcheck disable=SC1090
  source "${venv_path}/bin/activate"
  log "Installing Python package in editable dev mode"
  python -m pip install --upgrade pip setuptools wheel
  python -m pip install -e ".[dev]"
}

ensure_postgres_bin_path() {
  local pg_formula="${BREW_POSTGRES_FORMULA:-postgresql@17}"
  local brew_prefix
  brew_prefix="$(brew --prefix)"
  export PATH="${brew_prefix}/opt/${pg_formula}/bin:${PATH}"
}

init_local_postgres() {
  ensure_postgres_bin_path
  ensure_command initdb
  ensure_command pg_ctl
  ensure_command psql
  ensure_command pg_isready

  local db_port="${DB_PORT:-55432}"
  local db_host="${DB_HOST:-localhost}"
  local db_user="${DB_USER:-legislature}"
  local db_password="${DB_PASSWORD:-legislature}"
  local db_name="${DB_NAME:-legislature}"
  local pg_superuser="${LOCAL_PG_SUPERUSER:-${USER}}"
  local pgdata="${LOCAL_PGDATA:-${REPO_ROOT}/output/postgres17-local}"
  local pglog="${LOCAL_PGLOG:-${pgdata}/postgres.log}"
  local expected_major="${EXPECTED_POSTGRES_MAJOR:-17}"

  mkdir -p "${pgdata}"
  mkdir -p "$(dirname "${pglog}")"

  if [[ ! -f "${pgdata}/PG_VERSION" ]]; then
    log "Initializing local PostgreSQL cluster at ${pgdata}"
    initdb \
      --username="${pg_superuser}" \
      --auth-local=trust \
      --auth-host=scram-sha-256 \
      -D "${pgdata}"
    {
      echo ""
      echo "listen_addresses = 'localhost'"
      echo "port = ${db_port}"
    } >>"${pgdata}/postgresql.conf"
  else
    log "Using existing local PostgreSQL cluster: ${pgdata}"
  fi

  if pg_ctl -D "${pgdata}" status >/dev/null 2>&1; then
    log "Local PostgreSQL already running."
  else
    log "Starting local PostgreSQL on port ${db_port}"
    pg_ctl -D "${pgdata}" -l "${pglog}" start
  fi

  local wait_tries=30
  local waited=0
  until pg_isready -h "${db_host}" -p "${db_port}" >/dev/null 2>&1; do
    waited=$((waited + 1))
    if [[ "${waited}" -ge "${wait_tries}" ]]; then
      die "PostgreSQL did not become ready on ${db_host}:${db_port}. Log: ${pglog}"
    fi
    sleep 1
  done

  local db_url="postgresql://${db_user}:${db_password}@${db_host}:${db_port}/${db_name}"
  local escaped_user escaped_password escaped_db_name
  escaped_user="$(escape_sql_identifier "${db_user}")"
  escaped_password="$(escape_sql_literal "${db_password}")"
  escaped_db_name="$(escape_sql_literal "${db_name}")"

  if [[ "$(
    psql -p "${db_port}" -U "${pg_superuser}" -d postgres -tAc \
      "SELECT 1 FROM pg_roles WHERE rolname = '$(escape_sql_literal "${db_user}")';"
  )" != "1" ]]; then
    log "Creating DB role: ${db_user}"
    psql -p "${db_port}" -U "${pg_superuser}" -d postgres -v ON_ERROR_STOP=1 \
      -c "CREATE ROLE \"${escaped_user}\" LOGIN PASSWORD '${escaped_password}';"
  else
    log "Updating password for existing DB role: ${db_user}"
    psql -p "${db_port}" -U "${pg_superuser}" -d postgres -v ON_ERROR_STOP=1 \
      -c "ALTER ROLE \"${escaped_user}\" LOGIN PASSWORD '${escaped_password}';"
  fi

  if [[ "$(
    psql -p "${db_port}" -U "${pg_superuser}" -d postgres -tAc \
      "SELECT 1 FROM pg_database WHERE datname = '${escaped_db_name}';"
  )" != "1" ]]; then
    log "Creating database: ${db_name}"
    psql -p "${db_port}" -U "${pg_superuser}" -d postgres -v ON_ERROR_STOP=1 \
      -c "CREATE DATABASE \"${db_name}\" OWNER \"${escaped_user}\";"
  else
    log "Database already exists: ${db_name}"
  fi

  local server_version_num
  server_version_num="$(
    psql "${db_url}" -v ON_ERROR_STOP=1 -tAc "SHOW server_version_num;" | tr -d '[:space:]'
  )"
  if [[ -z "${server_version_num}" ]]; then
    die "Could not determine PostgreSQL server version."
  fi
  local server_major=$((server_version_num / 10000))
  if [[ "${server_major}" -ne "${expected_major}" ]]; then
    die "PostgreSQL major version mismatch. Expected ${expected_major}, got ${server_major}."
  fi
  log "PostgreSQL compatibility check passed (major=${server_major})."

  export TESTIFIER_AUDIT_DB_URL="${db_url}"
  export DATABASE_URL="${db_url}"
}

apply_schema() {
  local db_url="${TESTIFIER_AUDIT_DB_URL:-${DATABASE_URL:-}}"
  if [[ -z "${db_url}" ]]; then
    die "Database URL is not set; cannot apply schema."
  fi
  "${PROJECT_ROOT}/scripts/db/apply_schema.sh" "${db_url}"
}

download_vrdb_if_needed() {
  if [[ "${FORCE_VRDB_DOWNLOAD:-0}" == "1" ]]; then
    FORCE_VRDB_DOWNLOAD=1 "${PROJECT_ROOT}/scripts/vrdb/download_latest_vrdb.sh" || return 1
    return 0
  fi
  "${PROJECT_ROOT}/scripts/vrdb/download_latest_vrdb.sh" || return 1
}

main() {
  cd "${PROJECT_ROOT}"

  if [[ "${SKIP_DEPS}" -eq 0 ]]; then
    install_system_deps
  else
    log "Skipping dependency installation (--skip-deps)."
    install_homebrew_if_requested
  fi

  ensure_brew_in_path || die "Homebrew is required for this local setup workflow."

  if [[ "${SKIP_PYTHON}" -eq 0 ]]; then
    setup_python_env
  else
    log "Skipping Python environment setup (--skip-python)."
  fi

  if [[ "${SKIP_DB}" -eq 0 ]]; then
    init_local_postgres
  else
    log "Skipping PostgreSQL setup (--skip-db)."
  fi

  if [[ "${SKIP_SCHEMA}" -eq 0 ]]; then
    apply_schema
  else
    log "Skipping schema apply (--skip-schema)."
  fi

  if [[ "${SKIP_VRDB}" -eq 0 ]]; then
    if ! download_vrdb_if_needed; then
      warn "VRDB download step failed. Continue with existing local data or set VRDB_URL to a fresh archive URL."
    fi
  else
    log "Skipping VRDB fetch (--skip-vrdb)."
  fi

  local db_url="${TESTIFIER_AUDIT_DB_URL:-postgresql://legislature:legislature@localhost:55432/legislature}"
  log "Local setup complete."
  cat <<EOF

Next shell exports (for local non-Docker runs):
  export TESTIFIER_AUDIT_DB_URL="${db_url}"
  export DATABASE_URL="${db_url}"
  export TESTIFIER_AUDIT_SKIP_DOCKER_POSTGRES=1

Then run:
  ./scripts/report/run_unified_report.sh <submissions_csv> <vrdb_extract> [hearing_metadata]
EOF
}

main "$@"

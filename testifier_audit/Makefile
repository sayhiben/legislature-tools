.PHONY: \
	help install lint test run-help docker-build docker-help \
	setup-env teardown-db reset-db \
	start stop restart status \
	start-postgres stop-postgres restart-postgres status-postgres \
	start-web stop-web restart-web status-web \
	rerender-all-reports

help:
	@echo "Available targets:"
	@echo "  help                  Show this help."
	@echo "  install               Install project + dev dependencies."
	@echo "  lint                  Run lint checks."
	@echo "  test                  Run test suite."
	@echo "  run-help              Show CLI help."
	@echo "  docker-build          Build docker image."
	@echo "  docker-help           Show CLI help from docker image."
	@echo "  setup-env             Setup local environment."
	@echo "  teardown-db           Teardown local database."
	@echo "  reset-db              Recreate local database."
	@echo "  start                 Start postgres + web."
	@echo "  stop                  Stop web + postgres."
	@echo "  restart               Restart web + postgres."
	@echo "  status                Show web + postgres status."
	@echo "  start-postgres        Start local postgres."
	@echo "  stop-postgres         Stop local postgres."
	@echo "  restart-postgres      Restart local postgres."
	@echo "  status-postgres       Show local postgres status."
	@echo "  start-web             Start local web server."
	@echo "  stop-web              Stop local web server."
	@echo "  restart-web           Restart local web server."
	@echo "  status-web            Show local web server status."
	@echo "  rerender-all-reports  Rerender all existing reports."

install:
	python -m pip install -e .[dev]

lint:
	./scripts/ci/lint.sh

test:
	./scripts/ci/test.sh

run-help:
	testifier-audit --help

docker-build:
	docker build -t testifier-audit .

docker-help:
	docker run --rm testifier-audit --help

setup-env:
	./scripts/local/setup_local.sh

teardown-db:
	./scripts/local/teardown_local_db.sh

reset-db:
	./scripts/local/teardown_local_db.sh
	./scripts/local/setup_local.sh --skip-deps --skip-python --skip-vrdb

start: start-postgres start-web

stop: stop-web stop-postgres

restart: stop start

status: status-postgres status-web

start-postgres:
	./scripts/local/postgres_local.sh start

stop-postgres:
	./scripts/local/postgres_local.sh stop

restart-postgres:
	./scripts/local/postgres_local.sh restart

status-postgres:
	@./scripts/local/postgres_local.sh status || true

start-web:
	./scripts/local/web_server.sh start

stop-web:
	./scripts/local/web_server.sh stop

restart-web:
	./scripts/local/web_server.sh restart

status-web:
	@./scripts/local/web_server.sh status || true

rerender-all-reports:
	./scripts/report/rerender_all_reports.sh

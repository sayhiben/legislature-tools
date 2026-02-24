.PHONY: \
	install lint test run-help docker-build docker-help \
	setup-env teardown-db reset-db \
	start stop restart status \
	start-postgres stop-postgres restart-postgres status-postgres \
	start-web stop-web restart-web status-web

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

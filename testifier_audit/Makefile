.PHONY: install lint test run-help docker-build docker-help

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

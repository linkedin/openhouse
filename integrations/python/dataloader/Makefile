.PHONY: help sync clean lint format format-check typecheck check test verify build package-check build-itest integration-tests

help:
	@echo "Available commands:"
	@echo "  make sync          - Sync dependencies using uv"
	@echo "  make lint          - Run ruff linter"
	@echo "  make format        - Format code with ruff"
	@echo "  make typecheck     - Run mypy type checker"
	@echo "  make check         - Run all checks (lint + format check + typecheck)"
	@echo "  make test          - Run tests with pytest"
	@echo "  make verify        - Run all checks and tests"
	@echo "  make build         - Build package distributions"
	@echo "  make package-check - Validate built distributions with twine"
	@echo "  make clean         - Clean build artifacts"

sync:
	uv sync --all-extras

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

format-check:
	uv run ruff format --check src/ tests/

typecheck:
	uv run mypy src/

check: lint format-check typecheck

test:
	uv run pytest

verify: check test

build:
	uv build

package-check:
	uv run twine check dist/*

ITEST_IMAGE ?= oh-dataloader-itest
DOCKER_NETWORK ?= oh-hadoop-spark_default
ITEST_PYZ = tests/itest.pyz

build-itest: $(ITEST_PYZ)

$(ITEST_PYZ): pyproject.toml src/ tests/shiv_preamble.py
	pip install --quiet shiv
	SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 shiv --compile --preamble tests/shiv_preamble.py -p '/usr/bin/env python3' -o $(ITEST_PYZ) ".[dev]"

integration-tests: $(ITEST_PYZ)
	docker build --platform linux/amd64 -t $(ITEST_IMAGE) -f tests/Dockerfile .
	docker run --rm --platform linux/amd64 --network $(DOCKER_NETWORK) \
		-e OH_TOKEN="$$(cat $(TOKEN_FILE))" \
		$(ITEST_IMAGE) python3 itest.pyz

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .venv/
	rm -f $(ITEST_PYZ)
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

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
ITEST_SITE = build/itest-site-packages

build-itest: $(ITEST_SITE)

$(ITEST_SITE): pyproject.toml uv.lock src/
	rm -rf $(ITEST_SITE)
	SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 uv pip install --target $(ITEST_SITE) --python-platform x86_64-unknown-linux-gnu --python-version 3.10 ".[dev]"

integration-tests: $(ITEST_SITE)
	docker build --platform linux/amd64 -t $(ITEST_IMAGE) -f tests/Dockerfile .
	docker run --rm --platform linux/amd64 --network $(DOCKER_NETWORK) \
		-e OH_TOKEN="$$(cat $(TOKEN_FILE))" \
		$(ITEST_IMAGE) python3 tests/integration_tests.py

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .venv/
	rm -rf $(ITEST_SITE)
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

.PHONY: help sync clean lint format format-check typecheck check test verify build package-check integration-tests

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

integration-tests:
	uv run python tests/integration_tests.py $(TOKEN_FILE)

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .venv/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

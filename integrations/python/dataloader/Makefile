.PHONY: help sync clean lint format format-check check test verify build package-check

help:
	@echo "Available commands:"
	@echo "  make sync          - Sync dependencies using uv"
	@echo "  make lint          - Run ruff linter"
	@echo "  make format        - Format code with ruff"
	@echo "  make check         - Run all checks (lint + format check)"
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

check: lint format-check

test:
	uv run pytest

verify: check test

build:
	uv build

package-check:
	uv run twine check dist/*

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .venv/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

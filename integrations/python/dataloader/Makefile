.PHONY: help sync clean

help:
	@echo "Available commands:"
	@echo "  make sync   - Sync dependencies using uv"
	@echo "  make clean  - Clean build artifacts"

sync:
	uv sync

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .venv/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

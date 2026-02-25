# Claude Instructions for OpenHouse DataLoader

## Project Overview

Python library for distributed data loading of OpenHouse tables. Uses DataFusion for query execution and PyIceberg for table access.

## Common Commands

```bash
make sync      # Install dependencies
make check     # Run lint + format checks
make test      # Run tests
make verify    # Run all checks and tests
make format    # Auto-format code
make build          # Build package distributions
make package-check  # Validate built distributions with twine
make clean          # Clean build artifacts
make integration-tests TOKEN_FILE=<path>  # Run integration tests against Docker OpenHouse
```

## Workflows
When making a change run `make verify` to ensure all tests and checks pass

## Integration Tests

Integration tests run against an OpenHouse instance in Docker. To run them:

1. Start the Docker services from the repo root:
   ```bash
   docker compose -f infra/recipes/docker-compose/oh-only/docker-compose.yml up -d
   ```
2. Run the tests with the dummy token (uses `DummyTokenInterceptor`, no real auth needed):
   ```bash
   make integration-tests TOKEN_FILE=../../../tables-test-fixtures/tables-test-fixtures-iceberg-1.2/src/main/resources/dummy.token
   ```

## Project Structure

```
src/openhouse/dataloader/
├── __init__.py              # Public API exports
├── data_loader.py           # Main API: OpenHouseDataLoader
├── data_loader_splits.py    # DataLoaderSplits (iterable of splits)
├── data_loader_split.py     # DataLoaderSplit (single callable split)
├── table_identifier.py      # TableIdentifier dataclass
├── table_transformer.py     # TableTransformer ABC (internal)
└── udf_registry.py          # UDFRegistry ABC (internal)
```

## Public API

Only these are exported in `__init__.py`:
- `OpenHouseDataLoader` - Main entry point
- `TableIdentifier` - Table reference (database, table, branch)

Internal modules (TableTransformer, UDFRegistry) can be imported directly if needed but expose DataFusion types.

## Code Style

- Uses `ruff` for linting and formatting
- Line length: 120
- Python 3.12+
- Use modern type hints (`list`, `dict`, `X | None` instead of `List`, `Dict`, `Optional`)
- Use `raise NotImplementedError` for unimplemented methods in concrete classes
- Use `pass` for abstract methods decorated with `@abstractmethod`

## Versioning

- Version is derived from git tags via `hatch-vcs` (no hardcoded version in `pyproject.toml`)
- `__version__` in `__init__.py` reads from package metadata at runtime
- CI sets `SETUPTOOLS_SCM_PRETEND_VERSION` to inject the monorepo semVer tag at build time
- For local builds, use `SETUPTOOLS_SCM_PRETEND_VERSION=x.y.z make build` to override

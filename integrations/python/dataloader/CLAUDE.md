# Claude Instructions for OpenHouse DataLoader

## Project Overview

Python library for distributed data loading of OpenHouse tables. Uses PyIceberg for Iceberg table access, DataFusion for query execution and SQL transforms, and SQLGlot for SQL transpilation across dialects.

## Common Commands

```bash
make sync           # Install dependencies (uses uv)
make check          # Run lint + format checks + typecheck (mypy)
make test           # Run unit tests (pytest)
make verify         # Run all checks and tests
make format         # Auto-format code (ruff)
make build          # Build package distributions
make package-check  # Validate built distributions with twine
make clean          # Clean build artifacts
```

## Workflows

When validating a change, always run both:

1. `make verify` — lint, format checks, typecheck, and unit tests
2. Integration tests against Docker OpenHouse — start the Docker services, then run `make integration-tests`. These test the dataloader end-to-end against a real OpenHouse instance and must pass before a change is considered correct.

Run `make format` before pushing to avoid CI formatting failures.

```bash
# From the repo root, start Docker services (once per session):
docker compose -f infra/recipes/docker-compose/oh-hadoop-spark/docker-compose.yml up -d

# From the dataloader directory:
make format
make verify
make integration-tests TOKEN_FILE=../../../tables-test-fixtures/tables-test-fixtures-iceberg-1.2/src/main/resources/dummy.token
```

## Integration Tests

Integration tests run inside a Docker container on the same network as the oh-hadoop-spark services. The `make integration-tests` target builds a test image and runs it automatically. Tables are created and populated via Spark SQL submitted through Livy.

1. Start the Docker services from the repo root:
   ```bash
   docker compose -f infra/recipes/docker-compose/oh-hadoop-spark/docker-compose.yml up -d
   ```
2. Wait for all services to be healthy (especially Livy and namenode), then run:
   ```bash
   make integration-tests TOKEN_FILE=../../../tables-test-fixtures/tables-test-fixtures-iceberg-1.2/src/main/resources/dummy.token
   ```

## Project Structure

```
src/openhouse/dataloader/
├── __init__.py              # Public API exports
├── data_loader.py           # Main API: OpenHouseDataLoader, DataLoaderContext
├── data_loader_split.py     # DataLoaderSplit (single callable split, yields RecordBatches)
├── catalog.py               # OpenHouseCatalog (REST-based PyIceberg Catalog impl)
├── filters.py               # Filter expression DSL (col(), always_true(), combinators)
├── datafusion_sql.py        # SQLGlot DataFusion dialect + SQL transpilation
├── table_identifier.py      # TableIdentifier dataclass
├── table_transformer.py     # TableTransformer ABC (SQL-based transforms with dialect)
├── udf_registry.py          # UDFRegistry ABC + NoOpRegistry default
├── _table_scan_context.py   # TableScanContext (shared scan state, pickle-safe)
└── _timer.py                # log_duration context manager
```

## Public API

Exported in `__init__.py`:
- `OpenHouseDataLoader` — Main entry point; accepts a `Catalog`, table coordinates, optional filters/columns/context
- `DataLoaderContext` — Execution context with optional `TableTransformer` and `UDFRegistry`
- `OpenHouseCatalog` — REST catalog that loads tables via the OpenHouse Tables Service API
- `OpenHouseCatalogError` — Error raised when catalog fails to load a table
- `col()` — Column reference for building filter expressions
- `always_true()` — Filter that matches all rows

### Filter DSL (`filters.py`)

Build row filters using `col()` with comparison operators (`==`, `!=`, `>`, `>=`, `<`, `<=`) and predicates (`is_null()`, `is_not_null()`, `is_nan()`, `is_not_nan()`, `is_in()`, `is_not_in()`, `starts_with()`, `not_starts_with()`, `between()`). Combine with `&` (AND), `|` (OR), `~` (NOT). Filters are converted to PyIceberg expressions internally for partition pruning and file-level filtering.

### Internal modules (not in `__init__.py`)

- `TableTransformer` — ABC for SQL-based table transforms; subclass must provide a `dialect` (e.g. `"spark"`) and implement `transform()` returning SQL or `None`
- `UDFRegistry` / `NoOpRegistry` — ABC for registering DataFusion UDFs; `NoOpRegistry` is the default no-op
- `TableScanContext` — Frozen dataclass holding table metadata, FileIO, projected schema, row filter, and table ID; pickle-safe for distributed execution
- `DataFusion` dialect in `datafusion_sql.py` — Custom SQLGlot dialect for transpiling SQL from other dialects (e.g. Spark) to DataFusion
- `to_datafusion_sql()` — Transpiles a SQL statement from a source dialect to DataFusion using SQLGlot

## Key Dependencies

- `pyiceberg ~= 0.11.0` — Iceberg table access, metadata, scan planning
- `datafusion == 51.0.0` — Query execution engine for transforms
- `sqlglot >= 29.0.0` — SQL transpilation (custom DataFusion dialect)
- `requests >= 2.31.0` — HTTP client for OpenHouseCatalog
- `tenacity >= 8.0.0` — Retry logic for transient failures

## Code Style

- Uses `ruff` for linting and formatting, `mypy` for type checking
- Line length: 120
- Python 3.12+
- Use modern type hints (`list`, `dict`, `X | None` instead of `List`, `Dict`, `Optional`)
- Use `pass` for abstract methods decorated with `@abstractmethod`
- Use `raise NotImplementedError` for unsupported operations in concrete classes
- Ruff lint rules: E, F, I, UP, B, SIM

## Versioning

- Version is derived from git tags via `hatch-vcs` (no hardcoded version in `pyproject.toml`)
- `__version__` in `__init__.py` reads from package metadata at runtime
- CI sets `SETUPTOOLS_SCM_PRETEND_VERSION` to inject the monorepo semVer tag at build time
- For local builds, use `SETUPTOOLS_SCM_PRETEND_VERSION=x.y.z make build` to override

## Architecture Notes

- **OpenHouseDataLoader** iterates over `DataLoaderSplit` objects (one per Iceberg file scan task). Each split is independently callable and pickle-safe for distributed frameworks. It supports context manager usage (`with OpenHouseDataLoader(...) as loader:`) to ensure the underlying catalog is closed on exit.
- **Transforms** are applied per-split at read time: the `TableTransformer` produces SQL in its native dialect, which is transpiled to DataFusion SQL once, then executed against each RecordBatch via a DataFusion `SessionContext`.
- **Retry logic** in `data_loader.py` retries `OSError` (network/storage I/O) and HTTP 5xx errors with exponential backoff. Non-transient `HTTPError` (4xx) is not retried.
- **OpenHouseCatalog** is read-only; all write operations raise `NotImplementedError`. It supports context manager usage (`with OpenHouseCatalog(...) as cat:`).
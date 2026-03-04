# OpenHouse DataLoader

A Python library for distributed data loading of OpenHouse tables

## Quickstart

```python
from openhouse.dataloader import OpenHouseDataLoader

loader = OpenHouseDataLoader("my_database", "my_table")

# Access table metadata
loader.table_properties  # table properties as a dict
loader.snapshot_id        # snapshot ID of the loaded table

for split in loader:
    # Get table properties (also available per-split)
    split.table_properties

    # Load data
    for batch in split:
        process(batch)
```

## Filters

Use `col()` to build filter expressions for row filtering:

```python
from openhouse.dataloader import OpenHouseDataLoader, col

# Simple comparison
loader = OpenHouseDataLoader("my_database", "my_table", filters=col("age") > 21)

# Combine filters with & (and), | (or), ~ (not)
filters = (col("age") > 21) & (col("country") == "US")
loader = OpenHouseDataLoader("my_database", "my_table", filters=filters)

# Null checks
filters = col("email").is_not_null()

# Set membership
filters = col("status").is_in(["active", "pending"])

# String prefix matching
filters = col("name").starts_with("John")

# Range filter (inclusive)
filters = col("score").between(0.5, 1.0)

# Complex composition
filters = (col("age") >= 18) & (col("country").is_in(["US", "CA"])) & ~col("email").is_null()
```

## Development

```bash
# Set up environment
make sync

# Run all checks and unit tests
make verify

# See all available commands
make help
```

### Integration Tests

Integration tests run the data loader end to end against an instance of OpenHouse running in Docker.

**Prerequisites:** Docker and a Gradle build of OpenHouse.

```bash
# Create docker OpenHouse instance from repo root
./gradlew clean build
docker compose -f infra/recipes/docker-compose/oh-only/docker-compose.yml up -d --build

# Run integration tests
make -C integrations/python/dataloader sync
make -C integrations/python/dataloader integration-tests TOKEN_FILE=../../../tables-test-fixtures/tables-test-fixtures-iceberg-1.2/src/main/resources/dummy.token

# Stop docker
docker compose -f infra/recipes/docker-compose/oh-only/docker-compose.yml down
```
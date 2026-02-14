# OpenHouse DataLoader

A Python library for distributed data loading of OpenHouse tables

## Quickstart

```python
from openhouse.dataloader import OpenHouseDataLoader

loader = OpenHouseDataLoader("my_database", "my_table")

for split in loader:
    # Get table properties
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

# Run tests
make test

# See all available commands
make help
```
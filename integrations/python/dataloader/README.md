# OpenHouse DataLoader

A Python library for distributed data loading of OpenHouse tables

## Quickstart

```python
from openhouse.dataloader import OpenHouseDataLoader, TableIdentifier


table_properties, splits = OpenHouseDataLoader().create_splits(
    table=TableIdentifier("db", "tbl"),
    columns={"colA", "colB"},
)

# Loading can be distributed compute engines like Ray
for split in splits:
    data_batch = split()
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
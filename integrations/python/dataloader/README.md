# OpenHouse DataLoader

A Python library for distributed data loading of OpenHouse tables

## Quickstart

```python
from openhouse.dataloader import OpenHouseDataLoader, TableIdentifier


splits = OpenHouseDataLoader().create_splits(
    table=TableIdentifier("db", "tbl"),
    columns=["colA", "colB"],  # omit for SELECT *
)

# Loading can be distributed to compute engines like Ray
for split in splits:
    print(split.table_properties)  # available on every split
    for batch in split:
        process(batch)
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
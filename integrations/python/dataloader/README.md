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

## Development

```bash
# Set up environment
make sync

# Run tests
make test

# See all available commands
make help
```
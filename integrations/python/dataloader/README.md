# OpenHouse DataLoader

A Python library for distributed data loading of OpenHouse tables

## Features

TODO

## Quick Start

TODO

## Installation

TODO

## Developer Environment

### Prerequisites

1. Python 3.8 or higher
2. uv

### Setup Steps

1. **Navigate to the dataloader directory:**
   ```bash
   cd integrations/python/dataloader
   ```

2. **Sync dependencies and create virtual environment:**
   ```bash
   make sync
   ```

   This will:
   - Create a `.venv/` directory with a virtual environment
   - Install all project dependencies
   - Set up the project in development mode

3. **Activate the virtual environment** (optional but recommended):
   ```bash
   source .venv/bin/activate
   ```

### Running Tests

To run the test suite:

```bash
# With virtual environment activated
pytest

# Or using uv directly
uv run pytest

# Run with coverage
uv run pytest --cov=src/openhouse/dataloader --cov-report=html
```
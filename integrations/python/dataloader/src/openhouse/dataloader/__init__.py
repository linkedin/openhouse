from importlib.metadata import version

from openhouse.dataloader.data_loader import DataLoaderContext, OpenHouseDataLoader
from openhouse.dataloader.filters import always_true, col
from openhouse.dataloader.table_catalog import OpenHouseCatalog

__version__ = version("openhouse.dataloader")
__all__ = [
    "OpenHouseDataLoader",
    "DataLoaderContext",
    "OpenHouseCatalog",
    "always_true",
    "col",
]

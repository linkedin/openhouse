from importlib.metadata import version

from openhouse.dataloader.data_loader import DataLoaderContext, OpenHouseDataLoader
from openhouse.dataloader.filters import always_true, col
from openhouse.dataloader.openhouse_table_catalog import OpenHouseTableCatalog, TableCatalog

__version__ = version("openhouse.dataloader")
__all__ = [
    "OpenHouseDataLoader",
    "DataLoaderContext",
    "OpenHouseTableCatalog",
    "TableCatalog",
    "always_true",
    "col",
]

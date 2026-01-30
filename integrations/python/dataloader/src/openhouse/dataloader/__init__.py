from importlib.metadata import version

from openhouse.dataloader.data_loader import OpenHouseDataLoader
from openhouse.dataloader.table_identifier import TableIdentifier

__version__ = version("openhouse-dataloader")
__all__ = ["OpenHouseDataLoader", "TableIdentifier"]

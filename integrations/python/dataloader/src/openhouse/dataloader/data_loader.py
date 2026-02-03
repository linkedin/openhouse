from collections.abc import Mapping, Sequence

from openhouse.dataloader.data_loader_splits import DataLoaderSplits
from openhouse.dataloader.table_identifier import TableIdentifier
from openhouse.dataloader.table_transformer import TableTransformer
from openhouse.dataloader.udf_registry import UDFRegistry


class OpenHouseDataLoader:
    """An API for distributed data loading of OpenHouse tables"""

    def __init__(
        self,
        table_transformer: TableTransformer = None,
        udf_registry: UDFRegistry = None,
    ):
        """
        Args:
            table_transformer: A prerequisite transformation to apply to the table before loading the data
            udf_registry: UDFs required for the prerequisite table transformation
        """
        self._table_transformer = table_transformer
        self._udf_registry = udf_registry

    def create_splits(
        self,
        table: TableIdentifier,
        columns: Sequence[str],
        context: Mapping[str, str],
    ) -> tuple[dict[str, str], DataLoaderSplits]:
        """Create data splits for distributed data loading of the table

        Args:
            table: Identifier for the table to load
            columns: List of column names to load from the table
            context: Dictionary of context information (e.g. tenant, environment, etc.)

        Returns:
            A tuple of (table_properties, data splits) where data splits is an iterable of callable splits
        """
        raise NotImplementedError

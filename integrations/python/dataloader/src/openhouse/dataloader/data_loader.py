from collections.abc import Iterable, Mapping, Sequence

from openhouse.dataloader.data_loader_split import DataLoaderSplit
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
        columns: Sequence[str] | None = None,
        context: Mapping[str, str] | None = None,
    ) -> Iterable[DataLoaderSplit]:
        """Create data splits for distributed data loading of the table

        Args:
            table: Identifier for the table to load
            columns: Column names to load, or None to load all columns (SELECT *)
            context: Dictionary of context information (e.g. tenant, environment, etc.)

        Returns:
            Iterable of DataLoaderSplit, each containing table_properties
        """
        raise NotImplementedError

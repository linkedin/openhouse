from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from openhouse.dataloader.data_loader_split import DataLoaderSplit
from openhouse.dataloader.table_identifier import TableIdentifier
from openhouse.dataloader.table_transformer import TableTransformer
from openhouse.dataloader.udf_registry import UDFRegistry


@dataclass
class DataLoaderContext:
    """Context and customization for the DataLoader.

    Provides execution context (e.g. tenant, environment) and optional customizations
    like table transformations applied before loading data.

    Args:
        execution_context: Dictionary of execution context information (e.g. tenant, environment)
        table_transformer: Transformation to apply to the table before loading (e.g. column masking)
        udf_registry: UDFs required for the table transformation
    """

    execution_context: Mapping[str, str] | None = None
    table_transformer: TableTransformer | None = None
    udf_registry: UDFRegistry | None = None


class OpenHouseDataLoader:
    """An API for distributed data loading of OpenHouse tables"""

    def __init__(
        self,
        database: str,
        table: str,
        branch: str | None = None,
        columns: Sequence[str] | None = None,
        context: DataLoaderContext | None = None,
    ):
        """
        Args:
            database: Database name
            table: Table name
            branch: Optional branch name
            columns: Column names to load, or None to load all columns
            context: Data loader context
        """
        self._table = TableIdentifier(database, table, branch)
        self._columns = columns
        self._context = context or DataLoaderContext()

    def __iter__(self) -> Iterable[DataLoaderSplit]:
        """Iterate over data splits for distributed data loading of the table.

        Returns:
            Iterable of DataLoaderSplit, each containing table_properties
        """
        raise NotImplementedError

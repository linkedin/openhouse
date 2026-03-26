from abc import ABC, abstractmethod
from collections.abc import Mapping

from openhouse.dataloader.table_identifier import TableIdentifier


class TableTransformer(ABC):
    """Applies transformation logic to the base table that is being loaded.

    Implementations must preserve the original column names in their output
    SQL.  Columns may be transformed (e.g. masked or derived), but the output
    column names must match the original table schema so that user-supplied
    projections and filters can reference them.

    Args:
        dialect: The SQL dialect used by ``transform()`` (e.g. ``"spark"``).
    """

    def __init__(self, dialect: str) -> None:
        self.dialect: str = dialect

    @abstractmethod
    def transform(self, table: TableIdentifier, context: Mapping[str, str]) -> str | None:
        """Builds a SQL string representing the transformation to apply.

        The returned SQL must be a SELECT statement that reads from the table
        identified by *table*.  Output column names must match the original
        table schema.

        Args:
            table: Identifier for the table
            context: Dictionary of context information (e.g. tenant, environment, etc.)

        Returns:
            A SQL string, or None if no transformation is needed.
        """
        pass

from abc import ABC, abstractmethod
from collections.abc import Mapping

from openhouse.dataloader.table_identifier import TableIdentifier


class TableTransformer(ABC):
    """Applies transformation logic to the base table that is being loaded.

    Args:
        dialect: The SQL dialect used by ``transform()`` (e.g. ``"spark"``).
    """

    def __init__(self, dialect: str) -> None:
        self.dialect: str = dialect

    @abstractmethod
    def transform(self, table: TableIdentifier, context: Mapping[str, str]) -> str | None:
        """Builds a SQL string representing the transformation to apply.

        Args:
            table: Identifier for the table
            context: Dictionary of context information (e.g. tenant, environment, etc.)

        Returns:
            A SQL string, or None if no transformation is needed.
        """
        pass

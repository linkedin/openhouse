from abc import ABC, abstractmethod
from collections.abc import Mapping

from openhouse.dataloader.table_identifier import TableIdentifier


class TableTransformer(ABC):
    """Interface for applying additional transformation logic to the data
    being loaded (e.g. column masking, row filtering)
    """

    @abstractmethod
    def transform(self, table: TableIdentifier, context: Mapping[str, str]) -> str | None:
        """Builds a SQL string representing the transformation to apply.

        Called once to extract the SQL.  The SQL is then executed per batch in
        each split against a DataFusion session where the batch is registered
        under ``table.sql_name``.

        The decision to return a SQL string or ``None`` **must not** depend on
        row data — it should be based solely on the table identifier and context.

        Args:
            table: Identifier for the table
            context: Dictionary of context information (e.g. tenant, environment, etc.)

        Returns:
            A SQL string to execute against each batch, or None if no transformation is needed.
        """
        pass

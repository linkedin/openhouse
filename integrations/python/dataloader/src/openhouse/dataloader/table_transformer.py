from abc import ABC, abstractmethod
from collections.abc import Mapping

from openhouse.dataloader.table_identifier import TableIdentifier


class TableTransformer(ABC):
    """Interface for applying additional transformation logic to the data
    being loaded (e.g. column masking, row filtering).

    Subclasses must call ``super().__init__(dialect=...)`` to declare the SQL
    dialect used by their ``transform()`` method.  Common values are
    ``"datafusion"`` and ``"spark"``, but any dialect accepted by SQLGlot may
    be used.

    Args:
        dialect: The SQL dialect that ``transform()`` produces.
            When not ``"datafusion"``, the data loader transpiles the
            returned SQL from this dialect to DataFusion via SQLGlot.
    """

    def __init__(self, dialect: str) -> None:
        self.dialect: str = dialect

    @abstractmethod
    def transform(self, table: TableIdentifier, context: Mapping[str, str]) -> str | None:
        """Builds a SQL string representing the transformation to apply.

        Called once to extract the SQL.  The SQL is then executed per batch in
        each split against a DataFusion session where the batch is registered
        under ``to_sql_identifier(table)`` (from ``data_loader_split.to_sql_identifier``).

        The decision to return a SQL string or ``None`` **must not** depend on
        row data — it should be based solely on the table identifier and context.

        Args:
            table: Identifier for the table
            context: Dictionary of context information (e.g. tenant, environment, etc.)

        Returns:
            A SQL string in ``self.dialect``, or None if no transformation is needed.
        """
        pass

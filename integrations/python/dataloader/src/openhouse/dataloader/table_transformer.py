from abc import ABC, abstractmethod
from collections.abc import Mapping

from datafusion.context import SessionContext
from datafusion.dataframe import DataFrame

from openhouse.dataloader.table_identifier import TableIdentifier


class TableTransformer(ABC):
    """Interface for applying additional transformation logic to the data
    being loaded (e.g. column masking, row filtering)
    """

    @abstractmethod
    def transform(
        self, session_context: SessionContext, table: TableIdentifier, context: Mapping[str, str]
    ) -> DataFrame | None:
        """Applies transformation logic to the base table that is being loaded.

        This method is first probed with an empty table (zero rows) to determine
        whether a transformation is active.  The decision to return a ``DataFrame``
        or ``None`` **must not** depend on row data — it should be based solely on
        the table identifier and context.

        Args:
            session_context: DataFusion session with the base table already registered
            table: Identifier for the table
            context: Dictionary of context information (e.g. tenant, environment, etc.)

        Returns:
            The DataFrame representing the transformation. This is expected to read from the exact
            base table identifier passed in as input. If no transformation is required, None is returned.
        """
        pass

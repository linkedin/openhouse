from abc import ABC, abstractmethod

from datafusion.context import SessionContext


class UDFRegistry(ABC):
    """Used to register DataFusion UDFs"""

    @abstractmethod
    def register_udfs(self, session_context: SessionContext) -> None:
        """Registers UDFs with DataFusion

        Args:
            session_context: The session context to register the UDFs in
        """
        pass

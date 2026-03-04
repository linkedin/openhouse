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


class NoOpRegistry(UDFRegistry):
    """Default no-op UDF registry implementation.

    This registry performs no operations when register_udfs is called.
    Use this as the default when custom UDF registration is not needed,
    or subclass UDFRegistry to provide custom UDF registration logic.
    """

    def register_udfs(self, session_context: SessionContext) -> None:
        """No-op implementation that registers no UDFs.

        Args:
            session_context: The session context (unused)
        """
        pass

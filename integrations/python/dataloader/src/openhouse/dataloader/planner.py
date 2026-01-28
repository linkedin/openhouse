"""Query planner for OpenHouse DataLoader."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datafusion.context import SessionContext
from datafusion.dataframe import DataFrame
from datafusion.plan import LogicalPlan
from pyiceberg.io import FileScanTask
from typing import Dict, List, Optional
from openhouse.dataloader.udf_registry import UDFRegistry


@dataclass
class TableIdentifier:
    """Identifier for a table in OpenHouse.

    Args:
        database: Database name
        table: Table name
    """
    database: str
    table: str

    def __str__(self) -> str:
        """Return the fully qualified table name."""
        return f"{self.database}.{self.table}"


@dataclass
class PlanResult:
    """Result of query planning containing logical plan and file splits.
    
    Args:
        table_properties: Dictionary of table properties
        logical_plan: Logical plan for the query to be executed on each file scan task
        file_scan_tasks: List of file scan tasks to load the table
    """
    table_properties: Dict[str, str]
    logical_plan: LogicalPlan
    file_scan_tasks: List[FileScanTask]

    # TODO are some serialization helper methods needed for each split to distribute to workers?


class TableModifier(ABC):
    """Abstract interface for applying additional transformation logic to the data
    being loaded (e.g. compliance filters).
    """

    @abstractmethod
    def modify(self, session_context: SessionContext, table: TableIdentifier, context: Dict[str, str]) -> Optional[DataFrame]:
        """Applies transformation logic to the base table that is being loaded.

        Args:
            table: Identifier for the table
            context: Dictionary of context information (e.g. tenant, environment, etc.)

        Returns:
            The DataFrame representing the transformation. This is expected to read from the exact
            base table pased in as input. If no transformation is required, None is returned.
        """
        pass


class Planner:
    """Public API for query planning with pluggable table resolution."""
    
    def __init__(
        self,
        # TODO default implementation that returns none for modify
        table_modifier: TableModifier = None,
        udf_registry: UDFRegistry = None,
    ):
        """Initialize the planner with optional resolver.
        
        Args:
            table_modifier: TableModifier implementation to apply prerequisite transformations on the table being loaded
            udf_registry: Data Fusion UDFs to register
        """
        self._table_modifier = table_modifier
        self._udf_registry = udf_registry

    # TODO figure out how to represent filters
    def create_load_plan(self, table: TableIdentifier, columns: List[str], filters: List[object], context: Dict[str, str]) -> PlanResult:
        """Create a plan to load the given table.

        Args:
            table: Identifier for the table to load
            columns: List of column names to load from the table
            filters: List of filters to apply to the table
            context: Dictionary of context information (e.g. tenant, environment, etc.)

        Returns:
            The plan for loading this table
        """
        pass
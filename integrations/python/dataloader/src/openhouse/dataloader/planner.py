"""Query planner for OpenHouse DataLoader."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datafusion.context import SessionContext
from datafusion.dataframe import DataFrame
from datafusion.plan import LogicalPlan
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class FileSplit:
    """Represents a file split to read with offset and length."""
    
    file_path: str
    offset: int
    length: int


@dataclass
class PlanResult:
    """Result of query planning containing logical plan and file splits."""
    
    logical_plan: LogicalPlan
    file_splits: List[FileSplit]
    select_columns: List[str]
    # TODO figure out how to format the filters
    predicate_pushdowns: List[object]


class TableModifier(ABC):
    """Abstract interface for applying additional transformation logic to the data
    being loaded (e.g. compliance filters).
    """
    
    @abstractmethod
    def modify(self, session_context: SessionContext, table_name: str) -> Optional[DataFrame]:
        """Applies transformation logic to the base table that is being loaded.
        
        Args:
            table_name: Name of the table
            
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
    ):
        """Initialize the planner with optional resolver.
        
        Args:
            table_modifier: TableModifier implementation to apply prerequisite transformations on the table being loaded
        """
        self._table_modifier = table_modifier

    # TODO add filters
    def create_load_plan(self, table_name: str, columns: List[str]) -> PlanResult:
        """Create a plan to load the given table.
        
        Args:
            table_name: Name of the table to load
            columns: List of column names to load from the table
            
        Returns:
            The plan for loading this table
        """
        raise NotImplementedError()
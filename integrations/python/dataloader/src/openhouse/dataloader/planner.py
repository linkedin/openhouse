"""Query planner for OpenHouse DataLoader."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class FileSplit:
    """Represents a file split to read with offset and length."""
    
    file_path: str
    offset: int
    length: int


@dataclass
class ResolvedTable:
    """Result of table resolution.
    
    If is_view is True, query and dialect fields will be populated.
    """
    
    name: str
    is_view: bool
    query: Optional[str] = None
    dialect: Optional[str] = None


@dataclass
class PlanResult:
    """Result of query planning containing logical plan and file splits."""
    
    logical_plan: object
    file_splits: List[FileSplit]


class TableResolver(ABC):
    """Abstract interface for resolving table names.
    
    Implementations should resolve table names to either physical tables or views.
    If the result is a view, the query and dialect fields should be populated.
    """
    
    @abstractmethod
    def resolve(self, table_name: str) -> ResolvedTable:
        """Resolve a table name to a ResolvedTable.
        
        Args:
            table_name: Name of the table to resolve
            
        Returns:
            ResolvedTable with name, is_view flag, and optionally query/dialect
        """
        pass


class Planner:
    """Public API for query planning with pluggable table resolution."""
    
    def __init__(
        self,
        table_resolver: Optional[TableResolver] = None,
    ):
        """Initialize the planner with optional resolver.
        
        Args:
            table_resolver: Optional TableResolver implementation
        """
        self.table_resolver = table_resolver

    def plan(self, table_name: str, columns: List[str]) -> PlanResult:
        """Create a query plan for the given table and columns.
        
        Args:
            table_name: Name of the table to query
            columns: List of column names to project
            
        Returns:
            PlanResult containing the logical plan and file splits
        """
        raise NotImplementedError("plan() method will be implemented later")
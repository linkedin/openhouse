"""Query planner for OpenHouse DataLoader."""

from dataclasses import dataclass, field
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
    
    logical_plan: object
    file_splits: List[FileSplit]


@dataclass
class QueryPlan:
    """Represents a query plan with columns, filters, and optimization settings."""
    
    columns: List[str]
    filters: List[Tuple[str, str, Any]] = field(default_factory=list)
    limit: Optional[int] = None
    partition_pruning: bool = True
    file_pruning: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    VALID_OPERATORS = {"=", "!=", "<", "<=", ">", ">=", "in", "not in", "is null", "is not null"}
    
    def __post_init__(self) -> None:
        """Validate the query plan after initialization."""
        if not self.columns:
            raise ValueError("QueryPlan must specify at least one column")
        
        for filter_tuple in self.filters:
            if len(filter_tuple) >= 2:
                operator = filter_tuple[1]
                if operator not in self.VALID_OPERATORS:
                    raise ValueError(
                        f"Unsupported filter operator: {operator}. "
                        f"Valid operators: {self.VALID_OPERATORS}"
                    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert query plan to dictionary."""
        return {
            "columns": self.columns,
            "filters": self.filters,
            "limit": self.limit,
            "partition_pruning": self.partition_pruning,
            "file_pruning": self.file_pruning,
            "metadata": self.metadata,
        }


class QueryPlanner:
    """Public API for query planning with DataFusion integration."""
    
    def __init__(self, enable_caching: bool = True) -> None:
        """Initialize the query planner.
        
        Args:
            enable_caching: Whether to enable query plan caching
        """
        self.enable_caching = enable_caching
        self._cache: Dict[str, QueryPlan] = {}
    
    def plan(
        self,
        table_name: str,
        columns: List[str],
    ) -> PlanResult:
        """Create a query plan for the given table and columns.
        
        This is the main public API method that returns both the logical plan
        (from DataFusion) and the file splits to read.
        
        Args:
            table_name: Name of the table to query
            columns: List of column names to project
            
        Returns:
            PlanResult containing the logical plan and file splits
        """
        raise NotImplementedError("plan() method will be implemented later")
    
    def create_plan(
        self,
        columns: List[str],
        filters: Optional[List[Tuple[str, str, Any]]] = None,
        limit: Optional[int] = None,
    ) -> QueryPlan:
        """Create a query plan with the specified parameters.
        
        Args:
            columns: List of column names to select
            filters: Optional list of filter tuples (column, operator, value)
            limit: Optional limit on number of rows
            
        Returns:
            QueryPlan object
        """
        return QueryPlan(
            columns=columns,
            filters=filters or [],
            limit=limit,
        )
    
    def optimize(self, plan: QueryPlan) -> QueryPlan:
        """Optimize a query plan.
        
        Args:
            plan: Query plan to optimize
            
        Returns:
            Optimized query plan
        """
        cache_key = str(plan.to_dict())
        
        if self.enable_caching and cache_key in self._cache:
            return self._cache[cache_key]
        
        optimized_columns = list(plan.columns)
        for filter_tuple in plan.filters:
            if len(filter_tuple) >= 1:
                filter_col = filter_tuple[0]
                if filter_col not in optimized_columns:
                    optimized_columns.append(filter_col)
        
        optimized_plan = QueryPlan(
            columns=optimized_columns,
            filters=plan.filters,
            limit=plan.limit,
            partition_pruning=plan.partition_pruning,
            file_pruning=plan.file_pruning,
            metadata={
                **plan.metadata,
                "optimizations": ["filter_column_inclusion"],
            },
        )
        
        if self.enable_caching:
            self._cache[cache_key] = optimized_plan
        
        return optimized_plan
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get statistics about the query plan cache.
        
        Returns:
            Dictionary with cache statistics
        """
        return {
            "size": len(self._cache),
            "enabled": self.enable_caching,
        }
    
    def clear_cache(self) -> None:
        """Clear the query plan cache."""
        self._cache.clear()

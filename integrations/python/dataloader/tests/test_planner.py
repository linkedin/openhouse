"""Tests for the QueryPlanner class."""

import pytest

from openhouse.dataloader.planner import QueryPlan, QueryPlanner


class TestQueryPlan:
    """Tests for QueryPlan dataclass."""

    def test_create_basic_plan(self) -> None:
        """Test creating a basic query plan."""
        plan = QueryPlan(columns=["col1", "col2"])
        assert plan.columns == ["col1", "col2"]
        assert plan.filters == []
        assert plan.limit is None
        assert plan.partition_pruning is True
        assert plan.file_pruning is True

    def test_create_plan_with_filters(self) -> None:
        """Test creating a query plan with filters."""
        plan = QueryPlan(
            columns=["col1"],
            filters=[("col2", ">=", 100)],
        )
        assert len(plan.filters) == 1
        assert plan.filters[0] == ("col2", ">=", 100)

    def test_invalid_operator(self) -> None:
        """Test that invalid operators raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported filter operator"):
            QueryPlan(
                columns=["col1"],
                filters=[("col2", "invalid_op", 100)],
            )

    def test_empty_columns(self) -> None:
        """Test that empty columns list raises ValueError."""
        with pytest.raises(ValueError, match="at least one column"):
            QueryPlan(columns=[])

    def test_to_dict(self) -> None:
        """Test converting plan to dictionary."""
        plan = QueryPlan(
            columns=["col1"],
            filters=[("col2", "=", "value")],
            limit=100,
        )
        plan_dict = plan.to_dict()

        assert plan_dict["columns"] == ["col1"]
        assert plan_dict["filters"] == [("col2", "=", "value")]
        assert plan_dict["limit"] == 100


class TestQueryPlanner:
    """Tests for QueryPlanner class."""

    def test_create_planner(self) -> None:
        """Test creating a query planner."""
        planner = QueryPlanner()
        assert planner.enable_caching is True

    def test_create_simple_plan(self) -> None:
        """Test creating a simple query plan."""
        planner = QueryPlanner()
        plan = planner.create_plan(columns=["col1", "col2"])

        assert plan.columns == ["col1", "col2"]
        assert plan.filters == []

    def test_create_plan_with_filters(self) -> None:
        """Test creating a plan with filters."""
        planner = QueryPlanner()
        plan = planner.create_plan(
            columns=["feature1", "label"],
            filters=[
                ("date", ">=", "2024-01-01"),
                ("date", "<", "2024-02-01"),
            ],
            limit=1000,
        )

        assert len(plan.filters) == 2
        assert plan.limit == 1000

    def test_optimize_plan(self) -> None:
        """Test optimizing a query plan."""
        planner = QueryPlanner()
        plan = planner.create_plan(
            columns=["col1"],
            filters=[("col2", "=", "value")],
        )

        optimized = planner.optimize(plan)

        # Check that filter column is included
        assert "col2" in optimized.columns
        assert "optimizations" in optimized.metadata

    def test_plan_caching(self) -> None:
        """Test that query plans are cached."""
        planner = QueryPlanner(enable_caching=True)
        plan1 = planner.create_plan(columns=["col1"])

        optimized1 = planner.optimize(plan1)
        optimized2 = planner.optimize(plan1)

        # Should return the same cached instance
        assert optimized1 is optimized2

        stats = planner.get_cache_stats()
        assert stats["size"] == 1
        assert stats["enabled"] is True

    def test_clear_cache(self) -> None:
        """Test clearing the query plan cache."""
        planner = QueryPlanner()
        plan = planner.create_plan(columns=["col1"])
        planner.optimize(plan)

        assert planner.get_cache_stats()["size"] == 1

        planner.clear_cache()
        assert planner.get_cache_stats()["size"] == 0

    def test_caching_disabled(self) -> None:
        """Test planner with caching disabled."""
        planner = QueryPlanner(enable_caching=False)
        plan = planner.create_plan(columns=["col1"])

        planner.optimize(plan)

        stats = planner.get_cache_stats()
        assert stats["size"] == 0
        assert stats["enabled"] is False

"""Tests for scan_optimizer.optimize_scan."""

import datafusion
import pyarrow as pa
import pytest
import sqlglot
from sqlglot import exp

from openhouse.dataloader.filters import (
    AlwaysTrue,
    And,
    Between,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNotNull,
    IsNull,
    LessThan,
    LessThanOrEqual,
    NotEqualTo,
    Or,
    col,
)
from openhouse.dataloader.scan_optimizer import (
    _extract_pushable_predicates,
    _filter_to_sql,
    optimize_scan,
)

# --- Helper ---


def _collect_filters(f) -> set:
    """Recursively collect all leaf filters from an And tree into a set."""
    if isinstance(f, And):
        return _collect_filters(f.left) | _collect_filters(f.right)
    return {f}


def _run_sql(sql: str, schema: pa.Schema, data: dict) -> list[pa.RecordBatch]:
    """Register a batch and execute SQL in DataFusion, returning result batches."""
    batch = pa.record_batch(data, schema=schema)
    ctx = datafusion.SessionContext()
    ctx.sql('CREATE SCHEMA IF NOT EXISTS "db"').collect()
    ctx.register_record_batches('"db"."tbl"', [[batch]])
    return ctx.sql(sql).collect()


# --- Basic projection tests (updated from compute_scan_projection) ---


def test_basic_pushdown_prunes_unused_columns():
    """Requesting a,b from a query that selects a,b,c → source columns exclude c."""
    plan = optimize_scan('SELECT "a", "b", "c" FROM "db"."tbl"', ["a", "b"])

    assert plan.source_columns == ["a", "b"]


def test_expression_alias_extracts_source_column():
    """Inner query has UPPER(name) AS masked → source column is name."""
    plan = optimize_scan('SELECT upper("name") AS "masked" FROM "db"."tbl"', ["masked"])

    assert plan.source_columns == ["name"]


def test_all_columns_used():
    """Outer requests everything inner produces → all source columns kept."""
    plan = optimize_scan('SELECT "a", "b" FROM "db"."tbl"', ["a", "b"])

    assert plan.source_columns == ["a", "b"]


def test_literal_alias_needs_no_source_column():
    """Inner query has literal expression → no source column needed for it."""
    plan = optimize_scan('SELECT "id", \'MASKED\' AS "name" FROM "db"."tbl"', ["id", "name"])

    assert plan.source_columns is not None
    assert plan.source_columns == ["id"]


def test_invalid_sql_returns_fallback():
    """Invalid SQL → graceful fallback with None source columns and always_true."""
    plan = optimize_scan("NOT VALID SQL !!!", ["a"])

    assert plan.source_columns is None
    assert isinstance(plan.row_filter, AlwaysTrue)
    assert plan.sql is not None


def test_optimized_sql_executes_in_datafusion():
    """Verify the returned optimized SQL runs in DataFusion with only source columns."""
    plan = optimize_scan('SELECT "id", "name", "value" FROM "db"."tbl"', ["id", "name"])

    assert plan.source_columns == ["id", "name"]

    result = _run_sql(
        plan.sql,
        pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())]),
        {"id": [1], "name": ["alice"]},
    )
    assert len(result) == 1
    assert set(result[0].schema.names) == {"id", "name"}


# --- Full example from the plan ---


def test_full_example_transform_with_columns_and_filters():
    """Transform with WHERE, user columns, and user filters → combined pushdown."""
    plan = optimize_scan(
        "SELECT redact(a) AS a, b, c, d FROM t WHERE e = 'foo'",
        columns=["a", "b"],
        filters=col("c") > 10,
    )

    # Both predicates should be pushed
    filters = _collect_filters(plan.row_filter)
    assert EqualTo("e", "foo") in filters
    assert GreaterThan("c", 10) in filters

    # source_columns should include what's needed for the rewritten inner SELECT
    assert "a" in plan.source_columns
    assert "b" in plan.source_columns
    # Pushed predicate columns should NOT be in source_columns
    assert "c" not in plan.source_columns
    assert "d" not in plan.source_columns


# --- Inner predicate extraction ---


def test_inner_where_extracted():
    """Inner WHERE with simple predicate → extracted to row_filter, WHERE removed."""
    plan = optimize_scan('SELECT "a", "b" FROM "db"."tbl" WHERE "e" = \'foo\'')

    filters = _collect_filters(plan.row_filter)
    assert EqualTo("e", "foo") in filters
    # e should not be in source_columns since it's only in the pushed WHERE
    assert "e" not in (plan.source_columns or [])


def test_inner_where_columns_preserved_when_not_pushable():
    """Inner WHERE with non-pushable predicate → columns preserved in source_columns."""
    plan = optimize_scan('SELECT "a" FROM "db"."tbl" WHERE upper("x") = \'FOO\'', ["a"])

    # upper("x") = 'FOO' is not pushable (function on column side)
    assert "x" in plan.source_columns


# --- Outer predicate pushdown ---


def test_outer_passthrough_predicate_pushed():
    """Outer WHERE on passthrough column → pushed to Iceberg."""
    plan = optimize_scan('SELECT "a", "b" FROM "db"."tbl"', ["a"], filters=col("b") > 5)

    filters = _collect_filters(plan.row_filter)
    assert GreaterThan("b", 5) in filters


def test_outer_non_passthrough_stays_in_sql():
    """Outer WHERE on non-passthrough column → stays in SQL."""
    plan = optimize_scan(
        'SELECT redact(a) AS a, "b" FROM "db"."tbl"',
        ["a", "b"],
        filters=col("a") > 5,
    )

    # a is not a passthrough (it's redact(a)), so the predicate stays in SQL
    assert "WHERE" in plan.sql
    # a must be in the inner SELECT outputs
    assert "a" in plan.source_columns


def test_residual_filter_on_non_selected_column():
    """Filter on non-passthrough col not in user columns → inner SELECT produces both."""
    plan = optimize_scan(
        'SELECT redact(a) AS a, "b" FROM "db"."tbl"',
        ["b"],
        filters=col("a") > 5,
    )

    # a is not passthrough, so filter stays in SQL
    assert "WHERE" in plan.sql
    # Inner SELECT must produce both b (user column) and a (for the residual filter)
    assert "a" in plan.source_columns
    assert "b" in plan.source_columns


# --- Partial extraction ---


def test_partial_inner_extraction():
    """Inner WHERE with mix of pushable and non-pushable → partial extraction."""
    plan = optimize_scan(
        'SELECT "a" FROM "db"."tbl" WHERE "x" > 5 AND upper("z") = \'FOO\'',
        ["a"],
    )

    filters = _collect_filters(plan.row_filter)
    assert GreaterThan("x", 5) in filters
    # upper("z") stays in SQL
    assert "z" in plan.source_columns


# --- OR handling ---


def test_or_both_pushable():
    """OR where both sides are pushable → entire OR pushed."""
    plan = optimize_scan(
        'SELECT "a", "b", "c" FROM "db"."tbl"',
        ["a"],
        filters=(col("b") > 5) | (col("c") < 10),
    )

    assert isinstance(plan.row_filter, Or), f"Expected Or, got {plan.row_filter!r}"


def test_or_one_non_pushable_stays():
    """OR where one side references non-passthrough → stays in SQL."""
    plan = optimize_scan(
        'SELECT redact(a) AS a, "b" FROM "db"."tbl"',
        ["a", "b"],
        filters=(col("a") > 5) | (col("b") < 10),
    )

    # a is not passthrough, so the whole OR must stay in SQL
    assert "WHERE" in plan.sql


# --- Projection excludes pushed columns ---


def test_pushed_predicate_columns_not_in_source():
    """Columns only used in pushed predicates are excluded from source_columns."""
    plan = optimize_scan(
        'SELECT "a", "b", "c" FROM "db"."tbl"',
        ["a"],
        filters=col("b") > 5,
    )

    filters = _collect_filters(plan.row_filter)
    assert GreaterThan("b", 5) in filters
    # b was only needed for the pushed filter, not for SELECT
    assert "b" not in plan.source_columns


# --- Each comparison type ---


@pytest.mark.parametrize(
    "filter_expr,expected_type",
    [
        (col("x") == 1, EqualTo),
        (col("x") != 1, NotEqualTo),
        (col("x") > 1, GreaterThan),
        (col("x") >= 1, GreaterThanOrEqual),
        (col("x") < 1, LessThan),
        (col("x") <= 1, LessThanOrEqual),
        (col("x").is_null(), IsNull),
        (col("x").is_not_null(), IsNotNull),
        (col("x").is_in([1, 2, 3]), In),
        (col("x").between(1, 10), Between),
    ],
    ids=["eq", "neq", "gt", "gte", "lt", "lte", "is_null", "is_not_null", "in", "between"],
)
def test_comparison_type_round_trip(filter_expr, expected_type):
    """Each filter type survives the Filter→SQL→parse→extract round trip."""
    plan = optimize_scan(
        'SELECT "a", "x" FROM "db"."tbl"',
        ["a"],
        filters=filter_expr,
    )

    filters = _collect_filters(plan.row_filter)
    assert any(isinstance(f, expected_type) for f in filters), f"Expected {expected_type} in {filters}"


# --- Filter-to-SQL round trip ---


def test_filter_to_sql_round_trip():
    """Filter DSL → SQL → parse → extract → equivalent Filter."""
    original = (col("a") > 5) & (col("b") == "hello")
    sql = _filter_to_sql(original)
    parsed = sqlglot.parse_one(f"SELECT * FROM t WHERE {sql}", dialect="datafusion")
    where = parsed.find(exp.Where)
    extracted, remaining = _extract_pushable_predicates(where)

    assert remaining is None
    filters = _collect_filters(extracted)
    assert GreaterThan("a", 5) in filters
    assert EqualTo("b", "hello") in filters


# --- DataFusion execution with source_columns ---


def test_datafusion_execution_with_source_columns():
    """Final SQL runs with only source_columns in the batch."""
    plan = optimize_scan('SELECT "id", "name", "value" FROM "db"."tbl"', ["id"], filters=col("name") == "alice")

    # name is passthrough → pushed; only id needed in source_columns
    assert plan.source_columns == ["id"]

    result = _run_sql(
        plan.sql,
        pa.schema([pa.field("id", pa.int64())]),
        {"id": [1, 2]},
    )
    assert len(result) == 1
    assert result[0].schema.names == ["id"]


# --- No columns, no filters ---


def test_no_columns_no_filters_inner_where_extraction():
    """Just transform SQL with inner WHERE → only inner WHERE extraction."""
    plan = optimize_scan('SELECT "a", "b" FROM "db"."tbl" WHERE "c" = 1')

    filters = _collect_filters(plan.row_filter)
    assert EqualTo("c", 1) in filters
    assert plan.source_columns == ["a", "b"]


def test_no_columns_no_filters_no_where():
    """Plain transform SQL, no columns, no filters → pass-through."""
    plan = optimize_scan('SELECT "a", "b" FROM "db"."tbl"')

    assert isinstance(plan.row_filter, AlwaysTrue)
    assert plan.source_columns == ["a", "b"]

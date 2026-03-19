"""Tests for scan_optimizer.compute_scan_projection."""

import datafusion
import pyarrow as pa

from openhouse.dataloader.scan_optimizer import compute_scan_projection


def test_basic_pushdown_prunes_unused_columns():
    """Requesting a,b from a query that selects a,b,c → source columns exclude c."""
    transform_sql = 'SELECT "a", "b", "c" FROM "db"."tbl"'
    combined_sql, source_columns = compute_scan_projection(transform_sql, ["a", "b"])

    assert source_columns == ["a", "b"]


def test_where_columns_preserved():
    """Inner WHERE references column x not in outer SELECT → x still in source columns."""
    transform_sql = 'SELECT "a", "x" FROM "db"."tbl" WHERE "x" > 5'
    combined_sql, source_columns = compute_scan_projection(transform_sql, ["a"])

    assert source_columns == ["a", "x"]


def test_expression_alias_extracts_source_column():
    """Inner query has UPPER(name) AS masked → source column is name."""
    transform_sql = 'SELECT upper("name") AS "masked" FROM "db"."tbl"'
    combined_sql, source_columns = compute_scan_projection(transform_sql, ["masked"])

    assert source_columns == ["name"]


def test_all_columns_used():
    """Outer requests everything inner produces → all source columns kept."""
    transform_sql = 'SELECT "a", "b" FROM "db"."tbl"'
    combined_sql, source_columns = compute_scan_projection(transform_sql, ["a", "b"])

    assert source_columns == ["a", "b"]


def test_invalid_sql_returns_none_source_columns():
    """Invalid SQL → graceful fallback with None source columns."""
    combined_sql, source_columns = compute_scan_projection("NOT VALID SQL !!!", ["a"])

    assert source_columns is None
    assert combined_sql is not None


def test_optimized_sql_executes_in_datafusion():
    """Verify the returned optimized SQL runs in DataFusion with only source columns."""
    transform_sql = 'SELECT "id", "name", "value" FROM "db"."tbl"'
    optimized_sql, source_columns = compute_scan_projection(transform_sql, ["id", "name"])

    assert source_columns == ["id", "name"]

    # Register a batch with only the projected source columns
    batch = pa.record_batch(
        {"id": [1], "name": ["alice"]},
        schema=pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        ),
    )

    ctx = datafusion.SessionContext()
    ctx.sql('CREATE SCHEMA IF NOT EXISTS "db"').collect()
    ctx.register_record_batches('"db"."tbl"', [[batch]])

    result = ctx.sql(optimized_sql).collect()
    assert len(result) == 1
    assert set(result[0].schema.names) == {"id", "name"}


def test_literal_alias_needs_no_source_column():
    """Inner query has literal expression → no source column needed for it."""
    transform_sql = 'SELECT "id", \'MASKED\' AS "name" FROM "db"."tbl"'
    combined_sql, source_columns = compute_scan_projection(transform_sql, ["id", "name"])

    assert source_columns is not None
    # Only id is a source column; name is a literal alias
    assert source_columns == ["id"]

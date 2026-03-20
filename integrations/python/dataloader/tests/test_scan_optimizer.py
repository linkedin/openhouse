"""Tests for scan_optimizer.optimize_scan."""

from openhouse.dataloader.filters import (
    AlwaysTrue,
    And,
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
)
from openhouse.dataloader.scan_optimizer import optimize_scan

# --- Helper ---


def _collect_filters(f) -> set:
    """Recursively collect all leaf filters from an And tree into a set."""
    if isinstance(f, And):
        return _collect_filters(f.left) | _collect_filters(f.right)
    return {f}


# --- Projection ---


def test_prunes_unused_columns():
    plan = optimize_scan('SELECT "a", "b" FROM (SELECT "a", "b", "c, foo(d)" FROM "db"."tbl")')
    assert plan.source_columns == ["a", "b"]


def test_expression_alias_extracts_source_column():
    plan = optimize_scan('SELECT "masked" FROM (SELECT upper("name") AS "masked" FROM "db"."tbl")')

    assert plan.source_columns == ["name"]


def test_all_columns_used():
    plan = optimize_scan('SELECT "a", "b" FROM (SELECT "a", "b" FROM "db"."tbl")')

    assert plan.source_columns == ["a", "b"]


def test_literal_alias_needs_no_source_column():
    plan = optimize_scan('SELECT "id", "name" FROM (SELECT "id", \'MASKED\' AS "name" FROM "db"."tbl")')

    assert plan.source_columns == ["id"]


# --- Predicate pushdown ---


def test_where_predicate_extracted():
    plan = optimize_scan('SELECT "a", "b" FROM "db"."tbl" WHERE "e" = \'foo\'')

    filters = _collect_filters(plan.row_filter)
    assert EqualTo("e", "foo") in filters


def test_outer_passthrough_predicate_pushed():
    plan = optimize_scan('SELECT "a" FROM (SELECT "a", "b" FROM "db"."tbl") AS _t WHERE "b" > 5')

    filters = _collect_filters(plan.row_filter)
    assert GreaterThan("b", 5) in filters


def test_outer_non_passthrough_stays_in_sql():
    plan = optimize_scan('SELECT "a", "b" FROM (SELECT redact(a) AS a, "b" FROM "db"."tbl") AS _t WHERE "a" > 5')

    assert "WHERE" in plan.sql
    assert "a" in plan.source_columns


def test_non_pushable_where_preserved():
    plan = optimize_scan('SELECT "a" FROM (SELECT "a" FROM "db"."tbl" WHERE upper("x") = \'FOO\') AS _t')

    assert "x" in plan.source_columns


def test_partial_extraction():
    plan = optimize_scan('SELECT "a" FROM (SELECT "a" FROM "db"."tbl" WHERE "x" > 5 AND upper("z") = \'FOO\') AS _t')

    filters = _collect_filters(plan.row_filter)
    assert GreaterThan("x", 5) in filters
    assert "z" in plan.source_columns


def test_residual_filter_keeps_needed_columns():
    plan = optimize_scan('SELECT "b" FROM (SELECT redact(a) AS a, "b" FROM "db"."tbl") AS _t WHERE "a" > 5')

    assert "WHERE" in plan.sql
    assert "a" in plan.source_columns
    assert "b" in plan.source_columns


# --- Combined projection and predicate pushdown ---


def test_full_example():
    plan = optimize_scan(
        'SELECT "a", "b" FROM (SELECT redact(a) AS a, b, c, d FROM t WHERE e = \'foo\') AS _t WHERE "c" > 10'
    )

    filters = _collect_filters(plan.row_filter)
    assert EqualTo("e", "foo") in filters
    assert GreaterThan("c", 10) in filters
    assert "a" in plan.source_columns
    assert "b" in plan.source_columns
    assert "d" not in plan.source_columns


def test_unused_columns_pruned_with_filter():
    plan = optimize_scan('SELECT "a" FROM (SELECT "a", "b", "c" FROM "db"."tbl") AS _t WHERE "b" > 5')

    filters = _collect_filters(plan.row_filter)
    assert GreaterThan("b", 5) in filters
    assert "c" not in plan.source_columns


# --- OR handling ---


def test_or_both_pushable():
    plan = optimize_scan('SELECT "a" FROM (SELECT "a", "b", "c" FROM "db"."tbl") AS _t WHERE "b" > 5 OR "c" < 10')

    assert isinstance(plan.row_filter, Or), f"Expected Or, got {plan.row_filter!r}"


def test_or_one_non_pushable_stays():
    plan = optimize_scan(
        'SELECT "a", "b" FROM (SELECT redact(a) AS a, "b" FROM "db"."tbl") AS _t WHERE "a" > 5 OR "b" < 10'
    )

    assert "WHERE" in plan.sql


# --- Filter types ---


def test_comparison_types():
    """Each comparison type is extracted to the correct Filter type."""
    cases = [
        ('"x" = 1', EqualTo),
        ('"x" <> 1', NotEqualTo),
        ('"x" > 1', GreaterThan),
        ('"x" >= 1', GreaterThanOrEqual),
        ('"x" < 1', LessThan),
        ('"x" <= 1', LessThanOrEqual),
        ('"x" IS NULL', IsNull),
        ('"x" IS NOT NULL', IsNotNull),
        ('"x" IN (1, 2, 3)', In),
        ('"x" BETWEEN 1 AND 10', GreaterThanOrEqual),  # sqlglot decomposes BETWEEN
    ]
    for where_clause, expected_type in cases:
        plan = optimize_scan(f'SELECT "a" FROM "db"."tbl" WHERE {where_clause}')
        filters = _collect_filters(plan.row_filter)
        assert any(isinstance(f, expected_type) for f in filters), (
            f"Expected {expected_type} in {filters} for: {where_clause}"
        )


# --- Fallback ---


def test_invalid_sql_falls_back():
    plan = optimize_scan("NOT VALID SQL !!!")

    assert plan.source_columns is None
    assert isinstance(plan.row_filter, AlwaysTrue)
    assert plan.sql == "NOT VALID SQL !!!"


def test_no_table_raises():
    import pytest

    with pytest.raises(ValueError, match="Expected exactly 1 table scan, found 0"):
        optimize_scan("SELECT 1 AS a, 2 AS b")


def test_two_tables_raises():
    import pytest

    with pytest.raises(ValueError, match="Expected exactly 1 table scan, found 0"):
        optimize_scan('SELECT * FROM "db"."t1" JOIN "db"."t2" ON "t1"."id" = "t2"."id"')

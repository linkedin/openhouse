"""Tests for scan_optimizer.optimize_scan."""

import pytest
import sqlglot

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

# --- Projection ---


def test_prunes_unused_columns():
    plan = optimize_scan('SELECT "a", "b" FROM (SELECT "a", "b", "c" FROM "db"."tbl") AS _t')

    assert plan.source_columns == ["a", "b"]
    assert isinstance(plan.row_filter, AlwaysTrue)


def test_expression_alias_extracts_source_column():
    plan = optimize_scan('SELECT "masked" FROM (SELECT upper("name") AS "masked" FROM "db"."tbl") AS _t')

    assert plan.source_columns == ["name"]
    assert isinstance(plan.row_filter, AlwaysTrue)


def test_all_columns_used():
    plan = optimize_scan('SELECT "a", "b" FROM (SELECT "a", "b" FROM "db"."tbl") AS _t')

    assert plan.source_columns == ["a", "b"]
    assert isinstance(plan.row_filter, AlwaysTrue)


def test_literal_alias_needs_no_source_column():
    plan = optimize_scan('SELECT "id", "name" FROM (SELECT "id", \'MASKED\' AS "name" FROM "db"."tbl") AS _t')

    assert plan.source_columns == ["id"]
    assert isinstance(plan.row_filter, AlwaysTrue)


# --- Predicate pushdown ---


def test_where_predicate_extracted():
    plan = optimize_scan('SELECT "a", "b" FROM "db"."tbl" WHERE "e" = \'foo\'')

    assert plan.source_columns == ["a", "b", "e"]
    assert plan.row_filter == EqualTo("e", "foo")


def test_outer_passthrough_predicate_pushed():
    plan = optimize_scan('SELECT "a" FROM (SELECT "a", "b" FROM "db"."tbl") AS _t WHERE "b" > 5')

    assert plan.source_columns == ["a", "b"]
    assert plan.row_filter == GreaterThan("b", 5)


def test_outer_non_passthrough_not_pushed():
    plan = optimize_scan('SELECT "a", "b" FROM (SELECT redact(a) AS a, "b" FROM "db"."tbl") AS _t WHERE "a" > 5')

    assert plan.source_columns == ["a", "b"]
    # This cannot be pushed down because the filter is on projected column due to the redact function
    assert isinstance(plan.row_filter, AlwaysTrue)


def test_partial_extraction():
    plan = optimize_scan('SELECT "a" FROM (SELECT "a" FROM "db"."tbl" WHERE "x" > 5 AND upper("z") = \'FOO\') AS _t')

    assert plan.source_columns == ["a", "x", "z"]
    assert plan.row_filter == GreaterThan("x", 5)


# --- Combined projection and predicate pushdown ---


def test_full_example():
    plan = optimize_scan(
        'SELECT "a", "b" FROM (SELECT redact(a) AS a, b, c, d FROM t WHERE e = \'foo\') AS _t WHERE "c" > 10'
    )

    assert plan.source_columns == ["a", "b", "c", "e"]
    assert plan.row_filter == And(GreaterThan("c", 10), EqualTo("e", "foo"))


def test_unused_columns_pruned_with_filter():
    plan = optimize_scan('SELECT "a" FROM (SELECT "a", "b", "c" FROM "db"."tbl") AS _t WHERE "b" > 5')

    assert plan.source_columns == ["a", "b"]
    assert plan.row_filter == GreaterThan("b", 5)


# --- OR handling ---


def test_or_both_pushable():
    plan = optimize_scan('SELECT "a" FROM (SELECT "a", "b", "c" FROM "db"."tbl") AS _t WHERE "b" > 5 OR "c" < 10')

    assert plan.source_columns == ["a", "b", "c"]
    assert plan.row_filter == Or(GreaterThan("b", 5), LessThan("c", 10))


def test_or_one_non_pushable():
    plan = optimize_scan(
        'SELECT "a", "b" FROM (SELECT redact(a) AS a, "b" FROM "db"."tbl") AS _t WHERE "a" > 5 OR "b" < 10'
    )

    assert plan.source_columns == ["a", "b"]
    assert isinstance(plan.row_filter, AlwaysTrue)


# --- Filter types ---


def test_comparison_types():
    """Each comparison type is extracted to the correct Filter."""
    cases = [
        ('"x" = 1', ["a", "x"], EqualTo("x", 1)),
        ('"x" <> 1', ["a", "x"], NotEqualTo("x", 1)),
        ('"x" > 1', ["a", "x"], GreaterThan("x", 1)),
        ('"x" >= 1', ["a", "x"], GreaterThanOrEqual("x", 1)),
        ('"x" < 1', ["a", "x"], LessThan("x", 1)),
        ('"x" <= 1', ["a", "x"], LessThanOrEqual("x", 1)),
        ('"x" IS NULL', ["a", "x"], IsNull("x")),
        ('"x" IS NOT NULL', ["a", "x"], IsNotNull("x")),
        ('"x" IN (1, 2, 3)', ["a", "x"], In("x", (1, 2, 3))),
        ('"x" BETWEEN 1 AND 10', ["a", "x"], And(LessThanOrEqual("x", 10), GreaterThanOrEqual("x", 1))),
    ]
    for where_clause, expected_cols, expected_filter in cases:
        plan = optimize_scan(f'SELECT "a" FROM "db"."tbl" WHERE {where_clause}')
        assert plan.source_columns == expected_cols, f"source_columns mismatch for: {where_clause}"
        assert plan.row_filter == expected_filter, f"row_filter mismatch for: {where_clause}"


# --- No predicates ---


def test_no_where():
    plan = optimize_scan('SELECT "a", "b" FROM "db"."tbl"')

    assert plan.source_columns == ["a", "b"]
    assert isinstance(plan.row_filter, AlwaysTrue)


# --- Error cases ---


def test_invalid_sql_raises():
    with pytest.raises(sqlglot.errors.SqlglotError):
        optimize_scan("NOT VALID SQL !!!")


def test_no_table_raises():
    with pytest.raises(ValueError, match="Expected exactly 1 table scan, found 0"):
        optimize_scan("SELECT 1 AS a, 2 AS b")


def test_two_tables_raises():
    with pytest.raises(ValueError, match="Expected exactly 1 table scan, found 0"):
        optimize_scan('SELECT * FROM "db"."t1" JOIN "db"."t2" ON "t1"."id" = "t2"."id"')

"""Tests for scan_optimizer.optimize_scan."""

from openhouse.dataloader._query_builder import build_combined_query
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
    col,
)
from openhouse.dataloader.scan_optimizer import optimize_scan

# --- Helper ---


def _collect_filters(f) -> set:
    """Recursively collect all leaf filters from an And tree into a set."""
    if isinstance(f, And):
        return _collect_filters(f.left) | _collect_filters(f.right)
    return {f}


def _optimize(transform_sql, columns=None, filters=None):
    """Build combined query and optimize — mirrors the data_loader pipeline."""
    return optimize_scan(build_combined_query(transform_sql, columns, filters))


# --- Projection ---


def test_prunes_unused_columns():
    """Requesting a,b from a query that selects a,b,c → source columns exclude c."""
    plan = _optimize('SELECT "a", "b", "c" FROM "db"."tbl"', ["a", "b"])

    assert plan.source_columns == ["a", "b"]


def test_expression_alias_extracts_source_column():
    """UPPER(name) AS masked → source column is name."""
    plan = _optimize('SELECT upper("name") AS "masked" FROM "db"."tbl"', ["masked"])

    assert plan.source_columns == ["name"]


def test_all_columns_used():
    """Requesting all columns → all source columns kept."""
    plan = _optimize('SELECT "a", "b" FROM "db"."tbl"', ["a", "b"])

    assert plan.source_columns == ["a", "b"]


def test_literal_alias_needs_no_source_column():
    """Literal expression → no source column needed."""
    plan = _optimize('SELECT "id", \'MASKED\' AS "name" FROM "db"."tbl"', ["id", "name"])

    assert plan.source_columns == ["id"]


# --- Predicate pushdown ---


def test_inner_where_extracted():
    """Inner WHERE with simple predicate → extracted to row_filter."""
    plan = _optimize('SELECT "a", "b" FROM "db"."tbl" WHERE "e" = \'foo\'')

    filters = _collect_filters(plan.row_filter)
    assert EqualTo("e", "foo") in filters


def test_outer_passthrough_predicate_pushed():
    """User filter on passthrough column → pushed to row_filter."""
    plan = _optimize('SELECT "a", "b" FROM "db"."tbl"', ["a"], filters=col("b") > 5)

    filters = _collect_filters(plan.row_filter)
    assert GreaterThan("b", 5) in filters


def test_outer_non_passthrough_stays_in_sql():
    """User filter on non-passthrough column → stays in SQL, not in row_filter."""
    plan = _optimize(
        'SELECT redact(a) AS a, "b" FROM "db"."tbl"',
        ["a", "b"],
        filters=col("a") > 5,
    )

    assert "WHERE" in plan.sql
    assert "a" in plan.source_columns


def test_non_pushable_inner_where_preserved():
    """Non-pushable inner WHERE → columns preserved in source_columns."""
    plan = _optimize('SELECT "a" FROM "db"."tbl" WHERE upper("x") = \'FOO\'', ["a"])

    assert "x" in plan.source_columns


def test_partial_extraction():
    """Mix of pushable and non-pushable → pushable extracted, rest stays."""
    plan = _optimize(
        'SELECT "a" FROM "db"."tbl" WHERE "x" > 5 AND upper("z") = \'FOO\'',
        ["a"],
    )

    filters = _collect_filters(plan.row_filter)
    assert GreaterThan("x", 5) in filters
    assert "z" in plan.source_columns


def test_residual_filter_keeps_needed_columns():
    """Filter on non-passthrough col not in user columns → both columns in source."""
    plan = _optimize(
        'SELECT redact(a) AS a, "b" FROM "db"."tbl"',
        ["b"],
        filters=col("a") > 5,
    )

    assert "WHERE" in plan.sql
    assert "a" in plan.source_columns
    assert "b" in plan.source_columns


# --- Combined projection and predicate pushdown ---


def test_full_example():
    """Transform with WHERE, user columns, and user filters → combined pushdown."""
    plan = _optimize(
        "SELECT redact(a) AS a, b, c, d FROM t WHERE e = 'foo'",
        columns=["a", "b"],
        filters=col("c") > 10,
    )

    filters = _collect_filters(plan.row_filter)
    assert EqualTo("e", "foo") in filters
    assert GreaterThan("c", 10) in filters
    assert "a" in plan.source_columns
    assert "b" in plan.source_columns
    assert "d" not in plan.source_columns


def test_unused_columns_pruned_with_filter():
    """Predicate on passthrough column is extracted; unrelated columns pruned."""
    plan = _optimize(
        'SELECT "a", "b", "c" FROM "db"."tbl"',
        ["a"],
        filters=col("b") > 5,
    )

    filters = _collect_filters(plan.row_filter)
    assert GreaterThan("b", 5) in filters
    assert "c" not in plan.source_columns


# --- OR handling ---


def test_or_both_pushable():
    """OR where both sides are pushable → entire OR in row_filter."""
    plan = _optimize(
        'SELECT "a", "b", "c" FROM "db"."tbl"',
        ["a"],
        filters=(col("b") > 5) | (col("c") < 10),
    )

    assert isinstance(plan.row_filter, Or), f"Expected Or, got {plan.row_filter!r}"


def test_or_one_non_pushable_stays():
    """OR where one side references non-passthrough → stays in SQL."""
    plan = _optimize(
        'SELECT redact(a) AS a, "b" FROM "db"."tbl"',
        ["a", "b"],
        filters=(col("a") > 5) | (col("b") < 10),
    )

    assert "WHERE" in plan.sql


# --- Filter types survive round trip ---


def test_each_filter_type_round_trips():
    """Each filter type survives Filter→SQL→optimize→row_filter round trip."""
    cases = [
        (col("x") == 1, EqualTo),
        (col("x") != 1, NotEqualTo),
        (col("x") > 1, GreaterThan),
        (col("x") >= 1, GreaterThanOrEqual),
        (col("x") < 1, LessThan),
        (col("x") <= 1, LessThanOrEqual),
        (col("x").is_null(), IsNull),
        (col("x").is_not_null(), IsNotNull),
        (col("x").is_in([1, 2, 3]), In),
        (col("x").between(1, 10), GreaterThanOrEqual),  # sqlglot decomposes BETWEEN
    ]
    for filter_expr, expected_type in cases:
        plan = _optimize('SELECT "a", "x" FROM "db"."tbl"', ["a"], filters=filter_expr)
        filters = _collect_filters(plan.row_filter)
        assert any(isinstance(f, expected_type) for f in filters), (
            f"Expected {expected_type} in {filters} for {filter_expr!r}"
        )


# --- No columns, no filters ---


def test_no_user_inputs():
    """Plain transform SQL, no columns, no filters → pass-through."""
    plan = _optimize('SELECT "a", "b" FROM "db"."tbl"')

    assert isinstance(plan.row_filter, AlwaysTrue)
    assert plan.source_columns == ["a", "b"]


def test_no_user_inputs_with_inner_where():
    """Transform SQL with inner WHERE, no user columns/filters → WHERE extracted."""
    plan = _optimize('SELECT "a", "b" FROM "db"."tbl" WHERE "c" = 1')

    filters = _collect_filters(plan.row_filter)
    assert EqualTo("c", 1) in filters


# --- Fallback ---


def test_invalid_sql_falls_back():
    """Invalid SQL → graceful fallback with None source columns and always_true."""
    plan = optimize_scan("NOT VALID SQL !!!")

    assert plan.source_columns is None
    assert isinstance(plan.row_filter, AlwaysTrue)
    assert plan.sql == "NOT VALID SQL !!!"


# --- BETWEEN decomposition ---


def test_between_decomposes():
    """BETWEEN decomposes into >= AND <=, both pushed to row_filter."""
    plan = _optimize(
        'SELECT "id", "value" FROM "db"."tbl"',
        ["id"],
        filters=col("value").between(1, 10),
    )

    filters = _collect_filters(plan.row_filter)
    assert GreaterThanOrEqual("value", 1) in filters
    assert LessThanOrEqual("value", 10) in filters

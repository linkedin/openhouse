"""Tests for scan_optimizer.optimize_scan."""

import re

import pytest
import sqlglot
from sqlglot.optimizer.scope import build_scope

import openhouse.dataloader.datafusion_sql  # noqa: F401 — registers DataFusion dialect
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
    _to_datafusion_sql,
)
from openhouse.dataloader.scan_optimizer import optimize_scan as _optimize_scan

_DIALECT = "datafusion"


def optimize_scan(sql: str) -> object:
    return _optimize_scan(sql, _DIALECT)


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


def test_unresolved_star_reads_all_columns():
    """When SELECT * cannot be expanded (no schema), all columns must be read."""
    plan = optimize_scan('SELECT * FROM (SELECT * FROM "db"."tbl" WHERE some_udf("tbl"."viewerId", now())) AS _t')

    assert plan.source_columns is None
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
        ('"x" = 1', EqualTo("x", 1)),
        ('"x" <> 1', NotEqualTo("x", 1)),
        ('"x" > 1', GreaterThan("x", 1)),
        ('"x" >= 1', GreaterThanOrEqual("x", 1)),
        ('"x" < 1', LessThan("x", 1)),
        ('"x" <= 1', LessThanOrEqual("x", 1)),
        ('"x" IS NULL', IsNull("x")),
        ('"x" IS NOT NULL', IsNotNull("x")),
        ('"x" IN (1, 2, 3)', In("x", (1, 2, 3))),
        ("\"x\" IN ('a', 'b')", In("x", ("a", "b"))),
        ('"x" BETWEEN 1 AND 10', And(LessThanOrEqual("x", 10), GreaterThanOrEqual("x", 1))),
        ("\"x\" = 'hello'", EqualTo("x", "hello")),
        ('"x" > 3.14', GreaterThan("x", 3.14)),
    ]
    for where_clause, expected_filter in cases:
        plan = optimize_scan(f'SELECT "a" FROM "db"."tbl" WHERE {where_clause}')
        assert plan.source_columns == ["a", "x"], f"source_columns mismatch for: {where_clause}"
        assert plan.row_filter == expected_filter, f"row_filter mismatch for: {where_clause}"


def test_non_convertible_predicates_not_pushed():
    """Predicates with functions or column-vs-column are not pushed."""
    cases = [
        "upper(\"x\") = 'FOO'",
        '"x" > "y"',
    ]
    for where_clause in cases:
        plan = optimize_scan(f'SELECT "a" FROM "db"."tbl" WHERE {where_clause}')
        assert isinstance(plan.row_filter, AlwaysTrue), f"Expected AlwaysTrue for: {where_clause}"


def test_filter_dsl_to_sql_round_trip():
    """Each Filter type survives _to_datafusion_sql → optimize_scan round trip."""
    cases = [
        EqualTo("x", 1),
        NotEqualTo("x", 1),
        GreaterThan("x", 1),
        GreaterThanOrEqual("x", 1),
        LessThan("x", 1),
        LessThanOrEqual("x", 1),
        EqualTo("x", "hello"),
        EqualTo("x", 3.14),
        IsNull("x"),
        IsNotNull("x"),
        In("x", (1, 2, 3)),
        And(LessThan("x", 5), GreaterThan("x", 1)),  # sqlglot may reorder operands
        Or(EqualTo("x", 1), EqualTo("x", 2)),
    ]
    for filter_dsl in cases:
        sql = f'SELECT "a" FROM "db"."tbl" WHERE {_to_datafusion_sql(filter_dsl)}'
        plan = optimize_scan(sql)
        assert plan.row_filter == filter_dsl, f"Round trip failed for {filter_dsl!r}: got {plan.row_filter!r}"


# --- Complex filter combinations ---


def test_or_of_ands():
    plan = optimize_scan('SELECT "a" FROM "db"."tbl" WHERE ("x" > 1 AND "y" = 2) OR ("z" < 3 AND "w" >= 4)')

    assert plan.source_columns == ["a", "w", "x", "y", "z"]
    assert plan.row_filter == Or(
        And(GreaterThanOrEqual("w", 4), LessThan("z", 3)),
        And(GreaterThan("x", 1), EqualTo("y", 2)),
    )


def test_double_nested():
    plan = optimize_scan('SELECT "a" FROM "db"."tbl" WHERE ("x" > 1 OR ("y" = 2 AND "z" < 3)) AND "w" >= 4')

    assert plan.source_columns == ["a", "w", "x", "y", "z"]
    assert plan.row_filter == And(
        GreaterThanOrEqual("w", 4),
        Or(GreaterThan("x", 1), And(EqualTo("y", 2), LessThan("z", 3))),
    )


def test_double_nested_with_non_pushable():
    plan = optimize_scan(
        'SELECT "a" FROM "db"."tbl" WHERE ("x" > 1 OR (upper("y") = \'FOO\' AND "z" < 3)) AND "w" >= 4'
    )

    assert plan.source_columns == ["a", "w", "x", "y", "z"]
    assert plan.row_filter == GreaterThanOrEqual("w", 4)


def test_and_of_ors():
    plan = optimize_scan('SELECT "a" FROM "db"."tbl" WHERE "x" > 1 AND ("y" = 2 OR "z" < 3) AND "w" >= 4')

    assert plan.source_columns == ["a", "w", "x", "y", "z"]
    assert plan.row_filter == And(
        And(GreaterThanOrEqual("w", 4), GreaterThan("x", 1)),
        Or(EqualTo("y", 2), LessThan("z", 3)),
    )


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
    with pytest.raises(ValueError, match="Expected exactly 1 table scan, found 2"):
        optimize_scan('SELECT * FROM "db"."t1" JOIN "db"."t2" ON "t1"."id" = "t2"."id"')


# --- Regression: predicate pushdown through nested subqueries ---
# Queries from https://docs.google.com/spreadsheets/d/1kUIA3b257Ne_ABg27H72_xzyUhkCzZr_3uWcrqk9nU0
#
# Bug: pushdown_predicates pushes outer WHERE clauses into inner scopes but
# does not rewrite alias references, leaving dangling references like
# "t"."memberId" inside a scope where only "tbl" exists.


def _assert_column_references_in_scope(sql: str) -> None:
    """Assert every table-qualified column reference resolves within its scope."""
    ast = sqlglot.parse_one(sql, dialect=_DIALECT)
    root = build_scope(ast)
    if root is None:
        return
    for scope in root.traverse():
        source_names = set(scope.sources.keys())
        for col_node in scope.columns:
            if col_node.table and col_node.table not in source_names:
                raise AssertionError(
                    f"Column '{col_node.sql(dialect=_DIALECT)}' references '{col_node.table}' "
                    f"not in scope {source_names}. Output SQL: {sql}"
                )


def test_pushdown_rewrites_alias_single_nesting():
    """Predicate referencing subquery alias must be rewritten when pushed into inner scope.

    Regression for: policy_coverage.MSFT_GAI_Model_Training
    """
    plan = optimize_scan(
        "SELECT * "
        "FROM (SELECT * "
        '      FROM "db"."tbl" AS "tbl" '
        '      WHERE some_udf(\'arg1\', "tbl"."memberId", now())) AS "t" '
        'WHERE some_udf(\'arg2\', "t"."memberId", now())'
    )
    _assert_column_references_in_scope(plan.sql)


def test_pushdown_rewrites_alias_double_nesting():
    """All outer aliases must be rewritten when predicates are pushed through two nesting levels.

    Regression for: policy_coverage.Flagship
    """
    plan = optimize_scan(
        "SELECT * "
        "FROM (SELECT * "
        "      FROM (SELECT * "
        '            FROM "db"."tbl" AS "tbl" '
        '            WHERE some_udf(\'arg1\', "tbl"."memberId", now())) AS "t" '
        '      WHERE some_udf(\'arg2\', "t"."memberId", now())) AS "t0" '
        'WHERE some_udf(\'arg3\', "t0"."memberId", now())'
    )
    _assert_column_references_in_scope(plan.sql)


# --- Regression: projection pushdown must quote mixed-case column names ---
#
# Bug: pushdown_projections expands SELECT * into explicit column names but
# does not quote them. DataFusion is case-sensitive and lowercases unquoted
# identifiers, so `memberId` becomes `memberid`. The outer SELECT references
# `"memberId"` (quoted), causing "column not found" errors.


def _assert_identifier_quoted(sql: str, identifier: str) -> None:
    """Assert a mixed-case identifier only appears inside double quotes in the SQL.

    DataFusion lowercases unquoted identifiers, so mixed-case names like
    ``memberId`` must be quoted as ``"memberId"`` to preserve case.
    """
    # Strip all quoted identifiers and string literals to find unquoted occurrences
    stripped = re.sub(r'"[^"]*"', '""', sql)
    stripped = re.sub(r"'[^']*'", "''", stripped)
    assert identifier not in stripped, (
        f"Unquoted '{identifier}' in SQL — DataFusion will lowercase to '{identifier.lower()}'. SQL: {sql}"
    )


def test_projection_pushdown_quotes_mixed_case_columns():
    """pushdown_projections expanding SELECT * must quote mixed-case column names.

    Regression for: integration_test.fla/Ads_Safe,
    policy_coverage.Ads_Microsoft_Data_Sharing_entity_annotation
    """
    plan = optimize_scan(
        'SELECT "t"."memberId", "t"."policyField" '
        "FROM (SELECT * "
        '      FROM "db"."tbl" AS "tbl" '
        '      WHERE some_udf("tbl"."memberId", now())) AS "t"'
    )
    _assert_identifier_quoted(plan.sql, "memberId")
    _assert_identifier_quoted(plan.sql, "policyField")


def test_projection_pushdown_quotes_columns_with_redact_udf():
    """Projection + redact UDF: inner SELECT * expansion must preserve column casing.

    Regression for: policy_coverage.Ads_Sales_Insights_And_attributions
    """
    plan = optimize_scan(
        'SELECT "t"."policyField", '
        '       redact_field_if(NOT some_udf(\'arg\', "t"."memberId", now()), '
        '                       "t"."unknownField", \'unknownField\', NULL) AS "unknownField", '
        '       "t"."memberId" '
        "FROM (SELECT * "
        '      FROM "db"."tbl" AS "tbl" '
        '      WHERE some_udf("tbl"."memberId", now())) AS "t"'
    )
    _assert_identifier_quoted(plan.sql, "memberId")
    _assert_identifier_quoted(plan.sql, "unknownField")


def test_projection_pushdown_quotes_many_mixed_case_columns():
    """Wide table with many mixed-case columns: all must be quoted after projection expansion.

    Regression for: u_adsdl.FixedClicktrainingdatadma/ads_audience
    """
    plan = optimize_scan(
        'SELECT "t"."campaignId", "t"."memberId", "t"."channelId" '
        "FROM (SELECT * "
        '      FROM "db"."tbl" AS "tbl" '
        '      WHERE some_udf("tbl"."memberId", now())) AS "t"'
    )
    for col_name in ("campaignId", "memberId", "channelId"):
        _assert_identifier_quoted(plan.sql, col_name)

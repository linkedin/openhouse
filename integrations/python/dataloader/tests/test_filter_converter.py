"""Tests for _filter_converter: sqlglot AST → Filter DSL conversion."""

import pytest
import sqlglot
from sqlglot import exp

from openhouse.dataloader._filter_converter import convert
from openhouse.dataloader.filters import (
    And,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNotNull,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    Or,
)

_DIALECT = "datafusion"


def _parse_where(sql_fragment: str) -> exp.Where:
    """Parse a WHERE fragment and return the Where node."""
    ast = sqlglot.parse_one(f"SELECT * FROM t WHERE {sql_fragment}", dialect=_DIALECT)
    where = ast.find(exp.Where)
    assert where is not None
    return where


def _parse_expr(sql_fragment: str) -> exp.Expression:
    """Parse a WHERE fragment and return the inner expression (not the Where wrapper)."""
    return _parse_where(sql_fragment).this


# --- convert: comparisons ---


@pytest.mark.parametrize(
    "sql,expected",
    [
        ('"x" = 1', EqualTo("x", 1)),
        ('"x" <> 1', NotEqualTo("x", 1)),
        ('"x" > 1', GreaterThan("x", 1)),
        ('"x" >= 1', GreaterThanOrEqual("x", 1)),
        ('"x" < 1', LessThan("x", 1)),
        ('"x" <= 1', LessThanOrEqual("x", 1)),
    ],
    ids=["eq", "neq", "gt", "gte", "lt", "lte"],
)
def test_comparison(sql, expected):
    assert convert(_parse_expr(sql)) == expected


def test_string_literal():
    assert convert(_parse_expr("\"x\" = 'hello'")) == EqualTo("x", "hello")


def test_float_literal():
    assert convert(_parse_expr('"x" > 3.14')) == GreaterThan("x", 3.14)


# --- convert: flipped comparisons (literal on left) ---


def test_flipped_gt():
    assert convert(_parse_expr('5 > "x"')) == LessThan("x", 5)


def test_flipped_lt():
    assert convert(_parse_expr('5 < "x"')) == GreaterThan("x", 5)


def test_flipped_eq():
    assert convert(_parse_expr('1 = "x"')) == EqualTo("x", 1)


# --- convert: IS NULL / IS NOT NULL ---


def test_is_null():
    assert convert(_parse_expr('"x" IS NULL')) == IsNull("x")


def test_is_not_null():
    assert convert(_parse_expr('"x" IS NOT NULL')) == IsNotNull("x")


# --- convert: IN ---


def test_in():
    assert convert(_parse_expr('"x" IN (1, 2, 3)')) == In("x", (1, 2, 3))


def test_in_strings():
    assert convert(_parse_expr("\"x\" IN ('a', 'b')")) == In("x", ("a", "b"))


def test_in_with_non_literal_returns_none():
    """IN with a subquery or expression is not convertible."""
    assert convert(_parse_expr('"x" IN (1, "y")')) is None


# --- convert: logical operators ---


def test_and():
    result = convert(_parse_expr('"x" > 1 AND "y" = 2'))
    assert result == And(GreaterThan("x", 1), EqualTo("y", 2))


def test_or():
    result = convert(_parse_expr('"x" > 1 OR "y" = 2'))
    assert result == Or(GreaterThan("x", 1), EqualTo("y", 2))


def test_not():
    result = convert(_parse_expr('NOT "x" > 1'))
    assert result == Not(GreaterThan("x", 1))


def test_and_with_non_convertible_returns_none():
    """AND where one side is non-convertible → None (can't partially convert AND)."""
    assert convert(_parse_expr('"x" > 1 AND upper("y") = \'FOO\'')) is None


def test_or_with_non_convertible_returns_none():
    assert convert(_parse_expr('"x" > 1 OR upper("y") = \'FOO\'')) is None


# --- convert: non-convertible expressions ---


def test_function_on_column_returns_none():
    assert convert(_parse_expr("upper(\"x\") = 'FOO'")) is None


def test_column_vs_column_returns_none():
    assert convert(_parse_expr('"x" > "y"')) is None


def test_is_null_on_expression_returns_none():
    assert convert(_parse_expr('upper("x") IS NULL')) is None


# --- Filter DSL → SQL → AST → Filter round trip ---


@pytest.mark.parametrize(
    "filter_dsl,expected",
    [
        (EqualTo("a", 1), EqualTo("a", 1)),
        (NotEqualTo("a", 1), NotEqualTo("a", 1)),
        (GreaterThan("a", 1), GreaterThan("a", 1)),
        (GreaterThanOrEqual("a", 1), GreaterThanOrEqual("a", 1)),
        (LessThan("a", 1), LessThan("a", 1)),
        (LessThanOrEqual("a", 1), LessThanOrEqual("a", 1)),
        (EqualTo("a", "hello"), EqualTo("a", "hello")),
        (EqualTo("a", 3.14), EqualTo("a", 3.14)),
        (IsNull("a"), IsNull("a")),
        (IsNotNull("a"), IsNotNull("a")),
        (In("a", (1, 2, 3)), In("a", (1, 2, 3))),
        (Not(GreaterThan("a", 1)), Not(GreaterThan("a", 1))),
        (And(GreaterThan("a", 1), LessThan("b", 5)), And(GreaterThan("a", 1), LessThan("b", 5))),
        (Or(EqualTo("a", 1), EqualTo("b", 2)), Or(EqualTo("a", 1), EqualTo("b", 2))),
    ],
    ids=[
        "eq",
        "neq",
        "gt",
        "gte",
        "lt",
        "lte",
        "string",
        "float",
        "is_null",
        "is_not_null",
        "in",
        "not",
        "and",
        "or",
    ],
)
def test_filter_to_sql_round_trip(filter_dsl, expected):
    """Each filter type survives _to_datafusion_sql → parse → convert round trip."""
    sql = filter_dsl._to_datafusion_sql()
    result = convert(_parse_expr(sql))
    assert result == expected, f"Round trip failed for {filter_dsl!r}: got {result!r}"

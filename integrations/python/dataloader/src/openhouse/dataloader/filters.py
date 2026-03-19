from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from pyiceberg import expressions as ice


class Filter(ABC):
    """Abstract base for all filter expressions.

    Supports ``&`` (and), ``|`` (or), and ``~`` (not) operators for combining filters.
    """

    @abstractmethod
    def __repr__(self) -> str:
        pass

    def __and__(self, other: Filter) -> And:
        return And(self, other)

    def __or__(self, other: Filter) -> Or:
        return Or(self, other)

    def __invert__(self) -> Not:
        return Not(self)


class AlwaysTrue(Filter):
    """A filter that matches all rows."""

    def __repr__(self) -> str:
        return "always_true()"


def always_true() -> AlwaysTrue:
    """Create a filter that matches all rows."""
    return AlwaysTrue()


def col(name: str) -> Column:
    """Create a column reference for building row filter expressions.

    Example::

        # Comparisons: ==, !=, >, >=, <, <=
        col("age") > 21
        col("country") == "US"

        # Predicates
        col("email").is_null()
        col("email").is_not_null()
        col("value").is_nan()
        col("status").is_in(["active", "pending"])
        col("name").starts_with("John")
        col("score").between(0.5, 1.0)

        # Combine with & (AND), | (OR), ~ (NOT)
        (col("age") > 21) & (col("country") == "US") | ~col("email").is_null()

    Args:
        name: The column name to filter on.
    """
    return Column(name)


@dataclass(frozen=True)
class Column:
    """A column reference that supports comparison and predicate operators.

    ``__eq__`` and ``__ne__`` are overridden to return Filter objects
    instead of bool, enabling the ``col("x") == value`` syntax for
    building filter expressions. As a side effect, Column instances
    should not be used in sets or as dict keys expecting equality
    semantics.
    """

    name: str

    def __repr__(self) -> str:
        return f"col('{self.name}')"

    def __eq__(self, value: Any) -> EqualTo:  # type: ignore[override]
        return EqualTo(self.name, value)

    def __ne__(self, value: Any) -> NotEqualTo:  # type: ignore[override]
        return NotEqualTo(self.name, value)

    def __gt__(self, value: Any) -> GreaterThan:
        return GreaterThan(self.name, value)

    def __ge__(self, value: Any) -> GreaterThanOrEqual:
        return GreaterThanOrEqual(self.name, value)

    def __lt__(self, value: Any) -> LessThan:
        return LessThan(self.name, value)

    def __le__(self, value: Any) -> LessThanOrEqual:
        return LessThanOrEqual(self.name, value)

    def __hash__(self) -> int:
        return hash(self.name)

    def is_null(self) -> IsNull:
        return IsNull(self.name)

    def is_not_null(self) -> IsNotNull:
        return IsNotNull(self.name)

    def is_nan(self) -> IsNaN:
        return IsNaN(self.name)

    def is_not_nan(self) -> IsNotNaN:
        return IsNotNaN(self.name)

    def is_in(self, values: list | tuple | set) -> In:
        return In(self.name, tuple(values))

    def is_not_in(self, values: list | tuple | set) -> NotIn:
        return NotIn(self.name, tuple(values))

    def starts_with(self, prefix: str) -> StartsWith:
        return StartsWith(self.name, prefix)

    def not_starts_with(self, prefix: str) -> NotStartsWith:
        return NotStartsWith(self.name, prefix)

    def between(self, lower: Any, upper: Any) -> Between:
        return Between(self.name, lower, upper)


# --- Comparison filters ---


@dataclass(frozen=True)
class EqualTo(Filter):
    column: str
    value: Any

    def __repr__(self) -> str:
        return f"col('{self.column}') == {self.value!r}"


@dataclass(frozen=True)
class NotEqualTo(Filter):
    column: str
    value: Any

    def __repr__(self) -> str:
        return f"col('{self.column}') != {self.value!r}"


@dataclass(frozen=True)
class GreaterThan(Filter):
    column: str
    value: Any

    def __repr__(self) -> str:
        return f"col('{self.column}') > {self.value!r}"


@dataclass(frozen=True)
class GreaterThanOrEqual(Filter):
    column: str
    value: Any

    def __repr__(self) -> str:
        return f"col('{self.column}') >= {self.value!r}"


@dataclass(frozen=True)
class LessThan(Filter):
    column: str
    value: Any

    def __repr__(self) -> str:
        return f"col('{self.column}') < {self.value!r}"


@dataclass(frozen=True)
class LessThanOrEqual(Filter):
    column: str
    value: Any

    def __repr__(self) -> str:
        return f"col('{self.column}') <= {self.value!r}"


# --- Null/NaN check filters ---


@dataclass(frozen=True)
class IsNull(Filter):
    column: str

    def __repr__(self) -> str:
        return f"col('{self.column}').is_null()"


@dataclass(frozen=True)
class IsNotNull(Filter):
    column: str

    def __repr__(self) -> str:
        return f"col('{self.column}').is_not_null()"


@dataclass(frozen=True)
class IsNaN(Filter):
    column: str

    def __repr__(self) -> str:
        return f"col('{self.column}').is_nan()"


@dataclass(frozen=True)
class IsNotNaN(Filter):
    column: str

    def __repr__(self) -> str:
        return f"col('{self.column}').is_not_nan()"


# --- Set membership filters ---


@dataclass(frozen=True)
class In(Filter):
    column: str
    values: tuple

    def __repr__(self) -> str:
        return f"col('{self.column}').is_in({list(self.values)!r})"


@dataclass(frozen=True)
class NotIn(Filter):
    column: str
    values: tuple

    def __repr__(self) -> str:
        return f"col('{self.column}').is_not_in({list(self.values)!r})"


# --- String prefix filters ---


@dataclass(frozen=True)
class StartsWith(Filter):
    column: str
    prefix: str

    def __repr__(self) -> str:
        return f"col('{self.column}').starts_with({self.prefix!r})"


@dataclass(frozen=True)
class NotStartsWith(Filter):
    column: str
    prefix: str

    def __repr__(self) -> str:
        return f"col('{self.column}').not_starts_with({self.prefix!r})"


# --- Range filter ---


@dataclass(frozen=True)
class Between(Filter):
    column: str
    lower: Any
    upper: Any

    def __repr__(self) -> str:
        return f"col('{self.column}').between({self.lower!r}, {self.upper!r})"


# --- Logical combinators ---


@dataclass(frozen=True)
class And(Filter):
    left: Filter
    right: Filter

    def __repr__(self) -> str:
        return f"({self.left!r} & {self.right!r})"


@dataclass(frozen=True)
class Or(Filter):
    left: Filter
    right: Filter

    def __repr__(self) -> str:
        return f"({self.left!r} | {self.right!r})"


@dataclass(frozen=True)
class Not(Filter):
    operand: Filter

    def __repr__(self) -> str:
        return f"~{self.operand!r}"


# ---------------------------------------------------------------------------
# Filter → SQL conversion
# ---------------------------------------------------------------------------

_COMPARISON_OPS: dict[type, str] = {
    EqualTo: "=",
    NotEqualTo: "!=",
    GreaterThan: ">",
    GreaterThanOrEqual: ">=",
    LessThan: "<",
    LessThanOrEqual: "<=",
}


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise TypeError(f"Unsupported literal type: {type(value).__name__}")


def _escape_like(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _quote_sql_identifier(name: str) -> str:
    """Escape a SQL identifier by doubling embedded double quotes and wrapping in double quotes."""
    return '"' + name.replace('"', '""') + '"'


def _filter_to_sql(f: Filter) -> str:
    """Convert a Filter to a SQL WHERE clause."""
    for filter_type, op in _COMPARISON_OPS.items():
        if isinstance(f, filter_type):
            return f"{_quote_sql_identifier(f.column)} {op} {_sql_literal(f.value)}"  # type: ignore[attr-defined]

    match f:
        case AlwaysTrue():
            return "TRUE"
        case IsNull(column=col):
            return f"{_quote_sql_identifier(col)} IS NULL"
        case IsNotNull(column=col):
            return f"{_quote_sql_identifier(col)} IS NOT NULL"
        case IsNaN(column=col):
            return f"isnan({_quote_sql_identifier(col)})"
        case IsNotNaN(column=col):
            return f"NOT isnan({_quote_sql_identifier(col)})"
        case In(column=col, values=vals):
            items = ", ".join(_sql_literal(v) for v in vals)
            return f"{_quote_sql_identifier(col)} IN ({items})"
        case NotIn(column=col, values=vals):
            items = ", ".join(_sql_literal(v) for v in vals)
            return f"{_quote_sql_identifier(col)} NOT IN ({items})"
        case StartsWith(column=col, prefix=pfx):
            return f"{_quote_sql_identifier(col)} LIKE '{_escape_like(pfx)}%'"
        case NotStartsWith(column=col, prefix=pfx):
            return f"{_quote_sql_identifier(col)} NOT LIKE '{_escape_like(pfx)}%'"
        case Between(column=col, lower=lo, upper=hi):
            return f"{_quote_sql_identifier(col)} BETWEEN {_sql_literal(lo)} AND {_sql_literal(hi)}"
        case And(left=left, right=right):
            return f"({_filter_to_sql(left)} AND {_filter_to_sql(right)})"
        case Or(left=left, right=right):
            return f"({_filter_to_sql(left)} OR {_filter_to_sql(right)})"
        case Not(operand=inner):
            return f"NOT ({_filter_to_sql(inner)})"
        case _:
            raise TypeError(f"Unsupported filter type: {type(f).__name__}")


# ---------------------------------------------------------------------------
# Filter → PyIceberg conversion
# ---------------------------------------------------------------------------


def _to_pyiceberg(expr: Filter) -> ice.BooleanExpression:
    """Convert a Filter expression tree to a PyIceberg BooleanExpression.

    PyIceberg constructors accept ``str`` for the ``term`` parameter at runtime
    (auto-wrapped to ``UnboundTerm``), but the type stubs don't reflect this, so
    we suppress type checking for the body of this function.
    See https://github.com/apache/iceberg-python/issues/3101
    """
    result: ice.BooleanExpression = _to_pyiceberg_impl(expr)
    return result


# Separated so @no_type_check only applies to the pyiceberg interop, not the public signature.
@typing.no_type_check
def _to_pyiceberg_impl(expr: Filter) -> ice.BooleanExpression:
    match expr:
        case AlwaysTrue():
            return ice.AlwaysTrue()

        # Comparison
        case EqualTo(column, value):
            return ice.EqualTo(column, value)
        case NotEqualTo(column, value):
            return ice.NotEqualTo(column, value)
        case GreaterThan(column, value):
            return ice.GreaterThan(column, value)
        case GreaterThanOrEqual(column, value):
            return ice.GreaterThanOrEqual(column, value)
        case LessThan(column, value):
            return ice.LessThan(column, value)
        case LessThanOrEqual(column, value):
            return ice.LessThanOrEqual(column, value)

        # Null / NaN
        case IsNull(column):
            return ice.IsNull(column)
        case IsNotNull(column):
            return ice.NotNull(column)
        case IsNaN(column):
            return ice.IsNaN(column)
        case IsNotNaN(column):
            return ice.NotNaN(column)

        # Set membership
        case In(column, values):
            return ice.In(column, values)
        case NotIn(column, values):
            return ice.NotIn(column, values)

        # String prefix
        case StartsWith(column, prefix):
            return ice.StartsWith(column, prefix)
        case NotStartsWith(column, prefix):
            return ice.NotStartsWith(column, prefix)

        # Range — no native PyIceberg Between; decompose
        case Between(column, lower, upper):
            return ice.And(ice.GreaterThanOrEqual(column, lower), ice.LessThanOrEqual(column, upper))

        # Logical combinators
        case And(left, right):
            return ice.And(_to_pyiceberg(left), _to_pyiceberg(right))
        case Or(left, right):
            return ice.Or(_to_pyiceberg(left), _to_pyiceberg(right))
        case Not(operand):
            return ice.Not(_to_pyiceberg(operand))

        case _:
            raise TypeError(f"Unsupported filter type: {type(expr).__name__}")

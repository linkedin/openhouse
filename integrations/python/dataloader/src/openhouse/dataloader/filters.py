from __future__ import annotations

import math
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pyiceberg import expressions as ice
from sqlglot import exp


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


# --- Conversion functions ---


def _quote_identifier(name: str) -> str:
    """Escape a SQL identifier using sqlglot."""
    return exp.to_identifier(name, quoted=True).sql()


def _escape_like(value: str) -> str:
    """Escape LIKE-special characters so they are matched literally."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _non_finite_double(value: float | Decimal) -> exp.Cast:
    """Build ``CAST('<canonical>' AS DOUBLE)`` for a non-finite float/decimal.

    Uses the canonical ``NaN``/``Infinity``/``-Infinity`` spellings, which both
    DataFusion and Spark parse. The lowercase forms from ``str(float(...))``
    (e.g. ``'inf'``) are not reliably cast by Spark.
    """
    if isinstance(value, Decimal):
        text = str(value)  # 'NaN' / 'Infinity' / '-Infinity'
    elif math.isnan(value):
        text = "NaN"
    elif value > 0:
        text = "Infinity"
    else:
        text = "-Infinity"
    return exp.Cast(this=exp.Literal.string(text), to=exp.DataType.build("DOUBLE"))


def _literal_to_expr(value: object) -> exp.Expression:
    """Convert a Python literal to a sqlglot literal expression.

    Datetime/date/time values are emitted as plain string literals (ISO format).
    DataFusion implicitly coerces string literals to the column type at execution,
    and PyIceberg promotes StringLiteral to the matching typed literal during expression binding.
    """
    if isinstance(value, str):
        return exp.Literal.string(value)
    if isinstance(value, bool):
        return exp.true() if value else exp.false()
    if isinstance(value, datetime):
        return exp.Literal.string(value.isoformat())
    if isinstance(value, date):
        return exp.Literal.string(value.isoformat())
    if isinstance(value, time):
        if value.tzinfo is not None:
            raise TypeError(
                "The SQL target does not support timezones for time data types. "
                "The time should match the timezone used in the dataset."
            )
        return exp.Literal.string(value.isoformat())
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            return _non_finite_double(value)
        return exp.Literal.number(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            return _non_finite_double(value)
        return exp.Literal.number(value)
    if isinstance(value, UUID):
        return exp.Literal.string(str(value))
    raise TypeError(f"Unsupported literal type: {type(value).__name__}")


def _column_expr(name: str) -> exp.Column:
    """Build a quoted sqlglot column reference."""
    return exp.column(exp.to_identifier(name, quoted=True))


def _like_prefix(column: str, prefix: str) -> exp.Expression:
    r"""Build ``col LIKE 'prefix%' ESCAPE '\'`` with the prefix matched literally."""
    pattern = exp.Literal.string(_escape_like(prefix) + "%")
    like = exp.Like(this=_column_expr(column), expression=pattern)
    return exp.Escape(this=like, expression=exp.Literal.string("\\"))


def _filter_to_expr(filter_expr: Filter) -> exp.Expression:
    """Build a dialect-agnostic sqlglot expression tree for a Filter.

    Render it for a target by calling ``.sql(dialect=...)``; see :func:`to_sql`
    and :func:`_to_datafusion_sql`. The tree is built once and sqlglot handles
    per-dialect identifier quoting, operators, and literal formatting.
    """
    match filter_expr:
        case AlwaysTrue():
            return exp.true()

        # Comparison
        case EqualTo(column, value):
            return exp.EQ(this=_column_expr(column), expression=_literal_to_expr(value))
        case NotEqualTo(column, value):
            return exp.NEQ(this=_column_expr(column), expression=_literal_to_expr(value))
        case GreaterThan(column, value):
            return exp.GT(this=_column_expr(column), expression=_literal_to_expr(value))
        case GreaterThanOrEqual(column, value):
            return exp.GTE(this=_column_expr(column), expression=_literal_to_expr(value))
        case LessThan(column, value):
            return exp.LT(this=_column_expr(column), expression=_literal_to_expr(value))
        case LessThanOrEqual(column, value):
            return exp.LTE(this=_column_expr(column), expression=_literal_to_expr(value))

        # Null / NaN. NaN uses the isnan() function (an Anonymous node, not sqlglot's
        # built-in IsNan which renders the unsupported `IS_NAN(...)`); isnan() is
        # accepted by both DataFusion and Spark.
        case IsNull(column):
            return exp.Is(this=_column_expr(column), expression=exp.Null())
        case IsNotNull(column):
            return exp.not_(exp.Is(this=_column_expr(column), expression=exp.Null()))
        case IsNaN(column):
            return exp.Anonymous(this="isnan", expressions=[_column_expr(column)])
        case IsNotNaN(column):
            return exp.not_(exp.Anonymous(this="isnan", expressions=[_column_expr(column)]))

        # Set membership
        case In(column, values):
            return exp.In(this=_column_expr(column), expressions=[_literal_to_expr(v) for v in values])
        case NotIn(column, values):
            return exp.not_(exp.In(this=_column_expr(column), expressions=[_literal_to_expr(v) for v in values]))

        # String prefix
        case StartsWith(column, prefix):
            return _like_prefix(column, prefix)
        case NotStartsWith(column, prefix):
            return exp.not_(_like_prefix(column, prefix))

        # Range
        case Between(column, lower, upper):
            return exp.Between(
                this=_column_expr(column),
                low=_literal_to_expr(lower),
                high=_literal_to_expr(upper),
            )

        # Logical combinators
        case And(left, right):
            return exp.and_(_filter_to_expr(left), _filter_to_expr(right))
        case Or(left, right):
            return exp.or_(_filter_to_expr(left), _filter_to_expr(right))
        case Not(operand):
            return exp.not_(_filter_to_expr(operand))

        case _:
            raise TypeError(f"Unsupported filter type: {type(filter_expr).__name__}")


class SqlTarget(Enum):
    """Target for :func:`to_sql`.

    Members name a concrete SQL flavor; the underlying SQL dialect is an internal
    implementation detail and is not part of the public contract.
    """

    SPARK = "spark"


def to_sql(filter_expr: Filter, target: SqlTarget = SqlTarget.SPARK) -> str:
    """Render a filter as a SQL boolean expression for the given target.

    The result is a WHERE-clause-ready predicate (without a leading ``WHERE``).

    Example::

        to_sql(col("age") > 21)                       # `age` > 21
        to_sql((col("a") == 1) & col("b").is_null())  # `a` = 1 AND `b` IS NULL

    Args:
        filter_expr: The filter expression to render.
        target: The SQL flavor to render for. Defaults to Spark.
    """
    return _filter_to_expr(filter_expr).sql(dialect=target.value)


def _to_datafusion_sql(expr: Filter) -> str:
    """Render a Filter as a DataFusion SQL boolean expression string (default dialect)."""
    return _filter_to_expr(expr).sql()


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

"""Public filter API for OpenHouseDataLoader.

Provides a pandas/PySpark-style operator-overloading interface for building
row filter expressions.

Usage::

    from openhouse.dataloader import col

    filters = (col("age") > 21) & (col("country") == "US")
    loader = OpenHouseDataLoader("db", "table", filters=filters)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


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


def col(name: str) -> Column:
    """Create a column reference for building filter expressions.

    Args:
        name: The column name.

    Returns:
        A Column object that supports comparison operators.
    """
    return Column(name)


@dataclass(frozen=True)
class Column(Filter):
    """A column reference that supports comparison and predicate operators."""

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

"""Build a combined SQL query from transform SQL, user columns, and user filters."""

from __future__ import annotations

from collections.abc import Sequence

from openhouse.dataloader.filters import (
    AlwaysTrue,
    And,
    Between,
    EqualTo,
    Filter,
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


def _quote_identifier(name: str) -> str:
    """Escape a SQL identifier by doubling embedded double quotes and wrapping in double quotes."""
    return '"' + name.replace('"', '""') + '"'


def build_combined_query(
    transform_sql: str,
    columns: Sequence[str] | None = None,
    filters: Filter | None = None,
) -> str:
    """Wrap transform SQL as a subquery and apply user columns and filters.

    Args:
        transform_sql: DataFusion-dialect SQL from the table transformer.
        columns: User-requested output columns, or None for all.
        filters: User-provided row filters, or None.

    Returns:
        A combined SQL string: ``SELECT <cols|*> FROM (<transform_sql>) AS _t [WHERE <filters>]``
    """
    outer_cols = ", ".join(_quote_identifier(c) for c in columns) if columns else "*"
    sql = f"SELECT {outer_cols} FROM ({transform_sql}) AS _t"
    if filters and not isinstance(filters, AlwaysTrue):
        sql += f" WHERE {_filter_to_sql(filters)}"
    return sql


def _literal_to_sql(value: object) -> str:
    """Convert a Python literal to a SQL literal string."""
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)


def _filter_to_sql(f: Filter) -> str:
    """Convert a Filter DSL expression to a SQL WHERE clause string."""
    match f:
        case AlwaysTrue():
            return "TRUE"
        case EqualTo(column, value):
            return f"{_quote_identifier(column)} = {_literal_to_sql(value)}"
        case NotEqualTo(column, value):
            return f"{_quote_identifier(column)} <> {_literal_to_sql(value)}"
        case GreaterThan(column, value):
            return f"{_quote_identifier(column)} > {_literal_to_sql(value)}"
        case GreaterThanOrEqual(column, value):
            return f"{_quote_identifier(column)} >= {_literal_to_sql(value)}"
        case LessThan(column, value):
            return f"{_quote_identifier(column)} < {_literal_to_sql(value)}"
        case LessThanOrEqual(column, value):
            return f"{_quote_identifier(column)} <= {_literal_to_sql(value)}"
        case IsNull(column):
            return f"{_quote_identifier(column)} IS NULL"
        case IsNotNull(column):
            return f"{_quote_identifier(column)} IS NOT NULL"
        case In(column, values):
            vals = ", ".join(_literal_to_sql(v) for v in values)
            return f"{_quote_identifier(column)} IN ({vals})"
        case Between(column, lower, upper):
            return f"{_quote_identifier(column)} BETWEEN {_literal_to_sql(lower)} AND {_literal_to_sql(upper)}"
        case And(left, right):
            return f"({_filter_to_sql(left)} AND {_filter_to_sql(right)})"
        case Or(left, right):
            return f"({_filter_to_sql(left)} OR {_filter_to_sql(right)})"
        case Not(operand):
            return f"NOT ({_filter_to_sql(operand)})"
        case _:
            raise TypeError(f"Cannot convert filter to SQL: {type(f).__name__}")

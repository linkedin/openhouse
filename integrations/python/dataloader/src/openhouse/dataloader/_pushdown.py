"""Pushdown analysis using DataFusion's query optimizer.

Determines which column projections and row filters can be pushed down to the
Iceberg scan when a table transformer is applied. Builds a combined SQL query,
optimizes it via DataFusion, and extracts pushdowns from the TableScan node.
"""

from __future__ import annotations

import typing
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from datafusion import SessionContext
from pyiceberg import expressions as ice
from pyiceberg.schema import Schema

from openhouse.dataloader.data_loader_split import _quote_identifier, to_sql_identifier
from openhouse.dataloader.filters import (
    AlwaysTrue,
    And,
    Between,
    EqualTo,
    Filter,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNotNaN,
    IsNotNull,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotStartsWith,
    Or,
    StartsWith,
)
from openhouse.dataloader.table_identifier import TableIdentifier

_SUBQUERY_ALIAS = "_oh_transformed"


@dataclass(frozen=True)
class ScanPushdown:
    """Results of pushdown analysis for an Iceberg scan through a transformer.

    Attributes:
        scan_columns: Column names that the Iceberg scan must read.
        scan_filter: Row filter to push down to the Iceberg scan (best-effort).
        combined_sql: The full SQL query for split-level execution.
    """

    scan_columns: tuple[str, ...]
    scan_filter: ice.BooleanExpression
    combined_sql: str


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


def _filter_to_sql(f: Filter) -> str:
    """Convert a Filter expression tree to a SQL WHERE clause."""
    for filter_type, op in _COMPARISON_OPS.items():
        if isinstance(f, filter_type):
            return f"{_quote_identifier(f.column)} {op} {_sql_literal(f.value)}"  # type: ignore[attr-defined]

    match f:
        case AlwaysTrue():
            return "TRUE"
        case IsNull(column=col):
            return f"{_quote_identifier(col)} IS NULL"
        case IsNotNull(column=col):
            return f"{_quote_identifier(col)} IS NOT NULL"
        case IsNaN(column=col):
            return f"isnan({_quote_identifier(col)})"
        case IsNotNaN(column=col):
            return f"NOT isnan({_quote_identifier(col)})"
        case In(column=col, values=vals):
            items = ", ".join(_sql_literal(v) for v in vals)
            return f"{_quote_identifier(col)} IN ({items})"
        case NotIn(column=col, values=vals):
            items = ", ".join(_sql_literal(v) for v in vals)
            return f"{_quote_identifier(col)} NOT IN ({items})"
        case StartsWith(column=col, prefix=pfx):
            return f"{_quote_identifier(col)} LIKE '{_escape_like(pfx)}%'"
        case NotStartsWith(column=col, prefix=pfx):
            return f"{_quote_identifier(col)} NOT LIKE '{_escape_like(pfx)}%'"
        case Between(column=col, lower=lo, upper=hi):
            return f"{_quote_identifier(col)} BETWEEN {_sql_literal(lo)} AND {_sql_literal(hi)}"
        case And(left=left, right=right):
            return f"({_filter_to_sql(left)} AND {_filter_to_sql(right)})"
        case Or(left=left, right=right):
            return f"({_filter_to_sql(left)} OR {_filter_to_sql(right)})"
        case Not(operand=inner):
            return f"NOT ({_filter_to_sql(inner)})"
        case _:
            raise TypeError(f"Unsupported filter type: {type(f).__name__}")


# ---------------------------------------------------------------------------
# DataFusion Expr → PyIceberg conversion
# ---------------------------------------------------------------------------

_DF_OP_TO_ICE: dict[str, type] = {
    "=": ice.EqualTo,
    "!=": ice.NotEqualTo,
    ">": ice.GreaterThan,
    ">=": ice.GreaterThanOrEqual,
    "<": ice.LessThan,
    "<=": ice.LessThanOrEqual,
}


@typing.no_type_check
def _df_expr_to_pyiceberg(expr: Any) -> ice.BooleanExpression | None:
    """Best-effort conversion of a DataFusion Expr to a PyIceberg BooleanExpression.

    Returns None for expressions that can't be represented in PyIceberg.
    """
    variant = expr.variant_name()

    if variant == "BinaryExpr":
        v = expr.to_variant()
        op = v.op()

        if op == "AND":
            left_ice = _df_expr_to_pyiceberg(v.left())
            right_ice = _df_expr_to_pyiceberg(v.right())
            if left_ice and right_ice:
                return ice.And(left_ice, right_ice)
            return left_ice or right_ice

        if op == "OR":
            left_ice = _df_expr_to_pyiceberg(v.left())
            right_ice = _df_expr_to_pyiceberg(v.right())
            return ice.Or(left_ice, right_ice) if left_ice and right_ice else None

        ice_type = _DF_OP_TO_ICE.get(op)
        if ice_type and v.left().variant_name() == "Column" and v.right().variant_name() == "Literal":
            py_value = v.right().python_value()
            if isinstance(py_value, pa.Scalar):
                py_value = py_value.as_py()
            return ice_type(v.left().to_variant().name(), py_value)

    elif variant == "IsNull":
        inner = expr.to_variant().expr()
        if inner.variant_name() == "Column":
            return ice.IsNull(inner.to_variant().name())

    elif variant == "IsNotNull":
        inner = expr.to_variant().expr()
        if inner.variant_name() == "Column":
            return ice.NotNull(inner.to_variant().name())

    elif variant == "Not":
        inner_ice = _df_expr_to_pyiceberg(expr.to_variant().expr())
        if inner_ice:
            return ice.Not(inner_ice)

    return None


# ---------------------------------------------------------------------------
# Plan walking
# ---------------------------------------------------------------------------


def _find_table_scan(plan: Any) -> Any | None:
    """Walk the logical plan and return the first TableScan variant, or None."""
    v = plan.to_variant()
    if type(v).__name__ == "TableScan":
        return v
    for inp in plan.inputs():
        found = _find_table_scan(inp)
        if found is not None:
            return found
    return None


def _find_filter_above_scan(plan: Any) -> Any | None:
    """Return the Filter predicate directly above the TableScan, or None."""
    v = plan.to_variant()
    if type(v).__name__ == "Filter":
        for inp in plan.inputs():
            if _find_table_scan(inp) is not None:
                return v.predicate()
    for inp in plan.inputs():
        found = _find_filter_above_scan(inp)
        if found is not None:
            return found
    return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def _build_combined_sql(transform_sql: str, columns: Sequence[str] | None, filters: Filter) -> str:
    col_list = ", ".join(_quote_identifier(c) for c in columns) if columns else "*"
    sql = f"SELECT {col_list} FROM ({transform_sql}) AS {_SUBQUERY_ALIAS}"
    if not isinstance(filters, AlwaysTrue):
        sql += f" WHERE {_filter_to_sql(filters)}"
    return sql


def analyze_pushdown(
    transform_sql: str,
    table_schema: Schema,
    table_id: TableIdentifier,
    columns: Sequence[str] | None,
    filters: Filter,
) -> ScanPushdown:
    """Analyze which columns and filters can be pushed through a transformer.

    Registers the Iceberg table schema in DataFusion, optimizes the transformer
    SQL, and extracts scan columns and filters from the TableScan node.
    """
    combined_sql = _build_combined_sql(transform_sql, columns, filters)

    ctx = SessionContext()
    arrow_schema = table_schema.as_arrow()
    ctx.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(table_id.database)}").collect()
    arrays = [pa.array([], type=f.type) for f in arrow_schema]
    batch = pa.record_batch(arrays, schema=arrow_schema)
    ctx.register_record_batches(to_sql_identifier(table_id), [[batch]])

    # Optimize the transformer SQL to find which base table columns it needs.
    # We use the transformer SQL (not the combined SQL) because the outer
    # projection can prune columns the inner SQL still references, causing
    # DataFusion to fail at split execution time.
    transformer_plan = ctx.sql(transform_sql).optimized_logical_plan()
    scan = _find_table_scan(transformer_plan)
    scan_columns = [col_name for _, col_name in scan.projection()] if scan else []

    # Optimize the combined SQL to determine filter pushdown.
    combined_plan = ctx.sql(combined_sql).optimized_logical_plan()

    # Include any extra columns needed by pushed-down filters.
    combined_scan = _find_table_scan(combined_plan)
    if combined_scan:
        for _, col_name in combined_scan.projection():
            if col_name not in scan_columns:
                scan_columns.append(col_name)

    # Extract the pushed-down filter from the plan.
    scan_filter: ice.BooleanExpression = ice.AlwaysTrue()
    filter_pred = _find_filter_above_scan(combined_plan)
    if filter_pred is not None:
        converted = _df_expr_to_pyiceberg(filter_pred)
        if converted is not None:
            scan_filter = converted

    return ScanPushdown(
        scan_columns=tuple(scan_columns),
        scan_filter=scan_filter,
        combined_sql=combined_sql,
    )

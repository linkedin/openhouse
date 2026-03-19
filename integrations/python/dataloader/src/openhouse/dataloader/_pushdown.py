"""Pushdown analysis using DataFusion's query optimizer.

Determines which column projections and row filters can be pushed down to the
Iceberg scan when a table transformer is applied. This avoids reading unnecessary
columns or rows from the underlying data files.

The approach:
1. Build a combined SQL query: user's SELECT/WHERE wrapping the transformer subquery
2. Register the Iceberg table schema in DataFusion (empty data, schema only)
3. Let DataFusion's optimizer push projections and predicates through the subquery
4. Extract the optimized scan columns and filters from the logical plan
5. Convert pushed-down filters back to PyIceberg expressions for the scan
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


@dataclass(frozen=True)
class ScanPushdown:
    """Results of pushdown analysis for an Iceberg scan through a transformer.

    Attributes:
        scan_columns: Column names that the Iceberg scan must read.
        scan_filter: Row filter to push down to the Iceberg scan. This is a
            best-effort conversion of the filters DataFusion pushed to the scan;
            filters that cannot be converted are omitted (they are still applied
            by the combined SQL at split execution time).
        combined_sql: The full SQL query (user projection/filter wrapping the
            transformer subquery) in DataFusion dialect. This is executed at
            split level against each record batch.
    """

    scan_columns: tuple[str, ...]
    scan_filter: ice.BooleanExpression
    combined_sql: str


# ---------------------------------------------------------------------------
# Filter → SQL conversion (user Filter objects → SQL WHERE clause)
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
    """Format a Python value as a SQL literal."""
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
    """Escape special LIKE characters in a literal string."""
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
# DataFusion Expr → PyIceberg BooleanExpression conversion
# ---------------------------------------------------------------------------

# Mapping from DataFusion BinaryExpr op strings to PyIceberg expression constructors.
# Each constructor takes (column_name, literal_value).
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

    Returns ``None`` when the expression cannot be represented in PyIceberg
    (e.g. function calls, computed expressions). Callers should treat ``None``
    as "not pushable".
    """
    variant = expr.variant_name()

    if variant == "BinaryExpr":
        v = expr.to_variant()
        op = v.op()
        left = v.left()
        right = v.right()

        # AND: push both sides independently
        if op == "AND":
            left_ice = _df_expr_to_pyiceberg(left)
            right_ice = _df_expr_to_pyiceberg(right)
            if left_ice is not None and right_ice is not None:
                return ice.And(left_ice, right_ice)
            # Partial AND: return whichever side converted
            return left_ice or right_ice

        # OR: both sides must convert (can't partially push OR)
        if op == "OR":
            left_ice = _df_expr_to_pyiceberg(left)
            right_ice = _df_expr_to_pyiceberg(right)
            if left_ice is not None and right_ice is not None:
                return ice.Or(left_ice, right_ice)
            return None

        # Simple comparison: Column op Literal
        ice_type = _DF_OP_TO_ICE.get(op)
        if ice_type is not None and left.variant_name() == "Column" and right.variant_name() == "Literal":
            col_name = left.to_variant().name()
            py_value = right.python_value()
            if isinstance(py_value, pa.Scalar):
                py_value = py_value.as_py()
            return ice_type(col_name, py_value)

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
        if inner_ice is not None:
            return ice.Not(inner_ice)

    elif variant == "Like":
        v = expr.to_variant()
        col_expr = v.expr()
        pattern_expr = v.pattern()
        negated = v.negated()
        if col_expr.variant_name() == "Column" and pattern_expr.variant_name() == "Literal":
            pattern = pattern_expr.python_value()
            if isinstance(pattern, pa.Scalar):
                pattern = pattern.as_py()
            # Convert simple prefix LIKE patterns: 'prefix%'
            if isinstance(pattern, str) and pattern.endswith("%") and "%" not in pattern[:-1] and "_" not in pattern:
                prefix = pattern[:-1]
                if negated:
                    return ice.NotStartsWith(col_expr.to_variant().name(), prefix)
                return ice.StartsWith(col_expr.to_variant().name(), prefix)

    # Unsupported expression type
    return None


def _collect_filters(plan: Any) -> tuple[list[Any], list[Any]]:
    """Walk the logical plan and collect filters from Filter and TableScan nodes.

    Returns (filter_node_exprs, table_scan_filter_exprs) where each list
    contains DataFusion Expr objects.
    """
    filter_exprs: list[Any] = []
    scan_filter_exprs: list[Any] = []

    def walk(node: Any) -> None:
        v = node.to_variant()
        name = type(v).__name__
        if name == "Filter":
            filter_exprs.append(v.predicate())
        elif name == "TableScan":
            scan_filter_exprs.extend(v.filters())
        for inp in node.inputs():
            walk(inp)

    walk(plan)
    return filter_exprs, scan_filter_exprs


# ---------------------------------------------------------------------------
# Combined SQL construction
# ---------------------------------------------------------------------------

_SUBQUERY_ALIAS = "_oh_transformed"


def _build_combined_sql(
    transform_sql: str,
    columns: Sequence[str] | None,
    filters: Filter,
) -> str:
    """Build the combined query: user's projection/filter wrapping the transformer."""
    col_list = ", ".join(_quote_identifier(c) for c in columns) if columns else "*"
    sql = f"SELECT {col_list} FROM ({transform_sql}) AS {_SUBQUERY_ALIAS}"
    if not isinstance(filters, AlwaysTrue):
        sql += f" WHERE {_filter_to_sql(filters)}"
    return sql


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def _register_empty_table(ctx: SessionContext, table_id: TableIdentifier, arrow_schema: pa.Schema) -> None:
    """Register an empty table with the given schema for query optimization."""
    ctx.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(table_id.database)}").collect()
    arrays = [pa.array([], type=f.type) for f in arrow_schema]
    batch = pa.record_batch(arrays, schema=arrow_schema)
    ctx.register_record_batches(to_sql_identifier(table_id), [[batch]])


def _extract_scan_columns(plan: Any) -> list[str]:
    """Extract projected column names from the TableScan node in a logical plan."""
    columns: list[str] = []

    def walk(node: Any) -> None:
        v = node.to_variant()
        if type(v).__name__ == "TableScan":
            for _idx, col_name in v.projection():
                columns.append(col_name)
        for inp in node.inputs():
            walk(inp)

    walk(plan)
    return columns


def _extract_scan_filter(plan: Any) -> ice.BooleanExpression:
    """Extract pushed-down filters from the plan and convert to PyIceberg.

    Collects filters from both ``Filter`` nodes and ``TableScan.filters()``,
    converts each to a PyIceberg expression on a best-effort basis, and ANDs
    them together. Non-convertible filters are silently skipped.
    """
    filter_exprs, scan_filter_exprs = _collect_filters(plan)
    all_exprs = scan_filter_exprs + filter_exprs

    scan_filter: ice.BooleanExpression = ice.AlwaysTrue()
    for expr in all_exprs:
        converted = _df_expr_to_pyiceberg(expr)
        if converted is not None:
            scan_filter = ice.And(scan_filter, converted) if not isinstance(scan_filter, ice.AlwaysTrue) else converted
    return scan_filter


def analyze_pushdown(
    transform_sql: str,
    table_schema: Schema,
    table_id: TableIdentifier,
    columns: Sequence[str] | None,
    filters: Filter,
) -> ScanPushdown:
    """Analyze which columns and filters can be pushed through a transformer to the Iceberg scan.

    Uses DataFusion's query optimizer to push projections and predicates through
    the transformer subquery, then extracts the optimized scan requirements.

    Args:
        transform_sql: The transformer SQL in DataFusion dialect (the inner subquery).
        table_schema: The Iceberg table schema.
        table_id: The table identifier used in the transformer SQL.
        columns: User-requested output columns, or None for all.
        filters: User-specified row filters.

    Returns:
        A ScanPushdown with the optimized scan columns, pushable filter, and
        the combined SQL for split-level execution.
    """
    combined_sql = _build_combined_sql(transform_sql, columns, filters)
    arrow_schema = table_schema.as_arrow()

    ctx = SessionContext()
    _register_empty_table(ctx, table_id, arrow_schema)

    # Projection pushdown: optimize the transformer SQL alone to find which
    # base table columns it needs. We don't optimize the combined SQL because
    # the outer projection may prune columns that the inner SQL still references,
    # and at split execution time DataFusion would fail to resolve them.
    transformer_plan = ctx.sql(transform_sql).optimized_logical_plan()
    scan_columns = _extract_scan_columns(transformer_plan)

    # Also include any columns needed by user filters that get pushed to the scan.
    # Optimize the combined SQL to determine filter pushdown.
    combined_plan = ctx.sql(combined_sql).optimized_logical_plan()
    filter_scan_columns = _extract_scan_columns(combined_plan)
    for col in filter_scan_columns:
        if col not in scan_columns:
            scan_columns.append(col)

    scan_filter = _extract_scan_filter(combined_plan)

    return ScanPushdown(
        scan_columns=tuple(scan_columns),
        scan_filter=scan_filter,
        combined_sql=combined_sql,
    )

"""Optimize Iceberg scans by extracting column projections and predicate pushdowns.

Parses transform SQL combined with user columns and filters to determine:
- The minimal set of source columns for the Iceberg scan
- Predicates that can be pushed down to Iceberg's row_filter
- Rewritten SQL that only produces needed columns
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

import openhouse.dataloader.datafusion_sql  # noqa: F401 — registers DataFusion dialect
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
    always_true,
)

logger = logging.getLogger(__name__)

_DIALECT = "datafusion"


def _quote_identifier(name: str) -> str:
    """Escape a SQL identifier by doubling embedded double quotes and wrapping in double quotes."""
    return '"' + name.replace('"', '""') + '"'


@dataclass
class ScanPlan:
    """Result of scan optimization.

    Attributes:
        sql: Combined SQL for DataFusion execution.
        source_columns: Columns for Iceberg scan, or None for all.
        row_filter: Extracted predicates for Iceberg row_filter.
    """

    sql: str
    source_columns: list[str] | None
    row_filter: Filter


def optimize_scan(
    transform_sql: str,
    columns: Sequence[str] | None = None,
    filters: Filter | None = None,
) -> ScanPlan:
    """Combine user columns, user filters, and transform SQL, then extract pushdowns.

    Builds a combined query wrapping transform_sql as a subquery, analyzes it for
    predicates that can be pushed to Iceberg, and rewrites the SQL to only produce
    needed columns.

    Args:
        transform_sql: DataFusion-dialect SQL (already transpiled).
        columns: User-requested output columns, or None for all.
        filters: User-provided row filters, or None.

    Returns:
        A ScanPlan with optimized SQL, source columns, and row filter.
    """
    try:
        return _optimize_scan_impl(transform_sql, columns, filters)
    except Exception:
        logger.warning("Failed to optimize scan; falling back", exc_info=True)
        combined = _build_combined_sql_string(transform_sql, columns, filters)
        return ScanPlan(sql=combined, source_columns=None, row_filter=always_true())


def _build_combined_sql_string(
    transform_sql: str,
    columns: Sequence[str] | None,
    filters: Filter | None,
) -> str:
    """Build the combined SQL string from parts (string concatenation, no parsing)."""
    outer_cols = ", ".join(_quote_identifier(c) for c in columns) if columns else "*"
    sql = f"SELECT {outer_cols} FROM ({transform_sql}) AS _t"
    if filters and not isinstance(filters, AlwaysTrue):
        sql += f" WHERE {_filter_to_sql(filters)}"
    return sql


def _optimize_scan_impl(
    transform_sql: str,
    columns: Sequence[str] | None,
    filters: Filter | None,
) -> ScanPlan:
    inner_ast = sqlglot.parse_one(transform_sql, dialect=_DIALECT)

    # Step 2-3: Build output map from inner SELECT
    output_map: dict[str, exp.Expression] = {}
    passthrough_map: dict[str, str] = {}
    for select_expr in inner_ast.expressions:
        alias = select_expr.alias_or_name
        # Get the underlying expression (unwrap alias)
        underlying = select_expr.args.get("this", select_expr) if isinstance(select_expr, exp.Alias) else select_expr
        output_map[alias] = select_expr
        if isinstance(underlying, exp.Column):
            passthrough_map[alias] = underlying.name

    # Step 4: Resolve outer WHERE predicates
    pushed_filters: list[Filter] = []
    remaining_outer_where: exp.Where | None = None
    if filters and not isinstance(filters, AlwaysTrue):
        outer_where_sql = f"SELECT * FROM _t WHERE {_filter_to_sql(filters)}"
        outer_ast = sqlglot.parse_one(outer_where_sql, dialect=_DIALECT)
        outer_where = outer_ast.find(exp.Where)
        if outer_where:
            outer_pushed, remaining_outer_where = _resolve_outer_predicates(outer_where, passthrough_map)
            if outer_pushed:
                pushed_filters.append(outer_pushed)

    # Step 5: Extract inner WHERE predicates
    inner_where = inner_ast.find(exp.Where)
    inner_pushed_filter: Filter | None = None
    remaining_inner_where: exp.Where | None = None
    if inner_where:
        inner_pushed_filter, remaining_inner_where = _extract_pushable_predicates(inner_where)
        if inner_pushed_filter:
            pushed_filters.append(inner_pushed_filter)

    # Step 6: Remove pushed predicates from inner WHERE
    if inner_where:
        if remaining_inner_where:
            inner_ast.set("where", remaining_inner_where)
        else:
            inner_ast.set("where", None)

    # Step 7: Determine needed inner outputs
    needed_outputs: set[str] = set()
    if columns:
        needed_outputs.update(columns)
    else:
        needed_outputs.update(output_map.keys())

    # Add columns referenced in remaining outer WHERE
    if remaining_outer_where:
        for col_ref in remaining_outer_where.find_all(exp.Column):
            needed_outputs.add(col_ref.name)

    # Step 8: Rewrite inner SELECT to only produce needed outputs
    if columns or remaining_outer_where:
        kept_expressions = []
        for name in list(needed_outputs):
            if name in output_map:
                kept_expressions.append(output_map[name])
        if kept_expressions:
            inner_ast.set("expressions", [e.copy() for e in kept_expressions])

    # Step 9: Compute source_columns from rewritten inner SELECT + remaining clauses
    source_columns: set[str] = set()
    for select_expr in inner_ast.expressions:
        source_columns.update(c.name for c in select_expr.find_all(exp.Column))

    for clause_type in (exp.Where, exp.Group, exp.Having, exp.Order):
        clause = inner_ast.find(clause_type)
        if clause:
            source_columns.update(c.name for c in clause.find_all(exp.Column))

    # Step 10: Rebuild combined SQL
    inner_sql = inner_ast.sql(dialect=_DIALECT)

    if remaining_outer_where:
        # Rebuild the outer WHERE SQL from the remaining AST
        remaining_where_sql = remaining_outer_where.this.sql(dialect=_DIALECT)
        outer_cols = ", ".join(_quote_identifier(c) for c in columns) if columns else "*"
        combined_sql = f"SELECT {outer_cols} FROM ({inner_sql}) AS _t WHERE {remaining_where_sql}"
    elif columns:
        outer_cols = ", ".join(_quote_identifier(c) for c in columns)
        combined_sql = f"SELECT {outer_cols} FROM ({inner_sql}) AS _t"
    else:
        combined_sql = inner_sql

    # Step 11: Build row_filter
    row_filter: Filter = always_true()
    if pushed_filters:
        row_filter = pushed_filters[0]
        for f in pushed_filters[1:]:
            row_filter = row_filter & f

    # Step 12: Return ScanPlan
    return ScanPlan(
        sql=combined_sql,
        source_columns=sorted(source_columns) if source_columns else None,
        row_filter=row_filter,
    )


# --- Filter ↔ SQL conversion ---


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


def _literal_to_python(node: exp.Literal) -> object:
    """Convert a sqlglot Literal AST node to a Python value."""
    if node.is_string:
        return node.this
    if node.is_int:
        return int(node.this)
    return float(node.this)


def _ast_to_filter(node: exp.Expression) -> Filter | None:
    """Convert a sqlglot AST expression to a Filter DSL object, or None if not convertible."""
    if isinstance(node, exp.And):
        left = _ast_to_filter(node.left)
        right = _ast_to_filter(node.right)
        if left and right:
            return And(left, right)
        return None

    if isinstance(node, exp.Or):
        left = _ast_to_filter(node.left)
        right = _ast_to_filter(node.right)
        if left and right:
            return Or(left, right)
        return None

    if isinstance(node, exp.Not):
        # NOT IS NULL → IsNotNull
        inner_expr = node.this
        if isinstance(inner_expr, exp.Paren):
            inner_expr = inner_expr.this
        if (
            isinstance(inner_expr, exp.Is)
            and isinstance(inner_expr.this, exp.Column)
            and isinstance(inner_expr.expression, exp.Null)
        ):
            return IsNotNull(inner_expr.this.name)
        inner = _ast_to_filter(node.this)
        if inner:
            return Not(inner)
        return None

    # Unwrap parentheses
    if isinstance(node, exp.Paren):
        return _ast_to_filter(node.this)

    # IS NULL: exp.Is with exp.Null
    if isinstance(node, exp.Is):
        if isinstance(node.this, exp.Column) and isinstance(node.expression, exp.Null):
            return IsNull(node.this.name)
        return None

    # comparison: one side Column, other side Literal
    if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        left, right = node.this, node.expression
        col_node = lit_node = None
        flipped = False
        if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
            col_node, lit_node = left, right
        elif isinstance(right, exp.Column) and isinstance(left, exp.Literal):
            col_node, lit_node = right, left
            flipped = True
        if col_node is None or lit_node is None:
            return None
        col_name = col_node.name
        value = _literal_to_python(lit_node)
        # When flipped (literal op column), reverse the comparison direction
        match node:
            case exp.EQ():
                return EqualTo(col_name, value)
            case exp.NEQ():
                return NotEqualTo(col_name, value)
            case exp.GT():
                return LessThan(col_name, value) if flipped else GreaterThan(col_name, value)
            case exp.GTE():
                return LessThanOrEqual(col_name, value) if flipped else GreaterThanOrEqual(col_name, value)
            case exp.LT():
                return GreaterThan(col_name, value) if flipped else LessThan(col_name, value)
            case exp.LTE():
                return GreaterThanOrEqual(col_name, value) if flipped else LessThanOrEqual(col_name, value)

    # IN
    if isinstance(node, exp.In):
        if isinstance(node.this, exp.Column):
            values = []
            for v in node.expressions:
                if isinstance(v, exp.Literal):
                    values.append(_literal_to_python(v))
                else:
                    return None
            return In(node.this.name, tuple(values))
        return None

    # BETWEEN
    if isinstance(node, exp.Between):
        if (
            isinstance(node.this, exp.Column)
            and isinstance(node.args.get("low"), exp.Literal)
            and isinstance(node.args.get("high"), exp.Literal)
        ):
            return Between(
                node.this.name,
                _literal_to_python(node.args["low"]),
                _literal_to_python(node.args["high"]),
            )
        return None

    return None


def _flatten_and(node: exp.Expression) -> list[exp.Expression]:
    """Flatten nested AND expressions into a list of conjuncts."""
    if isinstance(node, exp.And):
        return _flatten_and(node.left) + _flatten_and(node.right)
    return [node]


def _extract_pushable_predicates(where: exp.Where) -> tuple[Filter | None, exp.Where | None]:
    """Extract predicates from a WHERE clause that can be pushed to Iceberg.

    Returns (pushed_filter, remaining_where). remaining_where is None if all predicates were pushed.
    """
    conjuncts = _flatten_and(where.this)
    pushed: list[Filter] = []
    remaining: list[exp.Expression] = []

    for conjunct in conjuncts:
        f = _ast_to_filter(conjunct)
        if f is not None:
            pushed.append(f)
        else:
            remaining.append(conjunct)

    pushed_filter: Filter | None = None
    if pushed:
        pushed_filter = pushed[0]
        for f in pushed[1:]:
            pushed_filter = pushed_filter & f

    remaining_where: exp.Where | None = None
    if remaining:
        combined = remaining[0]
        for r in remaining[1:]:
            combined = exp.And(this=combined, expression=r)
        remaining_where = exp.Where(this=combined)

    return pushed_filter, remaining_where


def _resolve_outer_predicates(
    where: exp.Where, passthrough_map: dict[str, str]
) -> tuple[Filter | None, exp.Where | None]:
    """Resolve outer WHERE predicates against the passthrough map.

    For each conjunct, if all referenced columns are passthroughs, rewrite column
    names to source names and convert to Filter. Otherwise keep in remaining WHERE.
    """
    conjuncts = _flatten_and(where.this)
    pushed: list[Filter] = []
    remaining: list[exp.Expression] = []

    for conjunct in conjuncts:
        col_refs = list(conjunct.find_all(exp.Column))
        all_passthrough = col_refs and all(c.name in passthrough_map for c in col_refs)

        if all_passthrough:
            # Rewrite column names to source names
            rewritten = conjunct.copy()
            for col_ref in rewritten.find_all(exp.Column):
                source_name = passthrough_map[col_ref.name]
                col_ref.set("this", exp.to_identifier(source_name))
            f = _ast_to_filter(rewritten)
            if f is not None:
                pushed.append(f)
            else:
                remaining.append(conjunct)
        else:
            remaining.append(conjunct)

    pushed_filter: Filter | None = None
    if pushed:
        pushed_filter = pushed[0]
        for f in pushed[1:]:
            pushed_filter = pushed_filter & f

    remaining_where: exp.Where | None = None
    if remaining:
        combined = remaining[0]
        for r in remaining[1:]:
            combined = exp.And(this=combined, expression=r)
        remaining_where = exp.Where(this=combined)

    return pushed_filter, remaining_where

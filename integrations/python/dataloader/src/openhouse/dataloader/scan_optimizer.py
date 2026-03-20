"""Optimize Iceberg scans by extracting column projections and predicate pushdowns.

Given a SQL query, uses sqlglot's optimizer to push predicates and projections
down to the table scan, then extracts:
- The minimal set of source columns for the Iceberg scan
- Predicates that can be pushed down to Iceberg's row_filter
- Rewritten SQL that only produces needed columns
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.optimizer import pushdown_predicates, pushdown_projections, qualify
from sqlglot.optimizer.scope import build_scope

import openhouse.dataloader.datafusion_sql  # noqa: F401 — registers DataFusion dialect
from openhouse.dataloader.filters import (
    And,
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


@dataclass
class ScanPlan:
    """Result of scan optimization.

    Attributes:
        sql: Optimized SQL for DataFusion execution.
        source_columns: Columns for Iceberg scan, or None for all.
        row_filter: Extracted predicates for Iceberg row_filter.
    """

    sql: str
    source_columns: list[str] | None
    row_filter: Filter


def optimize_scan(sql: str) -> ScanPlan:
    """Optimize a SQL query by extracting projections and pushable predicates.

    Uses sqlglot's optimizer to push predicates and projections down to the
    table scan, then extracts simple column-op-literal predicates as an
    Iceberg row_filter and determines the minimal source column set.

    Args:
        sql: DataFusion-dialect SQL to optimize.

    Returns:
        A ScanPlan with optimized SQL, source columns, and row filter.
    """
    try:
        ast = sqlglot.parse_one(sql, dialect=_DIALECT)
        ast = qualify.qualify(ast, dialect=_DIALECT)
        ast = pushdown_predicates.pushdown_predicates(ast, dialect=_DIALECT)
        ast = pushdown_projections.pushdown_projections(ast, dialect=_DIALECT)

        table_scan = _find_table_scan(ast)
        if table_scan is None:
            return ScanPlan(sql=ast.sql(dialect=_DIALECT), source_columns=None, row_filter=always_true())

        # Extract convertible predicates from the table scan's WHERE as Iceberg row_filter
        where = table_scan.args.get("where")
        row_filter: Filter = always_true()
        if where:
            row_filter = _extract_row_filter(where)

        # Source columns = what the table scan's SELECT references
        source_columns = _collect_source_columns(table_scan)

        return ScanPlan(
            sql=ast.sql(dialect=_DIALECT),
            source_columns=sorted(source_columns) if source_columns else None,
            row_filter=row_filter,
        )
    except Exception:
        logger.warning("Failed to optimize scan; falling back", exc_info=True)
        return ScanPlan(sql=sql, source_columns=None, row_filter=always_true())


# --- Table scan discovery ---


def _find_table_scan(ast: exp.Expression) -> exp.Select | None:
    """Find the SELECT that reads directly from a table (not a subquery)."""
    root = build_scope(ast)
    if root is None:
        return None
    for scope in root.traverse():
        select = scope.expression
        from_clause = select.find(exp.From)
        if from_clause is None:
            continue
        has_table = bool(list(from_clause.find_all(exp.Table)))
        has_subquery = bool(list(from_clause.find_all(exp.Subquery)))
        if has_table and not has_subquery:
            result: exp.Select = select
            return result
    return None


# --- Source column collection ---


def _collect_source_columns(select: exp.Expression) -> set[str]:
    """Collect all column references from a SELECT's expressions and clauses."""
    source_columns: set[str] = set()
    for select_expr in select.expressions:
        source_columns.update(c.name for c in select_expr.find_all(exp.Column))
    for clause_type in (exp.Where, exp.Group, exp.Having, exp.Order):
        clause = select.find(clause_type)
        if clause:
            source_columns.update(c.name for c in clause.find_all(exp.Column))
    return source_columns


# --- Predicate extraction ---


def _flatten_and(node: exp.Expression) -> list[exp.Expression]:
    """Flatten nested AND expressions into a list of conjuncts."""
    if isinstance(node, exp.And):
        return _flatten_and(node.left) + _flatten_and(node.right)
    return [node]


def _extract_row_filter(where: exp.Where) -> Filter:
    """Extract convertible predicates from a WHERE clause as a Filter.

    Converts each conjunct that is a simple column-op-literal predicate.
    Non-convertible predicates (e.g. function calls) are skipped — they remain
    in the SQL for DataFusion to evaluate.
    """
    filters: list[Filter] = []
    for conjunct in _flatten_and(where.this):
        f = _ast_to_filter(conjunct)
        if f is not None:
            filters.append(f)

    if not filters:
        return always_true()
    result = filters[0]
    for f in filters[1:]:
        result = result & f
    return result


# --- AST → Filter conversion ---


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

    return None

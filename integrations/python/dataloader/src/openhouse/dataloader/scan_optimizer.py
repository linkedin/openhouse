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
from openhouse.dataloader._filter_converter import convert_where
from openhouse.dataloader.filters import Filter, always_true

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

        where = table_scan.args.get("where")
        row_filter: Filter = convert_where(where) if where else always_true()
        source_columns = _collect_source_columns(table_scan)

        return ScanPlan(
            sql=ast.sql(dialect=_DIALECT),
            source_columns=sorted(source_columns) if source_columns else None,
            row_filter=row_filter,
        )
    except Exception:
        logger.warning("Failed to optimize scan; falling back", exc_info=True)
        return ScanPlan(sql=sql, source_columns=None, row_filter=always_true())


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

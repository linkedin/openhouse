"""Optimize Iceberg scan projections when column selection is combined with transform SQL.

Parses the transform SQL to determine which source columns are needed for the
user-requested output columns, rewrites the SQL to only produce the requested
columns, and computes the minimal source column set for the Iceberg scan.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import sqlglot
from sqlglot import exp

import openhouse.dataloader.datafusion_sql  # noqa: F401 — registers DataFusion dialect

logger = logging.getLogger(__name__)

_DIALECT = "datafusion"


def _quote_identifier(name: str) -> str:
    """Escape a SQL identifier by doubling embedded double quotes and wrapping in double quotes."""
    return '"' + name.replace('"', '""') + '"'


def compute_scan_projection(
    transform_sql: str,
    columns: Sequence[str],
) -> tuple[str, list[str] | None]:
    """Compute optimized SQL and source columns for a column-projected transform.

    Rewrites *transform_sql* to only produce the requested *columns* and
    determines the minimal set of source columns needed from the Iceberg scan.

    Args:
        transform_sql: DataFusion-dialect SQL (already transpiled).
        columns: User-requested output columns.

    Returns:
        A ``(optimized_sql, source_columns)`` tuple.  *source_columns* is a
        sorted list of column names to project from the Iceberg scan, or
        ``None`` if the columns could not be determined (caller should fall
        back to reading all columns).
    """
    try:
        inner_ast = sqlglot.parse_one(transform_sql, dialect=_DIALECT)
    except sqlglot.errors.SqlglotError:
        logger.warning("Failed to parse transform SQL; falling back to reading all columns", exc_info=True)
        outer_cols = ", ".join(_quote_identifier(c) for c in columns)
        return f"SELECT {outer_cols} FROM ({transform_sql}) AS _t", None

    try:
        requested = set(columns)
        optimized_ast, source_columns = _rewrite_projection(inner_ast, requested)
        optimized_sql = optimized_ast.sql(dialect=_DIALECT)
        return optimized_sql, sorted(source_columns) if source_columns else None
    except Exception:
        logger.warning("Failed to rewrite transform SQL; falling back", exc_info=True)
        outer_cols = ", ".join(_quote_identifier(c) for c in columns)
        return f"SELECT {outer_cols} FROM ({transform_sql}) AS _t", None


def _rewrite_projection(ast: exp.Expression, requested: set[str]) -> tuple[exp.Expression, set[str]]:
    """Rewrite the inner query's SELECT to only include requested output columns.

    Returns the rewritten AST and the set of source column names needed.
    """
    # Map output alias/name → (expression, source column names)
    select_map: dict[str, tuple[exp.Expression, set[str]]] = {}
    for select_expr in ast.expressions:
        alias = select_expr.alias_or_name
        col_refs = {c.name for c in select_expr.find_all(exp.Column)}
        select_map[alias] = (select_expr, col_refs)

    # Keep only the SELECT expressions that produce requested columns
    kept_expressions = []
    source_columns: set[str] = set()
    for col_name in requested:
        if col_name in select_map:
            expr, refs = select_map[col_name]
            kept_expressions.append(expr)
            source_columns.update(refs)

    # Always include columns from WHERE, GROUP BY, HAVING, ORDER BY
    for clause_type in (exp.Where, exp.Group, exp.Having, exp.Order):
        clause = ast.find(clause_type)
        if clause:
            source_columns.update(c.name for c in clause.find_all(exp.Column))

    if not kept_expressions:
        return ast, source_columns

    # Replace the SELECT expressions in the AST
    rewritten = ast.copy()
    rewritten.set("expressions", [e.copy() for e in kept_expressions])

    return rewritten, source_columns

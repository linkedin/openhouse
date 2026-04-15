"""Optimize Iceberg scans by extracting column projections and predicate pushdowns.

Given a SQL query, uses sqlglot's optimizer to push predicates and projections
down to the table scan, then extracts:
- The minimal set of source columns for the Iceberg scan
- Predicates that can be pushed down to Iceberg's row_filter
- Rewritten SQL that only produces needed columns
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.optimizer import pushdown_predicates, pushdown_projections, qualify
from sqlglot.optimizer.scope import build_scope
from sqlglot.schema import MappingSchema

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


def optimize_scan(sql: str, dialect: str, column_names: Sequence[str]) -> ScanPlan:
    """Optimize a SQL query by extracting projections and pushable predicates.

    Uses sqlglot's optimizer to push predicates and projections down to the
    table scan, then extracts simple column-op-literal predicates as an
    Iceberg row_filter and determines the minimal source column set.

    A ``MappingSchema`` is built from *column_names* so that ``qualify``
    can expand ``SELECT *`` into explicit column references.  This is
    required for correct predicate pushdown — without it, sqlglot's
    ``replace_aliases`` cannot rewrite column references when pushing
    predicates into inner scopes.

    Args:
        sql: SQL query to optimize.
        dialect: SQL dialect for parsing and generation (e.g. "datafusion").
        column_names: Column names from the table schema (e.g. Iceberg).

    Returns:
        A ScanPlan with optimized SQL, source columns, and row filter.
    """
    ast = sqlglot.parse_one(sql, dialect=dialect)
    schema = _build_schema(ast, column_names, dialect)
    ast = qualify.qualify(ast, dialect=dialect, schema=schema)
    ast = pushdown_predicates.pushdown_predicates(ast, dialect=dialect)
    ast = pushdown_projections.pushdown_projections(ast, dialect=dialect)

    table_scan = _find_table_scan(ast, sql)

    where = table_scan.args.get("where")
    row_filter = _extract_row_filter(where) if where else always_true()
    source_columns = _collect_source_columns(table_scan)

    return ScanPlan(
        sql=ast.sql(dialect=dialect),
        source_columns=sorted(source_columns) if source_columns else None,
        row_filter=row_filter,
    )


def _build_schema(ast: exp.Expression, column_names: Sequence[str], dialect: str) -> MappingSchema | None:
    """Build a MappingSchema from the single table in *ast* and the Iceberg column names.

    Returns ``None`` if the AST does not contain exactly one table reference.
    """
    tables = list(ast.find_all(exp.Table))
    if len(tables) != 1:
        return None
    table = tables[0]

    def _quote(name: str) -> str:
        return exp.to_identifier(name, quoted=True).sql(dialect=dialect)

    columns = {_quote(c): "VARCHAR" for c in column_names}
    return MappingSchema({_quote(table.db): {_quote(table.name): columns}}, dialect=dialect)


def _find_table_scan(ast: exp.Expression, original_sql: str) -> exp.Select:
    """Find the single SELECT that reads directly from exactly one table.

    Raises ValueError if the query does not contain exactly one table scan
    across all scopes (e.g. JOINs have multiple table sources).
    """
    root = build_scope(ast)
    if root is None:
        raise ValueError(f"Expected exactly 1 table scan, found 0 in: {original_sql}")
    table_scans: list[exp.Select] = []
    for scope in root.traverse():
        table_sources = [s for s in scope.sources.values() if isinstance(s, exp.Table)]
        for _ in table_sources:
            table_scans.append(scope.expression)
    if len(table_scans) != 1:
        raise ValueError(f"Expected exactly 1 table scan, found {len(table_scans)} in: {original_sql}")
    return table_scans[0]


def _extract_row_filter(where: exp.Where) -> Filter:
    """Extract pushable predicates from a WHERE clause.

    Flattens top-level ANDs and converts each conjunct independently via
    the filter converter. Non-convertible conjuncts are skipped — they
    remain in the SQL for DataFusion to evaluate.
    """
    filters: list[Filter] = []
    for conjunct in _flatten_and(where.this):
        f = _convert_ast_to_filter(conjunct)
        if f is not None:
            filters.append(f)
    if not filters:
        return always_true()
    result = filters[0]
    for f in filters[1:]:
        result = result & f
    return result


def _flatten_and(node: exp.Expression) -> list[exp.Expression]:
    """Flatten nested AND expressions into a list of conjuncts."""
    if isinstance(node, exp.And):
        return _flatten_and(node.left) + _flatten_and(node.right)
    return [node]


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


# --- AST → Filter conversion ---


def _convert_ast_to_filter(node: exp.Expression) -> Filter | None:
    """Convert a sqlglot AST expression to a Filter, or None if not convertible."""
    if isinstance(node, exp.And):
        left = _convert_ast_to_filter(node.left)
        right = _convert_ast_to_filter(node.right)
        if left and right:
            return And(left, right)
        return None

    if isinstance(node, exp.Or):
        left = _convert_ast_to_filter(node.left)
        right = _convert_ast_to_filter(node.right)
        if left and right:
            return Or(left, right)
        return None

    if isinstance(node, exp.Not):
        inner_expr = node.this
        if isinstance(inner_expr, exp.Paren):
            inner_expr = inner_expr.this
        if (
            isinstance(inner_expr, exp.Is)
            and isinstance(inner_expr.this, exp.Column)
            and isinstance(inner_expr.expression, exp.Null)
        ):
            return IsNotNull(inner_expr.this.name)
        inner = _convert_ast_to_filter(node.this)
        if inner:
            return Not(inner)
        return None

    if isinstance(node, exp.Paren):
        return _convert_ast_to_filter(node.this)

    if isinstance(node, exp.Is):
        if isinstance(node.this, exp.Column) and isinstance(node.expression, exp.Null):
            return IsNull(node.this.name)
        return None

    if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        return _convert_comparison(node)

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


def _convert_comparison(node: exp.EQ | exp.NEQ | exp.GT | exp.GTE | exp.LT | exp.LTE) -> Filter | None:
    """Convert a comparison AST node (column op literal) to a Filter."""
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
    return None  # pragma: no cover


def _literal_to_python(node: exp.Literal) -> object:
    """Convert a sqlglot Literal AST node to a Python value."""
    if node.is_string:
        return node.this
    if node.is_int:
        return int(node.this)
    return float(node.this)

"""Convert sqlglot AST expressions to Filter DSL objects."""

from __future__ import annotations

from sqlglot import exp

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


def convert_where(where: exp.Where) -> Filter:
    """Convert a WHERE clause to a Filter, skipping non-convertible conjuncts.

    Each AND-separated conjunct is independently converted. Conjuncts that
    cannot be expressed as a Filter (e.g. function calls on columns) are
    silently skipped — they remain in the SQL for DataFusion to evaluate.
    """
    filters: list[Filter] = []
    for conjunct in _flatten_and(where.this):
        f = convert(conjunct)
        if f is not None:
            filters.append(f)

    if not filters:
        return always_true()
    result = filters[0]
    for f in filters[1:]:
        result = result & f
    return result


def convert(node: exp.Expression) -> Filter | None:
    """Convert a sqlglot AST expression to a Filter, or None if not convertible.

    Convertible expressions are simple column-op-literal predicates and logical
    combinations (AND, OR, NOT) of them.
    """
    if isinstance(node, exp.And):
        left = convert(node.left)
        right = convert(node.right)
        if left and right:
            return And(left, right)
        return None

    if isinstance(node, exp.Or):
        left = convert(node.left)
        right = convert(node.right)
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
        inner = convert(node.this)
        if inner:
            return Not(inner)
        return None

    if isinstance(node, exp.Paren):
        return convert(node.this)

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


def _flatten_and(node: exp.Expression) -> list[exp.Expression]:
    """Flatten nested AND expressions into a list of conjuncts."""
    if isinstance(node, exp.And):
        return _flatten_and(node.left) + _flatten_and(node.right)
    return [node]

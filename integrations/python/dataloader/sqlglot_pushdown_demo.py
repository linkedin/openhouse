"""Demo: extracting pushed-down predicates and projected columns from sqlglot's optimized AST."""

import sqlglot
from sqlglot import exp
from sqlglot.optimizer import optimize
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.pushdown_projections import pushdown_projections
from sqlglot.optimizer.qualify import qualify

SCHEMA = {
    "t": {
        "a": "INT",
        "b": "TEXT",
        "c": "TEXT",
        "d": "TEXT",
        "e": "TEXT",
        "first_name": "TEXT",
        "last_name": "TEXT",
    }
}

TABLE_NAME = "t"
RULES = [qualify, pushdown_predicates, pushdown_projections]


def find_inner_scan(node: exp.Expression) -> exp.Select | None:
    """Find the SELECT that scans directly from the target table."""
    for select in node.find_all(exp.Select):
        from_ = select.find(exp.From)
        if from_ and from_.find(exp.Table) and from_.find(exp.Table).name == TABLE_NAME:
            # Make sure this SELECT doesn't contain a subquery in its FROM
            # (i.e., it's the innermost scan on the table)
            subqueries = [s for s in select.find_all(exp.Subquery) if s != select]
            if not any(sq.find(exp.Table, bfs=False) for sq in subqueries):
                return select
    return None


def extract_pushdown(sql: str) -> dict:
    """Return the projected columns and pushed-down filter for the inner scan."""
    ast = sqlglot.parse_one(sql)
    optimized = optimize(ast, schema=SCHEMA, rules=RULES)

    inner = find_inner_scan(optimized)
    if inner is None:
        raise ValueError("Could not find inner scan on table")

    # Projected columns: the SELECT expressions of the inner query
    columns = []
    for expr in inner.expressions:
        if isinstance(expr, exp.Alias):
            columns.append(expr.alias)
        elif isinstance(expr, exp.Column):
            columns.append(expr.name)

    # Pushed-down filter: the WHERE clause of the inner query
    where = inner.find(exp.Where)
    pushed_filter = where.this.sql() if where else None

    # Outer filter: anything remaining on the outermost SELECT
    outermost = optimized
    outer_where = outermost.find(exp.Where, bfs=False)
    # Check if it's the outer WHERE (not the inner one)
    outer_filter = None
    if outer_where and outer_where is not where:
        sql_text = outer_where.this.sql()
        if sql_text != "TRUE" and sql_text != "TRUE AND TRUE":
            outer_filter = sql_text

    return {
        "optimized_sql": optimized.sql(pretty=True),
        "projected_columns": columns,
        "pushed_filter": pushed_filter,
        "remaining_outer_filter": outer_filter,
    }


cases = [
    (
        "Projection pruning + inner filter",
        "SELECT a, b FROM (SELECT a, b, c, d FROM t WHERE e = 'foo') sub",
    ),
    (
        "Filter pushdown on passthrough column",
        "SELECT a, b FROM (SELECT a, b, c, d FROM t) sub WHERE a > 10",
    ),
    (
        "Filter on computed column",
        "SELECT a, masked_b FROM (SELECT a, CONCAT('***', SUBSTRING(b, 4)) AS masked_b FROM t) sub WHERE masked_b LIKE '***%'",
    ),
    (
        "Mixed: pushable + computed filter",
        "SELECT a, masked_b FROM (SELECT a, CONCAT('***', SUBSTRING(b, 4)) AS masked_b FROM t) sub WHERE a > 10 AND masked_b LIKE '***%'",
    ),
    (
        "Inner + outer filters merged",
        "SELECT a, b FROM (SELECT a, b, c, d FROM t WHERE e = 'foo') sub WHERE a > 10",
    ),
    (
        "Filter col not in outer projection",
        "SELECT a FROM (SELECT a, b, c FROM t) sub WHERE b = 'bar'",
    ),
    (
        "Computed + passthrough with mixed filters",
        "SELECT a, b, full_name FROM (SELECT a, b, CONCAT(first_name, ' ', last_name) AS full_name FROM t) sub WHERE a > 5 AND full_name = 'John Doe'",
    ),
]

for name, sql in cases:
    print(f"\n{'=' * 70}")
    print(f"Case: {name}")
    print(f"{'=' * 70}")
    print(f"Input: {sql}\n")

    result = extract_pushdown(sql)
    print(f"Optimized SQL:\n{result['optimized_sql']}\n")
    print(f"Projected columns: {result['projected_columns']}")
    print(f"Pushed-down filter: {result['pushed_filter']}")
    print(f"Remaining outer filter: {result['remaining_outer_filter']}")

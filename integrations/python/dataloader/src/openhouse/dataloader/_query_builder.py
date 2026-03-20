"""Build a combined SQL query from transform SQL, user columns, and user filters."""

from __future__ import annotations

from collections.abc import Sequence

from openhouse.dataloader.filters import AlwaysTrue, Filter, _quote_identifier


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
        sql += f" WHERE {filters._to_datafusion_sql()}"
    return sql

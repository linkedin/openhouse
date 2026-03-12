"""Demo: extracting pushed-down predicates and projected columns from DataFusion's
optimized logical plan."""

from datafusion import SessionContext
import pyarrow as pa


def register_iceberg_schema(ctx: SessionContext, table_name: str, schema: pa.Schema) -> None:
    """Register an empty table with the given schema so DataFusion can optimize queries against it."""
    arrays = [pa.array([], type=f.type) for f in schema]
    batch = pa.record_batch(arrays, schema=schema)
    ctx.register_record_batches(table_name, [[batch]])


def extract_pushdown(ctx: SessionContext, sql: str) -> dict:
    """Extract projected columns and pushed-down filters from DataFusion's optimized plan."""
    df = ctx.sql(sql)
    plan = df.optimized_logical_plan()

    output_columns = []
    scan_columns = []
    filters = []

    def walk(node):
        v = node.to_variant()
        name = type(v).__name__

        if name == "Projection":
            for proj in v.projections():
                output_columns.append(proj.schema_name())

        elif name == "Filter":
            filters.append(str(v.predicate()))

        elif name == "TableScan":
            for _idx, col_name in v.projection():
                scan_columns.append(col_name)
            for f in v.filters():
                filters.append(str(f))

        for inp in node.inputs():
            walk(inp)

    walk(plan)

    return {
        "output_columns": output_columns,
        "scan_columns": scan_columns,
        "filters": filters,
    }


TABLE_NAME = "t"
SCHEMA = pa.schema([
    ("a", pa.int64()),
    ("b", pa.string()),
    ("c", pa.string()),
    ("d", pa.string()),
    ("e", pa.string()),
    ("first_name", pa.string()),
    ("last_name", pa.string()),
])

cases = [
    (
        "Projection pruning + inner filter",
        f"SELECT a, b FROM (SELECT a, b, c, d FROM {TABLE_NAME} WHERE e = 'foo') sub",
    ),
    (
        "Filter pushdown on passthrough column",
        f"SELECT a, b FROM (SELECT a, b, c, d FROM {TABLE_NAME}) sub WHERE a > 10",
    ),
    (
        "Filter on computed column",
        f"SELECT a, masked_b FROM (SELECT a, CONCAT('***', SUBSTR(b, 4)) AS masked_b FROM {TABLE_NAME}) sub WHERE masked_b LIKE '***%'",
    ),
    (
        "Mixed: pushable + computed filter",
        f"SELECT a, masked_b FROM (SELECT a, CONCAT('***', SUBSTR(b, 4)) AS masked_b FROM {TABLE_NAME}) sub WHERE a > 10 AND masked_b LIKE '***%'",
    ),
    (
        "Inner + outer filters merged",
        f"SELECT a, b FROM (SELECT a, b, c, d FROM {TABLE_NAME} WHERE e = 'foo') sub WHERE a > 10",
    ),
    (
        "Filter col not in outer projection",
        f"SELECT a FROM (SELECT a, b, c FROM {TABLE_NAME}) sub WHERE b = 'bar'",
    ),
    (
        "Computed + passthrough with mixed filters",
        f"SELECT a, b, full_name FROM (SELECT a, b, CONCAT(first_name, ' ', last_name) AS full_name FROM {TABLE_NAME}) sub WHERE a > 5 AND full_name = 'John Doe'",
    ),
]

ctx = SessionContext()
register_iceberg_schema(ctx, TABLE_NAME, SCHEMA)

for name, sql in cases:
    print(f"\n{'=' * 70}")
    print(f"Case: {name}")
    print(f"{'=' * 70}")
    print(f"Input: {sql}\n")

    result = extract_pushdown(ctx, sql)
    print(f"Output columns: {result['output_columns']}")
    print(f"Scan columns:   {result['scan_columns']}")
    print(f"Filters:        {result['filters']}")

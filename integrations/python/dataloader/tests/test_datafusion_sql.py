from __future__ import annotations

import pytest

from openhouse.dataloader.datafusion_sql import to_datafusion_sql

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

SPARK = "spark"


# ---------------------------------------------------------------------------
# Transpilation tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql, dialect, expected",
    [
        # Spark → DataFusion
        ("SELECT `col1`, `col2` FROM `my_table`", SPARK, 'SELECT "col1", "col2" FROM "my_table"'),
        ("SELECT SIZE(arr) FROM t", SPARK, "SELECT cardinality(arr) FROM t"),
        ("SELECT ARRAY(1, 2, 3)", SPARK, "SELECT make_array(1, 2, 3)"),
        ("SELECT UPPER(name) FROM t", SPARK, "SELECT upper(name) FROM t"),
        ("SELECT my_udf(col1, col2) FROM t", SPARK, "SELECT my_udf(col1, col2) FROM t"),
        ("SELECT IF(x > 0, 'pos', 'neg') FROM t", SPARK, "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t"),
        (
            "SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM t",
            SPARK,
            "SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM t",
        ),
        (
            "SELECT * FROM (SELECT id, name FROM t WHERE id > 10) sub WHERE sub.name IS NOT NULL",
            SPARK,
            "SELECT * FROM (SELECT id, name FROM t WHERE id > 10) AS sub WHERE NOT sub.name IS NULL",
        ),
        ("SELECT 'hello world' AS greeting", SPARK, "SELECT 'hello world' AS greeting"),
        ("SELECT CURRENT_TIMESTAMP()", SPARK, "SELECT now()"),
        ("SELECT CAST(x AS BINARY)", SPARK, "SELECT TRY_CAST(x AS BYTEA)"),
        # MySQL → DataFusion
        ("SELECT CAST(x AS CHAR)", "mysql", "SELECT CAST(x AS VARCHAR)"),
        ("SELECT CAST(x AS DATETIME)", "mysql", "SELECT CAST(x AS TIMESTAMP)"),
        # Postgres → DataFusion
        ("SELECT CAST(x AS TEXT)", "postgres", "SELECT CAST(x AS VARCHAR)"),
        # DataFusion → DataFusion (noop)
        (
            "SELECT cardinality(arr) FROM t WHERE x > 10 ORDER BY x LIMIT 5",
            "datafusion",
            "SELECT cardinality(arr) FROM t WHERE x > 10 ORDER BY x LIMIT 5",
        ),
    ],
)
def test_transpilation(sql: str, dialect: str, expected: str) -> None:
    assert to_datafusion_sql(sql, dialect) == expected


# ---------------------------------------------------------------------------
# to_datafusion_sql error handling and edge cases
# ---------------------------------------------------------------------------


class TestTranslatorEdgeCases:
    def test_multi_statement_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected exactly one"):
            to_datafusion_sql("SELECT 1; SELECT 2", SPARK)

    def test_unsupported_dialect_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported source dialect 'nosuchdialect'"):
            to_datafusion_sql("SELECT 1", "nosuchdialect")

    def test_datafusion_dialect_is_noop(self) -> None:
        sql = "SELECT make_array(1, 2, 3)"
        assert to_datafusion_sql(sql, "datafusion") is sql


# ---------------------------------------------------------------------------
# DataFusion execution tests (requires datafusion package)
# ---------------------------------------------------------------------------


def test_datafusion_execution() -> None:
    import datafusion

    ctx = datafusion.SessionContext()
    translated = to_datafusion_sql("SELECT SIZE(ARRAY(1, 2, 3))", SPARK)
    batch = ctx.sql(translated).collect()[0]
    assert batch.column(0)[0].as_py() == 3

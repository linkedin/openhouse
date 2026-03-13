from __future__ import annotations

import pytest
import sqlglot

from openhouse.dataloader.datafusion_sql import to_datafusion_sql

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

SPARK = "spark"


def _identity(sql: str) -> str:
    """Round-trip: parse as DataFusion, generate as DataFusion."""
    return sqlglot.transpile(sql, read="datafusion", write="datafusion")[0]


# ---------------------------------------------------------------------------
# Spark → DataFusion translation tests
# ---------------------------------------------------------------------------


class TestSparkToDataFusion:
    def test_backtick_to_double_quote(self) -> None:
        assert (
            to_datafusion_sql("SELECT `col1`, `col2` FROM `my_table`", SPARK) == 'SELECT "col1", "col2" FROM "my_table"'
        )

    def test_size_to_cardinality(self) -> None:
        assert to_datafusion_sql("SELECT SIZE(arr) FROM t", SPARK) == "SELECT cardinality(arr) FROM t"

    def test_array_to_make_array(self) -> None:
        assert to_datafusion_sql("SELECT ARRAY(1, 2, 3)", SPARK) == "SELECT make_array(1, 2, 3)"

    def test_function_names_lowercased(self) -> None:
        assert to_datafusion_sql("SELECT UPPER(name) FROM t", SPARK) == "SELECT upper(name) FROM t"

    def test_udf_passthrough(self) -> None:
        assert to_datafusion_sql("SELECT my_udf(col1, col2) FROM t", SPARK) == "SELECT my_udf(col1, col2) FROM t"

    def test_if_expression(self) -> None:
        assert to_datafusion_sql("SELECT IF(x > 0, 'pos', 'neg') FROM t", SPARK) == (
            "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t"
        )

    def test_case_when(self) -> None:
        assert to_datafusion_sql("SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM t", SPARK) == (
            "SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM t"
        )

    def test_nested_subquery(self) -> None:
        assert (
            to_datafusion_sql(
                "SELECT * FROM (SELECT id, name FROM t WHERE id > 10) sub WHERE sub.name IS NOT NULL", SPARK
            )
            == "SELECT * FROM (SELECT id, name FROM t WHERE id > 10) AS sub WHERE NOT sub.name IS NULL"
        )

    def test_string_literal_not_rewritten(self) -> None:
        assert to_datafusion_sql("SELECT 'hello world' AS greeting", SPARK) == "SELECT 'hello world' AS greeting"

    def test_current_timestamp(self) -> None:
        assert to_datafusion_sql("SELECT CURRENT_TIMESTAMP()", SPARK) == "SELECT now()"


# ---------------------------------------------------------------------------
# DataFusion identity / round-trip tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT 1", "SELECT 1"),
        ("SELECT x FROM t WHERE x > 10", "SELECT x FROM t WHERE x > 10"),
        ("SELECT x FROM t ORDER BY x LIMIT 5", "SELECT x FROM t ORDER BY x LIMIT 5"),
        ("SELECT x, COUNT(*) FROM t GROUP BY ALL", "SELECT x, count(*) FROM t GROUP BY ALL"),
        ("SELECT CAST(x AS VARCHAR) FROM t", "SELECT CAST(x AS VARCHAR) FROM t"),
        ("SELECT CAST(x AS TIMESTAMP) FROM t", "SELECT CAST(x AS TIMESTAMP) FROM t"),
        ("SELECT cardinality(arr) FROM t", "SELECT cardinality(arr) FROM t"),
        ("SELECT make_array(1, 2, 3)", "SELECT make_array(1, 2, 3)"),
        ("SELECT array_sort(arr) FROM t", "SELECT array_sort(arr) FROM t"),
        ("SELECT array_has(arr, 1) FROM t", "SELECT array_has(arr, 1) FROM t"),
        ("SELECT bool_and(flag) FROM t", "SELECT bool_and(flag) FROM t"),
        ("SELECT bool_or(flag) FROM t", "SELECT bool_or(flag) FROM t"),
        ("SELECT string_agg(name, ',') FROM t", "SELECT string_agg(name, ',') FROM t"),
        ("SELECT now()", "SELECT now()"),
        ("SELECT DATE_TRUNC('month', ts) FROM t", "SELECT date_trunc('MONTH', ts) FROM t"),
        (
            "SELECT ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) FROM t",
            "SELECT row_number() OVER (PARTITION BY x ORDER BY y) FROM t",
        ),
        ("SELECT RANK() OVER (ORDER BY x) FROM t", "SELECT rank() OVER (ORDER BY x) FROM t"),
    ],
)
def test_datafusion_identity(sql: str, expected: str) -> None:
    assert _identity(sql) == expected


# ---------------------------------------------------------------------------
# Type mapping tests
# ---------------------------------------------------------------------------


class TestTypeMappings:
    def test_char_to_varchar(self) -> None:
        assert to_datafusion_sql("SELECT CAST(x AS CHAR)", "mysql") == "SELECT CAST(x AS VARCHAR)"

    def test_text_to_varchar(self) -> None:
        assert to_datafusion_sql("SELECT CAST(x AS TEXT)", "postgres") == "SELECT CAST(x AS VARCHAR)"

    def test_binary_to_bytea(self) -> None:
        assert to_datafusion_sql("SELECT CAST(x AS BINARY)", SPARK) == "SELECT TRY_CAST(x AS BYTEA)"

    def test_datetime_to_timestamp(self) -> None:
        assert to_datafusion_sql("SELECT CAST(x AS DATETIME)", "mysql") == "SELECT CAST(x AS TIMESTAMP)"


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


class TestDataFusionExecution:
    def setup_method(self) -> None:
        import datafusion

        self.ctx = datafusion.SessionContext()

    def test_make_array_executes(self) -> None:
        result = self.ctx.sql(to_datafusion_sql("SELECT ARRAY(1, 2, 3)", SPARK)).collect()
        assert len(result) == 1

    def test_cardinality_executes(self) -> None:
        translated = to_datafusion_sql("SELECT SIZE(ARRAY(1, 2, 3))", SPARK)
        result = self.ctx.sql(translated).collect()
        batch = result[0]
        assert batch.column(0)[0].as_py() == 3

    def test_now_executes(self) -> None:
        translated = to_datafusion_sql("SELECT CURRENT_TIMESTAMP()", SPARK)
        result = self.ctx.sql(translated).collect()
        assert len(result) == 1

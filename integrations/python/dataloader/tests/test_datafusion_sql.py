from __future__ import annotations

import pytest
import sqlglot

from openhouse.dataloader.datafusion_sql import to_datafusion_sql

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _identity(sql: str) -> str:
    """Round-trip: parse as DataFusion, generate as DataFusion."""
    return sqlglot.transpile(sql, read="datafusion", write="datafusion")[0]


def _spark_to_df(sql: str) -> str:
    """Transpile Spark SQL → DataFusion SQL."""
    return sqlglot.transpile(sql, read="spark", write="datafusion")[0]


# ---------------------------------------------------------------------------
# Spark → DataFusion translation tests
# ---------------------------------------------------------------------------


class TestSparkToDataFusion:
    def test_backtick_to_double_quote(self) -> None:
        assert _spark_to_df("SELECT `col1`, `col2` FROM `my_table`") == 'SELECT "col1", "col2" FROM "my_table"'

    def test_size_to_cardinality(self) -> None:
        assert _spark_to_df("SELECT SIZE(arr) FROM t") == "SELECT cardinality(arr) FROM t"

    def test_array_to_make_array(self) -> None:
        assert _spark_to_df("SELECT ARRAY(1, 2, 3)") == "SELECT make_array(1, 2, 3)"

    def test_function_names_lowercased(self) -> None:
        assert _spark_to_df("SELECT UPPER(name) FROM t") == "SELECT upper(name) FROM t"

    def test_udf_passthrough(self) -> None:
        assert _spark_to_df("SELECT my_udf(col1, col2) FROM t") == "SELECT my_udf(col1, col2) FROM t"

    def test_if_expression(self) -> None:
        assert _spark_to_df("SELECT IF(x > 0, 'pos', 'neg') FROM t") == (
            "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t"
        )

    def test_case_when(self) -> None:
        assert _spark_to_df("SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM t") == (
            "SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM t"
        )

    def test_nested_subquery(self) -> None:
        assert (
            _spark_to_df("SELECT * FROM (SELECT id, name FROM t WHERE id > 10) sub WHERE sub.name IS NOT NULL")
            == "SELECT * FROM (SELECT id, name FROM t WHERE id > 10) AS sub WHERE NOT sub.name IS NULL"
        )

    def test_string_literal_not_rewritten(self) -> None:
        assert _spark_to_df("SELECT 'hello world' AS greeting") == "SELECT 'hello world' AS greeting"

    def test_current_timestamp(self) -> None:
        assert _spark_to_df("SELECT CURRENT_TIMESTAMP()") == "SELECT now()"


# ---------------------------------------------------------------------------
# DataFusion identity / round-trip tests
# ---------------------------------------------------------------------------


class TestDataFusionIdentity:
    def test_basic_select(self) -> None:
        assert _identity("SELECT 1") == "SELECT 1"

    def test_where_clause(self) -> None:
        assert _identity("SELECT x FROM t WHERE x > 10") == "SELECT x FROM t WHERE x > 10"

    def test_order_by_limit(self) -> None:
        assert _identity("SELECT x FROM t ORDER BY x LIMIT 5") == "SELECT x FROM t ORDER BY x LIMIT 5"

    def test_group_by_all(self) -> None:
        assert _identity("SELECT x, COUNT(*) FROM t GROUP BY ALL") == "SELECT x, count(*) FROM t GROUP BY ALL"

    def test_cast_varchar(self) -> None:
        assert _identity("SELECT CAST(x AS VARCHAR) FROM t") == "SELECT CAST(x AS VARCHAR) FROM t"

    def test_cast_timestamp(self) -> None:
        assert _identity("SELECT CAST(x AS TIMESTAMP) FROM t") == "SELECT CAST(x AS TIMESTAMP) FROM t"

    def test_cardinality(self) -> None:
        assert _identity("SELECT cardinality(arr) FROM t") == "SELECT cardinality(arr) FROM t"

    def test_make_array(self) -> None:
        assert _identity("SELECT make_array(1, 2, 3)") == "SELECT make_array(1, 2, 3)"

    def test_array_sort(self) -> None:
        assert _identity("SELECT array_sort(arr) FROM t") == "SELECT array_sort(arr) FROM t"

    def test_array_has(self) -> None:
        assert _identity("SELECT array_has(arr, 1) FROM t") == "SELECT array_has(arr, 1) FROM t"

    def test_bool_and(self) -> None:
        assert _identity("SELECT bool_and(flag) FROM t") == "SELECT bool_and(flag) FROM t"

    def test_bool_or(self) -> None:
        assert _identity("SELECT bool_or(flag) FROM t") == "SELECT bool_or(flag) FROM t"

    def test_string_agg(self) -> None:
        assert _identity("SELECT string_agg(name, ',') FROM t") == "SELECT string_agg(name, ',') FROM t"

    def test_now(self) -> None:
        assert _identity("SELECT now()") == "SELECT now()"

    def test_date_trunc(self) -> None:
        assert _identity("SELECT DATE_TRUNC('month', ts) FROM t") == "SELECT date_trunc('MONTH', ts) FROM t"

    def test_window_row_number(self) -> None:
        assert (
            _identity("SELECT ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) FROM t")
            == "SELECT row_number() OVER (PARTITION BY x ORDER BY y) FROM t"
        )

    def test_window_rank(self) -> None:
        assert _identity("SELECT RANK() OVER (ORDER BY x) FROM t") == "SELECT rank() OVER (ORDER BY x) FROM t"


# ---------------------------------------------------------------------------
# Type mapping tests
# ---------------------------------------------------------------------------


class TestTypeMappings:
    def test_char_to_varchar(self) -> None:
        assert sqlglot.transpile("SELECT CAST(x AS CHAR)", read="mysql", write="datafusion")[0] == (
            "SELECT CAST(x AS VARCHAR)"
        )

    def test_text_to_varchar(self) -> None:
        assert sqlglot.transpile("SELECT CAST(x AS TEXT)", read="postgres", write="datafusion")[0] == (
            "SELECT CAST(x AS VARCHAR)"
        )

    def test_binary_to_bytea(self) -> None:
        assert sqlglot.transpile("SELECT CAST(x AS BINARY)", read="spark", write="datafusion")[0] == (
            "SELECT TRY_CAST(x AS BYTEA)"
        )

    def test_datetime_to_timestamp(self) -> None:
        assert sqlglot.transpile("SELECT CAST(x AS DATETIME)", read="mysql", write="datafusion")[0] == (
            "SELECT CAST(x AS TIMESTAMP)"
        )


# ---------------------------------------------------------------------------
# to_datafusion_sql tests
# ---------------------------------------------------------------------------


class TestTranslator:
    def test_simple_translate(self) -> None:
        assert to_datafusion_sql("SELECT 1", source_dialect="spark") == "SELECT 1"

    def test_multi_statement_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected exactly one"):
            to_datafusion_sql("SELECT 1; SELECT 2", source_dialect="spark")

    def test_unsupported_dialect_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported source dialect 'nosuchdialect'"):
            to_datafusion_sql("SELECT 1", source_dialect="nosuchdialect")

    def test_different_source_dialect(self) -> None:
        assert to_datafusion_sql("SELECT 1", source_dialect="postgres") == "SELECT 1"

    def test_datafusion_dialect_is_noop(self) -> None:
        sql = "SELECT make_array(1, 2, 3)"
        assert to_datafusion_sql(sql, source_dialect="datafusion") is sql


# ---------------------------------------------------------------------------
# DataFusion execution tests (requires datafusion package)
# ---------------------------------------------------------------------------


class TestDataFusionExecution:
    def setup_method(self) -> None:
        import datafusion

        self.ctx = datafusion.SessionContext()

    def test_make_array_executes(self) -> None:
        result = self.ctx.sql(_spark_to_df("SELECT ARRAY(1, 2, 3)")).collect()
        assert len(result) == 1

    def test_cardinality_executes(self) -> None:
        translated = _spark_to_df("SELECT SIZE(ARRAY(1, 2, 3))")
        result = self.ctx.sql(translated).collect()
        batch = result[0]
        assert batch.column(0)[0].as_py() == 3

    def test_now_executes(self) -> None:
        translated = _spark_to_df("SELECT CURRENT_TIMESTAMP()")
        result = self.ctx.sql(translated).collect()
        assert len(result) == 1

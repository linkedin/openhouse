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
        result = _spark_to_df("SELECT `col1`, `col2` FROM `my_table`")
        assert "`" not in result
        assert '"col1"' in result

    def test_size_to_cardinality(self) -> None:
        result = _spark_to_df("SELECT SIZE(arr) FROM t")
        assert "cardinality" in result.lower()

    def test_array_to_make_array(self) -> None:
        result = _spark_to_df("SELECT ARRAY(1, 2, 3)")
        assert "make_array" in result.lower()

    def test_function_names_lowercased(self) -> None:
        result = _spark_to_df("SELECT UPPER(name) FROM t")
        assert "upper" in result

    def test_udf_passthrough(self) -> None:
        result = _spark_to_df("SELECT my_udf(col1, col2) FROM t")
        assert "my_udf" in result.lower()

    def test_if_expression(self) -> None:
        result = _spark_to_df("SELECT IF(x > 0, 'pos', 'neg') FROM t")
        assert result  # Should not error

    def test_case_when(self) -> None:
        sql = "SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM t"
        result = _spark_to_df(sql)
        assert "CASE" in result
        assert "WHEN" in result

    def test_nested_subquery(self) -> None:
        sql = "SELECT * FROM (SELECT id, name FROM t WHERE id > 10) sub WHERE sub.name IS NOT NULL"
        result = _spark_to_df(sql)
        assert "SELECT" in result

    def test_string_literal_not_rewritten(self) -> None:
        result = _spark_to_df("SELECT 'hello world' AS greeting")
        assert "'hello world'" in result

    def test_current_timestamp(self) -> None:
        result = _spark_to_df("SELECT CURRENT_TIMESTAMP()")
        assert "now()" in result.lower()


# ---------------------------------------------------------------------------
# DataFusion identity / round-trip tests
# ---------------------------------------------------------------------------


class TestDataFusionIdentity:
    def test_basic_select(self) -> None:
        assert "SELECT" in _identity("SELECT 1")

    def test_where_clause(self) -> None:
        result = _identity("SELECT x FROM t WHERE x > 10")
        assert "WHERE" in result

    def test_order_by_limit(self) -> None:
        result = _identity("SELECT x FROM t ORDER BY x LIMIT 5")
        assert "ORDER BY" in result
        assert "LIMIT" in result

    def test_group_by_all(self) -> None:
        result = _identity("SELECT x, COUNT(*) FROM t GROUP BY ALL")
        assert "GROUP BY ALL" in result.upper()

    def test_cast_varchar(self) -> None:
        result = _identity("SELECT CAST(x AS VARCHAR) FROM t")
        assert "VARCHAR" in result

    def test_cast_timestamp(self) -> None:
        result = _identity("SELECT CAST(x AS TIMESTAMP) FROM t")
        assert "TIMESTAMP" in result

    def test_cardinality(self) -> None:
        result = _identity("SELECT cardinality(arr) FROM t")
        assert "cardinality" in result.lower()

    def test_make_array(self) -> None:
        result = _identity("SELECT make_array(1, 2, 3)")
        assert "make_array" in result.lower()

    def test_array_sort(self) -> None:
        result = _identity("SELECT array_sort(arr) FROM t")
        assert "array_sort" in result.lower()

    def test_array_has(self) -> None:
        result = _identity("SELECT array_has(arr, 1) FROM t")
        assert "array_has" in result.lower()

    def test_bool_and(self) -> None:
        result = _identity("SELECT bool_and(flag) FROM t")
        assert "bool_and" in result.lower()

    def test_bool_or(self) -> None:
        result = _identity("SELECT bool_or(flag) FROM t")
        assert "bool_or" in result.lower()

    def test_string_agg(self) -> None:
        result = _identity("SELECT string_agg(name, ',') FROM t")
        assert "string_agg" in result.lower()

    def test_now(self) -> None:
        result = _identity("SELECT now()")
        assert "now()" in result.lower()

    def test_date_trunc(self) -> None:
        result = _identity("SELECT DATE_TRUNC('month', ts) FROM t")
        assert "date_trunc" in result.lower()

    def test_window_row_number(self) -> None:
        result = _identity("SELECT ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) FROM t")
        assert "ROW_NUMBER" in result.upper()
        assert "OVER" in result.upper()

    def test_window_rank(self) -> None:
        result = _identity("SELECT RANK() OVER (ORDER BY x) FROM t")
        assert "RANK" in result.upper()


# ---------------------------------------------------------------------------
# Type mapping tests
# ---------------------------------------------------------------------------


class TestTypeMappings:
    def test_char_to_varchar(self) -> None:
        result = sqlglot.transpile("SELECT CAST(x AS CHAR)", read="mysql", write="datafusion")[0]
        assert "VARCHAR" in result

    def test_text_to_varchar(self) -> None:
        result = sqlglot.transpile("SELECT CAST(x AS TEXT)", read="postgres", write="datafusion")[0]
        assert "VARCHAR" in result

    def test_binary_to_bytea(self) -> None:
        result = sqlglot.transpile("SELECT CAST(x AS BINARY)", read="spark", write="datafusion")[0]
        assert "BYTEA" in result

    def test_datetime_to_timestamp(self) -> None:
        result = sqlglot.transpile("SELECT CAST(x AS DATETIME)", read="mysql", write="datafusion")[0]
        assert "TIMESTAMP" in result


# ---------------------------------------------------------------------------
# to_datafusion_sql tests
# ---------------------------------------------------------------------------


class TestTranslator:
    def test_simple_translate(self) -> None:
        result = to_datafusion_sql("SELECT 1", source_dialect="spark")
        assert "SELECT" in result

    def test_multi_statement_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected exactly one"):
            to_datafusion_sql("SELECT 1; SELECT 2", source_dialect="spark")

    def test_unsupported_dialect_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported source dialect 'nosuchdialect'"):
            to_datafusion_sql("SELECT 1", source_dialect="nosuchdialect")

    def test_different_source_dialect(self) -> None:
        result = to_datafusion_sql("SELECT 1", source_dialect="postgres")
        assert "SELECT" in result

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

from __future__ import annotations

import datafusion
import pyarrow as pa
import pytest

from openhouse.dataloader.datafusion_sql import to_datafusion_sql
from openhouse.dataloader.filters import SqlTarget
from openhouse.dataloader.table_identifier import TableIdentifier

_DB_TBL = TableIdentifier(database="db", table="tbl")

# ---------------------------------------------------------------------------
# Transpilation tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql, dialect, expected",
    [
        # Spark → DataFusion
        ("SELECT `col1`, `col2` FROM `my_table`", SqlTarget.SPARK, 'SELECT "col1", "col2" FROM "my_table"'),
        ("SELECT SIZE(arr) FROM t", SqlTarget.SPARK, "SELECT cardinality(arr) FROM t"),
        ("SELECT ARRAY(1, 2, 3)", SqlTarget.SPARK, "SELECT make_array(1, 2, 3)"),
        ("SELECT UPPER(name) FROM t", SqlTarget.SPARK, "SELECT upper(name) FROM t"),
        ("SELECT my_udf(col1, col2) FROM t", SqlTarget.SPARK, "SELECT my_udf(col1, col2) FROM t"),
        (
            "SELECT IF(x > 0, 'pos', 'neg') FROM t",
            SqlTarget.SPARK,
            "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t",
        ),
        (
            "SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM t",
            SqlTarget.SPARK,
            "SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM t",
        ),
        (
            "SELECT * FROM (SELECT id, name FROM t WHERE id > 10) sub WHERE sub.name IS NOT NULL",
            SqlTarget.SPARK,
            "SELECT * FROM (SELECT id, name FROM t WHERE id > 10) AS sub WHERE NOT sub.name IS NULL",
        ),
        ("SELECT 'hello world' AS greeting", SqlTarget.SPARK, "SELECT 'hello world' AS greeting"),
        ("SELECT CURRENT_TIMESTAMP()", SqlTarget.SPARK, "SELECT now()"),
        ("SELECT CAST(x AS BINARY)", SqlTarget.SPARK, "SELECT TRY_CAST(x AS BYTEA)"),
        # DataFusion → DataFusion (noop)
        (
            "SELECT cardinality(arr) FROM t WHERE x > 10 ORDER BY x LIMIT 5",
            SqlTarget.DATA_FUSION,
            "SELECT cardinality(arr) FROM t WHERE x > 10 ORDER BY x LIMIT 5",
        ),
    ],
)
def test_transpilation(sql: str, dialect: SqlTarget, expected: str) -> None:
    assert to_datafusion_sql(sql, dialect) == expected


# ---------------------------------------------------------------------------
# to_datafusion_sql error handling and edge cases
# ---------------------------------------------------------------------------


class TestTranslatorEdgeCases:
    def test_multi_statement_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected exactly one"):
            to_datafusion_sql("SELECT 1; SELECT 2", SqlTarget.SPARK)

    def test_syntax_error_raises(self) -> None:
        with pytest.raises(ValueError, match="Failed to transpile SQL from 'spark' to DataFusion"):
            to_datafusion_sql("SELECT * FROM", SqlTarget.SPARK)

    def test_datafusion_dialect_is_noop(self) -> None:
        sql = "SELECT make_array(1, 2, 3)"
        assert to_datafusion_sql(sql, SqlTarget.DATA_FUSION) is sql


# ---------------------------------------------------------------------------
# Filter injection tests
# ---------------------------------------------------------------------------


class TestTableValidation:
    def test_validates_single_table(self) -> None:
        result = to_datafusion_sql('SELECT id FROM "db"."tbl"', SqlTarget.DATA_FUSION, table=_DB_TBL)
        assert result == 'SELECT id FROM "db"."tbl"'

    def test_wrong_table_name_raises(self) -> None:
        with pytest.raises(ValueError, match="references db.other, expected db.tbl"):
            to_datafusion_sql('SELECT id FROM "db"."other"', SqlTarget.DATA_FUSION, table=_DB_TBL)

    def test_wrong_database_raises(self) -> None:
        with pytest.raises(ValueError, match="references other.tbl, expected db.tbl"):
            to_datafusion_sql('SELECT id FROM "other"."tbl"', SqlTarget.DATA_FUSION, table=_DB_TBL)

    def test_multiple_tables_raises(self) -> None:
        with pytest.raises(ValueError, match="exactly 1 table, found 2"):
            to_datafusion_sql(
                'SELECT * FROM "db"."tbl" JOIN "db"."tbl" AS t2 ON tbl.id = t2.id',
                SqlTarget.DATA_FUSION,
                table=_DB_TBL,
            )

    def test_no_table_raises(self) -> None:
        with pytest.raises(ValueError, match="exactly 1 table, found 0"):
            to_datafusion_sql("SELECT 1 AS x", SqlTarget.DATA_FUSION, table=_DB_TBL)

    def test_case_insensitive_table_match(self) -> None:
        result = to_datafusion_sql('SELECT id FROM "DB"."TBL"', SqlTarget.DATA_FUSION, table=_DB_TBL)
        assert result == 'SELECT id FROM "DB"."TBL"'

    def test_spark_table_validated_after_transpilation(self) -> None:
        result = to_datafusion_sql("SELECT id FROM `db`.`tbl`", SqlTarget.SPARK, table=_DB_TBL)
        assert result == 'SELECT id FROM "db"."tbl"'


class TestNoOp:
    def test_no_filter_no_table_is_noop(self) -> None:
        sql = 'SELECT id FROM "db"."tbl"'
        assert to_datafusion_sql(sql, SqlTarget.DATA_FUSION) is sql


# ---------------------------------------------------------------------------
# DataFusion execution tests (requires datafusion package)
# ---------------------------------------------------------------------------


def test_datafusion_execution() -> None:
    ctx = datafusion.SessionContext()
    translated = to_datafusion_sql("SELECT SIZE(ARRAY(1, 2, 3))", SqlTarget.SPARK)
    batch = ctx.sql(translated).collect()[0]
    assert batch.column(0)[0].as_py() == 3


def test_datafusion_execution_median() -> None:
    ctx = datafusion.SessionContext()
    translated = to_datafusion_sql("SELECT MEDIAN(x) FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)", SqlTarget.SPARK)
    assert translated == "SELECT median(x) FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)"
    batch = ctx.sql(translated).collect()[0]
    assert batch.column(0)[0].as_py() == 3


def test_datafusion_execution_percentile_cont() -> None:
    ctx = datafusion.SessionContext()
    translated = to_datafusion_sql(
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)",
        SqlTarget.SPARK,
    )
    expected = (
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x NULLS FIRST)"
        " FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)"
    )
    assert translated == expected
    batch = ctx.sql(translated).collect()[0]
    assert batch.column(0)[0].as_py() == 3.0


def test_datafusion_execution_approx_percentile_cont() -> None:
    ctx = datafusion.SessionContext()
    translated = to_datafusion_sql(
        "SELECT PERCENTILE_APPROX(x, 0.5) FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)",
        SqlTarget.SPARK,
    )
    assert translated == "SELECT approx_percentile_cont(x, 0.5) FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)"
    batch = ctx.sql(translated).collect()[0]
    assert batch.column(0)[0].as_py() == 3


def test_datafusion_execution_udf() -> None:
    ctx = datafusion.SessionContext()

    def double_it(arr: pa.Array) -> pa.Array:
        return pa.array([x * 2 for x in arr.to_pylist()])

    ctx.register_udf(datafusion.udf(double_it, [pa.int64()], pa.int64(), "stable", name="double_it"))

    translated = to_datafusion_sql("SELECT double_it(x) FROM (VALUES (5)) AS t(x)", SqlTarget.SPARK)
    assert translated == "SELECT double_it(x) FROM (VALUES (5)) AS t(x)"
    batch = ctx.sql(translated).collect()[0]
    assert batch.column(0)[0].as_py() == 10

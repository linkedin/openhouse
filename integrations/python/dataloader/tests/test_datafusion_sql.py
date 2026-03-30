from __future__ import annotations

import datafusion
import pyarrow as pa
import pytest

from openhouse.dataloader.datafusion_sql import to_datafusion_sql
from openhouse.dataloader.table_identifier import TableIdentifier

_DB_TBL = TableIdentifier(database="db", table="tbl")

# ---------------------------------------------------------------------------
# Transpilation tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql, dialect, expected",
    [
        # Spark → DataFusion
        ("SELECT `col1`, `col2` FROM `my_table`", "spark", 'SELECT "col1", "col2" FROM "my_table"'),
        ("SELECT SIZE(arr) FROM t", "spark", "SELECT cardinality(arr) FROM t"),
        ("SELECT ARRAY(1, 2, 3)", "spark", "SELECT make_array(1, 2, 3)"),
        ("SELECT UPPER(name) FROM t", "spark", "SELECT upper(name) FROM t"),
        ("SELECT my_udf(col1, col2) FROM t", "spark", "SELECT my_udf(col1, col2) FROM t"),
        ("SELECT IF(x > 0, 'pos', 'neg') FROM t", "spark", "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t"),
        (
            "SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM t",
            "spark",
            "SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM t",
        ),
        (
            "SELECT * FROM (SELECT id, name FROM t WHERE id > 10) sub WHERE sub.name IS NOT NULL",
            "spark",
            "SELECT * FROM (SELECT id, name FROM t WHERE id > 10) AS sub WHERE NOT sub.name IS NULL",
        ),
        ("SELECT 'hello world' AS greeting", "spark", "SELECT 'hello world' AS greeting"),
        ("SELECT CURRENT_TIMESTAMP()", "spark", "SELECT now()"),
        ("SELECT CAST(x AS BINARY)", "spark", "SELECT TRY_CAST(x AS BYTEA)"),
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
            to_datafusion_sql("SELECT 1; SELECT 2", "spark")

    def test_unsupported_dialect_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported source dialect 'nosuchdialect'"):
            to_datafusion_sql("SELECT 1", "nosuchdialect")

    def test_syntax_error_raises(self) -> None:
        with pytest.raises(ValueError, match="Failed to transpile SQL from 'spark' to DataFusion"):
            to_datafusion_sql("SELECT * FROM", "spark")

    def test_datafusion_dialect_is_noop(self) -> None:
        sql = "SELECT make_array(1, 2, 3)"
        assert to_datafusion_sql(sql, "datafusion") is sql


# ---------------------------------------------------------------------------
# Filter injection tests
# ---------------------------------------------------------------------------


class TestTableValidation:
    def test_validates_single_table(self) -> None:
        result = to_datafusion_sql('SELECT id FROM "db"."tbl"', "datafusion", table=_DB_TBL)
        assert result == 'SELECT id FROM "db"."tbl"'

    def test_wrong_table_name_raises(self) -> None:
        with pytest.raises(ValueError, match="references db.other, expected db.tbl"):
            to_datafusion_sql('SELECT id FROM "db"."other"', "datafusion", table=_DB_TBL)

    def test_wrong_database_raises(self) -> None:
        with pytest.raises(ValueError, match="references other.tbl, expected db.tbl"):
            to_datafusion_sql('SELECT id FROM "other"."tbl"', "datafusion", table=_DB_TBL)

    def test_multiple_tables_raises(self) -> None:
        with pytest.raises(ValueError, match="exactly 1 table, found 2"):
            to_datafusion_sql(
                'SELECT * FROM "db"."tbl" JOIN "db"."tbl" AS t2 ON tbl.id = t2.id',
                "datafusion",
                table=_DB_TBL,
            )

    def test_no_table_raises(self) -> None:
        with pytest.raises(ValueError, match="exactly 1 table, found 0"):
            to_datafusion_sql("SELECT 1 AS x", "datafusion", table=_DB_TBL)

    def test_case_insensitive_table_match(self) -> None:
        result = to_datafusion_sql('SELECT id FROM "DB"."TBL"', "datafusion", table=_DB_TBL)
        assert result == 'SELECT id FROM "DB"."TBL"'

    def test_spark_table_validated_after_transpilation(self) -> None:
        result = to_datafusion_sql("SELECT id FROM `db`.`tbl`", "spark", table=_DB_TBL)
        assert result == 'SELECT id FROM "db"."tbl"'


class TestFilterInjection:
    def test_injects_filter_into_table_scan(self) -> None:
        result = to_datafusion_sql(
            'SELECT id, \'MASKED\' AS name, value FROM "db"."tbl" WHERE value > 1.5',
            "datafusion",
            table=_DB_TBL,
            filter_sql='"id" > 10',
        )
        assert result == (
            "SELECT id, 'MASKED' AS name, value"
            ' FROM (SELECT * FROM "db"."tbl" WHERE "id" > 10) AS tbl'
            " WHERE value > 1.5"
        )

    def test_injects_filter_with_dialect_transpilation(self) -> None:
        result = to_datafusion_sql(
            "SELECT `id`, `name` FROM `db`.`tbl`",
            "spark",
            table=_DB_TBL,
            filter_sql='"id" > 10',
        )
        assert result == 'SELECT "id", "name" FROM (SELECT * FROM "db"."tbl" WHERE "id" > 10) AS tbl'

    def test_no_filter_no_table_is_noop(self) -> None:
        sql = 'SELECT id FROM "db"."tbl"'
        assert to_datafusion_sql(sql, "datafusion") is sql


# ---------------------------------------------------------------------------
# DataFusion execution tests (requires datafusion package)
# ---------------------------------------------------------------------------


def test_datafusion_execution() -> None:
    ctx = datafusion.SessionContext()
    translated = to_datafusion_sql("SELECT SIZE(ARRAY(1, 2, 3))", "spark")
    batch = ctx.sql(translated).collect()[0]
    assert batch.column(0)[0].as_py() == 3


def test_datafusion_execution_median() -> None:
    ctx = datafusion.SessionContext()
    translated = to_datafusion_sql("SELECT MEDIAN(x) FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)", "spark")
    assert translated == "SELECT median(x) FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)"
    batch = ctx.sql(translated).collect()[0]
    assert batch.column(0)[0].as_py() == 3


def test_datafusion_execution_percentile_cont() -> None:
    ctx = datafusion.SessionContext()
    translated = to_datafusion_sql(
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)",
        "spark",
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
        "spark",
    )
    assert translated == "SELECT approx_percentile_cont(x, 0.5) FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)"
    batch = ctx.sql(translated).collect()[0]
    assert batch.column(0)[0].as_py() == 3


def test_datafusion_execution_udf() -> None:
    ctx = datafusion.SessionContext()

    def double_it(arr: pa.Array) -> pa.Array:
        return pa.array([x * 2 for x in arr.to_pylist()])

    ctx.register_udf(datafusion.udf(double_it, [pa.int64()], pa.int64(), "stable", name="double_it"))

    translated = to_datafusion_sql("SELECT double_it(x) FROM (VALUES (5)) AS t(x)", "spark")
    assert translated == "SELECT double_it(x) FROM (VALUES (5)) AS t(x)"
    batch = ctx.sql(translated).collect()[0]
    assert batch.column(0)[0].as_py() == 10

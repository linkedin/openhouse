import math
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from pyiceberg import expressions as ice

from openhouse.dataloader import col
from openhouse.dataloader.filters import (
    AlwaysTrue,
    And,
    Between,
    Column,
    EqualTo,
    Filter,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNotNaN,
    IsNotNull,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotStartsWith,
    Or,
    StartsWith,
    _to_datafusion_sql,
    _to_pyiceberg,
    always_true,
)


class TestColumnCreation:
    def test_col_returns_column(self):
        c = col("x")
        assert isinstance(c, Column)
        assert c.name == "x"

    def test_col_is_not_a_filter(self):
        assert not isinstance(col("x"), Filter)


class TestAlwaysTrue:
    def test_always_true_is_a_filter(self):
        assert isinstance(always_true(), Filter)

    def test_always_true_returns_always_true(self):
        assert isinstance(always_true(), AlwaysTrue)

    def test_always_true_repr(self):
        assert repr(always_true()) == "always_true()"


class TestComparisonOperators:
    def test_equal(self):
        f = col("x") == 5
        assert isinstance(f, EqualTo)
        assert f.column == "x"
        assert f.value == 5

    def test_not_equal(self):
        f = col("x") != 5
        assert isinstance(f, NotEqualTo)
        assert f.column == "x"
        assert f.value == 5

    def test_greater_than(self):
        f = col("x") > 5
        assert isinstance(f, GreaterThan)
        assert f.column == "x"
        assert f.value == 5

    def test_greater_than_or_equal(self):
        f = col("x") >= 5
        assert isinstance(f, GreaterThanOrEqual)
        assert f.column == "x"
        assert f.value == 5

    def test_less_than(self):
        f = col("x") < 5
        assert isinstance(f, LessThan)
        assert f.column == "x"
        assert f.value == 5

    def test_less_than_or_equal(self):
        f = col("x") <= 5
        assert isinstance(f, LessThanOrEqual)
        assert f.column == "x"
        assert f.value == 5

    def test_comparison_with_string(self):
        f = col("name") == "alice"
        assert isinstance(f, EqualTo)
        assert f.value == "alice"

    def test_comparison_with_float(self):
        f = col("score") > 3.14
        assert isinstance(f, GreaterThan)
        assert f.value == 3.14

    def test_comparison_with_datetime(self):
        dt = datetime(2026, 4, 27, tzinfo=UTC)
        f = col("datepartition") >= dt
        assert isinstance(f, GreaterThanOrEqual)
        assert f.value == dt

    def test_comparison_with_date(self):
        d = date(2026, 4, 27)
        f = col("datepartition") >= d
        assert isinstance(f, GreaterThanOrEqual)
        assert f.value == d


class TestNullAndNanChecks:
    def test_is_null(self):
        f = col("x").is_null()
        assert isinstance(f, IsNull)
        assert f.column == "x"

    def test_is_not_null(self):
        f = col("x").is_not_null()
        assert isinstance(f, IsNotNull)
        assert f.column == "x"

    def test_is_nan(self):
        f = col("x").is_nan()
        assert isinstance(f, IsNaN)
        assert f.column == "x"

    def test_is_not_nan(self):
        f = col("x").is_not_nan()
        assert isinstance(f, IsNotNaN)
        assert f.column == "x"


class TestSetOperations:
    def test_is_in(self):
        f = col("x").is_in([1, 2, 3])
        assert isinstance(f, In)
        assert f.column == "x"
        assert f.values == (1, 2, 3)

    def test_is_not_in(self):
        f = col("x").is_not_in([1, 2, 3])
        assert isinstance(f, NotIn)
        assert f.column == "x"
        assert f.values == (1, 2, 3)

    def test_is_in_with_tuple(self):
        f = col("x").is_in((4, 5))
        assert f.values == (4, 5)

    def test_is_in_with_set(self):
        f = col("x").is_in({1})
        assert isinstance(f, In)
        assert f.values == (1,)


class TestStringPrefixFilters:
    def test_starts_with(self):
        f = col("name").starts_with("abc")
        assert isinstance(f, StartsWith)
        assert f.column == "name"
        assert f.prefix == "abc"

    def test_not_starts_with(self):
        f = col("name").not_starts_with("abc")
        assert isinstance(f, NotStartsWith)
        assert f.column == "name"
        assert f.prefix == "abc"


class TestBetween:
    def test_between(self):
        f = col("x").between(1, 10)
        assert isinstance(f, Between)
        assert f.column == "x"
        assert f.lower == 1
        assert f.upper == 10


class TestLogicalCombinators:
    def test_and(self):
        f = (col("x") > 5) & (col("y") == "a")
        assert isinstance(f, And)
        assert isinstance(f.left, GreaterThan)
        assert isinstance(f.right, EqualTo)

    def test_or(self):
        f = (col("x") > 5) | (col("y") == "a")
        assert isinstance(f, Or)
        assert isinstance(f.left, GreaterThan)
        assert isinstance(f.right, EqualTo)

    def test_not(self):
        f = ~col("z").is_null()
        assert isinstance(f, Not)
        assert isinstance(f.operand, IsNull)

    def test_complex_composition(self):
        f = (col("x") > 5) & (col("y") == "a") | ~col("z").is_null()
        # Due to operator precedence: ((x > 5) & (y == 'a')) | (~z.is_null())
        assert isinstance(f, Or)
        assert isinstance(f.left, And)
        assert isinstance(f.right, Not)


class TestFilterImmutability:
    def test_dataclass_frozen(self):
        f = col("x") == 5
        try:
            f.column = "y"  # type: ignore[misc]
            raise AssertionError("Should have raised AttributeError")
        except AttributeError:
            pass


class TestRepr:
    def test_column_repr(self):
        assert repr(col("x")) == "col('x')"

    def test_equal_repr(self):
        assert repr(col("x") == 5) == "col('x') == 5"

    def test_and_repr(self):
        f = (col("x") > 1) & (col("y") < 2)
        assert repr(f) == "(col('x') > 1 & col('y') < 2)"


# --- PyIceberg conversion tests ---


class TestPyIcebergAlwaysTrueConversion:
    def test_always_true(self):
        result = _to_pyiceberg(always_true())
        assert isinstance(result, ice.AlwaysTrue)


class TestPyIcebergComparisonConversion:
    def test_equal_to(self):
        result = _to_pyiceberg(col("x") == 5)
        assert isinstance(result, ice.EqualTo)
        assert result.term.name == "x"

    def test_not_equal_to(self):
        result = _to_pyiceberg(col("x") != 5)
        assert isinstance(result, ice.NotEqualTo)

    def test_greater_than(self):
        result = _to_pyiceberg(col("x") > 5)
        assert isinstance(result, ice.GreaterThan)

    def test_greater_than_or_equal(self):
        result = _to_pyiceberg(col("x") >= 5)
        assert isinstance(result, ice.GreaterThanOrEqual)

    def test_less_than(self):
        result = _to_pyiceberg(col("x") < 5)
        assert isinstance(result, ice.LessThan)

    def test_less_than_or_equal(self):
        result = _to_pyiceberg(col("x") <= 5)
        assert isinstance(result, ice.LessThanOrEqual)


class TestPyIcebergNullNanConversion:
    def test_is_null(self):
        result = _to_pyiceberg(col("x").is_null())
        assert isinstance(result, ice.IsNull)

    def test_is_not_null(self):
        result = _to_pyiceberg(col("x").is_not_null())
        assert isinstance(result, ice.NotNull)

    def test_is_nan(self):
        result = _to_pyiceberg(col("x").is_nan())
        assert isinstance(result, ice.IsNaN)

    def test_is_not_nan(self):
        result = _to_pyiceberg(col("x").is_not_nan())
        assert isinstance(result, ice.NotNaN)


class TestPyIcebergSetConversion:
    def test_in(self):
        result = _to_pyiceberg(col("x").is_in([1, 2, 3]))
        assert isinstance(result, ice.In)

    def test_not_in(self):
        result = _to_pyiceberg(col("x").is_not_in([1, 2, 3]))
        assert isinstance(result, ice.NotIn)

    def test_in_single_value_becomes_equal(self):
        # PyIceberg optimizes single-element In to EqualTo
        result = _to_pyiceberg(col("x").is_in([1]))
        assert isinstance(result, ice.EqualTo)


class TestPyIcebergStringPrefixConversion:
    def test_starts_with(self):
        result = _to_pyiceberg(col("x").starts_with("abc"))
        assert isinstance(result, ice.StartsWith)

    def test_not_starts_with(self):
        result = _to_pyiceberg(col("x").not_starts_with("abc"))
        assert isinstance(result, ice.NotStartsWith)


class TestPyIcebergBetweenConversion:
    def test_between_decomposes_to_and(self):
        result = _to_pyiceberg(col("x").between(1, 10))
        assert isinstance(result, ice.And)
        assert isinstance(result.left, ice.GreaterThanOrEqual)
        assert isinstance(result.right, ice.LessThanOrEqual)


class TestPyIcebergLogicalConversion:
    def test_and(self):
        result = _to_pyiceberg((col("x") > 5) & (col("y") == "a"))
        assert isinstance(result, ice.And)

    def test_or(self):
        result = _to_pyiceberg((col("x") > 5) | (col("y") == "a"))
        assert isinstance(result, ice.Or)

    def test_not(self):
        result = _to_pyiceberg(~col("x").is_null())
        assert isinstance(result, ice.Not)

    def test_complex_composition(self):
        expr = (col("x") > 5) & (col("y") == "a") | ~col("z").is_null()
        result = _to_pyiceberg(expr)
        assert isinstance(result, ice.Or)
        assert isinstance(result.left, ice.And)
        assert isinstance(result.right, ice.Not)


class TestNamespacedImport:
    """Verify the ``import filters as F`` convention works."""

    def test_f_col_builds_filter(self):
        from openhouse.dataloader import filters as F

        f = (F.col("age") > 21) & (F.col("country") == "US")
        assert isinstance(f, And)
        assert isinstance(f.left, GreaterThan)
        assert isinstance(f.right, EqualTo)


class TestDataFusionLiteralConversion:
    def test_datetime_greater_than_or_equal(self):
        dt = datetime(2026, 4, 27, tzinfo=UTC)
        result = _to_datafusion_sql(col("datepartition") >= dt)
        assert result == "\"datepartition\" >= CAST('2026-04-27 00:00:00.000000+0000' AS TIMESTAMP)"

    def test_datetime_equal(self):
        dt = datetime(2026, 4, 27, 12, 30, 45, tzinfo=UTC)
        result = _to_datafusion_sql(col("ts") == dt)
        assert result == "\"ts\" = CAST('2026-04-27 12:30:45.000000+0000' AS TIMESTAMP)"

    def test_datetime_with_microseconds(self):
        dt = datetime(2026, 4, 27, 12, 30, 45, 123456, tzinfo=UTC)
        result = _to_datafusion_sql(col("ts") == dt)
        assert result == "\"ts\" = CAST('2026-04-27 12:30:45.123456+0000' AS TIMESTAMP)"

    def test_datetime_non_utc_timezone_preserved(self):
        dt = datetime(2026, 4, 27, 12, 0, 0, tzinfo=timezone(timedelta(hours=5)))
        result = _to_datafusion_sql(col("ts") >= dt)
        assert result == "\"ts\" >= CAST('2026-04-27 12:00:00.000000+0500' AS TIMESTAMP)"

    def test_datetime_naive_no_offset(self):
        dt = datetime(2026, 4, 27, 12, 0, 0)
        result = _to_datafusion_sql(col("ts") >= dt)
        assert result == "\"ts\" >= CAST('2026-04-27 12:00:00.000000' AS TIMESTAMP)"

    def test_date_greater_than_or_equal(self):
        d = date(2026, 4, 27)
        result = _to_datafusion_sql(col("datepartition") >= d)
        assert result == "\"datepartition\" >= CAST('2026-04-27' AS DATE)"

    def test_datetime_between(self):
        dt1 = datetime(2026, 4, 27, tzinfo=UTC)
        dt2 = datetime(2026, 5, 1, tzinfo=UTC)
        result = _to_datafusion_sql(col("ts").between(dt1, dt2))
        assert result == (
            "\"ts\" BETWEEN CAST('2026-04-27 00:00:00.000000+0000' AS TIMESTAMP)"
            " AND CAST('2026-05-01 00:00:00.000000+0000' AS TIMESTAMP)"
        )

    def test_datetime_in_compound_filter(self):
        dt = datetime(2026, 4, 27, tzinfo=UTC)
        f = (col("datepartition") >= dt) & (col("status") == "active")
        result = _to_datafusion_sql(f)
        assert "CAST('2026-04-27 00:00:00.000000+0000' AS TIMESTAMP)" in result
        assert "\"status\" = 'active'" in result

    def test_time_equal(self):
        t = time(14, 30, 0)
        result = _to_datafusion_sql(col("event_time") == t)
        assert result == "\"event_time\" = CAST('14:30:00.000000' AS TIME)"

    def test_time_with_microseconds(self):
        t = time(14, 30, 0, 500000)
        result = _to_datafusion_sql(col("event_time") == t)
        assert result == "\"event_time\" = CAST('14:30:00.500000' AS TIME)"

    def test_time_with_timezone_rejected(self):
        t = time(14, 30, 0, tzinfo=timezone(timedelta(hours=5)))
        with pytest.raises(TypeError, match="does not support timezones for time"):
            _to_datafusion_sql(col("event_time") == t)

    def test_decimal_greater_than(self):
        d = Decimal("99.95")
        result = _to_datafusion_sql(col("price") > d)
        assert result == '"price" > 99.95'

    def test_decimal_between(self):
        result = _to_datafusion_sql(col("price").between(Decimal("10.00"), Decimal("50.00")))
        assert result == '"price" BETWEEN 10.00 AND 50.00'

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (float("nan"), "CAST('nan' AS DOUBLE)"),
            (float("inf"), "CAST('inf' AS DOUBLE)"),
            (float("-inf"), "CAST('-inf' AS DOUBLE)"),
        ],
    )
    def test_non_finite_float(self, value, expected):
        result = _to_datafusion_sql(col("x") == value)
        assert result == f'"x" = {expected}'

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (Decimal("NaN"), "CAST('NaN' AS DOUBLE)"),
            (Decimal("Inf"), "CAST('Infinity' AS DOUBLE)"),
            (Decimal("-Inf"), "CAST('-Infinity' AS DOUBLE)"),
        ],
    )
    def test_non_finite_decimal(self, value, expected):
        result = _to_datafusion_sql(col("x") == value)
        assert result == f'"x" = {expected}'

    def test_uuid_equal(self):
        u = UUID("12345678-1234-5678-1234-567812345678")
        result = _to_datafusion_sql(col("id") == u)
        assert result == "\"id\" = '12345678-1234-5678-1234-567812345678'"


class TestDataFusionFilterExecution:
    """Execute generated filter SQL against a real DataFusion SessionContext."""

    @pytest.fixture()
    def ctx(self):
        import datafusion
        import pyarrow as pa

        ctx = datafusion.SessionContext()
        batch = pa.record_batch(
            {
                "ts": pa.array(
                    [
                        datetime(2026, 4, 25, tzinfo=UTC),
                        datetime(2026, 4, 27, tzinfo=UTC),
                        datetime(2026, 4, 29, tzinfo=UTC),
                    ],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "dt": pa.array([date(2026, 4, 25), date(2026, 4, 27), date(2026, 4, 29)]),
                "t": pa.array([time(10, 0, 0), time(14, 30, 0), time(20, 0, 0)], type=pa.time64("ns")),
                "price": pa.array([Decimal("9.99"), Decimal("49.99"), Decimal("99.99")], type=pa.decimal128(10, 2)),
                "id": pa.array(
                    [
                        "12345678-1234-5678-1234-567812345678",
                        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                        "ffffffff-ffff-ffff-ffff-ffffffffffff",
                    ]
                ),
            }
        )
        ctx.register_record_batches("t", [[batch]])
        return ctx

    def _query(self, ctx, where: str):
        batches = ctx.sql(f'SELECT * FROM "t" WHERE {where}').collect()
        import pyarrow as pa

        return pa.Table.from_batches(batches)

    def test_datetime_filter(self, ctx):
        where = _to_datafusion_sql(col("ts") >= datetime(2026, 4, 27, tzinfo=UTC))
        table = self._query(ctx, where)
        assert table.num_rows == 2
        ts_values = [v.as_py() for v in table.column("ts")]
        assert ts_values == [datetime(2026, 4, 27, tzinfo=UTC), datetime(2026, 4, 29, tzinfo=UTC)]

    def test_datetime_less_than(self, ctx):
        where = _to_datafusion_sql(col("ts") < datetime(2026, 4, 27, tzinfo=UTC))
        table = self._query(ctx, where)
        assert table.num_rows == 1
        assert table.column("ts")[0].as_py() == datetime(2026, 4, 25, tzinfo=UTC)

    def test_datetime_non_utc_timezone(self, ctx):
        # 2026-04-27 05:00:00+05:00 == 2026-04-27 00:00:00 UTC, same as row 2
        dt = datetime(2026, 4, 27, 5, 0, 0, tzinfo=timezone(timedelta(hours=5)))
        where = _to_datafusion_sql(col("ts") >= dt)
        table = self._query(ctx, where)
        assert table.num_rows == 2
        ts_values = [v.as_py() for v in table.column("ts")]
        assert ts_values == [datetime(2026, 4, 27, tzinfo=UTC), datetime(2026, 4, 29, tzinfo=UTC)]

    def test_date_filter(self, ctx):
        where = _to_datafusion_sql(col("dt") >= date(2026, 4, 27))
        table = self._query(ctx, where)
        assert table.num_rows == 2
        dt_values = [v.as_py() for v in table.column("dt")]
        assert dt_values == [date(2026, 4, 27), date(2026, 4, 29)]

    def test_time_filter(self, ctx):
        where = _to_datafusion_sql(col("t") == time(14, 30, 0))
        table = self._query(ctx, where)
        assert table.num_rows == 1
        assert table.column("t")[0].as_py() == time(14, 30, 0)

    def test_decimal_filter(self, ctx):
        where = _to_datafusion_sql(col("price") > Decimal("10.00"))
        table = self._query(ctx, where)
        assert table.num_rows == 2
        price_values = [v.as_py() for v in table.column("price")]
        assert price_values == [Decimal("49.99"), Decimal("99.99")]

    @pytest.mark.parametrize(
        ("value", "expected_count", "check"),
        [
            (float("nan"), 1, lambda v: math.isnan(v)),
            (float("inf"), 1, lambda v: v == float("inf")),
            (float("-inf"), 1, lambda v: v == float("-inf")),
            (Decimal("NaN"), 1, lambda v: math.isnan(v)),
            (Decimal("Inf"), 1, lambda v: v == float("inf")),
            (Decimal("-Inf"), 1, lambda v: v == float("-inf")),
        ],
    )
    def test_non_finite_filter(self, value, expected_count, check):
        import datafusion
        import pyarrow as pa

        ctx = datafusion.SessionContext()
        batch = pa.record_batch({"x": pa.array([1.0, float("nan"), float("inf"), float("-inf"), 5.0])})
        ctx.register_record_batches("t", [[batch]])

        where = _to_datafusion_sql(col("x") == value)
        batches = ctx.sql(f'SELECT * FROM "t" WHERE {where}').collect()
        table = pa.Table.from_batches(batches)
        assert table.num_rows == expected_count
        assert check(table.column("x")[0].as_py())

    def test_high_precision_decimal_filter(self, ctx):
        where = _to_datafusion_sql(col("price") > Decimal("49.9899999999999999"))
        table = self._query(ctx, where)
        assert table.num_rows == 1
        assert table.column("price")[0].as_py() == Decimal("99.99")

    def test_uuid_filter(self, ctx):
        where = _to_datafusion_sql(col("id") == UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))
        table = self._query(ctx, where)
        assert table.num_rows == 1
        assert table.column("id")[0].as_py() == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def test_datetime_between_filter(self, ctx):
        where = _to_datafusion_sql(
            col("ts").between(datetime(2026, 4, 26, tzinfo=UTC), datetime(2026, 4, 28, tzinfo=UTC))
        )
        table = self._query(ctx, where)
        assert table.num_rows == 1
        assert table.column("ts")[0].as_py() == datetime(2026, 4, 27, tzinfo=UTC)

    def test_compound_filter(self, ctx):
        f = (col("ts") >= datetime(2026, 4, 27, tzinfo=UTC)) & (col("price") > Decimal("50.00"))
        where = _to_datafusion_sql(f)
        table = self._query(ctx, where)
        assert table.num_rows == 1
        assert table.column("ts")[0].as_py() == datetime(2026, 4, 29, tzinfo=UTC)
        assert table.column("price")[0].as_py() == Decimal("99.99")

    def test_datetime_microseconds(self, ctx):
        import pyarrow as pa

        ctx2 = __import__("datafusion").SessionContext()
        batch = pa.record_batch(
            {
                "ts": pa.array(
                    [datetime(2026, 4, 27, 12, 0, 0, 500000, tzinfo=UTC)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
            }
        )
        ctx2.register_record_batches("t2", [[batch]])
        where = _to_datafusion_sql(col("ts") == datetime(2026, 4, 27, 12, 0, 0, 500000, tzinfo=UTC))
        table = ctx2.sql(f'SELECT * FROM "t2" WHERE {where}').collect()
        assert len(table[0]) == 1
        assert table[0].column("ts")[0].as_py() == datetime(2026, 4, 27, 12, 0, 0, 500000, tzinfo=UTC)

        where_no_match = _to_datafusion_sql(col("ts") == datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC))
        table2 = ctx2.sql(f'SELECT * FROM "t2" WHERE {where_no_match}').collect()
        assert sum(len(b) for b in table2) == 0


class TestPyIcebergUnsupportedType:
    def test_raises_on_unknown_filter(self):
        class CustomFilter(Filter):
            def __repr__(self) -> str:
                return "custom"

        with pytest.raises(TypeError, match="Unsupported filter type"):
            _to_pyiceberg(CustomFilter())

    def test_datafusion_sql_raises_on_unknown_filter(self):
        class CustomFilter(Filter):
            def __repr__(self) -> str:
                return "custom"

        with pytest.raises(TypeError, match="Unsupported filter type"):
            _to_datafusion_sql(CustomFilter())

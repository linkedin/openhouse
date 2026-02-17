import pytest
from pyiceberg import expressions as ice

from openhouse.dataloader import col
from openhouse.dataloader.filters import (
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
    _to_pyiceberg,
)


class TestColumnCreation:
    def test_col_returns_column(self):
        c = col("x")
        assert isinstance(c, Column)
        assert c.name == "x"

    def test_col_is_not_a_filter(self):
        assert not isinstance(col("x"), Filter)


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


class TestPyIcebergUnsupportedType:
    def test_raises_on_unknown_filter(self):
        class CustomFilter(Filter):
            def __repr__(self) -> str:
                return "custom"

        with pytest.raises(TypeError, match="Unsupported filter type"):
            _to_pyiceberg(CustomFilter())

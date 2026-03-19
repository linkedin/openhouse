"""Tests for filter-to-SQL conversion and combined SQL building."""

from collections.abc import Sequence

from openhouse.dataloader.data_loader import OpenHouseDataLoader
from openhouse.dataloader.data_loader_split import to_sql_identifier
from openhouse.dataloader.filters import AlwaysTrue, Filter, _filter_to_sql, col
from openhouse.dataloader.table_identifier import TableIdentifier

TABLE_ID = TableIdentifier("db", "tbl")
TABLE_REF = to_sql_identifier(TABLE_ID)


def _combined(transform_sql: str, columns: Sequence[str] | None = None, filters: Filter | None = None) -> str:
    """Build combined SQL the same way the data loader does."""
    return OpenHouseDataLoader._build_combined_sql(transform_sql, columns, filters or AlwaysTrue())


# ---------------------------------------------------------------------------
# _filter_to_sql tests
# ---------------------------------------------------------------------------


class TestFilterToSql:
    def test_always_true(self):
        assert _filter_to_sql(AlwaysTrue()) == "TRUE"

    def test_equal_to(self):
        assert _filter_to_sql(col("a") == 10) == '"a" = 10'

    def test_not_equal_to(self):
        assert _filter_to_sql(col("a") != 10) == '"a" != 10'

    def test_greater_than(self):
        assert _filter_to_sql(col("a") > 10) == '"a" > 10'

    def test_greater_than_or_equal(self):
        assert _filter_to_sql(col("a") >= 10) == '"a" >= 10'

    def test_less_than(self):
        assert _filter_to_sql(col("a") < 10) == '"a" < 10'

    def test_less_than_or_equal(self):
        assert _filter_to_sql(col("a") <= 10) == '"a" <= 10'

    def test_string_literal(self):
        assert _filter_to_sql(col("name") == "foo") == "\"name\" = 'foo'"

    def test_string_literal_with_single_quote(self):
        assert _filter_to_sql(col("name") == "it's") == "\"name\" = 'it''s'"

    def test_is_null(self):
        assert _filter_to_sql(col("a").is_null()) == '"a" IS NULL'

    def test_is_not_null(self):
        assert _filter_to_sql(col("a").is_not_null()) == '"a" IS NOT NULL'

    def test_is_nan(self):
        assert _filter_to_sql(col("a").is_nan()) == 'isnan("a")'

    def test_is_not_nan(self):
        assert _filter_to_sql(col("a").is_not_nan()) == 'NOT isnan("a")'

    def test_in(self):
        assert _filter_to_sql(col("a").is_in([1, 2, 3])) == '"a" IN (1, 2, 3)'

    def test_not_in(self):
        assert _filter_to_sql(col("a").is_not_in([1, 2])) == '"a" NOT IN (1, 2)'

    def test_starts_with(self):
        assert _filter_to_sql(col("name").starts_with("pre")) == "\"name\" LIKE 'pre%'"

    def test_not_starts_with(self):
        assert _filter_to_sql(col("name").not_starts_with("pre")) == "\"name\" NOT LIKE 'pre%'"

    def test_between(self):
        assert _filter_to_sql(col("a").between(1, 10)) == '"a" BETWEEN 1 AND 10'

    def test_and(self):
        f = (col("a") > 1) & (col("b") == "x")
        assert _filter_to_sql(f) == '("a" > 1 AND "b" = \'x\')'

    def test_or(self):
        f = (col("a") > 1) | (col("b") == "x")
        assert _filter_to_sql(f) == '("a" > 1 OR "b" = \'x\')'

    def test_not(self):
        f = ~(col("a") > 1)
        assert _filter_to_sql(f) == 'NOT ("a" > 1)'


# ---------------------------------------------------------------------------
# _build_combined_sql tests
# ---------------------------------------------------------------------------


class TestBuildCombinedSql:
    def test_no_columns_no_filters(self):
        sql = _combined(f"SELECT id FROM {TABLE_REF}")
        assert sql == f"SELECT * FROM (SELECT id FROM {TABLE_REF}) AS _oh_transformed"

    def test_with_columns(self):
        sql = _combined(f"SELECT id, name FROM {TABLE_REF}", ["id"])
        assert sql == f'SELECT "id" FROM (SELECT id, name FROM {TABLE_REF}) AS _oh_transformed'

    def test_with_filters(self):
        sql = _combined(f"SELECT id FROM {TABLE_REF}", filters=col("id") > 10)
        assert sql == f'SELECT * FROM (SELECT id FROM {TABLE_REF}) AS _oh_transformed WHERE "id" > 10'

    def test_with_columns_and_filters(self):
        sql = _combined(f"SELECT id, name FROM {TABLE_REF}", ["id"], col("id") > 10)
        assert sql == f'SELECT "id" FROM (SELECT id, name FROM {TABLE_REF}) AS _oh_transformed WHERE "id" > 10'

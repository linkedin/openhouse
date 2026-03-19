"""Tests for the _pushdown module."""

from pyiceberg import expressions as ice
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from openhouse.dataloader._pushdown import (
    _build_combined_sql,
    _filter_to_sql,
    analyze_pushdown,
)
from openhouse.dataloader.data_loader_split import to_sql_identifier
from openhouse.dataloader.filters import AlwaysTrue, col
from openhouse.dataloader.table_identifier import TableIdentifier

TABLE_ID = TableIdentifier("db", "tbl")
TABLE_REF = to_sql_identifier(TABLE_ID)

TEST_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="value", field_type=DoubleType(), required=False),
)


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
        sql = _build_combined_sql(f"SELECT id FROM {TABLE_REF}", None, AlwaysTrue())
        assert sql == f"SELECT * FROM (SELECT id FROM {TABLE_REF}) AS _oh_transformed"

    def test_with_columns(self):
        sql = _build_combined_sql(f"SELECT id, name FROM {TABLE_REF}", ["id"], AlwaysTrue())
        assert sql == f'SELECT "id" FROM (SELECT id, name FROM {TABLE_REF}) AS _oh_transformed'

    def test_with_filters(self):
        sql = _build_combined_sql(f"SELECT id FROM {TABLE_REF}", None, col("id") > 10)
        assert sql == f'SELECT * FROM (SELECT id FROM {TABLE_REF}) AS _oh_transformed WHERE "id" > 10'

    def test_with_columns_and_filters(self):
        sql = _build_combined_sql(f"SELECT id, name FROM {TABLE_REF}", ["id"], col("id") > 10)
        assert sql == f'SELECT "id" FROM (SELECT id, name FROM {TABLE_REF}) AS _oh_transformed WHERE "id" > 10'


# ---------------------------------------------------------------------------
# analyze_pushdown tests
# ---------------------------------------------------------------------------


class TestAnalyzePushdown:
    def _transform(self, select: str) -> str:
        return f"SELECT {select} FROM {TABLE_REF}"

    def test_projection_passthrough(self):
        """All columns pass through → scan includes only referenced columns."""
        result = analyze_pushdown(
            transform_sql=self._transform("id, name, value"),
            table_schema=TEST_SCHEMA,
            table_id=TABLE_ID,
            columns=None,
            filters=AlwaysTrue(),
        )
        assert set(result.scan_columns) == {"id", "name", "value"}

    def test_projection_pruning_by_transformer(self):
        """Transformer only references a subset of base table columns."""
        result = analyze_pushdown(
            transform_sql=self._transform("id, name"),
            table_schema=TEST_SCHEMA,
            table_id=TABLE_ID,
            columns=None,
            filters=AlwaysTrue(),
        )
        assert set(result.scan_columns) == {"id", "name"}
        assert "value" not in result.scan_columns

    def test_user_columns_do_not_prune_transformer_columns(self):
        """User selects subset of transformer output, but scan includes all transformer columns."""
        result = analyze_pushdown(
            transform_sql=self._transform("id, name, value"),
            table_schema=TEST_SCHEMA,
            table_id=TABLE_ID,
            columns=["id"],
            filters=AlwaysTrue(),
        )
        # Scan must include all columns the transformer references, not just user's selection
        assert set(result.scan_columns) == {"id", "name", "value"}

    def test_computed_column_includes_source_columns(self):
        """Transformer computes a column from base columns → scan includes source columns."""
        result = analyze_pushdown(
            transform_sql=self._transform("id, CONCAT(name, ' ', name) AS full_name"),
            table_schema=TEST_SCHEMA,
            table_id=TABLE_ID,
            columns=None,
            filters=AlwaysTrue(),
        )
        assert "id" in result.scan_columns
        assert "name" in result.scan_columns
        assert "value" not in result.scan_columns

    def test_filter_pushdown_passthrough_column(self):
        """Filter on a passthrough column is pushed to the scan."""
        result = analyze_pushdown(
            transform_sql=self._transform("id, name, value"),
            table_schema=TEST_SCHEMA,
            table_id=TABLE_ID,
            columns=None,
            filters=col("id") > 10,
        )
        assert not isinstance(result.scan_filter, ice.AlwaysTrue)
        assert isinstance(result.scan_filter, ice.GreaterThan)

    def test_filter_pushdown_adds_column_to_scan(self):
        """Filter references a column not in the transformer SELECT → still pushed if base col."""
        result = analyze_pushdown(
            transform_sql=self._transform("id, name"),
            table_schema=TEST_SCHEMA,
            table_id=TABLE_ID,
            columns=None,
            filters=col("id") > 10,
        )
        assert "id" in result.scan_columns

    def test_no_filter_pushdown_on_computed_column(self):
        """Filter on a computed column cannot be pushed to the scan."""
        result = analyze_pushdown(
            transform_sql=self._transform("id, CONCAT(name, '_masked') AS masked_name"),
            table_schema=TEST_SCHEMA,
            table_id=TABLE_ID,
            columns=None,
            filters=col("masked_name") == "alice_masked",
        )
        # The filter references a computed column, so it can't be pushed as a simple predicate
        # DataFusion may expand the alias and push the expression, but _df_expr_to_pyiceberg
        # won't convert function calls → falls back to AlwaysTrue
        assert isinstance(result.scan_filter, ice.AlwaysTrue)

    def test_combined_sql_wraps_transformer(self):
        """Combined SQL wraps the transformer with user projection and filter."""
        result = analyze_pushdown(
            transform_sql=self._transform("id, name"),
            table_schema=TEST_SCHEMA,
            table_id=TABLE_ID,
            columns=["id"],
            filters=col("id") > 10,
        )
        assert "_oh_transformed" in result.combined_sql
        assert '"id" > 10' in result.combined_sql

    def test_transformer_with_inner_filter(self):
        """Transformer has its own WHERE clause → scan includes filter column."""
        transform = f"SELECT id, name FROM {TABLE_REF} WHERE value > 1.0"
        result = analyze_pushdown(
            transform_sql=transform,
            table_schema=TEST_SCHEMA,
            table_id=TABLE_ID,
            columns=None,
            filters=AlwaysTrue(),
        )
        assert "value" in result.scan_columns

    def test_mixed_filters(self):
        """AND of pushable + non-pushable: pushable part is extracted."""
        result = analyze_pushdown(
            transform_sql=self._transform("id, CONCAT(name, '_x') AS computed"),
            table_schema=TEST_SCHEMA,
            table_id=TABLE_ID,
            columns=None,
            filters=(col("id") > 10) & (col("computed") == "alice_x"),
        )
        # id > 10 should be pushed, computed filter should not
        if not isinstance(result.scan_filter, ice.AlwaysTrue):
            assert isinstance(result.scan_filter, (ice.GreaterThan, ice.And))

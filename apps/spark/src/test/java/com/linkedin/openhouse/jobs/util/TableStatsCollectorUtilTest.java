package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.common.stats.model.ColumnData;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TableStatsCollectorUtil utility methods.
 *
 * <p>Tests focus on pure utility functions that don't require Spark runtime. DataFrame-to-object
 * conversion via Spark encoders is tested in integration tests.
 *
 * @see com.linkedin.openhouse.jobs.spark.TableStatsCollectionSparkAppTest
 */
public class TableStatsCollectorUtilTest {

  @Test
  public void testGetDatabaseName_withTwoPartName() {
    // Test: db.table format
    String result = TableStatsCollectorUtil.getDatabaseName("db.test_table");
    Assertions.assertEquals("db", result);
  }

  @Test
  public void testGetDatabaseName_withThreePartName() {
    // Test: catalog.db.table format
    String result = TableStatsCollectorUtil.getDatabaseName("openhouse.db.test_table");
    Assertions.assertEquals("db", result);
  }

  @Test
  public void testGetDatabaseName_withFourPartName() {
    // Test: Multiple namespace levels (rare but valid)
    String result = TableStatsCollectorUtil.getDatabaseName("catalog.schema.db.test_table");
    Assertions.assertEquals("db", result);
  }

  @Test
  public void testGetDatabaseName_withInvalidFormat() {
    // Test: Invalid format (no namespace)
    String result = TableStatsCollectorUtil.getDatabaseName("invalid_table");
    Assertions.assertNull(result);
  }

  @Test
  public void testGetDatabaseName_withNull() {
    // Test: Null input
    String result = TableStatsCollectorUtil.getDatabaseName(null);
    Assertions.assertNull(result);
  }

  @Test
  public void testGetDatabaseName_withEmptyString() {
    // Test: Empty string input
    String result = TableStatsCollectorUtil.getDatabaseName("");
    Assertions.assertNull(result);
  }

  // ==================== Partition Data Transformation Tests ====================

  @Test
  public void testTransformPartitionRowToColumnData_withLongType() {
    // Test: Partition column with Long value (e.g., year=2024)
    StructType schema =
        new StructType().add("year", DataTypes.LongType).add("month", DataTypes.LongType);

    Row partitionRow = new GenericRowWithSchema(new Object[] {2024L, 12L}, schema);
    List<String> columnNames = Arrays.asList("year", "month");

    List<ColumnData> result =
        TableStatsCollectorUtil.transformPartitionRowToColumnData(partitionRow, columnNames);

    Assertions.assertEquals(2, result.size());
    Assertions.assertInstanceOf(ColumnData.LongColumnData.class, result.get(0));
    Assertions.assertEquals("year", result.get(0).getColumnName());
    Assertions.assertEquals(2024L, ((ColumnData.LongColumnData) result.get(0)).getValue());
  }

  @Test
  public void testTransformPartitionRowToColumnData_withIntegerType() {
    // Test: Partition column with Integer value (converted to Long)
    StructType schema = new StructType().add("day", DataTypes.IntegerType);

    Row partitionRow = new GenericRowWithSchema(new Object[] {15}, schema);
    List<String> columnNames = Arrays.asList("day");

    List<ColumnData> result =
        TableStatsCollectorUtil.transformPartitionRowToColumnData(partitionRow, columnNames);

    Assertions.assertEquals(1, result.size());
    Assertions.assertInstanceOf(ColumnData.LongColumnData.class, result.get(0));
    Assertions.assertEquals("day", result.get(0).getColumnName());
    Assertions.assertEquals(15L, ((ColumnData.LongColumnData) result.get(0)).getValue());
  }

  @Test
  public void testTransformPartitionRowToColumnData_withDoubleType() {
    // Test: Partition column with Double value
    StructType schema = new StructType().add("score", DataTypes.DoubleType);

    Row partitionRow = new GenericRowWithSchema(new Object[] {99.5}, schema);
    List<String> columnNames = Arrays.asList("score");

    List<ColumnData> result =
        TableStatsCollectorUtil.transformPartitionRowToColumnData(partitionRow, columnNames);

    Assertions.assertEquals(1, result.size());
    Assertions.assertInstanceOf(ColumnData.DoubleColumnData.class, result.get(0));
    Assertions.assertEquals("score", result.get(0).getColumnName());
    Assertions.assertEquals(99.5, ((ColumnData.DoubleColumnData) result.get(0)).getValue());
  }

  @Test
  public void testTransformPartitionRowToColumnData_withFloatType() {
    // Test: Partition column with Float value (converted to Double)
    StructType schema = new StructType().add("rating", DataTypes.FloatType);

    Row partitionRow = new GenericRowWithSchema(new Object[] {4.5f}, schema);
    List<String> columnNames = Arrays.asList("rating");

    List<ColumnData> result =
        TableStatsCollectorUtil.transformPartitionRowToColumnData(partitionRow, columnNames);

    Assertions.assertEquals(1, result.size());
    Assertions.assertInstanceOf(ColumnData.DoubleColumnData.class, result.get(0));
    Assertions.assertEquals("rating", result.get(0).getColumnName());
    Assertions.assertEquals(4.5, ((ColumnData.DoubleColumnData) result.get(0)).getValue(), 0.01);
  }

  @Test
  public void testTransformPartitionRowToColumnData_withStringType() {
    // Test: Partition column with String value
    StructType schema = new StructType().add("region", DataTypes.StringType);

    Row partitionRow = new GenericRowWithSchema(new Object[] {"US"}, schema);
    List<String> columnNames = Arrays.asList("region");

    List<ColumnData> result =
        TableStatsCollectorUtil.transformPartitionRowToColumnData(partitionRow, columnNames);

    Assertions.assertEquals(1, result.size());
    Assertions.assertInstanceOf(ColumnData.StringColumnData.class, result.get(0));
    Assertions.assertEquals("region", result.get(0).getColumnName());
    Assertions.assertEquals("US", ((ColumnData.StringColumnData) result.get(0)).getValue());
  }

  @Test
  public void testTransformPartitionRowToColumnData_withMultipleColumns() {
    // Test: Multiple partition columns with different types
    StructType schema =
        new StructType()
            .add("year", DataTypes.LongType)
            .add("region", DataTypes.StringType)
            .add("score", DataTypes.DoubleType);

    Row partitionRow = new GenericRowWithSchema(new Object[] {2024L, "EU", 95.5}, schema);
    List<String> columnNames = Arrays.asList("year", "region", "score");

    List<ColumnData> result =
        TableStatsCollectorUtil.transformPartitionRowToColumnData(partitionRow, columnNames);

    Assertions.assertEquals(3, result.size());

    // Verify year (Long)
    Assertions.assertInstanceOf(ColumnData.LongColumnData.class, result.get(0));
    Assertions.assertEquals("year", result.get(0).getColumnName());
    Assertions.assertEquals(2024L, ((ColumnData.LongColumnData) result.get(0)).getValue());

    // Verify region (String)
    Assertions.assertInstanceOf(ColumnData.StringColumnData.class, result.get(1));
    Assertions.assertEquals("region", result.get(1).getColumnName());
    Assertions.assertEquals("EU", ((ColumnData.StringColumnData) result.get(1)).getValue());

    // Verify score (Double)
    Assertions.assertInstanceOf(ColumnData.DoubleColumnData.class, result.get(2));
    Assertions.assertEquals("score", result.get(2).getColumnName());
    Assertions.assertEquals(95.5, ((ColumnData.DoubleColumnData) result.get(2)).getValue());
  }

  @Test
  public void testTransformPartitionRowToColumnData_withNullValue() {
    // Test: Null partition value is skipped (logged but not added to result)
    StructType schema = new StructType().add("region", DataTypes.StringType);

    Row partitionRow = new GenericRowWithSchema(new Object[] {null}, schema);
    List<String> columnNames = Arrays.asList("region");

    List<ColumnData> result =
        TableStatsCollectorUtil.transformPartitionRowToColumnData(partitionRow, columnNames);

    // Null value should be skipped
    Assertions.assertEquals(0, result.size());
  }

  @Test
  public void testTransformPartitionRowToColumnData_withMixedNullAndValid() {
    // Test: Mix of null and valid values - only valid values in result
    StructType schema =
        new StructType()
            .add("year", DataTypes.LongType)
            .add("region", DataTypes.StringType)
            .add("month", DataTypes.IntegerType);

    Row partitionRow = new GenericRowWithSchema(new Object[] {2024L, null, 12}, schema);
    List<String> columnNames = Arrays.asList("year", "region", "month");

    List<ColumnData> result =
        TableStatsCollectorUtil.transformPartitionRowToColumnData(partitionRow, columnNames);

    // Only 2 values (region is null and skipped)
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals("year", result.get(0).getColumnName());
    Assertions.assertEquals("month", result.get(1).getColumnName());
  }

  @Test
  public void testTransformPartitionRowToColumnData_withEmptyRow() {
    // Test: Empty row (no columns) returns empty list
    StructType schema = new StructType();
    Row partitionRow = new GenericRowWithSchema(new Object[] {}, schema);
    List<String> columnNames = Arrays.asList();

    List<ColumnData> result =
        TableStatsCollectorUtil.transformPartitionRowToColumnData(partitionRow, columnNames);

    Assertions.assertEquals(0, result.size());
  }

  // ==================== Column Aggregation Expression Tests ====================

  @Test
  public void testBuildColumnAggregationExpressions_withSingleColumn() {
    // Test: Single column generates 5 aggregation expressions
    List<String> columnNames = Arrays.asList("id");

    List<String> result = TableStatsCollectorUtil.buildColumnAggregationExpressions(columnNames);

    Assertions.assertEquals(5, result.size());
    Assertions.assertTrue(result.get(0).contains("null_value_count"));
    Assertions.assertTrue(result.get(1).contains("nan_value_count"));
    Assertions.assertTrue(result.get(2).contains("lower_bound"));
    Assertions.assertTrue(result.get(3).contains("upper_bound"));
    Assertions.assertTrue(result.get(4).contains("column_size"));
  }

  @Test
  public void testBuildColumnAggregationExpressions_withMultipleColumns() {
    // Test: Multiple columns generate correct number of expressions
    List<String> columnNames = Arrays.asList("id", "name", "age");

    List<String> result = TableStatsCollectorUtil.buildColumnAggregationExpressions(columnNames);

    // 3 columns * 5 expressions each = 15
    Assertions.assertEquals(15, result.size());
  }

  @Test
  public void testBuildColumnAggregationExpressions_withSpecialCharacters() {
    // Test: Column names with special characters are properly escaped with backticks
    // and dots in column names are replaced with underscores in alias names
    List<String> columnNames = Arrays.asList("user-id", "timestamp.value");

    List<String> result = TableStatsCollectorUtil.buildColumnAggregationExpressions(columnNames);

    // Verify backtick escaping for column references
    Assertions.assertTrue(result.get(0).contains("`user-id`"));
    Assertions.assertTrue(result.get(5).contains("`timestamp.value`"));

    // Verify dots are replaced with underscores in alias names
    // result.get(5) is the null_count expression for "timestamp.value"
    Assertions.assertTrue(result.get(5).contains("as timestamp_value_null_count"));
  }

  @Test
  public void testBuildColumnAggregationExpressions_withEmptyList() {
    // Test: Empty column list returns empty expression list
    List<String> columnNames = Arrays.asList();

    List<String> result = TableStatsCollectorUtil.buildColumnAggregationExpressions(columnNames);

    Assertions.assertEquals(0, result.size());
  }

  @Test
  public void testBuildColumnAggregationExpressions_verifyCoalesceForNullSafety() {
    // Test: Expressions include coalesce() for null safety
    List<String> columnNames = Arrays.asList("value");

    List<String> result = TableStatsCollectorUtil.buildColumnAggregationExpressions(columnNames);

    // null_count, nan_count, and column_size should use coalesce()
    Assertions.assertTrue(result.get(0).contains("coalesce"));
    Assertions.assertTrue(result.get(1).contains("coalesce"));
    Assertions.assertTrue(result.get(4).contains("coalesce"));

    // min and max don't need coalesce (SQL handles nulls)
    Assertions.assertFalse(result.get(2).contains("coalesce"));
    Assertions.assertFalse(result.get(3).contains("coalesce"));
  }

  @Test
  public void testBuildColumnAggregationExpressions_verifyAliasNames() {
    // Test: Verify alias naming convention (colname_metric_type)
    List<String> columnNames = Arrays.asList("age");

    List<String> result = TableStatsCollectorUtil.buildColumnAggregationExpressions(columnNames);

    Assertions.assertTrue(result.get(0).contains("as age_null_count"));
    Assertions.assertTrue(result.get(1).contains("as age_nan_count"));
    Assertions.assertTrue(result.get(2).contains("as age_min_value"));
    Assertions.assertTrue(result.get(3).contains("as age_max_value"));
    Assertions.assertTrue(result.get(4).contains("as age_column_size"));
  }

  // ==================== Convert Value to ColumnData Tests ====================

  @Test
  public void testConvertValueToColumnData_withLongType() {
    // Test: Long value converts to LongColumnData
    org.apache.iceberg.types.Type icebergType = org.apache.iceberg.types.Types.LongType.get();

    ColumnData result =
        TableStatsCollectorUtil.convertValueToColumnData("count", 12345L, icebergType);

    Assertions.assertInstanceOf(ColumnData.LongColumnData.class, result);
    Assertions.assertEquals("count", result.getColumnName());
    Assertions.assertEquals(12345L, ((ColumnData.LongColumnData) result).getValue());
  }

  @Test
  public void testConvertValueToColumnData_withIntegerType() {
    // Test: Integer value converts to LongColumnData
    org.apache.iceberg.types.Type icebergType = org.apache.iceberg.types.Types.IntegerType.get();

    ColumnData result = TableStatsCollectorUtil.convertValueToColumnData("age", 25, icebergType);

    Assertions.assertInstanceOf(ColumnData.LongColumnData.class, result);
    Assertions.assertEquals(25L, ((ColumnData.LongColumnData) result).getValue());
  }

  @Test
  public void testConvertValueToColumnData_withDoubleType() {
    // Test: Double value converts to DoubleColumnData
    org.apache.iceberg.types.Type icebergType = org.apache.iceberg.types.Types.DoubleType.get();

    ColumnData result =
        TableStatsCollectorUtil.convertValueToColumnData("price", 99.99, icebergType);

    Assertions.assertInstanceOf(ColumnData.DoubleColumnData.class, result);
    Assertions.assertEquals(99.99, ((ColumnData.DoubleColumnData) result).getValue());
  }

  @Test
  public void testConvertValueToColumnData_withFloatType() {
    // Test: Float value converts to DoubleColumnData
    org.apache.iceberg.types.Type icebergType = org.apache.iceberg.types.Types.FloatType.get();

    ColumnData result =
        TableStatsCollectorUtil.convertValueToColumnData("rating", 4.5f, icebergType);

    Assertions.assertInstanceOf(ColumnData.DoubleColumnData.class, result);
    Assertions.assertEquals(4.5, ((ColumnData.DoubleColumnData) result).getValue(), 0.01);
  }

  @Test
  public void testConvertValueToColumnData_withStringType() {
    // Test: String value converts to StringColumnData
    org.apache.iceberg.types.Type icebergType = org.apache.iceberg.types.Types.StringType.get();

    ColumnData result =
        TableStatsCollectorUtil.convertValueToColumnData("name", "Alice", icebergType);

    Assertions.assertInstanceOf(ColumnData.StringColumnData.class, result);
    Assertions.assertEquals("Alice", ((ColumnData.StringColumnData) result).getValue());
  }

  @Test
  public void testConvertValueToColumnData_withDateType() {
    // Test: Date type falls back to StringColumnData
    org.apache.iceberg.types.Type icebergType = org.apache.iceberg.types.Types.DateType.get();

    ColumnData result =
        TableStatsCollectorUtil.convertValueToColumnData("date", "2024-01-01", icebergType);

    Assertions.assertInstanceOf(ColumnData.StringColumnData.class, result);
    Assertions.assertEquals("2024-01-01", ((ColumnData.StringColumnData) result).getValue());
  }

  @Test
  public void testConvertValueToColumnData_withNullValue() {
    // Test: Null value returns null
    org.apache.iceberg.types.Type icebergType = org.apache.iceberg.types.Types.StringType.get();

    ColumnData result =
        TableStatsCollectorUtil.convertValueToColumnData("field", null, icebergType);

    Assertions.assertNull(result);
  }

  @Test
  public void testConvertValueToColumnData_withInvalidStringForNumericType() {
    // Test: Invalid string for numeric type falls back to StringColumnData
    org.apache.iceberg.types.Type icebergType = org.apache.iceberg.types.Types.LongType.get();

    ColumnData result =
        TableStatsCollectorUtil.convertValueToColumnData("value", "not-a-number", icebergType);

    // Should fall back to string (due to NumberFormatException)
    Assertions.assertInstanceOf(ColumnData.StringColumnData.class, result);
    Assertions.assertEquals("not-a-number", ((ColumnData.StringColumnData) result).getValue());
  }

  // Note: The following methods require Spark runtime and are tested in integration tests:
  //   - getColumnNamesFromReadableMetrics() - requires Spark SQL execution
  //   - extractColumnMetricsFromAggregatedRow() - requires Spark Row structure
  //   - transformRowsToPartitionStatsFromAggregatedSQL() - requires complex Row structures
  //   - populateCommitEventTablePartitionStats() - requires full Iceberg table
  //   - populateStatsForUnpartitionedTable() - requires full Iceberg table
  //
  // Integration tests provide better coverage for these methods:
  //   - testPartitionStatsForPartitionedTable()
  //   - testPartitionStatsForUnpartitionedTable()
  //   - testPartitionStatsWithNestedColumns()
  //   - testPartitionStatsSchemaValidation()
}

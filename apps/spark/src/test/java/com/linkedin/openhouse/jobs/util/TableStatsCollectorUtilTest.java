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

  // Note: transformRowsToPartitionEvents() is tested via integration tests in
  // TableStatsCollectionSparkAppTest because it requires:
  //   1. Complex Row structure with nested fields (partition, summary map, etc.)
  //   2. Scala collection conversion (summary map)
  //   3. Realistic Iceberg metadata structures
  //
  // Integration tests provide better coverage:
  //   - testPartitionEventsForPartitionedTable()
  //   - testPartitionEventsSchemaValidation()
  //   - testPartitionEventsWithMultiplePartitions()
}

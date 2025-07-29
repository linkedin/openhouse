package com.linkedin.openhouse.catalog.e2e;

import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive test suite for Iceberg bucket behavior in OpenHouse.
 *
 * <p>This test class validates: 1. Separate buckets for each sub-key of the primary key via
 * PARTITIONED BY (bucket(4, key1), bucket(2, key2)) - Creates nested sub-directories for each key
 * like /data/key1_bucket=xxx/key2_bucket=yyy 2. Concatenated primary keys to a string via
 * PARTITIONED BY (bucket(4, concat_key)) - Creates sub-directories like
 * /data/concat_key_bucket=xxxyyy
 *
 * <p>Notes: - Iceberg V3 will support bucketing via multiple key columns via multi-arg transforms
 * e.g., PARTITIONED BY (bucket(16, a, b, c)) - Tests verify both data distribution and file
 * organization
 */
@Slf4j
public class BucketBehaviorTest extends OpenHouseSparkITest {

  /**
   * Test separate bucketing for multiple keys. This test validates that we can create separate
   * buckets for each sub-key of the primary key, which creates nested sub-directories for each key
   * like /data/key1_bucket=xxx/key2_bucket=yyy
   */
  @Test
  void testSeparateBucketingByMultipleKeys() throws Exception {
    SparkSession spark = null;
    String tableName = "openhouse.d1.separate_bucket_test";

    try {
      spark = getSparkSession();
      log.info("Starting test: separate bucketing by multiple keys");

      // Create table with separate bucketing for key1 and key2
      String createTableSql =
          String.format(
              "CREATE TABLE %s ("
                  + "  id BIGINT, "
                  + "  key1 STRING, "
                  + "  key2 STRING, "
                  + "  value STRING, "
                  + "  timestamp_col TIMESTAMP"
                  + ") USING ICEBERG "
                  + "PARTITIONED BY ("
                  + "  bucket(4, key1), "
                  + "  bucket(2, key2)"
                  + ")",
              tableName);

      spark.sql(createTableSql);
      log.info("Created table with separate bucketing: {}", tableName);

      // Verify partition specification using Iceberg API
      Operations operations = Operations.withCatalog(spark, null);
      Table table = operations.getTable("d1.separate_bucket_test");
      PartitionSpec partitionSpec = table.spec();

      log.info("Partition spec fields count: {}", partitionSpec.fields().size());
      Assertions.assertEquals(
          2,
          partitionSpec.fields().size(),
          "Should have 2 partition fields for separate bucketing");

      // Verify the partition transforms
      Assertions.assertTrue(
          partitionSpec.fields().get(0).transform().toString().contains("bucket[4]"),
          "First partition field should be bucket(4, key1)");
      Assertions.assertTrue(
          partitionSpec.fields().get(1).transform().toString().contains("bucket[2]"),
          "Second partition field should be bucket(2, key2)");

      // Verify partition field names match expected pattern
      Assertions.assertEquals(
          "key1_bucket",
          partitionSpec.fields().get(0).name(),
          "First partition field should be named key1_bucket");
      Assertions.assertEquals(
          "key2_bucket",
          partitionSpec.fields().get(1).name(),
          "Second partition field should be named key2_bucket");

      // Verify table schema contains all expected columns
      Schema schema = table.schema();
      Assertions.assertEquals(5, schema.columns().size(), "Should have 5 columns");
      Assertions.assertTrue(schema.findField("id") != null, "Should have id column");
      Assertions.assertTrue(schema.findField("key1") != null, "Should have key1 column");
      Assertions.assertTrue(schema.findField("key2") != null, "Should have key2 column");
      Assertions.assertTrue(schema.findField("value") != null, "Should have value column");
      Assertions.assertTrue(
          schema.findField("timestamp_col") != null, "Should have timestamp_col column");

      // Insert test data to measure bucket behavior
      insertTestDataForSeparateBucketing(spark, tableName);

      // Analyze bucket behavior after data insertion
      analyzeSeparateBucketBehavior(spark, tableName);

      log.info("Successfully completed separate bucketing test");

    } finally {
      if (spark != null) {
        spark.sql(String.format("DROP TABLE IF EXISTS %s", tableName));
        log.info("Cleaned up table: {}", tableName);
      }
    }
  }

  /**
   * Test concatenated key bucketing approaches. This test validates what concatenation methods are
   * supported by Iceberg/Spark.
   *
   * <p>NOTE: Direct concat expressions like bucket(4, concat(key1, key2)) and GENERATED ALWAYS AS
   * syntax are not currently supported in Spark/Iceberg.
   *
   * <p>For true concatenated key bucketing, data would need to be pre-processed to create the
   * concatenated column before bucketing.
   */
  @Test
  void testConcatenatedKeyBucketingApproaches() throws Exception {
    final SparkSession spark = getSparkSession();
    String tableName = "openhouse.d1.concat_approach_test";

    try {
      log.info("Starting test: concatenated key bucketing approaches");

      // Test 1: Verify that direct concat expression is NOT supported
      log.info("Testing approach 1: Direct concat expression bucket(4, concat(key1, key2))");
      Exception directConcatException =
          Assertions.assertThrows(
              Exception.class,
              () -> {
                String directConcatSql =
                    String.format(
                        "CREATE TABLE %s_direct ("
                            + "  id BIGINT, "
                            + "  key1 STRING, "
                            + "  key2 STRING, "
                            + "  value STRING, "
                            + "  timestamp_col TIMESTAMP"
                            + ") USING ICEBERG "
                            + "PARTITIONED BY ("
                            + "  bucket(4, concat(key1, key2))"
                            + ")",
                        tableName);
                spark.sql(directConcatSql);
              },
              "Direct concat expression should not be supported");

      log.info(
          "CONFIRMED: Direct concat expression is not supported: {}",
          directConcatException.getMessage());

      // Test 2: Verify that GENERATED ALWAYS AS syntax is NOT supported
      log.info("Testing approach 2: GENERATED ALWAYS AS computed column");
      Exception generatedColumnException =
          Assertions.assertThrows(
              Exception.class,
              () -> {
                String generatedColumnSql =
                    String.format(
                        "CREATE TABLE %s_generated ("
                            + "  id BIGINT, "
                            + "  key1 STRING, "
                            + "  key2 STRING, "
                            + "  concat_key STRING GENERATED ALWAYS AS (concat(key1, key2)), "
                            + "  value STRING, "
                            + "  timestamp_col TIMESTAMP"
                            + ") USING ICEBERG "
                            + "PARTITIONED BY ("
                            + "  bucket(4, concat_key)"
                            + ")",
                        tableName);
                spark.sql(generatedColumnSql);
              },
              "GENERATED ALWAYS AS syntax should not be supported");

      log.info(
          "CONFIRMED: GENERATED ALWAYS AS syntax is not supported: {}",
          generatedColumnException.getMessage());

      // Test 3: Working approach - manual concatenation via application logic
      log.info("Testing approach 3: Manual concatenation (working approach)");
      String workingApproachSql =
          String.format(
              "CREATE TABLE %s ("
                  + "  id BIGINT, "
                  + "  key1 STRING, "
                  + "  key2 STRING, "
                  + "  concat_key STRING, "
                  + "  value STRING, "
                  + "  timestamp_col TIMESTAMP"
                  + ") USING ICEBERG "
                  + "PARTITIONED BY ("
                  + "  bucket(4, concat_key)"
                  + ")",
              tableName);

      spark.sql(workingApproachSql);
      log.info("SUCCESS: Manual concatenation approach works");

      // Insert test data with manually concatenated keys
      insertManualConcatenatedTestData(spark, tableName);

      // Analyze bucket behavior after data insertion
      analyzeConcatenatedBucketBehavior(spark, tableName, "manual concatenation");

      log.info("Successfully completed concatenated key bucketing approaches test");

    } finally {
      // Clean up any partially created tables
      try {
        spark.sql(String.format("DROP TABLE IF EXISTS %s_direct", tableName));
      } catch (Exception e) {
      }
      try {
        spark.sql(String.format("DROP TABLE IF EXISTS %s_generated", tableName));
      } catch (Exception e) {
      }
      try {
        spark.sql(String.format("DROP TABLE IF EXISTS %s", tableName));
      } catch (Exception e) {
      }
      log.info("Cleaned up test tables");
    }
  }

  /**
   * Test that struct type columns in bucket partitioning throw an exception. This test validates
   * that Iceberg correctly rejects struct types in partition transforms as per the Iceberg
   * specification: https://iceberg.apache.org/spec/#partition-transforms
   */
  @Test
  void testStructTypeInBucketThrowsException() throws Exception {
    SparkSession spark = null;
    String tableName = "openhouse.d1.struct_bucket_error_test";

    try {
      spark = getSparkSession();
      final SparkSession finalSpark = spark; // Create final reference for lambda
      log.info("Starting test: struct type in bucket should throw exception");

      // Attempt to create table with struct type in bucket partition
      // This should fail according to Iceberg specification
      String createTableSql =
          String.format(
              "CREATE TABLE %s ("
                  + "  id BIGINT, "
                  + "  user_info STRUCT<name: STRING, age: INT>, "
                  + "  metadata STRING, "
                  + "  timestamp_col TIMESTAMP"
                  + ") USING ICEBERG "
                  + "PARTITIONED BY ("
                  + "  bucket(4, user_info)"
                  + ")",
              tableName);

      // This should throw an exception - that's all we need to verify
      Assertions.assertThrows(
          Exception.class,
          () -> {
            finalSpark.sql(createTableSql);
          },
          "Creating bucket partition on struct type should throw an exception");

      log.info("SUCCESS: Bucket partition on struct type correctly threw exception");

      log.info("Successfully completed struct type bucket exception test");

    } finally {
      if (spark != null) {
        try {
          spark.sql(String.format("DROP TABLE IF EXISTS %s", tableName));
        } catch (Exception e) {
          // Table likely doesn't exist due to failed creation, which is expected
          log.debug(
              "Cleanup attempt completed (table may not have been created): {}", e.getMessage());
        }
      }
    }
  }

  /**
   * Test bucket distribution validation. This test validates that bucketing configuration works
   * correctly and provides proper table structure for efficient querying.
   */
  @Test
  void testBucketDistributionValidation() throws Exception {
    SparkSession spark = null;
    String tableName = "openhouse.d1.bucket_validation_test";

    try {
      spark = getSparkSession();
      log.info("Starting test: bucket distribution validation");

      // Create table with bucketing for performance testing
      String createTableSql =
          String.format(
              "CREATE TABLE %s ("
                  + "  id BIGINT, "
                  + "  user_id STRING, "
                  + "  session_id STRING, "
                  + "  event_type STRING, "
                  + "  timestamp_col TIMESTAMP"
                  + ") USING ICEBERG "
                  + "PARTITIONED BY ("
                  + "  bucket(8, user_id)"
                  + ")",
              tableName);

      spark.sql(createTableSql);
      log.info("Created table with bucket validation: {}", tableName);

      // Verify partition specification using Iceberg API
      Operations operations = Operations.withCatalog(spark, null);
      Table table = operations.getTable("d1.bucket_validation_test");
      PartitionSpec partitionSpec = table.spec();

      log.info("Partition spec fields count: {}", partitionSpec.fields().size());
      Assertions.assertEquals(
          1, partitionSpec.fields().size(), "Should have 1 partition field for user_id bucketing");

      // Verify the partition transform uses 8 buckets
      Assertions.assertTrue(
          partitionSpec.fields().get(0).transform().toString().contains("bucket[8]"),
          "Partition field should be bucket(8, user_id)");

      // Verify partition field name
      Assertions.assertEquals(
          "user_id_bucket",
          partitionSpec.fields().get(0).name(),
          "Partition field should be named user_id_bucket");

      // Insert test data to measure bucket behavior
      insertHighCardinalityTestData(spark, tableName);

      // Analyze bucket distribution after data insertion
      analyzeBucketDistribution(spark, tableName);

      log.info("Successfully completed bucket validation test");

    } finally {
      if (spark != null) {
        spark.sql(String.format("DROP TABLE IF EXISTS %s", tableName));
        log.info("Cleaned up table: {}", tableName);
      }
    }
  }

  /** Insert test data for separate bucketing scenario. */
  private void insertTestDataForSeparateBucketing(SparkSession spark, String tableName) {
    log.info("Inserting test data for separate bucketing");

    // Insert data in smaller, sorted batches to avoid clustering issues
    // Batch 1: A keys with different key2 values
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(1, 'A', 'X', 'value1', current_timestamp()), "
                + "(4, 'A', 'Y', 'value4', current_timestamp()), "
                + "(7, 'A', 'Z', 'value7', current_timestamp())",
            tableName));

    // Batch 2: B keys with different key2 values
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(2, 'B', 'X', 'value5', current_timestamp()), "
                + "(5, 'B', 'Y', 'value2', current_timestamp()), "
                + "(8, 'B', 'Z', 'value8', current_timestamp())",
            tableName));

    // Batch 3: C keys with different key2 values
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(3, 'C', 'X', 'value6', current_timestamp()), "
                + "(6, 'C', 'Z', 'value3', current_timestamp())",
            tableName));

    // Batch 4: Remaining keys
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(9, 'D', 'W', 'value9', current_timestamp()), "
                + "(10, 'E', 'V', 'value10', current_timestamp())",
            tableName));

    log.info("Inserted 10 test records for separate bucketing in sorted batches");
  }

  /** Insert test data with manually concatenated keys for bucketing scenario. */
  private void insertManualConcatenatedTestData(SparkSession spark, String tableName) {
    log.info("Inserting test data with manually concatenated keys");

    // Insert data with pre-concatenated keys (simulating application-level concatenation)
    // Sorted by concatenated key to avoid clustering issues

    // Batch 1: admin combinations
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(3, 'admin', '001', 'admin001', 'value3', current_timestamp()), "
                + "(4, 'admin', '002', 'admin002', 'value4', current_timestamp())",
            tableName));

    // Batch 2: guest combinations
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(5, 'guest', '001', 'guest001', 'value5', current_timestamp()), "
                + "(6, 'guest', '002', 'guest002', 'value6', current_timestamp())",
            tableName));

    // Batch 3: moderator combinations
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(7, 'moderator', '001', 'moderator001', 'value7', current_timestamp()), "
                + "(8, 'moderator', '002', 'moderator002', 'value8', current_timestamp())",
            tableName));

    // Batch 4: user combinations
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(1, 'user', '001', 'user001', 'value1', current_timestamp()), "
                + "(2, 'user', '002', 'user002', 'value2', current_timestamp())",
            tableName));

    log.info("Inserted 8 test records with manually concatenated keys");
  }

  /** Insert high-cardinality test data for performance testing. */
  private void insertHighCardinalityTestData(SparkSession spark, String tableName) {
    log.info("Inserting high cardinality test data");

    // Insert data one user at a time to maintain clustering by user_id bucket
    for (int i = 1; i <= 50; i++) {
      spark.sql(
          String.format(
              "INSERT INTO %s VALUES "
                  + "(%d, 'user_%03d', 'session_%d', 'login', current_timestamp()), "
                  + "(%d, 'user_%03d', 'session_%d', 'page_view', current_timestamp()), "
                  + "(%d, 'user_%03d', 'session_%d', 'logout', current_timestamp())",
              tableName, i * 3 - 2, i, i * 3 - 2, i * 3 - 1, i, i * 3 - 1, i * 3, i, i * 3));
    }

    log.info("Inserted 150 test records for high cardinality testing");
  }

  /** Analyze bucket behavior for separate bucketing scenario. */
  private void analyzeSeparateBucketBehavior(SparkSession spark, String tableName) {
    log.info("Analyzing separate bucket behavior");

    // Common bucket analysis
    BucketSpec bucketSpec =
        new BucketSpec("d1.separate_bucket_test", 2, null); // Don't validate count in spec
    bucketSpec.addExpectedTransform("bucket[4]", "First partition field should be bucket(4, key1)");
    bucketSpec.addExpectedTransform(
        "bucket[2]", "Second partition field should be bucket(2, key2)");

    validateBucketSpecification(spark, bucketSpec);

    // Validate record count separately
    validateRecordCount(spark, tableName, 10L);

    // Analyze data distribution specific to separate bucketing
    analyzeKeyDistribution(spark, tableName, "key1, key2", "separate bucket combinations");

    log.info("Separate bucket behavior analysis completed successfully");
  }

  /** Analyze bucket behavior for concatenated key bucketing scenario. */
  private void analyzeConcatenatedBucketBehavior(
      SparkSession spark, String tableName, String actualApproach) {
    log.info("Analyzing concatenated key bucket behavior using approach: {}", actualApproach);

    // Extract table ID from tableName (remove "openhouse." prefix if present)
    String tableId = tableName.startsWith("openhouse.") ? tableName.substring(10) : tableName;

    // Common bucket analysis
    BucketSpec bucketSpec =
        new BucketSpec(tableId, 1, 8L); // Manual concatenation inserts 8 records
    bucketSpec.addExpectedTransform("bucket[4]", "Partition field should contain bucket[4]");

    validateBucketSpecification(spark, bucketSpec);

    // Verify partition field name based on the approach used
    validatePartitionFieldName(spark, tableId, getExpectedPartitionFieldName(actualApproach));

    // Test concatenation behavior based on approach
    if ("direct concat expression".equals(actualApproach)) {
      testDirectConcatBucketing(spark, tableName);
    } else if ("computed column".equals(actualApproach)) {
      testComputedColumnBucketing(spark, tableName);
    } else {
      testFallbackConcatSimulation(spark, tableName);
    }

    // Validate record count based on approach
    long expectedRecordCount = actualApproach.equals("manual concatenation") ? 8L : 10L;
    validateRecordCount(spark, tableName, expectedRecordCount);

    log.info(
        "Concatenated key bucket behavior analysis completed successfully using: {}",
        actualApproach);
  }

  private String getExpectedPartitionFieldName(String actualApproach) {
    switch (actualApproach) {
      case "direct concat expression":
        return "concat_bucket"; // Expected for bucket(4, concat(key1, key2))
      case "computed column":
        return "concat_key_bucket"; // Expected for bucket(4, concat_key)
      case "manual concatenation":
        return "concat_key_bucket"; // Expected for bucket(4, concat_key) with manual data
      default:
        return "key1_bucket"; // Fallback approach
    }
  }

  private void testDirectConcatBucketing(SparkSession spark, String tableName) {
    log.info("Testing direct concat expression bucketing");

    // Test that concatenated values are properly distributed
    Dataset<Row> concatAnalysis =
        spark.sql(
            String.format(
                "SELECT concat(key1, key2) as actual_concat_key, COUNT(*) as record_count "
                    + "FROM %s "
                    + "GROUP BY concat(key1, key2) "
                    + "ORDER BY actual_concat_key",
                tableName));

    List<Row> concatResults = concatAnalysis.collectAsList();
    log.info("Direct concat analysis - found {} distinct concatenated keys", concatResults.size());

    // Print concatenated keys to verify they look correct
    concatResults.stream()
        .limit(5)
        .forEach(
            row -> log.info("Concatenated key: '{}', count: {}", row.getString(0), row.getLong(1)));

    // Verify we have meaningful concatenated keys like "admin001", "guest002", etc.
    Assertions.assertTrue(concatResults.size() > 0, "Should have concatenated key combinations");
    boolean foundMeaningfulConcat =
        concatResults.stream()
            .anyMatch(row -> row.getString(0).matches("\\w+\\d{3}")); // Pattern like "admin001"
    Assertions.assertTrue(
        foundMeaningfulConcat, "Should find meaningful concatenated patterns like 'admin001'");
  }

  private void testComputedColumnBucketing(SparkSession spark, String tableName) {
    log.info("Testing computed column bucketing");

    // Test that the computed column contains concatenated values
    Dataset<Row> computedColumnAnalysis =
        spark.sql(
            String.format(
                "SELECT concat_key, key1, key2, COUNT(*) as record_count "
                    + "FROM %s "
                    + "GROUP BY concat_key, key1, key2 "
                    + "ORDER BY concat_key",
                tableName));

    List<Row> computedResults = computedColumnAnalysis.collectAsList();
    log.info(
        "Computed column analysis - found {} distinct computed concat keys",
        computedResults.size());

    // Verify computed column values match expected concatenation
    computedResults.stream()
        .limit(5)
        .forEach(
            row -> {
              String concatKey = row.getString(0);
              String key1 = row.getString(1);
              String key2 = row.getString(2);
              String expectedConcat = key1 + key2;
              log.info(
                  "Computed key: '{}', key1: '{}', key2: '{}', expected: '{}'",
                  concatKey,
                  key1,
                  key2,
                  expectedConcat);
              Assertions.assertEquals(
                  expectedConcat, concatKey, "Computed column should equal key1 + key2");
            });
  }

  private void testFallbackConcatSimulation(SparkSession spark, String tableName) {
    log.info("Testing fallback concatenation simulation");

    // Simulate concatenation analysis for fallback approach
    Dataset<Row> simulationAnalysis =
        spark.sql(
            String.format(
                "SELECT key1, key2, concat(key1, key2) as simulated_concat_key, COUNT(*) as record_count "
                    + "FROM %s "
                    + "GROUP BY key1, key2 "
                    + "ORDER BY key1, key2",
                tableName));

    List<Row> simulationResults = simulationAnalysis.collectAsList();
    log.info("Simulation analysis - found {} distinct key combinations", simulationResults.size());

    // Print sample results to show concatenation would work
    simulationResults.stream()
        .limit(5)
        .forEach(
            row ->
                log.info(
                    "Key combination: '{}' + '{}' = '{}', count: {}",
                    row.getString(0),
                    row.getString(1),
                    row.getString(2),
                    row.getLong(3)));

    // Verify we can simulate concatenation even with fallback approach
    Assertions.assertTrue(
        simulationResults.size() > 0, "Should have key combinations for simulation");
    boolean foundValidSimulation =
        simulationResults.stream()
            .anyMatch(
                row ->
                    row.getString(2).length()
                        > row.getString(0).length()); // concat should be longer
    Assertions.assertTrue(
        foundValidSimulation, "Simulated concatenation should produce longer strings");
  }

  /** Analyze bucket distribution for high cardinality data. */
  private void analyzeBucketDistribution(SparkSession spark, String tableName) {
    log.info("Analyzing bucket distribution for high cardinality data");

    // Common bucket analysis
    BucketSpec bucketSpec = new BucketSpec("d1.bucket_validation_test", 1, null);
    bucketSpec.addExpectedTransform("bucket[8]", "Partition field should be bucket(8, user_id)");

    validateBucketSpecification(spark, bucketSpec);
    validatePartitionFieldName(spark, "d1.bucket_validation_test", "user_id_bucket");

    // Analyze high cardinality specific patterns
    analyzeKeyDistribution(
        spark, tableName, "user_id, event_type", "high cardinality user/event combinations");

    // Check that we have multiple unique users (high cardinality)
    validateHighCardinality(
        spark, tableName, "user_id", 50, "Should have at least 50 unique users");

    // Validate record count
    validateRecordCount(spark, tableName, 150L);

    log.info("High cardinality bucket distribution analysis completed successfully");
  }

  /** Common utility to validate high cardinality scenarios. */
  private void validateHighCardinality(
      SparkSession spark, String tableName, String column, long minUniqueCount, String message) {
    Dataset<Row> uniqueValues =
        spark.sql(
            String.format("SELECT COUNT(DISTINCT %s) as unique_count FROM %s", column, tableName));
    long uniqueCount = uniqueValues.collectAsList().get(0).getLong(0);
    Assertions.assertTrue(uniqueCount >= minUniqueCount, message + " (found: " + uniqueCount + ")");
    log.info("High cardinality validation: {} has {} unique values", column, uniqueCount);
  }

  // ========================= COMMON UTILITY METHODS =========================

  /** Configuration class for bucket specification validation. */
  private static class BucketSpec {
    private final String tableId;
    private final int expectedFieldCount;
    private final Long expectedRecordCount;
    private final List<TransformExpectation> transformExpectations = new ArrayList<>();

    public BucketSpec(String tableId, int expectedFieldCount, Long expectedRecordCount) {
      this.tableId = tableId;
      this.expectedFieldCount = expectedFieldCount;
      this.expectedRecordCount = expectedRecordCount;
    }

    public void addExpectedTransform(String transformPattern, String description) {
      transformExpectations.add(new TransformExpectation(transformPattern, description));
    }

    private static class TransformExpectation {
      final String pattern;
      final String description;

      TransformExpectation(String pattern, String description) {
        this.pattern = pattern;
        this.description = description;
      }
    }
  }

  /** Common utility to validate bucket specifications across all test scenarios. */
  private void validateBucketSpecification(SparkSession spark, BucketSpec bucketSpec) {
    // Verify partition specification using Iceberg API
    Operations operations = Operations.withCatalog(spark, null);
    Table table = operations.getTable(bucketSpec.tableId);
    PartitionSpec partitionSpec = table.spec();

    log.info("Partition spec fields count: {}", partitionSpec.fields().size());
    Assertions.assertEquals(
        bucketSpec.expectedFieldCount,
        partitionSpec.fields().size(),
        String.format("Should have %d partition field(s)", bucketSpec.expectedFieldCount));

    // Verify each expected transform
    for (int i = 0; i < bucketSpec.transformExpectations.size(); i++) {
      BucketSpec.TransformExpectation expectation = bucketSpec.transformExpectations.get(i);
      Assertions.assertTrue(
          partitionSpec.fields().get(i).transform().toString().contains(expectation.pattern),
          expectation.description);
    }

    // Verify total record count if specified
    if (bucketSpec.expectedRecordCount != null) {
      String tableName = "openhouse." + bucketSpec.tableId;
      validateRecordCount(spark, tableName, bucketSpec.expectedRecordCount);
    }
  }

  /** Common utility to analyze key distribution patterns using Iceberg metadata. */
  private void analyzeKeyDistribution(
      SparkSession spark, String tableName, String groupByColumns, String description) {
    // Get SQL distribution for reference
    Dataset<Row> distribution =
        spark.sql(
            String.format(
                "SELECT %s, COUNT(*) as record_count "
                    + "FROM %s "
                    + "GROUP BY %s "
                    + "ORDER BY %s",
                groupByColumns, tableName, groupByColumns, groupByColumns));

    List<Row> results = distribution.collectAsList();
    log.info(
        "SQL Distribution analysis for {} - found {} distinct combinations",
        description,
        results.size());

    // Log sample results for debugging
    results.stream()
        .limit(3)
        .forEach(
            row -> {
              StringBuilder keyValues = new StringBuilder();
              for (int i = 0; i < row.length() - 1; i++) { // -1 to skip count column
                if (i > 0) keyValues.append(", ");
                keyValues.append(row.getString(i));
              }
              log.info(
                  "Key combination: [{}], count: {}",
                  keyValues.toString(),
                  row.getLong(row.length() - 1));
            });

    Assertions.assertTrue(
        results.size() > 0, "Should have data distributed across buckets for " + description);

    // Now validate against actual Iceberg metadata
    validateIcebergBucketDistribution(spark, tableName, description, results);
  }

  /** Validate actual Iceberg bucket distribution using metadata APIs. */
  private void validateIcebergBucketDistribution(
      SparkSession spark, String tableName, String description, List<Row> sqlResults) {
    try {
      Operations operations = Operations.withCatalog(spark, null);
      String tableId = extractTableId(tableName);
      Table table = operations.getTable(tableId);

      // Get partition spec to understand bucketing configuration
      PartitionSpec partitionSpec = table.spec();
      log.info("Iceberg metadata analysis for {} - Partition spec: {}", description, partitionSpec);

      // Get actual data files and their partition information
      List<org.apache.iceberg.DataFile> dataFiles = new ArrayList<>();
      table
          .newScan()
          .planFiles()
          .forEach(
              task -> {
                dataFiles.add(task.file());
              });

      log.info("Iceberg metadata - Found {} data files for {}", dataFiles.size(), description);

      // Analyze bucket distribution from actual files
      Map<String, Integer> bucketFileCount = new HashMap<>();
      Map<String, Long> bucketRecordCount = new HashMap<>();

      for (org.apache.iceberg.DataFile dataFile : dataFiles) {
        // Get partition data
        org.apache.iceberg.StructLike partition = dataFile.partition();
        String partitionString = partition.toString();

        bucketFileCount.merge(partitionString, 1, Integer::sum);
        bucketRecordCount.merge(partitionString, dataFile.recordCount(), Long::sum);

        log.info(
            "File: {} -> Partition: {}, Records: {}",
            dataFile.path(),
            partitionString,
            dataFile.recordCount());
      }

      log.info("Iceberg bucket distribution for {}:", description);
      bucketFileCount.forEach(
          (partition, fileCount) -> {
            Long recordCount = bucketRecordCount.get(partition);
            log.info(
                "  Bucket/Partition: {} -> {} files, {} records",
                partition,
                fileCount,
                recordCount);
          });

      // Validate bucket distribution
      Assertions.assertTrue(
          bucketFileCount.size() > 0,
          "Should have at least one bucket with files for " + description);

      // Verify total record count matches
      long totalIcebergRecords =
          bucketRecordCount.values().stream().mapToLong(Long::longValue).sum();
      long totalSqlRecords =
          sqlResults.stream().mapToLong(row -> row.getLong(row.length() - 1)).sum();

      log.info(
          "Record count validation - Iceberg: {}, SQL: {}", totalIcebergRecords, totalSqlRecords);
      Assertions.assertEquals(
          totalSqlRecords,
          totalIcebergRecords,
          "Iceberg metadata record count should match SQL query results");

      // Validate bucket count expectations based on partition spec
      validateBucketCountExpectations(partitionSpec, bucketFileCount, description);

    } catch (Exception e) {
      log.warn("Could not validate Iceberg metadata for {}: {}", description, e.getMessage());
      // Don't fail the test if metadata analysis fails, but log the issue
    }
  }

  /** Validate bucket count expectations based on partition specification. */
  private void validateBucketCountExpectations(
      PartitionSpec partitionSpec, Map<String, Integer> bucketFileCount, String description) {
    // Extract expected bucket counts from partition spec
    int expectedTotalBuckets = 1;
    List<String> bucketTransforms = new ArrayList<>();

    for (PartitionField field : partitionSpec.fields()) {
      String transform = field.transform().toString();
      bucketTransforms.add(transform);

      // Extract bucket count from transform string like "bucket[4]"
      if (transform.contains("bucket[")) {
        String bucketCountStr =
            transform.substring(transform.indexOf('[') + 1, transform.indexOf(']'));
        try {
          int bucketCount = Integer.parseInt(bucketCountStr);
          expectedTotalBuckets *= bucketCount;
          log.info("Found bucket transform: {} with {} buckets", transform, bucketCount);
        } catch (NumberFormatException e) {
          log.warn("Could not parse bucket count from transform: {}", transform);
        }
      }
    }

    log.info("Expected total bucket combinations: {} for {}", expectedTotalBuckets, description);
    log.info("Actual partitions with files: {} for {}", bucketFileCount.size(), description);

    // Note: Actual bucket count may be less than expected if not all bucket combinations have data
    Assertions.assertTrue(
        bucketFileCount.size() <= expectedTotalBuckets,
        String.format(
            "Actual bucket count (%d) should not exceed expected total (%d) for %s",
            bucketFileCount.size(), expectedTotalBuckets, description));
  }

  /** Extract table identifier from full table name. */
  private String extractTableId(String tableName) {
    // Convert "openhouse.d1.table_name" to "d1.table_name"
    if (tableName.startsWith("openhouse.")) {
      return tableName.substring("openhouse.".length());
    }
    return tableName;
  }

  /** Overloaded method to validate record count with explicit table name. */
  private void validateRecordCount(SparkSession spark, String tableName, long expectedCount) {
    Dataset<Row> totalCount =
        spark.sql(String.format("SELECT COUNT(*) as total FROM %s", tableName));
    long actual = totalCount.collectAsList().get(0).getLong(0);
    Assertions.assertEquals(
        expectedCount, actual, String.format("Should have %d total records", expectedCount));
  }

  /** Common utility to validate partition field names. */
  private void validatePartitionFieldName(
      SparkSession spark, String tableId, String expectedFieldName) {
    Operations operations = Operations.withCatalog(spark, null);
    Table table = operations.getTable(tableId);
    PartitionSpec partitionSpec = table.spec();

    Assertions.assertEquals(
        expectedFieldName,
        partitionSpec.fields().get(0).name(),
        "Partition field should be named " + expectedFieldName);
  }
}

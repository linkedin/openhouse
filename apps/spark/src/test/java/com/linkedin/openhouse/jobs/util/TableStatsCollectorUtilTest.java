package com.linkedin.openhouse.jobs.util;

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
}

package com.linkedin.openhouse.jobs.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DatabaseTableFilterTest {
  @Test
  void testAcceptAll() {
    DatabaseTableFilter filter = DatabaseTableFilter.of(".*", ".*", 0);
    for (String testName : new String[] {"", "a", "test"}) {
      Assertions.assertTrue(filter.applyDatabaseName(testName));
      Assertions.assertTrue(filter.applyTableName(testName));
    }
    Assertions.assertTrue(
        filter.apply(TableMetadata.builder().dbName("aba").tableName("caba").build()));
  }

  @Test
  void testFilterByDatabaseName() {
    DatabaseTableFilter filter = DatabaseTableFilter.of("prefix.*", ".*", 0);
    Assertions.assertTrue(
        filter.apply(TableMetadata.builder().dbName("prefix1").tableName("caba").build()));
    Assertions.assertFalse(
        filter.apply(TableMetadata.builder().dbName("prefi").tableName("caba").build()));
  }

  @Test
  void testFilterByTableName() {
    DatabaseTableFilter filter = DatabaseTableFilter.of(".*", "prefix.*", 0);
    Assertions.assertTrue(
        filter.apply(TableMetadata.builder().dbName("db").tableName("prefix1").build()));
    Assertions.assertFalse(
        filter.apply(TableMetadata.builder().dbName("db").tableName("prefi").build()));
  }

  @Test
  void testFilterExact() {
    DatabaseTableFilter filter = DatabaseTableFilter.of("db", "table", 0);
    Assertions.assertTrue(
        filter.apply(TableMetadata.builder().dbName("db").tableName("table").build()));
    Assertions.assertFalse(
        filter.apply(TableMetadata.builder().dbName("db").tableName("tabl").build()));
    Assertions.assertFalse(
        filter.apply(TableMetadata.builder().dbName("dbs").tableName("table").build()));
  }

  @Test
  void testFilterByMinAgeThresholdHours() {
    // TODO: Implement the test
  }
}

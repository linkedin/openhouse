package com.linkedin.openhouse.jobs.util;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DatabaseTableFilterTest {
  private static final Long CREATION_TIME =
      System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(5);

  @Test
  void testAcceptAll() {
    DatabaseTableFilter filter = DatabaseTableFilter.of(".*", ".*", 0);
    for (String testName : new String[] {"", "a", "test"}) {
      Assertions.assertTrue(filter.applyDatabaseName(testName));
      Assertions.assertTrue(filter.applyTableName(testName));
    }
    Assertions.assertTrue(
        filter.apply(
            TableMetadata.builder()
                .dbName("aba")
                .tableName("caba")
                .creationTime(CREATION_TIME)
                .build()));
  }

  @Test
  void testFilterByDatabaseName() {
    DatabaseTableFilter filter = DatabaseTableFilter.of("prefix.*", ".*", 0);
    Assertions.assertTrue(
        filter.apply(
            TableMetadata.builder()
                .dbName("prefix1")
                .tableName("caba")
                .creationTime(CREATION_TIME)
                .build()));
    Assertions.assertFalse(
        filter.apply(
            TableMetadata.builder()
                .dbName("prefi")
                .tableName("caba")
                .creationTime(CREATION_TIME)
                .build()));
  }

  @Test
  void testFilterByTableName() {
    DatabaseTableFilter filter = DatabaseTableFilter.of(".*", "prefix.*", 0);
    Assertions.assertTrue(
        filter.apply(
            TableMetadata.builder()
                .dbName("db")
                .tableName("prefix1")
                .creationTime(CREATION_TIME)
                .build()));
    Assertions.assertFalse(
        filter.apply(
            TableMetadata.builder()
                .dbName("db")
                .tableName("prefi")
                .creationTime(CREATION_TIME)
                .build()));
  }

  @Test
  void testFilterExact() {
    DatabaseTableFilter filter = DatabaseTableFilter.of("db", "table", 0);
    Assertions.assertTrue(
        filter.apply(
            TableMetadata.builder()
                .dbName("db")
                .tableName("table")
                .creationTime(CREATION_TIME)
                .build()));
    Assertions.assertFalse(
        filter.apply(
            TableMetadata.builder()
                .dbName("db")
                .tableName("tabl")
                .creationTime(CREATION_TIME)
                .build()));
    Assertions.assertFalse(
        filter.apply(
            TableMetadata.builder()
                .dbName("dbs")
                .tableName("table")
                .creationTime(CREATION_TIME)
                .build()));
  }
}

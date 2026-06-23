package com.linkedin.openhouse.jobs.util;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionWhereClauseBuilderTest {

  @Test
  void testSinglePartitionSingleColumn() {
    Assertions.assertEquals(
        "(`date` = '2024-01-01')",
        PartitionWhereClauseBuilder.build("date", Collections.singletonList("2024-01-01")));
  }

  @Test
  void testMultiColumnMultiPartition() {
    Assertions.assertEquals(
        "(`date` = 'a' AND `hour` = 'b' AND `late` = 'c') "
            + "OR (`date` = 'a' AND `hour` = 'b' AND `late` = 'd')",
        PartitionWhereClauseBuilder.build("date,hour,late", Arrays.asList("a,b,c", "a,b,d")));
  }

  @Test
  void testToleratesGeneratorWhitespaceJoin() {
    // strategy generator joins values/columns with ", "
    Assertions.assertEquals(
        "(`date` = 'a' AND `hour` = 'b')",
        PartitionWhereClauseBuilder.build("date, hour", Collections.singletonList("a, b")));
  }

  @Test
  void testNullValueRendersIsNull() {
    Assertions.assertEquals(
        "(`date` = 'a' AND `hour` IS NULL)",
        PartitionWhereClauseBuilder.build("date,hour", Collections.singletonList("a,null")));
  }

  @Test
  void testSingleQuoteIsEscaped() {
    Assertions.assertEquals(
        "(`country` = 'O''Brien')",
        PartitionWhereClauseBuilder.build("country", Collections.singletonList("O'Brien")));
  }

  @Test
  void testMismatchedTupleSizeThrows() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PartitionWhereClauseBuilder.build("date,hour", Collections.singletonList("a,b,c")));
  }

  @Test
  void testEmptyValuesThrows() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PartitionWhereClauseBuilder.build("date", Collections.emptyList()));
  }
}

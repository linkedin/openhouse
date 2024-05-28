package com.linkedin.openhouse.jobs.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkJobUtilTest {
  @Test
  void testCreateDeleteStatement() {
    String expected =
        String.format(
            "DELETE FROM `db`.`table-name` WHERE timestamp < date_trunc('day', current_timestamp() - INTERVAL 2 days)");
    Assertions.assertEquals(
        expected, SparkJobUtil.createDeleteStatement("db.table-name", "timestamp", "", "day", 2));
  }

  @Test
  void testGetQuotedFqtn() {
    Assertions.assertEquals("`db`.`table-name`", SparkJobUtil.getQuotedFqtn("db.table-name"));
    Assertions.assertEquals(
        "`db-dashed`.`table-name`", SparkJobUtil.getQuotedFqtn("db-dashed.table-name"));
  }

  @Test
  void testCreateDeleteStatementWithStringColumnPartition() {
    String expected =
        "DELETE FROM `db`.`table-name` WHERE string_partition < cast(date_format(current_timestamp() - INTERVAL 2 DAYs, 'yyyy-MM-dd-HH') as string)";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "string_partition", "yyyy-MM-dd-HH", "DAY", 2));
  }
}

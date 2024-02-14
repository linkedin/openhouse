package com.linkedin.openhouse.jobs.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkJobUtilTest {
  @Test
  void testCreateDeleteStatement() {
    String expected =
        String.format(
            "DELETE FROM `db`.`table-name` WHERE date_trunc('day', timestamp) < date_trunc('day', current_timestamp() - INTERVAL 2 days)");
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
  void testCreateSelectLimitStatement() {
    String statement =
        SparkJobUtil.createSelectLimitStatement("table.name", "attribute", "", "day", 3, 1);
    Assertions.assertEquals(
        statement,
        "SELECT * FROM `table`.`name` WHERE date_trunc('day', attribute) < date_trunc('day', current_timestamp() - INTERVAL 3 days) limit 1");
  }

  @Test
  void testCreateSelectLimitStatementWithStringColumnPartition() {
    String expected =
        "SELECT * FROM `table`.`name` WHERE date_trunc('day', to_timestamp(attribute, 'yyyy-MM-dd')) < date_trunc('day', current_timestamp() - INTERVAL 3 days) limit 1";
    String actual =
        SparkJobUtil.createSelectLimitStatement(
            "table.name", "attribute", "yyyy-MM-dd", "day", 3, 1);
    Assertions.assertEquals(expected, actual);
  }

  @Test
  void testCreateDeleteStatementWithStringColumnPartition() {
    String expected =
        "DELETE FROM `db`.`table-name` WHERE date_trunc('day', to_timestamp(string_partition, 'yyyy-MM-dd-HH')) < date_trunc('day', current_timestamp() - INTERVAL 2 days)";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "string_partition", "yyyy-MM-dd-HH", "day", 2));
  }
}

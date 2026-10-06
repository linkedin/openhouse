package com.linkedin.openhouse.spark.statementtest;

import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;

final class StatementTestUtils {
  private StatementTestUtils() {}

  @SneakyThrows
  static String planWithoutExecuting(SparkSession spark, String statement) {
    // These tests validate policy parsing and planning, not catalog-specific command execution.
    return spark
        .sessionState()
        .executePlan(spark.sessionState().sqlParser().parsePlan(statement))
        .executedPlan()
        .treeString();
  }
}

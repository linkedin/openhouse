package com.linkedin.openhouse.spark.statementtest;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;

final class StatementTestUtils {
  private static final Pattern IDENTIFIER_SEQUENCE = Pattern.compile("ArrayBuffer\\(([^)]*)\\)");

  private StatementTestUtils() {}

  @SneakyThrows
  static String planWithoutExecuting(SparkSession spark, String statement) {
    // These tests validate policy parsing and planning, not catalog-specific command execution.
    String plan = spark.sessionState().sqlParser().parsePlan(statement).simpleString(1000);
    Matcher identifier = IDENTIFIER_SEQUENCE.matcher(plan);
    if (identifier.find()) {
      String[] identifierParts = identifier.group(1).split(", ");
      String dottedIdentifier = String.join(".", identifierParts);
      return identifier.replaceFirst(Matcher.quoteReplacement(dottedIdentifier));
    }
    return plan;
  }
}

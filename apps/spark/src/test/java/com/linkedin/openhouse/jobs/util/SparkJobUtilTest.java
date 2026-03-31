package com.linkedin.openhouse.jobs.util;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import org.apache.iceberg.expressions.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkJobUtilTest {
  @Test
  void testCreateDeleteStatement() {
    ZonedDateTime now = ZonedDateTime.now();
    String expected =
        String.format(
            "DELETE FROM `db`.`table-name` WHERE timestamp < date_trunc('day', timestamp '%s' - INTERVAL 2 days)",
            now.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime());
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement("db.table-name", "timestamp", "", "day", 2, now));
  }

  @Test
  void testGetQuotedFqtn() {
    Assertions.assertEquals("`db`.`table-name`", SparkJobUtil.getQuotedFqtn("db.table-name"));
    Assertions.assertEquals(
        "`db-dashed`.`table-name`", SparkJobUtil.getQuotedFqtn("db-dashed.table-name"));
  }

  @Test
  void testCreateDeleteStatementWithStringColumnPartition() {
    ZonedDateTime now = ZonedDateTime.now();
    String expected =
        String.format(
            "DELETE FROM `db`.`table-name` WHERE string_partition < cast(date_format(timestamp '%s' - INTERVAL 2 DAYs, 'yyyy-MM-dd-HH') as string)",
            now.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime());
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "string_partition", "yyyy-MM-dd-HH", "DAY", 2, now));
  }

  @Test
  void testCreateDeleteStatementNormalizesNonUtcTimezoneToUtc() {
    // Regression test: a non-UTC ZonedDateTime must produce the same DELETE statement
    // as its UTC equivalent. Before the fix, the local time was embedded directly into
    // the SQL literal, shifting the retention cutoff and causing no-op deletes.
    ZonedDateTime utcNow = ZonedDateTime.of(2026, 3, 30, 3, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime pdtEquivalent = utcNow.withZoneSameInstant(ZoneId.of("America/Los_Angeles"));

    // Both should produce identical SQL using the UTC timestamp
    String fromUtc = SparkJobUtil.createDeleteStatement("db.table", "ts", "", "day", 1, utcNow);
    String fromPdt =
        SparkJobUtil.createDeleteStatement("db.table", "ts", "", "day", 1, pdtEquivalent);
    Assertions.assertEquals(fromUtc, fromPdt);
    Assertions.assertTrue(
        fromUtc.contains("2026-03-30T03:00"),
        "DELETE statement should use UTC time 03:00, not local time");
  }

  @Test
  void testCreateDeleteStatementWithPatternNormalizesNonUtcTimezoneToUtc() {
    ZonedDateTime utcNow = ZonedDateTime.of(2026, 3, 30, 3, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime pdtEquivalent = utcNow.withZoneSameInstant(ZoneId.of("America/Los_Angeles"));

    String fromUtc =
        SparkJobUtil.createDeleteStatement("db.table", "col", "yyyy-MM-dd-HH", "DAY", 1, utcNow);
    String fromPdt =
        SparkJobUtil.createDeleteStatement(
            "db.table", "col", "yyyy-MM-dd-HH", "DAY", 1, pdtEquivalent);
    Assertions.assertEquals(fromUtc, fromPdt);
    Assertions.assertTrue(
        fromUtc.contains("2026-03-30T03:00"),
        "DELETE statement should use UTC time 03:00, not local time");
  }

  @Test
  public void testCreateDeleteFilterWithoutColumnPattern() {
    ZonedDateTime now = ZonedDateTime.now();
    String column = "ts";
    String columnPattern = "";
    String granularity = "DAY";
    int count = 1;

    Expression expr =
        SparkJobUtil.createDeleteFilter(column, columnPattern, granularity, count, now);
    long expectedCutoffDate =
        ZonedDateTime.now().minusDays(1).truncatedTo(ChronoUnit.DAYS).toEpochSecond() * 1000 * 1000;

    UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
    Assertions.assertEquals(Expression.Operation.LT, predicate.op());
    Assertions.assertEquals(column, predicate.ref().name());
    Assertions.assertEquals(predicate.literal().value(), expectedCutoffDate);
  }

  @Test
  public void testCreateDeleteFilterWithColumnPattern() {
    ZonedDateTime now = ZonedDateTime.now();
    String column = "ts";
    String columnPattern = "yyyy-MM-dd-HH";
    String granularity = "HOUR";
    int count = 30;

    Expression expr =
        SparkJobUtil.createDeleteFilter(column, columnPattern, granularity, count, now);
    String expectedCutoffDate =
        DateTimeFormatter.ofPattern(columnPattern).format(ZonedDateTime.now().minusHours(30));

    UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
    Assertions.assertEquals(Expression.Operation.LT, predicate.op());
    Assertions.assertEquals(column, predicate.ref().name());
    Assertions.assertEquals(predicate.literal().value(), expectedCutoffDate);
  }
}

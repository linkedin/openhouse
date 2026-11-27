package com.linkedin.openhouse.jobs.util;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import org.apache.iceberg.expressions.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SparkJobUtilTest {
  @Test
  void testCreateDeleteStatement() {
    ZonedDateTime now = ZonedDateTime.now();
    String expected =
        String.format(
            "DELETE FROM `db`.`table-name` WHERE timestamp < date_trunc('day', timestamp '%s' - INTERVAL 2 days)",
            now.toLocalDateTime());
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
            now.toLocalDateTime());
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "string_partition", "yyyy-MM-dd-HH", "DAY", 2, now));
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

  @Test
  void zonedStringStatementUsesZoneWallClock() {
    ZonedDateTime evaluationTime =
        ZonedDateTime.parse("2024-02-01T02:00Z")
            .withZoneSameInstant(ZoneId.of("America/Los_Angeles"));
    Assertions.assertEquals(
        "DELETE FROM `db`.`t` WHERE dp < cast(date_format(timestamp '2024-01-31T18:00' - INTERVAL 2 DAYs, 'yyyy-MM-dd') as string)",
        SparkJobUtil.createDeleteStatement("db.t", "dp", "yyyy-MM-dd", "DAY", 2, evaluationTime));
  }

  @Test
  void zonedStringFilterUsesZoneWallClock() {
    ZonedDateTime evaluationTime =
        ZonedDateTime.parse("2024-02-01T02:00Z")
            .withZoneSameInstant(ZoneId.of("America/Los_Angeles"));
    UnboundPredicate<?> predicate =
        (UnboundPredicate<?>)
            SparkJobUtil.createDeleteFilter("dp", "yyyy-MM-dd", "DAY", 2, evaluationTime);
    Assertions.assertEquals("2024-01-29", predicate.literal().value());
  }

  @ParameterizedTest
  @CsvSource({
    "America/Los_Angeles,2024-01-10T01:00Z,DAY,3,yyyy-MM-dd,2024-01-06",
    "UTC,2024-01-10T01:00Z,DAY,3,yyyy-MM-dd,2024-01-07",
    "Asia/Tokyo,2024-01-09T20:45Z,DAY,3,yyyy-MM-dd,2024-01-07",
    "+05:30,2024-02-01T02:00Z,HOUR,1,yyyy-MM-dd-HH,2024-02-01-06",
    "-08:00,2024-03-11T07:30Z,DAY,1,yyyy-MM-dd,2024-03-09",
    "America/Los_Angeles,2024-03-11T07:30Z,DAY,1,yyyy-MM-dd,2024-03-10",
    "America/Los_Angeles,2024-11-04T07:30Z,DAY,1,yyyy-MM-dd,2024-11-02",
    "America/Los_Angeles,2024-03-10T10:30Z,HOUR,1,yyyy-MM-dd-HH,2024-03-10-02",
    "America/Los_Angeles,2024-11-03T09:30Z,HOUR,1,yyyy-MM-dd-HH,2024-11-03-00",
    "America/Los_Angeles,2024-02-01T02:30Z,DAY,1,yyyy-MM-dd-HH,2024-01-30-18",
    "Europe/London,2024-11-01T00:30Z,DAY,7,yyyy-MM-dd,2024-10-25",
    "America/Los_Angeles,2024-03-31T20:00Z,MONTH,1,yyyy-MM-dd,2024-02-29",
    "America/Los_Angeles,2024-02-29T20:00Z,YEAR,1,yyyy-MM-dd,2023-02-28"
  })
  void zonedStringFilterUsesDeclaredCalendar(
      String timeZone,
      String evaluationTime,
      String granularity,
      int count,
      String columnPattern,
      String expectedCutoff) {
    UnboundPredicate<?> predicate =
        (UnboundPredicate<?>)
            SparkJobUtil.createDeleteFilter(
                "dp",
                columnPattern,
                granularity,
                count,
                ZonedDateTime.parse(evaluationTime).withZoneSameInstant(ZoneId.of(timeZone)));
    Assertions.assertEquals(expectedCutoff, predicate.literal().value());
  }
}

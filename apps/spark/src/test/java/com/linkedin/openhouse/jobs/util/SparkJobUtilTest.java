package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.jobs.exception.RetentionConfigurationException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.iceberg.expressions.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class SparkJobUtilTest {

  private static final ZonedDateTime FIXED_UTC =
      ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);

  @Test
  void testGetQuotedFqtn() {
    Assertions.assertEquals("`db`.`table-name`", SparkJobUtil.getQuotedFqtn("db.table-name"));
    Assertions.assertEquals(
        "`db-dashed`.`table-name`", SparkJobUtil.getQuotedFqtn("db-dashed.table-name"));
  }

  @Test
  void nativeStatementDay() {
    Assertions.assertEquals(
        "DELETE FROM `db`.`t` WHERE ts < date_trunc('day', timestamp '2024-02-01T02:00' - INTERVAL 2 days)",
        SparkJobUtil.createDeleteStatement("db.t", "ts", "", "day", 2, FIXED_UTC));
  }

  @Test
  void nativeStatementHour() {
    Assertions.assertEquals(
        "DELETE FROM `db`.`t` WHERE ts < date_trunc('HOUR', timestamp '2024-02-01T02:00' - INTERVAL 3 HOURs)",
        SparkJobUtil.createDeleteStatement("db.t", "ts", "", "HOUR", 3, FIXED_UTC));
  }

  @Test
  void nativeStatementMonth() {
    Assertions.assertEquals(
        "DELETE FROM `db`.`t` WHERE ts < date_trunc('MONTH', timestamp '2024-02-01T02:00' - INTERVAL 1 MONTHs)",
        SparkJobUtil.createDeleteStatement("db.t", "ts", "", "MONTH", 1, FIXED_UTC));
  }

  @Test
  void nativeStatementYear() {
    Assertions.assertEquals(
        "DELETE FROM `db`.`t` WHERE ts < date_trunc('YEAR', timestamp '2024-02-01T02:00' - INTERVAL 1 YEARs)",
        SparkJobUtil.createDeleteStatement("db.t", "ts", "", "YEAR", 1, FIXED_UTC));
  }

  @Test
  void stringStatementDay() {
    Assertions.assertEquals(
        "DELETE FROM `db`.`t` WHERE dp < cast(date_format(timestamp '2024-02-01T02:00' - INTERVAL 2 DAYs, 'yyyy-MM-dd') as string)",
        SparkJobUtil.createDeleteStatement("db.t", "dp", "yyyy-MM-dd", "DAY", 2, FIXED_UTC));
  }

  @Test
  void stringStatementHour() {
    Assertions.assertEquals(
        "DELETE FROM `db`.`t` WHERE dp < cast(date_format(timestamp '2024-02-01T02:00' - INTERVAL 5 HOURs, 'yyyy-MM-dd-HH') as string)",
        SparkJobUtil.createDeleteStatement("db.t", "dp", "yyyy-MM-dd-HH", "HOUR", 5, FIXED_UTC));
  }

  @Test
  void stringStatementMonth() {
    Assertions.assertEquals(
        "DELETE FROM `db`.`t` WHERE dp < cast(date_format(timestamp '2024-02-01T02:00' - INTERVAL 1 MONTHs, 'yyyy-MM') as string)",
        SparkJobUtil.createDeleteStatement("db.t", "dp", "yyyy-MM", "MONTH", 1, FIXED_UTC));
  }

  @Test
  void nativeFilterDay() {
    long expected =
        ZonedDateTime.of(2024, 1, 31, 0, 0, 0, 0, ZoneOffset.UTC).toEpochSecond() * 1000 * 1000;
    UnboundPredicate<?> predicate =
        (UnboundPredicate<?>) SparkJobUtil.createDeleteFilter("ts", "", "DAY", 1, FIXED_UTC);
    Assertions.assertEquals(Expression.Operation.LT, predicate.op());
    Assertions.assertEquals("ts", predicate.ref().name());
    Assertions.assertEquals(expected, predicate.literal().value());
  }

  @Test
  void nativeFilterHour() {
    long expected =
        ZonedDateTime.of(2024, 1, 31, 20, 0, 0, 0, ZoneOffset.UTC).toEpochSecond() * 1000 * 1000;
    UnboundPredicate<?> predicate =
        (UnboundPredicate<?>) SparkJobUtil.createDeleteFilter("ts", "", "HOUR", 6, FIXED_UTC);
    Assertions.assertEquals(expected, predicate.literal().value());
  }

  @Test
  void stringFilterHour() {
    UnboundPredicate<?> predicate =
        (UnboundPredicate<?>)
            SparkJobUtil.createDeleteFilter("dp", "yyyy-MM-dd-HH", "HOUR", 30, FIXED_UTC);
    Assertions.assertEquals("2024-01-30-20", predicate.literal().value());
  }

  @Test
  void nativeFilterMonthAndYearAreUnsupportedOnMain() {
    // Main does not support month or year retention with backup enabled: the Iceberg backup filter
    // truncates the cutoff to the granularity in Java, and java.time cannot truncate to a month or
    // year. Locked in so the limitation is not silently changed.
    Assertions.assertThrows(
        DateTimeException.class,
        () -> SparkJobUtil.createDeleteFilter("ts", "", "MONTH", 1, FIXED_UTC));
    Assertions.assertThrows(
        DateTimeException.class,
        () -> SparkJobUtil.createDeleteFilter("ts", "", "YEAR", 1, FIXED_UTC));
  }

  @Test
  void zonedStringStatementUsesZoneWallClock() {
    // now is 2024-01-31T18:00 in America/Los_Angeles; the statement interpolates that zoned wall
    // clock, so the label reflects the local calendar day.
    ZonedDateTime laNow = FIXED_UTC.withZoneSameInstant(ZoneId.of("America/Los_Angeles"));
    Assertions.assertEquals(
        "DELETE FROM `db`.`t` WHERE dp < cast(date_format(timestamp '2024-01-31T18:00' - INTERVAL 2 DAYs, 'yyyy-MM-dd') as string)",
        SparkJobUtil.createDeleteStatement("db.t", "dp", "yyyy-MM-dd", "DAY", 2, laNow));
  }

  @Test
  void zonedStringFilterUsesZoneWallClock() {
    ZonedDateTime laNow = FIXED_UTC.withZoneSameInstant(ZoneId.of("America/Los_Angeles"));
    UnboundPredicate<?> predicate =
        (UnboundPredicate<?>) SparkJobUtil.createDeleteFilter("dp", "yyyy-MM-dd", "DAY", 2, laNow);
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
    "America/Los_Angeles,2024-02-01T02:30Z,DAY,1,yyyy-MM-dd-HH,2024-01-30-18",
    "Europe/London,2024-11-01T00:30Z,DAY,7,yyyy-MM-dd,2024-10-25",
    "America/Los_Angeles,2024-03-31T20:00Z,MONTH,1,yyyy-MM-dd,2024-02-29",
    "America/Los_Angeles,2024-02-29T20:00Z,YEAR,1,yyyy-MM-dd,2023-02-28"
  })
  void zonedStringCutoffUsesDeclaredCalendar(
      String timeZone,
      String evaluationTime,
      String granularity,
      int count,
      String columnPattern,
      String expectedCutoff)
      throws RetentionConfigurationException {
    Assertions.assertEquals(
        expectedCutoff,
        SparkJobUtil.createZonedStringCutoff(
            columnPattern,
            granularity,
            count,
            ZonedDateTime.parse(evaluationTime),
            ZoneId.of(timeZone)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "yyyy-MM-dd-X", "yyyy-MM-dd VV"})
  void zonedStringCutoffRejectsInvalidPattern(String pattern) {
    Assertions.assertThrows(
        RetentionConfigurationException.class,
        () ->
            SparkJobUtil.createZonedStringCutoff(
                pattern, "DAY", 1, FIXED_UTC, ZoneId.of("America/Los_Angeles")));
  }
}

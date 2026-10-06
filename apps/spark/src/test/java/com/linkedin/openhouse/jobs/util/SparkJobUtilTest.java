package com.linkedin.openhouse.jobs.util;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.iceberg.expressions.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Retention SQL is a query over partition values: it deletes {@code col < truncate(now,
 * granularity) - count periods}, exclusive. The regression tests lock in the no-zone delete SQL and
 * filter that ship on main and must not change. The zoned tests cover the added retention time
 * zone, which is accepted on a native timestamp column and on a zone-free string pattern; for a
 * zoned native column the DELETE statement and the Iceberg backup filter resolve to the same
 * instant.
 */
public class SparkJobUtilTest {

  private static final ZonedDateTime FIXED_UTC =
      ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);

  @Test
  void testGetQuotedFqtn() {
    Assertions.assertEquals("`db`.`table-name`", SparkJobUtil.getQuotedFqtn("db.table-name"));
    Assertions.assertEquals(
        "`db-dashed`.`table-name`", SparkJobUtil.getQuotedFqtn("db-dashed.table-name"));
  }

  // ---------- Regression: the no-zone delete SQL that ships on main ----------

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

  // ---------- Zoned columns: the added time zone ----------

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

  @Test
  void zonedNativeStatementUsesLocalDateEdge() {
    // now is 2024-01-31T18:00 in America/Los_Angeles; keeping two days cuts at the start of
    // 2024-01-29 in Los Angeles. Rendered for the UTC session that start-of-day is
    // 2024-01-29T08:00,
    // the same instant the backup filter uses.
    ZonedDateTime laNow = FIXED_UTC.withZoneSameInstant(ZoneId.of("America/Los_Angeles"));
    Assertions.assertEquals(
        "DELETE FROM `db`.`t` WHERE ts < timestamp '2024-01-29T08:00'",
        SparkJobUtil.createDeleteStatement("db.t", "ts", "", "day", 2, laNow));
    assertStatementMatchesFilter("day", 2, laNow);
  }

  @Test
  void zonedNativeStatementEastOfUtcUsesLocalDateEdge() {
    // +05:30 is east of UTC. now is 2024-02-01T07:30+05:30, today is 2024-02-01 locally, so keeping
    // one day cuts at the start of 2024-01-31 local = 2024-01-30T18:30Z. The naive main SQL dropped
    // the offset and cut at 2024-01-31T00:00Z, deleting part of local 2024-01-31 that should stay.
    ZonedDateTime istNow = FIXED_UTC.withZoneSameInstant(ZoneOffset.ofHoursMinutes(5, 30));
    Assertions.assertEquals(
        "DELETE FROM `db`.`t` WHERE ts < timestamp '2024-01-30T18:30'",
        SparkJobUtil.createDeleteStatement("db.t", "ts", "", "DAY", 1, istNow));
    assertStatementMatchesFilter("DAY", 1, istNow);
  }

  @Test
  void zonedNativeStatementAgreesWithFilterAcrossSpringForward() {
    // Spring-forward in America/Los_Angeles is 2024-03-10; stepping two days back from 2024-03-11
    // crosses it, and the statement and filter still resolve to the same instant.
    ZonedDateTime laNow =
        ZonedDateTime.of(2024, 3, 11, 12, 0, 0, 0, ZoneId.of("America/Los_Angeles"));
    assertStatementMatchesFilter("DAY", 2, laNow);
  }

  @Test
  void zonedNativeStatementAgreesWithFilterAcrossFallBack() {
    // Fall-back in America/Los_Angeles is 2024-11-03; stepping two days back from 2024-11-04
    // crosses
    // it, and the statement and filter still resolve to the same instant.
    ZonedDateTime laNow =
        ZonedDateTime.of(2024, 11, 4, 12, 0, 0, 0, ZoneId.of("America/Los_Angeles"));
    assertStatementMatchesFilter("DAY", 2, laNow);
  }

  private static void assertStatementMatchesFilter(
      String granularity, int count, ZonedDateTime now) {
    String statement =
        SparkJobUtil.createDeleteStatement("db.t", "ts", "", granularity, count, now);
    UnboundPredicate<?> filter =
        (UnboundPredicate<?>) SparkJobUtil.createDeleteFilter("ts", "", granularity, count, now);
    String literal =
        statement.substring(statement.indexOf("timestamp '") + 11, statement.lastIndexOf('\''));
    long statementMicros = LocalDateTime.parse(literal).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    long filterMicros = ((Number) filter.literal().value()).longValue();
    Assertions.assertEquals(filterMicros, statementMicros);
  }
}

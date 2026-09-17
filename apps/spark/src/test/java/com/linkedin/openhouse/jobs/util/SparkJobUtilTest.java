package com.linkedin.openhouse.jobs.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.iceberg.expressions.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkJobUtilTest {
  @Test
  void testCreateDeleteStatement() {
    // Native timestamp column, no zone (UTC): the range start is the current day start moved back 2
    // days, emitted as absolute micros via timestamp_micros. The query shape depends on the column
    // type, not the zone.
    ZonedDateTime now = ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);
    long micros =
        LocalDateTime.of(2024, 1, 30, 0, 0).toInstant(ZoneOffset.UTC).getEpochSecond()
            * 1000
            * 1000;
    String expected =
        "DELETE FROM `db`.`table-name` WHERE timestamp < timestamp_micros(" + micros + ")";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement("db.table-name", "timestamp", "", "day", 2, now, ""));
  }

  @Test
  void testGetQuotedFqtn() {
    Assertions.assertEquals("`db`.`table-name`", SparkJobUtil.getQuotedFqtn("db.table-name"));
    Assertions.assertEquals(
        "`db-dashed`.`table-name`", SparkJobUtil.getQuotedFqtn("db-dashed.table-name"));
  }

  @Test
  void testCreateDeleteStatementWithStringColumnPartition() {
    // String-partitioned column, no zone (UTC): the range start is a formatted wall-clock label 2
    // days
    // back, compared lexicographically.
    ZonedDateTime now = ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);
    String expected = "DELETE FROM `db`.`table-name` WHERE string_partition < '2024-01-30-02'";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "string_partition", "yyyy-MM-dd-HH", "DAY", 2, now, ""));
  }

  @Test
  public void testCreateDeleteFilterWithoutColumnPattern() {
    ZonedDateTime now = ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);
    long expectedCutoffDate =
        LocalDateTime.of(2024, 1, 31, 0, 0).toInstant(ZoneOffset.UTC).getEpochSecond()
            * 1000
            * 1000;
    Expression expr = SparkJobUtil.createDeleteFilter("ts", "", "DAY", 1, now, "");
    UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
    Assertions.assertEquals(Expression.Operation.LT, predicate.op());
    Assertions.assertEquals("ts", predicate.ref().name());
    Assertions.assertEquals(expectedCutoffDate, predicate.literal().value());
  }

  @Test
  public void testCreateDeleteFilterWithColumnPattern() {
    ZonedDateTime now = ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);
    Expression expr = SparkJobUtil.createDeleteFilter("ts", "yyyy-MM-dd-HH", "HOUR", 30, now, "");
    UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
    Assertions.assertEquals(Expression.Operation.LT, predicate.op());
    Assertions.assertEquals("ts", predicate.ref().name());
    Assertions.assertEquals("2024-01-30-20", predicate.literal().value());
  }

  @Test
  void testCreateDeleteStatementZonedNativeAlignsToUtcPartitionEdge() {
    ZonedDateTime now = ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);
    // now is 2024-01-31T18:00 in America/Los_Angeles (PST, -08:00); the local day start moved back
    // 2 days is 2024-01-29T00:00 local = 2024-01-29T08:00Z, aligned down to the UTC day edge and
    // emitted as absolute micros so the executed delete is independent of the Spark session zone.
    long micros =
        LocalDateTime.of(2024, 1, 29, 0, 0).toInstant(ZoneOffset.UTC).getEpochSecond()
            * 1000
            * 1000;
    String expected = "DELETE FROM `db`.`table-name` WHERE ts < timestamp_micros(" + micros + ")";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "ts", "", "DAY", 2, now, "America/Los_Angeles"));
  }

  @Test
  void testCreateDeleteStatementZonedStringPatternAnchorsNowToZone() {
    ZonedDateTime now = ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);
    // now is 2024-01-31T18:00 in America/Los_Angeles; the wall-clock range start two days back is
    // 2024-01-29, formatted with the column pattern.
    String expected = "DELETE FROM `db`.`table-name` WHERE dp < '2024-01-29'";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "dp", "yyyy-MM-dd", "DAY", 2, now, "America/Los_Angeles"));
  }

  @Test
  void testCreateDeleteFilterZonedNativeAlignsToUtcPartitionEdge() {
    ZonedDateTime now = ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);
    long expected =
        LocalDateTime.of(2024, 1, 29, 0, 0).toInstant(ZoneOffset.UTC).getEpochSecond()
            * 1000
            * 1000;
    Expression expr =
        SparkJobUtil.createDeleteFilter("ts", "", "DAY", 2, now, "America/Los_Angeles");
    UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
    Assertions.assertEquals(Expression.Operation.LT, predicate.op());
    Assertions.assertEquals("ts", predicate.ref().name());
    Assertions.assertEquals(expected, predicate.literal().value());
  }

  @Test
  void testCreateDeleteStatementZonedFractionalHourZoneAlignsDown() {
    ZonedDateTime now = ZonedDateTime.of(2024, 6, 1, 12, 0, 0, 0, ZoneOffset.UTC);
    // Asia/Kolkata is +05:30. now in zone is 2024-06-01T17:30; hour start is 17:00 (= 11:30Z); one
    // hour back is 16:00 (= 10:30Z); aligned down to the UTC hour edge is 10:00Z.
    long micros =
        LocalDateTime.of(2024, 6, 1, 10, 0).toInstant(ZoneOffset.UTC).getEpochSecond()
            * 1000
            * 1000;
    String expected = "DELETE FROM `db`.`table-name` WHERE ts < timestamp_micros(" + micros + ")";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "ts", "", "HOUR", 1, now, "Asia/Kolkata"));
  }

  @Test
  void testZonedStringRangeConsistentAcrossDstBetweenStatementAndFilter() {
    // America/Los_Angeles spring-forward: 2024-03-10 02:00 -> 03:00. now=2024-03-10T10:30Z is
    // 03:30 PDT; the wall-clock range start one hour back is 02:30, formatted as 2024-03-10-02. The
    // executed SQL delete and the Iceberg backup filter must derive the same label.
    ZonedDateTime now = ZonedDateTime.of(2024, 3, 10, 10, 30, 0, 0, ZoneOffset.UTC);
    String statement =
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "dp", "yyyy-MM-dd-HH", "HOUR", 1, now, "America/Los_Angeles");
    Expression filter =
        SparkJobUtil.createDeleteFilter(
            "dp", "yyyy-MM-dd-HH", "HOUR", 1, now, "America/Los_Angeles");
    UnboundPredicate<?> predicate = (UnboundPredicate<?>) filter;
    Assertions.assertEquals("DELETE FROM `db`.`table-name` WHERE dp < '2024-03-10-02'", statement);
    Assertions.assertEquals("2024-03-10-02", predicate.literal().value());
  }

  @Test
  void testCreateDeleteStatementZonedNativeMonthAlignsToUtcMonthEdge() {
    // now=2024-03-15T05:00Z is 2024-03-14T22:00 in America/Los_Angeles; the local month start moved
    // back 1 month is 2024-02-01T00:00 local = 2024-02-01T08:00Z, aligned to the UTC month edge.
    ZonedDateTime now = ZonedDateTime.of(2024, 3, 15, 5, 0, 0, 0, ZoneOffset.UTC);
    long micros =
        LocalDateTime.of(2024, 2, 1, 0, 0).toInstant(ZoneOffset.UTC).getEpochSecond() * 1000 * 1000;
    String expected = "DELETE FROM `db`.`table-name` WHERE ts < timestamp_micros(" + micros + ")";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "ts", "", "MONTH", 1, now, "America/Los_Angeles"));
  }

  @Test
  void testCreateDeleteStatementZonedNativeYearAlignsToUtcYearEdge() {
    // now=2024-06-15T05:00Z is 2024-06-14T22:00 in America/Los_Angeles; the local year start moved
    // back 1 year is 2023-01-01T00:00 local = 2023-01-01T08:00Z, aligned to the UTC year edge.
    ZonedDateTime now = ZonedDateTime.of(2024, 6, 15, 5, 0, 0, 0, ZoneOffset.UTC);
    long micros =
        LocalDateTime.of(2023, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).getEpochSecond() * 1000 * 1000;
    String expected = "DELETE FROM `db`.`table-name` WHERE ts < timestamp_micros(" + micros + ")";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "ts", "", "YEAR", 1, now, "America/Los_Angeles"));
  }

  @Test
  void testZonedNativeStatementAndFilterUseIdenticalUtcMicros() {
    // The executed SQL delete (timestamp_micros) and the Iceberg backup filter must compare against
    // the identical absolute instant, so the certified metadata-only range matches what is deleted
    // regardless of the Spark session time zone.
    ZonedDateTime now = ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);
    long micros =
        LocalDateTime.of(2024, 1, 29, 0, 0).toInstant(ZoneOffset.UTC).getEpochSecond()
            * 1000
            * 1000;
    String statement =
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "ts", "", "DAY", 2, now, "America/Los_Angeles");
    Expression filter =
        SparkJobUtil.createDeleteFilter("ts", "", "DAY", 2, now, "America/Los_Angeles");
    UnboundPredicate<?> predicate = (UnboundPredicate<?>) filter;
    Assertions.assertEquals(
        "DELETE FROM `db`.`table-name` WHERE ts < timestamp_micros(" + micros + ")", statement);
    Assertions.assertEquals(micros, predicate.literal().value());
  }

  @Test
  void testZonedStringRangeConsistentAcrossFallBackDst() {
    // America/Los_Angeles fall-back: 2024-11-03 02:00 -> 01:00 (the 01:00 hour repeats).
    // now=2024-11-03T09:30Z is 01:30 PST (after the transition at 09:00Z); the wall-clock range
    // start
    // one hour back is 00:30, formatted as 2024-11-03-00. The executed SQL delete and the Iceberg
    // backup filter must derive the same label.
    ZonedDateTime now = ZonedDateTime.of(2024, 11, 3, 9, 30, 0, 0, ZoneOffset.UTC);
    String statement =
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "dp", "yyyy-MM-dd-HH", "HOUR", 1, now, "America/Los_Angeles");
    Expression filter =
        SparkJobUtil.createDeleteFilter(
            "dp", "yyyy-MM-dd-HH", "HOUR", 1, now, "America/Los_Angeles");
    UnboundPredicate<?> predicate = (UnboundPredicate<?>) filter;
    Assertions.assertEquals("DELETE FROM `db`.`table-name` WHERE dp < '2024-11-03-00'", statement);
    Assertions.assertEquals("2024-11-03-00", predicate.literal().value());
  }
}

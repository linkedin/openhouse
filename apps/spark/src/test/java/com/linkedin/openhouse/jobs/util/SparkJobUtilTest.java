package com.linkedin.openhouse.jobs.util;

import java.time.LocalDateTime;
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
            now.toLocalDateTime());
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
    ZonedDateTime now = ZonedDateTime.now();
    String expected =
        String.format(
            "DELETE FROM `db`.`table-name` WHERE string_partition < cast(date_format(timestamp '%s' - INTERVAL 2 DAYs, 'yyyy-MM-dd-HH') as string)",
            now.toLocalDateTime());
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "string_partition", "yyyy-MM-dd-HH", "DAY", 2, now, ""));
  }

  @Test
  public void testCreateDeleteFilterWithoutColumnPattern() {
    ZonedDateTime now = ZonedDateTime.now();
    String column = "ts";
    String columnPattern = "";
    String granularity = "DAY";
    int count = 1;

    Expression expr =
        SparkJobUtil.createDeleteFilter(column, columnPattern, granularity, count, now, "");
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
        SparkJobUtil.createDeleteFilter(column, columnPattern, granularity, count, now, "");
    String expectedCutoffDate =
        DateTimeFormatter.ofPattern(columnPattern).format(ZonedDateTime.now().minusHours(30));

    UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
    Assertions.assertEquals(Expression.Operation.LT, predicate.op());
    Assertions.assertEquals(column, predicate.ref().name());
    Assertions.assertEquals(predicate.literal().value(), expectedCutoffDate);
  }

  @Test
  void testCreateDeleteStatementZonedNativeSnapsToUtcPartitionEdge() {
    ZonedDateTime now = ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);
    // now is 2024-01-31T18:00 in America/Los_Angeles (PST, -08:00); the local day start moved back
    // 2 days is 2024-01-29T00:00 local = 2024-01-29T08:00Z, snapped down to the UTC day edge.
    String expected = "DELETE FROM `db`.`table-name` WHERE ts < timestamp '2024-01-29T00:00'";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "ts", "", "DAY", 2, now, "America/Los_Angeles"));
  }

  @Test
  void testCreateDeleteStatementZonedStringPatternAnchorsNowToZone() {
    ZonedDateTime now = ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);
    // now is 2024-01-31T18:00 in America/Los_Angeles; the wall-clock boundary two days back is
    // 2024-01-29, formatted with the column pattern.
    String expected = "DELETE FROM `db`.`table-name` WHERE dp < '2024-01-29'";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "dp", "yyyy-MM-dd", "DAY", 2, now, "America/Los_Angeles"));
  }

  @Test
  void testCreateDeleteFilterZonedNativeSnapsToUtcPartitionEdge() {
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
  void testCreateDeleteStatementZonedFractionalHourZoneSnapsDown() {
    ZonedDateTime now = ZonedDateTime.of(2024, 6, 1, 12, 0, 0, 0, ZoneOffset.UTC);
    // Asia/Kolkata is +05:30. now in zone is 2024-06-01T17:30; hour start is 17:00 (= 11:30Z); one
    // hour back is 16:00 (= 10:30Z); snapped down to the UTC hour edge is 10:00Z.
    String expected = "DELETE FROM `db`.`table-name` WHERE ts < timestamp '2024-06-01T10:00'";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "ts", "", "HOUR", 1, now, "Asia/Kolkata"));
  }

  @Test
  void testZonedStringBoundaryConsistentAcrossDstBetweenStatementAndFilter() {
    // America/Los_Angeles spring-forward: 2024-03-10 02:00 -> 03:00. now=2024-03-10T10:30Z is
    // 03:30 PDT; the wall-clock boundary one hour back is 02:30, formatted as 2024-03-10-02. The
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
  void testCreateDeleteStatementZonedNativeMonthSnapsToUtcMonthEdge() {
    // now=2024-03-15T05:00Z is 2024-03-14T22:00 in America/Los_Angeles; the local month start moved
    // back 1 month is 2024-02-01T00:00 local = 2024-02-01T08:00Z, snapped to the UTC month edge.
    ZonedDateTime now = ZonedDateTime.of(2024, 3, 15, 5, 0, 0, 0, ZoneOffset.UTC);
    String expected = "DELETE FROM `db`.`table-name` WHERE ts < timestamp '2024-02-01T00:00'";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "ts", "", "MONTH", 1, now, "America/Los_Angeles"));
  }

  @Test
  void testCreateDeleteStatementZonedNativeYearSnapsToUtcYearEdge() {
    // now=2024-06-15T05:00Z is 2024-06-14T22:00 in America/Los_Angeles; the local year start moved
    // back 1 year is 2023-01-01T00:00 local = 2023-01-01T08:00Z, snapped to the UTC year edge.
    ZonedDateTime now = ZonedDateTime.of(2024, 6, 15, 5, 0, 0, 0, ZoneOffset.UTC);
    String expected = "DELETE FROM `db`.`table-name` WHERE ts < timestamp '2023-01-01T00:00'";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "ts", "", "YEAR", 1, now, "America/Los_Angeles"));
  }
}

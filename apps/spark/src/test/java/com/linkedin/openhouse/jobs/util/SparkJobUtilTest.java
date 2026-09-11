package com.linkedin.openhouse.jobs.util;

import java.time.LocalDateTime;
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
    // now is 2024-01-31T18:00 in America/Los_Angeles (PST, -08:00). The local day start moved back
    // 2
    // days is 2024-01-29T00:00 local = 2024-01-29T08:00Z, snapped down to the UTC day edge.
    String expected = "DELETE FROM `db`.`table-name` WHERE ts < timestamp '2024-01-29T00:00'";
    Assertions.assertEquals(
        expected,
        SparkJobUtil.createDeleteStatement(
            "db.table-name", "ts", "", "DAY", 2, now, "America/Los_Angeles"));
  }

  @Test
  void testCreateDeleteStatementZonedStringPatternAnchorsNowToZone() {
    ZonedDateTime now = ZonedDateTime.of(2024, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC);
    LocalDateTime zoneNow =
        now.withZoneSameInstant(ZoneId.of("America/Los_Angeles")).toLocalDateTime();
    String expected =
        String.format(
            "DELETE FROM `db`.`table-name` WHERE dp < cast(date_format(timestamp '%s' - INTERVAL 2 DAYs, 'yyyy-MM-dd') as string)",
            zoneNow);
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
}

package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.tables.client.model.TimePartitionSpec;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

/** * Utility class to provide spark apps action semantics and SQL statements. */
@Slf4j
public final class SparkJobUtil {
  private SparkJobUtil() {}
  /*
  Example:
  Table: test_retention with retentionConfig:
    "retention":{
           "count": 30,
           "granularity": "DAY",
           "columnPattern": null }}
  Partitioned by time on datePartition column
  Query: datePartition < date_trunc('DAY', current_timestamp() - INTERVAL 30 DAYs)"
  */
  private static final String RETENTION_CONDITION_TEMPLATE =
      "%s < date_trunc('%s', timestamp '%s' - INTERVAL %d %ss)";

  /*
   A mismatch between data and pattern provided results in the datasets being filtered from deletion.
   Reason: to_date parsing returns null if it fails to parse date as per pattern.
   example:
   table: test_retention with retentionConfig:
     "retention":{
          "count": 30,
          "granularity": "DAY",
          "columnPattern":{
              "columnName": "datePartition",
              "pattern":"yyyy-MM-dd"}}
   Data in 'datePartition' column:
    Case1: "2024-01-01"
      query:  to_date(substring(datePartition, 0, CHAR_LENGTH('yyyy-MM-dd')), 'yyyy-MM-dd') <
              date_trunc('DAY', current_timestamp() - INTERVAL 30 DAYs)"
      result: record will be deleted
    Case2: "2024-01.01"
      query:  to_date(substring(datePartition, 0, CHAR_LENGTH('yyyy-MM-dd')), 'yyyy-MM-dd') <
              date_trunc('DAY', current_timestamp() - INTERVAL 3 DAYs)"
      result: records will be filtered from deletion
  */
  private static final String RETENTION_CONDITION_WITH_PATTERN_TEMPLATE =
      "%s < cast(date_format(timestamp '%s' - INTERVAL %s %ss, '%s') as string)";

  private static final long MICROS_PER_SECOND = 1000L * 1000L;

  public static String createDeleteStatement(
      String fqtn,
      String columnName,
      String columnPattern,
      String granularity,
      int count,
      ZonedDateTime now,
      String timeZone) {
    boolean hasTimeZoneOverride = !StringUtils.isBlank(timeZone);
    if (!StringUtils.isBlank(columnPattern)) {
      // String-partitioned column: the boundary is a formatted wall-clock label, compared
      // lexicographically. With a zone override, compute the boundary once (in the zone, on the
      // wall
      // clock) via zonedStringBoundary so this SQL delete and the Iceberg backup filter in
      // createDeleteFilter derive the same label even across daylight-saving transitions.
      String condition;
      if (hasTimeZoneOverride) {
        ZonedDateTime effectiveNow = now.withZoneSameInstant(ZoneId.of(timeZone));
        condition =
            String.format(
                "%s < '%s'",
                columnName, zonedStringBoundary(effectiveNow, granularity, count, columnPattern));
      } else {
        condition =
            String.format(
                RETENTION_CONDITION_WITH_PATTERN_TEMPLATE,
                columnName,
                now.toLocalDateTime(),
                count,
                granularity,
                columnPattern);
      }
      String query = String.format("DELETE FROM %s WHERE %s", getQuotedFqtn(fqtn), condition);
      log.info(
          "Table: {}. Column pattern: {}, columnName {}, granularity {}s, timeZone {}, retention query: {}",
          fqtn,
          columnPattern,
          columnName,
          granularity,
          timeZone,
          query);
      return query;
    } else if (hasTimeZoneOverride) {
      // Native timestamp column: evaluate the boundary in the zone and snap it down to the UTC
      // partition edge (metadata-only partition drop). Emit it as an absolute epoch value via
      // timestamp_micros so the executed delete matches the Iceberg backup filter regardless of the
      // Spark session time zone.
      long boundaryMicros =
          snappedNativeBoundaryUtcMicros(
              now.withZoneSameInstant(ZoneId.of(timeZone)), granularity, count);
      String query =
          String.format(
              "DELETE FROM %s WHERE %s < timestamp_micros(%d)",
              getQuotedFqtn(fqtn), columnName, boundaryMicros);
      log.info(
          "Table: {}. No column pattern, timeZone {}, retention query: {}", fqtn, timeZone, query);
      return query;
    } else {
      String query =
          String.format(
              "DELETE FROM %s WHERE %s",
              getQuotedFqtn(fqtn),
              String.format(
                  RETENTION_CONDITION_TEMPLATE,
                  columnName,
                  granularity,
                  now.toLocalDateTime(),
                  count,
                  granularity));
      log.info("Table: {}. No column pattern provided: deleteQuery: {}", fqtn, query);
      return query;
    }
  }

  public static Expression createDeleteFilter(
      String columnName,
      String columnPattern,
      String granularity,
      int count,
      ZonedDateTime now,
      String timeZone) {
    ChronoUnit timeUnitGranularity = convertGranularityToChrono(granularity.toUpperCase());
    if (StringUtils.isBlank(timeZone)) {
      ZonedDateTime cutoffDate = now.minus(timeUnitGranularity.getDuration().multipliedBy(count));
      if (!StringUtils.isBlank(columnPattern)) {
        String formattedCutoffDate = DateTimeFormatter.ofPattern(columnPattern).format(cutoffDate);
        return Expressions.lessThan(columnName, formattedCutoffDate);
      } else {
        long formattedCutoffDate =
            cutoffDate.truncatedTo(timeUnitGranularity).toEpochSecond() * MICROS_PER_SECOND;
        return Expressions.lessThan(columnName, formattedCutoffDate);
      }
    }
    ZonedDateTime effectiveNow = now.withZoneSameInstant(ZoneId.of(timeZone));
    if (!StringUtils.isBlank(columnPattern)) {
      return Expressions.lessThan(
          columnName, zonedStringBoundary(effectiveNow, granularity, count, columnPattern));
    } else {
      long micros = snappedNativeBoundaryUtcMicros(effectiveNow, granularity, count);
      return Expressions.lessThan(columnName, micros);
    }
  }

  /**
   * Compute the retention boundary for a string-partitioned column as a formatted wall-clock label.
   * "now" is taken in the requested zone, then moved back by {@code count} periods on the wall
   * clock (not the instant), so the executed SQL delete and the Iceberg backup filter derive the
   * same label even across daylight-saving transitions. Retention deletes rows whose string value
   * is lexicographically less than this label and keeps rows at or after it.
   */
  private static String zonedStringBoundary(
      ZonedDateTime effectiveNow, String granularity, int count, String columnPattern) {
    ChronoUnit unit = convertGranularityToChrono(granularity.toUpperCase());
    LocalDateTime boundary = effectiveNow.toLocalDateTime().minus(count, unit);
    return DateTimeFormatter.ofPattern(columnPattern).format(boundary);
  }

  /**
   * Compute the native-timestamp retention boundary as absolute microseconds since the UTC epoch,
   * so the executed SQL delete and the Iceberg backup filter compare against the identical instant
   * regardless of the Spark session time zone.
   */
  private static long snappedNativeBoundaryUtcMicros(
      ZonedDateTime nowInZone, String granularity, int count) {
    return snappedNativeBoundaryUtc(nowInZone, granularity, count)
            .toInstant(ZoneOffset.UTC)
            .getEpochSecond()
        * MICROS_PER_SECOND;
  }

  /**
   * Compute the retention boundary for a native timestamp column: the start of the current period
   * in the given zone, moved back by {@code count} periods, then snapped down to the UTC partition
   * edge so the resulting delete covers whole partitions (a metadata-only partition drop). The
   * returned value is the boundary as a UTC wall-clock timestamp; retention deletes rows strictly
   * before it and keeps rows at or after it.
   */
  private static LocalDateTime snappedNativeBoundaryUtc(
      ZonedDateTime nowInZone, String granularity, int count) {
    ChronoUnit unit = convertGranularityToChrono(granularity.toUpperCase());
    ZonedDateTime periodStart = truncateToGranularity(nowInZone, unit);
    ZonedDateTime boundaryInZone = periodStart.minus(count, unit);
    ZonedDateTime boundaryUtc = boundaryInZone.withZoneSameInstant(ZoneOffset.UTC);
    return truncateToGranularity(boundaryUtc, unit).toLocalDateTime();
  }

  private static ZonedDateTime truncateToGranularity(ZonedDateTime dateTime, ChronoUnit unit) {
    switch (unit) {
      case HOURS:
        return dateTime.truncatedTo(ChronoUnit.HOURS);
      case MONTHS:
        return dateTime.toLocalDate().withDayOfMonth(1).atStartOfDay(dateTime.getZone());
      case YEARS:
        return dateTime.toLocalDate().withDayOfYear(1).atStartOfDay(dateTime.getZone());
      case DAYS:
        return dateTime.truncatedTo(ChronoUnit.DAYS);
      default:
        throw new IllegalArgumentException(
            "Unsupported retention granularity for time-zone-aware boundary: " + unit);
    }
  }

  public static String getQuotedFqtn(String fqtn) {
    String[] fqtnTokens = fqtn.split("\\.");
    // adding single quotes around fqtn for cases when db and/or tableName has special character(s),
    // like '-'
    return String.format("`%s`.`%s`", fqtnTokens[0], fqtnTokens[1]);
  }

  public static void setModifiedTimeStamp(FileSystem fs, Path dirPath, int daysOld)
      throws IOException {
    long timestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(daysOld);
    FileStatus[] files = fs.listStatus(dirPath);
    for (FileStatus file : files) {
      fs.setTimes(file.getPath(), timestamp, -1);
    }
  }

  public static ChronoUnit convertGranularityToChrono(String granularity) {
    if (Arrays.stream(TimePartitionSpec.GranularityEnum.values())
        .anyMatch(e -> e.name().equals(granularity))) {
      switch (TimePartitionSpec.GranularityEnum.valueOf(granularity)) {
        case HOUR:
          return ChronoUnit.HOURS;
        case DAY:
          return ChronoUnit.DAYS;
        case MONTH:
          return ChronoUnit.MONTHS;
        case YEAR:
          return ChronoUnit.YEARS;
      }
    }
    return ChronoUnit.valueOf(granularity);
  }
}

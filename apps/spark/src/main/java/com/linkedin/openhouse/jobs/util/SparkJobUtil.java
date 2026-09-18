package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.tables.client.model.TimePartitionSpec;
import java.io.IOException;
import java.time.OffsetDateTime;
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

  private static final long MICROS_PER_SECOND = 1000L * 1000L;

  // Native timestamp column, no zone: reproduces today's behavior. Spark truncates the retention
  // clock in the session zone. Example (30-day daily): datepartition < date_trunc('DAY', timestamp
  // '<now>' - INTERVAL 30 DAYs).
  private static final String RETENTION_CONDITION_TEMPLATE =
      "%s < date_trunc('%s', timestamp '%s' - INTERVAL %d %ss)";

  // String-partitioned column: retention compares the formatted label lexicographically. Spark
  // moves the retention clock (shifted to the zone when one is set) back count periods and formats
  // it with the column pattern. Example: datepartition < date_format(timestamp '<now>' - INTERVAL
  // 30 DAYs, 'yyyy-MM-dd').
  private static final String RETENTION_CONDITION_WITH_PATTERN_TEMPLATE =
      "%s < cast(date_format(timestamp '%s' - INTERVAL %s %ss, '%s') as string)";

  // Native timestamp column with a zone: native time partitions are bucketed in UTC, so the range
  // start is computed in the zone, floored to the UTC partition edge, and emitted as an absolute
  // instant. The date_trunc template above cannot express this: feeding a zoned wall clock into a
  // UTC truncation shifts the range start by the zone offset and, for a positive offset such as
  // +05:30, moves it past now and deletes recent data.
  private static final String RETENTION_CONDITION_ZONED_NATIVE_TEMPLATE =
      "%s < timestamp_micros(%d)";

  public static String createDeleteStatement(
      String fqtn,
      String columnName,
      String columnPattern,
      String granularity,
      int count,
      ZonedDateTime now,
      String timeZone) {
    ZonedDateTime clock = atRetentionZone(now, timeZone);
    String predicate;
    if (!StringUtils.isBlank(columnPattern)) {
      predicate =
          String.format(
              RETENTION_CONDITION_WITH_PATTERN_TEMPLATE,
              columnName,
              clock.toLocalDateTime(),
              count,
              granularity,
              columnPattern);
    } else if (StringUtils.isBlank(timeZone)) {
      predicate =
          String.format(
              RETENTION_CONDITION_TEMPLATE,
              columnName,
              granularity,
              clock.toLocalDateTime(),
              count,
              granularity);
    } else {
      predicate =
          String.format(
              RETENTION_CONDITION_ZONED_NATIVE_TEMPLATE,
              columnName,
              retentionRangeStartEpochMicros(clock, granularity, count));
    }
    String query = String.format("DELETE FROM %s WHERE %s", getQuotedFqtn(fqtn), predicate);
    log.info(
        "Table: {}. columnName {}, columnPattern {}, granularity {}s, timeZone {}, retention query: {}",
        fqtn,
        columnName,
        columnPattern,
        granularity,
        timeZone,
        query);
    return query;
  }

  public static Expression createDeleteFilter(
      String columnName,
      String columnPattern,
      String granularity,
      int count,
      ZonedDateTime now,
      String timeZone) {
    ZonedDateTime clock = atRetentionZone(now, timeZone);
    if (StringUtils.isBlank(columnPattern)) {
      return Expressions.lessThan(
          columnName, retentionRangeStartEpochMicros(clock, granularity, count));
    }
    ChronoUnit period = convertGranularityToChrono(granularity.toUpperCase());
    // Subtract on the frozen wall clock (toOffsetDateTime keeps now's offset) so the label matches
    // what Spark's date_format/INTERVAL produces in the statement, even across a daylight-saving
    // transition, and carries an offset for patterns that format one.
    String rangeStartLabel =
        DateTimeFormatter.ofPattern(columnPattern)
            .format(clock.toOffsetDateTime().minus(count, period));
    return Expressions.lessThan(columnName, rangeStartLabel);
  }

  /**
   * The reference time expressed in the retention zone: the requested IANA zone id or fixed offset
   * when set, otherwise the zone the caller already chose for {@code now} (UTC in the retention
   * app).
   */
  private static ZonedDateTime atRetentionZone(ZonedDateTime now, String timeZone) {
    return StringUtils.isBlank(timeZone) ? now : now.withZoneSameInstant(ZoneId.of(timeZone));
  }

  /**
   * The oldest instant to keep for a native timestamp column, as microseconds since the UTC epoch.
   * The current period start in the retention zone, moved back {@code count} periods, then taken to
   * the UTC partition edge so the delete removes whole Iceberg partitions instead of rewriting the
   * edge one. Inclusive: at 12:01 with one-period retention this keeps the current partial period,
   * so an hourly period retains 24 hours and a daily period 36 hours.
   */
  private static long retentionRangeStartEpochMicros(
      ZonedDateTime asOf, String granularity, int count) {
    ChronoUnit period = convertGranularityToChrono(granularity.toUpperCase());
    ZonedDateTime rangeStart = startOfPeriod(asOf, period).minus(count, period);
    ZonedDateTime utcPartitionEdge =
        startOfPeriod(rangeStart.withZoneSameInstant(ZoneOffset.UTC), period);
    return utcPartitionEdge.toEpochSecond() * MICROS_PER_SECOND;
  }

  /** Start of the period containing {@code time}, in {@code time}'s own zone. */
  private static ZonedDateTime startOfPeriod(ZonedDateTime time, ChronoUnit period) {
    if (period == ChronoUnit.MONTHS) {
      return time.toLocalDate().withDayOfMonth(1).atStartOfDay(time.getZone());
    }
    if (period == ChronoUnit.YEARS) {
      return time.toLocalDate().withDayOfYear(1).atStartOfDay(time.getZone());
    }
    return time.truncatedTo(period);
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

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

  public static String createDeleteStatement(
      String fqtn,
      String columnName,
      String columnPattern,
      String granularity,
      int count,
      ZonedDateTime now,
      String timeZone) {
    ZonedDateTime asOf = atRetentionZone(now, timeZone);
    String predicate =
        StringUtils.isBlank(columnPattern)
            ? String.format(
                "%s < timestamp_micros(%d)",
                columnName, retentionRangeStartEpochMicros(asOf, granularity, count))
            : String.format(
                "%s < '%s'",
                columnName, retentionRangeStartLabel(asOf, granularity, count, columnPattern));
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
    ZonedDateTime asOf = atRetentionZone(now, timeZone);
    if (StringUtils.isBlank(columnPattern)) {
      return Expressions.lessThan(
          columnName, retentionRangeStartEpochMicros(asOf, granularity, count));
    }
    return Expressions.lessThan(
        columnName, retentionRangeStartLabel(asOf, granularity, count, columnPattern));
  }

  /**
   * The reference time expressed in the retention zone: the requested IANA zone id or fixed offset
   * when set, otherwise the zone the caller already chose for {@code now} (UTC in the retention
   * app). The zone shifts the retention range; it never changes the shape of the emitted query.
   */
  private static ZonedDateTime atRetentionZone(ZonedDateTime now, String timeZone) {
    return StringUtils.isBlank(timeZone) ? now : now.withZoneSameInstant(ZoneId.of(timeZone));
  }

  /**
   * The oldest label to keep for a string-partitioned column: the retention-zone wall clock moved
   * back {@code count} periods and formatted with the column's pattern, carrying the zone offset so
   * patterns that include an offset field format correctly. Rows whose label sorts before this are
   * deleted; rows at or after it are kept. {@link #createDeleteStatement} and {@link
   * #createDeleteFilter} both call this, so the executed delete and the Iceberg backup filter agree
   * even across daylight-saving transitions.
   */
  private static String retentionRangeStartLabel(
      ZonedDateTime asOf, String granularity, int count, String columnPattern) {
    OffsetDateTime rangeStart =
        asOf.toOffsetDateTime().minus(count, convertGranularityToChrono(granularity.toUpperCase()));
    return DateTimeFormatter.ofPattern(columnPattern).format(rangeStart);
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

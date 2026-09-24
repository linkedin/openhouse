package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.tables.client.model.TimePartitionSpec;
import java.io.IOException;
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

  // Native timestamp column: retention truncates the clock to the granularity and moves it back
  // count periods. Example (30-day daily): datepartition < date_trunc('DAY', timestamp '<now>' -
  // INTERVAL 30 DAYs).
  private static final String RETENTION_CONDITION_TEMPLATE =
      "%s < date_trunc('%s', timestamp '%s' - INTERVAL %d %ss)";

  // String-partitioned column: retention compares the formatted label lexicographically. The clock
  // moves back count periods and is formatted with the column pattern. When a retention time zone
  // is set, the clock arrives already in that zone, so the label reflects the local wall clock.
  // Example: datepartition < date_format(timestamp '<now>' - INTERVAL 30 DAYs, 'yyyy-MM-dd').
  private static final String RETENTION_CONDITION_WITH_PATTERN_TEMPLATE =
      "%s < cast(date_format(timestamp '%s' - INTERVAL %s %ss, '%s') as string)";

  public static String createDeleteStatement(
      String fqtn,
      String columnName,
      String columnPattern,
      String granularity,
      int count,
      ZonedDateTime now) {
    if (!StringUtils.isBlank(columnPattern)) {
      String query =
          String.format(
              "DELETE FROM %s WHERE %s",
              getQuotedFqtn(fqtn),
              String.format(
                  RETENTION_CONDITION_WITH_PATTERN_TEMPLATE,
                  columnName,
                  now.toLocalDateTime(),
                  count,
                  granularity,
                  columnPattern));
      log.info(
          "Table: {}. Column pattern: {}, columnName {}, granularity {}s, retention query: {}",
          fqtn,
          columnPattern,
          columnName,
          granularity,
          query);
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
      String columnName, String columnPattern, String granularity, int count, ZonedDateTime now) {
    ChronoUnit timeUnitGranularity =
        ChronoUnit.valueOf(convertGranularityToChrono(granularity.toUpperCase()).name());
    ZonedDateTime cutoffDate = now.minus(timeUnitGranularity.getDuration().multipliedBy(count));
    if (!StringUtils.isBlank(columnPattern)) {
      String formattedCutoffDate = DateTimeFormatter.ofPattern(columnPattern).format(cutoffDate);
      return Expressions.lessThan(columnName, formattedCutoffDate);
    } else {
      long formattedCutoffDate =
          cutoffDate.truncatedTo(timeUnitGranularity).toEpochSecond() * 1000 * 1000; // microsecond
      return Expressions.lessThan(columnName, formattedCutoffDate);
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

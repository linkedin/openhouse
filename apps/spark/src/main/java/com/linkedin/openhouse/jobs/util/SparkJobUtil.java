package com.linkedin.openhouse.jobs.util;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** * Utility class to provide spark apps action semantics and SQL statements. */
@Slf4j
public final class SparkJobUtil {
  private SparkJobUtil() {}

  private static final String RETENTION_CONDITION =
      "date_trunc('%s', %s) < date_trunc('%s', current_timestamp() - INTERVAL %d %ss)";

  private static final String RETENTION_CONDITION_WITH_PATTERN =
      "date_trunc('%s', to_timestamp(%s, '%s')) < date_trunc('%s', current_timestamp() - INTERVAL %d %ss)";

  public static String createDeleteStatement(
      String fqtn, String columnName, String columnPattern, String granularity, int count) {
    if (!StringUtils.isBlank(columnPattern)) {
      String query =
          String.format(
              "DELETE FROM %s WHERE %s",
              getQuotedFqtn(fqtn),
              String.format(
                  RETENTION_CONDITION_WITH_PATTERN,
                  granularity,
                  columnName,
                  columnPattern,
                  granularity,
                  count,
                  granularity));
      log.info("Table: {} column pattern provided: {} retention_query: {}", query);
      return query;
    } else {
      String query =
          String.format(
              "DELETE FROM %s WHERE %s",
              getQuotedFqtn(fqtn),
              String.format(
                  RETENTION_CONDITION, granularity, columnName, granularity, count, granularity));
      log.info("Table: {} No column pattern provided: selectQuery: {}", fqtn, query);
      return query;
    }
  }

  public static String createSelectLimitStatement(
      String fqtn,
      String columnName,
      String columnPattern,
      String granularity,
      int count,
      int limit) {
    if (!StringUtils.isBlank(columnPattern)) {
      String query =
          String.format(
              "SELECT * FROM %s WHERE %s limit %d",
              getQuotedFqtn(fqtn),
              String.format(
                  RETENTION_CONDITION_WITH_PATTERN,
                  granularity,
                  columnName,
                  columnPattern,
                  granularity,
                  count,
                  granularity),
              limit);
      log.info("Table: {} column pattern provided: {} selectQuery: {}", fqtn, columnPattern, query);
      return query;
    } else {
      String query =
          String.format(
              "SELECT * FROM %s WHERE %s limit %d",
              getQuotedFqtn(fqtn),
              String.format(
                  RETENTION_CONDITION, granularity, columnName, granularity, count, granularity),
              limit);
      log.info("Table: {} No column pattern provided: selectQuery: {}", fqtn, query);
      return query;
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
}

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
  /*
  Example:
  Table: test_retention with retentionConfig:
    "retention":{
           "count": 30,
           "granularity": "DAY",
           "columnPattern": null }}
  Partitioned by time on datePartition column
  Query: date_trunc('DAY', datePartition) < date_trunc('DAY', current_timestamp() - INTERVAL 30 DAYs)"
  */
  private static final String RETENTION_CONDITION_TEMPLATE =
      "date_trunc('%s', %s) < date_trunc('%s', current_timestamp() - INTERVAL %d %ss)";

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
      "%s < cast(date_format(current_timestamp() - INTERVAL %s %ss, '%s') as string)";

  public static String createDeleteStatement(
      String fqtn, String columnName, String columnPattern, String granularity, int count) {
    if (!StringUtils.isBlank(columnPattern)) {
      String query =
          String.format(
              "DELETE FROM %s WHERE %s",
              getQuotedFqtn(fqtn),
              String.format(
                  RETENTION_CONDITION_WITH_PATTERN_TEMPLATE,
                  columnName,
                  count,
                  granularity,
                  columnPattern));
      log.info(
          "Table: {}. Column pattern: {}, columnName {}, granularity {}s, " + "retention query: {}",
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
                  granularity,
                  columnName,
                  granularity,
                  count,
                  granularity));
      log.info("Table: {}. No column pattern provided: deleteQuery: {}", fqtn, query);
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

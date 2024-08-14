package com.linkedin.openhouse.jobs.util;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DatabaseTableFilter {
  private final Pattern databasePattern;
  private final Pattern tablePattern;
  private final int cutoffHour;

  public static DatabaseTableFilter of(String databaseRegex, String tableRegex, int cutoffHour) {
    return new DatabaseTableFilter(
        Pattern.compile(databaseRegex), Pattern.compile(tableRegex), cutoffHour);
  }

  public boolean apply(TableMetadata metadata) {
    return applyDatabaseName(metadata.getDbName())
        && applyTableName(metadata.getTableName())
        && applyTimeFilter(metadata);
  }

  public boolean applyDatabaseName(String databaseName) {
    return databasePattern.matcher(databaseName).matches();
  }

  public boolean applyTableName(String tableName) {
    return tablePattern.matcher(tableName).matches();
  }

  public boolean applyTableDirectoryPath(String tableDirectoryName) {
    return tablePattern.matcher(tableDirectoryName).matches();
  }

  public boolean applyTimeFilter(TableMetadata metadata) {
    return metadata.getCreationTime()
        < System.currentTimeMillis() - TimeUnit.HOURS.toMillis(cutoffHour);
  }
}

package com.linkedin.openhouse.jobs.util;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DatabaseTableFilter {
  private final Pattern databasePattern;
  private final Pattern tablePattern;
  private final int minAgeThresholdHours;

  public static DatabaseTableFilter of(
      String databaseRegex, String tableRegex, int minAgeThresholdHours) {
    return new DatabaseTableFilter(Pattern.compile(databaseRegex), Pattern.compile(tableRegex), 0);
  }

  public boolean apply(TableMetadata metadata) {
    return applyDatabaseName(metadata.getDbName())
        && applyTableName(metadata.getTableName())
        && applyTableCreationTime(metadata.getCreationTimeMs());
  }

  public boolean applyDatabaseName(String databaseName) {
    return databasePattern.matcher(databaseName).matches();
  }

  public boolean applyTableName(String tableName) {
    return tablePattern.matcher(tableName).matches();
  }

  public boolean applyTableCreationTime(long creationTime) {
    return creationTime
        < System.currentTimeMillis() - TimeUnit.HOURS.toMillis(minAgeThresholdHours);
  }

  public boolean applyTableDirectoryPath(String tableDirectoryName) {
    return tablePattern.matcher(tableDirectoryName).matches();
  }
}

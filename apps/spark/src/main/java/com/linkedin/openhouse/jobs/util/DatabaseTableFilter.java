package com.linkedin.openhouse.jobs.util;

import java.util.regex.Pattern;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DatabaseTableFilter {
  private final Pattern databasePattern;
  private final Pattern tablePattern;

  public static DatabaseTableFilter of(String databaseRegex, String tableRegex) {
    return new DatabaseTableFilter(Pattern.compile(databaseRegex), Pattern.compile(tableRegex));
  }

  public boolean apply(TableMetadata metadata) {
    return applyDatabaseName(metadata.getDbName()) && applyTableName(metadata.getTableName());
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
}

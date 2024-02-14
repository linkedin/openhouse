package com.linkedin.openhouse.jobs.util;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Table metadata, including database name, table name and owner. */
@Builder
@Getter
@EqualsAndHashCode
public class TableMetadata {
  String dbName;
  String tableName;
  String creator;

  @Override
  public String toString() {
    return String.format("name: %s.%s, creator: %s", dbName, tableName, creator);
  }

  public String fqtn() {
    return String.format("%s.%s", dbName, tableName);
  }
}

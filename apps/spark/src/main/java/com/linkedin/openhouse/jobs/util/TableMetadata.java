package com.linkedin.openhouse.jobs.util;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Table metadata, including database name, table name and owner. */
@Getter
@EqualsAndHashCode(callSuper = true)
public class TableMetadata extends Metadata {
  String dbName;
  String tableName;

  @Builder
  public TableMetadata(String creator, String dbName, String tableName) {
    super(creator);
    this.dbName = dbName;
    this.tableName = tableName;
  }

  @Override
  public String toString() {
    return String.format("dbName: %s, tableName: %s, creator: %s", dbName, tableName, creator);
  }

  public String fqtn() {
    return String.format("%s.%s", dbName, tableName);
  }

  @Override
  public String getValue() {
    return fqtn();
  }
}

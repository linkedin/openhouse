package com.linkedin.openhouse.common.exception;

/** An RTAS request drops a source column referenced by a current or historical partition spec. */
public class HistoricPartitionColumnRemovedException extends RuntimeException {
  private final String columnName;

  public HistoricPartitionColumnRemovedException(String columnName) {
    super(
        String.format(
            "RTAS does not support dropping a partition column [%s]. Only removing partitioning is allowed",
            columnName));
    this.columnName = columnName;
  }

  public String getColumnName() {
    return columnName;
  }
}

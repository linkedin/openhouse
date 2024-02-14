package com.linkedin.openhouse.common.exception;

import lombok.Getter;

/** Specifically mapped to {@link org.apache.iceberg.exceptions.CommitStateUnknownException} */
@Getter
public class OpenHouseCommitStateUnknownException extends RuntimeException {
  private String tableId;

  private Throwable cause;

  public OpenHouseCommitStateUnknownException(String tableId, String message, Throwable cause) {
    super(message);
    this.tableId = tableId;
    this.cause = cause;
  }
}

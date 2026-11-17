package com.linkedin.openhouse.common.exception;

/** A create or restore would exceed the database's configured live-table limit. */
public class TableCountLimitExceededException extends UnprocessableEntityException {

  public TableCountLimitExceededException(String message) {
    super(message);
  }
}

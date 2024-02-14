package com.linkedin.openhouse.common.exception;

import java.util.NoSuchElementException;
import lombok.Getter;

/**
 * Exception indicating User Table not Found. TODO: This exception could be generalized to be
 * NoSuchEntityException, since the actual type of entity doesn't have to be a user table at all.
 * This will simplify com.linkedin.openhouse.housetables.controller.HtsExceptionHandler
 */
@Getter
public class NoSuchUserTableException extends NoSuchElementException {
  private String databaseId;
  private String tableId;
  private final Throwable cause;
  private static final String ERROR_MSG_TEMPLATE = "User table $db.$tbl cannot be found";

  public NoSuchUserTableException(String databaseId, String tableId) {
    this(
        databaseId,
        tableId,
        ERROR_MSG_TEMPLATE.replace("$db", databaseId).replace("$tbl", tableId),
        null);
  }

  public NoSuchUserTableException(String databaseId, String tableId, Throwable cause) {
    this(
        databaseId,
        tableId,
        ERROR_MSG_TEMPLATE.replace("$db", databaseId).replace("$tbl", tableId),
        cause);
  }

  public NoSuchUserTableException(
      String databaseId, String tableId, String errorMsg, Throwable cause) {
    super(errorMsg);
    this.databaseId = databaseId;
    this.tableId = tableId;
    this.cause = cause;
  }
}

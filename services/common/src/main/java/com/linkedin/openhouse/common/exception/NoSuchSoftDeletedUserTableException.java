package com.linkedin.openhouse.common.exception;

import java.util.NoSuchElementException;
import lombok.Getter;

/**
 * Exception indicating User Table not Found. TODO: This exception could be generalized to be
 * NoSuchEntityException, since the actual type of entity doesn't have to be a user table at all.
 * This will simplify com.linkedin.openhouse.housetables.controller.HtsExceptionHandler
 */
@Getter
public class NoSuchSoftDeletedUserTableException extends NoSuchElementException {
  private final String databaseId;
  private final String tableId;
  private final Long deletedAtMs;
  private final Throwable cause;
  private static final String ERROR_MSG_TEMPLATE =
      "Soft deleted user table $db.$tbl with deletedAtMs $deletedAtMs cannot be found";

  public NoSuchSoftDeletedUserTableException(String databaseId, String tableId, Long deletedAtMs) {
    this(
        databaseId,
        tableId,
        deletedAtMs,
        ERROR_MSG_TEMPLATE
            .replace("$db", databaseId)
            .replace("$tbl", tableId)
            .replace("$deletedAtMs", String.valueOf(deletedAtMs)),
        null);
  }

  public NoSuchSoftDeletedUserTableException(
      String databaseId, String tableId, Long deletedAtMs, Throwable cause) {
    this(
        databaseId,
        tableId,
        deletedAtMs,
        ERROR_MSG_TEMPLATE
            .replace("$db", databaseId)
            .replace("$tbl", tableId)
            .replace("$deletedAtMs", String.valueOf(deletedAtMs)),
        cause);
  }

  public NoSuchSoftDeletedUserTableException(
      String databaseId, String tableId, Long deletedAtMs, String errorMsg, Throwable cause) {
    super(errorMsg);
    this.databaseId = databaseId;
    this.tableId = tableId;
    this.deletedAtMs = deletedAtMs;
    this.cause = cause;
  }
}

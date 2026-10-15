package com.linkedin.openhouse.internal.catalog.exception;

/**
 * Thrown when a commit violates a table property rule that {@code OpenHouseInternalTableOperations}
 * enforces. The commit has written nothing; the caller decides how to report it.
 */
public class TablePropertyRuleException extends RuntimeException {

  public TablePropertyRuleException(String message) {
    super(message);
  }
}

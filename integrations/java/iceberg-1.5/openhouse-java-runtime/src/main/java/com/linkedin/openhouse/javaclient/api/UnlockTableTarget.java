package com.linkedin.openhouse.javaclient.api;

import com.linkedin.openhouse.javaclient.exception.UnlockValidationException;

/** Immutable database/table components validated before any UNLOCK catalog operation. */
public final class UnlockTableTarget {
  private final String databaseId;
  private final String tableId;

  private UnlockTableTarget(String databaseId, String tableId) {
    this.databaseId = databaseId;
    this.tableId = tableId;
  }

  /**
   * Build a target from resolved namespace components without changing quoted identifier contents.
   *
   * @throws UnlockValidationException if the namespace is not one level or a component is unsafe
   */
  public static UnlockTableTarget of(String[] namespace, String tableId)
      throws UnlockValidationException {
    if (namespace == null || namespace.length != 1) {
      throw new UnlockValidationException("UNLOCK requires exactly one database namespace");
    }
    String databaseId = namespace[0];
    validatePathSegment("Database name", databaseId);
    validatePathSegment("Table name", tableId);
    return new UnlockTableTarget(databaseId, tableId);
  }

  public String getDatabaseId() {
    return databaseId;
  }

  public String getTableId() {
    return tableId;
  }

  @Override
  public String toString() {
    return databaseId + "." + tableId;
  }

  // A normalizing proxy could reroute empty, "." or ".." URL segments.
  private static void validatePathSegment(String name, String value)
      throws UnlockValidationException {
    if (value == null || value.isEmpty()) {
      throw new UnlockValidationException(name + " must not be null or empty");
    }
    if (".".equals(value) || "..".equals(value)) {
      throw new UnlockValidationException(name + " must not be '" + value + "'");
    }
  }
}

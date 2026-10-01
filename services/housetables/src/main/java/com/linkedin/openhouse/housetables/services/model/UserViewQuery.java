package com.linkedin.openhouse.housetables.services.model;

import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * The vocabulary the view query service accepts, built in the handler so no query-shaped {@code
 * UserTable} reaches the service and the inert {@code entityType} is discarded there.
 *
 * <p>Only the three reachable states are constructible: there is no factory for a table pattern
 * without a database, which the validator already rejects.
 */
@EqualsAndHashCode
@ToString
public final class UserViewQuery {

  private final String databaseId;

  private final String tableIdPattern;

  private UserViewQuery(String databaseId, String tableIdPattern) {
    this.databaseId = databaseId;
    this.tableIdPattern = tableIdPattern;
  }

  /** Unbounded: the empty view query is not a database-name projection. */
  public static UserViewQuery all() {
    return new UserViewQuery(null, null);
  }

  public static UserViewQuery inDatabase(String databaseId) {
    if (databaseId == null) {
      throw new IllegalArgumentException("databaseId is required to scope a view query");
    }
    return new UserViewQuery(databaseId, null);
  }

  /** {@code tableIdPattern} is a SQL {@code LIKE} pattern. */
  public static UserViewQuery matchingPattern(String databaseId, String tableIdPattern) {
    if (databaseId == null) {
      throw new IllegalArgumentException("tableIdPattern cannot be provided without databaseId");
    }
    if (tableIdPattern == null) {
      throw new IllegalArgumentException("tableIdPattern is required for a pattern query");
    }
    return new UserViewQuery(databaseId, tableIdPattern);
  }

  public Optional<String> getDatabaseId() {
    return Optional.ofNullable(databaseId);
  }

  public Optional<String> getTableIdPattern() {
    return Optional.ofNullable(tableIdPattern);
  }
}

package com.linkedin.openhouse.internal.catalog.repository.exception;

/**
 * House Table answered a typed route with a row of the wrong entity type: a server contract
 * violation, not a miss and not a transport failure.
 *
 * <p>Deliberately not an {@link IllegalStateException}. This repository is a Spring
 * {@code @Repository}, so in a JPA context a raw {@code IllegalStateException} is rewritten by the
 * persistence exception translator into {@code InvalidDataAccessApiUsageException}, which would
 * disguise a corruption report as a data-access misuse. It is also outside the read retry policy,
 * so a contract violation can never be mistaken for something worth attempting again.
 */
public class HouseTableEntityTypeCorruptException extends HouseTableRepositoryException {

  private final String databaseId;

  private final String tableId;

  /** The value as received, so the report names what was actually wrong. Null when absent. */
  private final String entityType;

  public HouseTableEntityTypeCorruptException(
      String databaseId, String tableId, String entityType, String expectedEntityType) {
    super(
        String.format(
            "House Table answered the %s route for %s.%s with a row whose entity type is %s",
            expectedEntityType,
            databaseId,
            tableId,
            entityType == null ? "missing" : "'" + entityType + "'"),
        null);
    this.databaseId = databaseId;
    this.tableId = tableId;
    this.entityType = entityType;
  }

  public String getDatabaseId() {
    return databaseId;
  }

  public String getTableId() {
    return tableId;
  }

  public String getEntityType() {
    return entityType;
  }
}

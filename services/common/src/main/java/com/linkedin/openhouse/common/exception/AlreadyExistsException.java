package com.linkedin.openhouse.common.exception;

/**
 * Exception indicating an entity already exists.
 *
 * <p>{@link #entityType} identifies the type for ex: "Table" or "Database" {@link #entityUrn}
 * identifier of the entity for ex: "db1.tb1"
 */
public class AlreadyExistsException extends RuntimeException {
  private String entityType;
  private String entityUrn;
  private final Throwable cause;
  private static final String ERROR_MSG_TEMPLATE = "$ent $id already exists";

  public AlreadyExistsException(String entityType, String entityUrn) {
    this(
        entityType,
        entityUrn,
        ERROR_MSG_TEMPLATE.replace("$ent", entityType).replace("$id", entityUrn),
        null);
  }

  public AlreadyExistsException(String entityType, String entityUrn, Throwable cause) {
    this(
        entityType,
        entityUrn,
        ERROR_MSG_TEMPLATE.replace("$ent", entityType).replace("$id", entityUrn),
        cause);
  }

  public AlreadyExistsException(
      String entityType, String entityUrn, String errorMsg, Throwable cause) {
    super(errorMsg);
    this.entityType = entityType;
    this.entityUrn = entityUrn;
    this.cause = cause;
  }
}

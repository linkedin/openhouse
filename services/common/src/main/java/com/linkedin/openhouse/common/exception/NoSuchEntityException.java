package com.linkedin.openhouse.common.exception;

import java.util.NoSuchElementException;

/**
 * Exception indicating an entity does not exists.
 *
 * <p>{@link #entityType} identifies the type for ex: "Table" or "Job" {@link #entityUrn} identifier
 * of the entity for ex: "db1.tb1"
 */
public class NoSuchEntityException extends NoSuchElementException {

  private String entityType;
  private String entityUrn;
  private final Throwable cause;
  private static final String ERROR_MSG_TEMPLATE = "$ent $id cannot be found";

  public NoSuchEntityException(String entityType, String entityUrn) {
    this(
        entityType,
        entityUrn,
        ERROR_MSG_TEMPLATE.replace("$ent", entityType).replace("$id", entityUrn),
        null);
  }

  public NoSuchEntityException(String entityType, String entityUrn, Throwable cause) {
    this(
        entityType,
        entityUrn,
        ERROR_MSG_TEMPLATE.replace("$ent", entityType).replace("$id", entityUrn),
        cause);
  }

  public NoSuchEntityException(
      String entityType, String entityUrn, String errorMsg, Throwable cause) {
    super(errorMsg);
    this.entityType = entityType;
    this.entityUrn = entityUrn;
    this.cause = cause;
  }
}

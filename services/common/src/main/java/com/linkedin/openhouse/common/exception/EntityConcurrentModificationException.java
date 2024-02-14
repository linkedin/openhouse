package com.linkedin.openhouse.common.exception;

import lombok.Getter;

/** An exception thrown to surface concurrent modification for the user table. */
@Getter
public class EntityConcurrentModificationException extends RuntimeException {
  private String entityId;

  private Throwable cause;

  public EntityConcurrentModificationException(String entityId, Throwable cause) {
    this(entityId, "", cause);
  }

  public EntityConcurrentModificationException(String entityId, String message, Throwable cause) {
    super(message);
    this.entityId = entityId;
    this.cause = cause;
  }
}

package com.linkedin.openhouse.common.exception.checked;

public class VersionConflictException extends NamespaceException {
  public VersionConflictException(String message) {
    super(message);
  }

  public VersionConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}

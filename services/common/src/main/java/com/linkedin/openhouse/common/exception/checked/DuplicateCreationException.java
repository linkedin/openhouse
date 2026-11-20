package com.linkedin.openhouse.common.exception.checked;

public class DuplicateCreationException extends Exception {
  public DuplicateCreationException(String message) {
    super(message);
  }

  public DuplicateCreationException(String message, Throwable cause) {
    super(message, cause);
  }
}

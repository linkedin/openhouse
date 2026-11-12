package com.linkedin.openhouse.tables.exception;

public abstract class NamespaceException extends Exception {
  protected NamespaceException(String message) {
    super(message);
  }

  protected NamespaceException(String message, Throwable cause) {
    super(message, cause);
  }
}

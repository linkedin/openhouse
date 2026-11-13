package com.linkedin.openhouse.common.exception.checked;

/** Checked failure propagated between namespace operations in this service process. */
public abstract class NamespaceException extends Exception {
  protected NamespaceException(String message) {
    super(message);
  }

  protected NamespaceException(String message, Throwable cause) {
    super(message, cause);
  }
}

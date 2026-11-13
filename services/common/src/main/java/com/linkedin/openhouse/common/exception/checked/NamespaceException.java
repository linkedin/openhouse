package com.linkedin.openhouse.common.exception.checked;

/** Checked namespace failure. A failed write may have changed the stored metadata. */
public class NamespaceException extends Exception {
  public NamespaceException(String message) {
    super(message);
  }

  public NamespaceException(String message, Throwable cause) {
    super(message, cause);
  }
}

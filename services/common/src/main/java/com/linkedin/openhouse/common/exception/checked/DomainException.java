package com.linkedin.openhouse.common.exception.checked;

/** Checked semantic failure propagated between operations in this service process. */
public abstract class DomainException extends Exception {
  protected DomainException(String message) {
    super(message);
  }

  protected DomainException(String message, Throwable cause) {
    super(message, cause);
  }
}

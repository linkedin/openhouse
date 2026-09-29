package com.linkedin.openhouse.common.exception;

/**
 * A service OpenHouse depends on could not answer, so the request cannot be served right now. The
 * request is not at fault and retrying may succeed, so the advice maps it to 503.
 */
public class DependencyUnavailableException extends RuntimeException {

  public DependencyUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}

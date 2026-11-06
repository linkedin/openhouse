package com.linkedin.openhouse.javaclient.exception;

import java.util.Optional;

/** Checked failure of an explicit namespace metadata request or its input validation. */
public class NamespaceMetadataException extends Exception {
  private final int statusCode;
  private final Optional<String> errorType;

  public NamespaceMetadataException(int statusCode, String message) {
    super(message);
    this.statusCode = statusCode;
    this.errorType = Optional.empty();
  }

  public NamespaceMetadataException(int statusCode, String message, Throwable cause) {
    super(message, cause);
    this.statusCode = statusCode;
    this.errorType = Optional.empty();
  }

  public NamespaceMetadataException(
      int statusCode, String message, Throwable cause, Optional<String> errorType) {
    super(message, cause);
    this.statusCode = statusCode;
    this.errorType = errorType;
  }

  public Optional<String> getErrorType() {
    return errorType;
  }

  /**
   * Returns the HTTP error status, 400 for input validation, or zero for transport, decoding, and
   * response validation failures.
   */
  public int getStatusCode() {
    return statusCode;
  }
}

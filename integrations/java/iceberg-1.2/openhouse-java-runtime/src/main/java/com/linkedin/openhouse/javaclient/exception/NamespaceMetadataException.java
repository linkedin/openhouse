package com.linkedin.openhouse.javaclient.exception;

/** Checked failure of an explicit namespace metadata request or its input validation. */
public class NamespaceMetadataException extends Exception {
  private final int statusCode;

  public NamespaceMetadataException(int statusCode, String message) {
    super(message);
    this.statusCode = statusCode;
  }

  public NamespaceMetadataException(int statusCode, String message, Throwable cause) {
    super(message, cause);
    this.statusCode = statusCode;
  }

  /**
   * Returns the HTTP error status, 400 for input validation, or zero for transport, decoding, and
   * response validation failures.
   */
  public int getStatusCode() {
    return statusCode;
  }
}

package com.linkedin.openhouse.javaclient.exception;

/**
 * An exception thrown in Openhouse clients to indicate an error in a tables API response or
 * request, acting as a wrapper around WebClientException.
 */
public abstract class WebClientWithMessageException extends RuntimeException {
  public abstract String getMessage();

  public abstract int getStatusCode();
}

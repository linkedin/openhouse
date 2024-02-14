package com.linkedin.openhouse.common.exception;

/** An exception to indicate request is understood by server but can't be processed. */
public class UnprocessableEntityException extends RuntimeException {

  public UnprocessableEntityException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public UnprocessableEntityException(String message) {
    super(message);
  }
}

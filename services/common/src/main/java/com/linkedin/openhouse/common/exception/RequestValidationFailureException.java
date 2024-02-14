package com.linkedin.openhouse.common.exception;

import java.util.List;

/** A preliminary version for exception to indicate validation failure. */
public class RequestValidationFailureException extends RuntimeException {

  public RequestValidationFailureException() {}

  public RequestValidationFailureException(String message, Exception internalException) {
    super(message + ", internal exception:" + internalException);
  }

  public RequestValidationFailureException(String message) {
    super(message);
  }

  public RequestValidationFailureException(List<String> reasons) {
    super(String.join("; ", reasons));
  }
}

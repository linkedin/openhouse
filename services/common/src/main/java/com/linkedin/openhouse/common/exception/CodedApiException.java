package com.linkedin.openhouse.common.exception;

import org.springframework.http.HttpStatus;

/**
 * Base class for exceptions that already know the HTTP status they should map to.
 *
 * <p>This is deliberately a bare seam. It carries no error-code vocabulary of its own: a subclass
 * in a downstream service owns whatever taxonomy it needs and reduces that taxonomy to an {@link
 * HttpStatus} here. That keeps {@code services/common} free of any service-specific enum while
 * still letting {@link com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler}
 * map the exception to a response with a single handler.
 *
 * <p>The status is the only thing that reaches the wire. The error body shape is unchanged, so no
 * subclass taxonomy is serialized.
 */
public abstract class CodedApiException extends RuntimeException {

  protected CodedApiException(String message) {
    super(message);
  }

  protected CodedApiException(String message, Throwable cause) {
    super(message, cause);
  }

  /** @return the HTTP status this failure maps to. */
  public abstract HttpStatus getHttpStatus();
}

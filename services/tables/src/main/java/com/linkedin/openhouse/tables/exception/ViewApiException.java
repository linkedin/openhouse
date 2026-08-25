package com.linkedin.openhouse.tables.exception;

import com.linkedin.openhouse.common.exception.CodedApiException;
import org.springframework.http.HttpStatus;

/**
 * Failure of a /v2 views API operation, carrying the internal {@link ViewErrorCode} that selects
 * the response status.
 *
 * <p>The code is deliberately tables-local and never serialized: {@link #getHttpStatus()} is the
 * only thing {@code services/common} sees, which keeps the error body shape unchanged. The typed
 * {@link #getErrorCode()} getter exists so unit tests can assert the internal taxonomy directly.
 *
 * <p>Messages carried by this exception are copied verbatim into the error response body and into
 * service audit events, so callers must never interpolate SQL text, schema text or a base version
 * token into them.
 */
public class ViewApiException extends CodedApiException {

  private final ViewErrorCode errorCode;

  public ViewApiException(ViewErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public ViewApiException(ViewErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public ViewErrorCode getErrorCode() {
    return errorCode;
  }

  @Override
  public HttpStatus getHttpStatus() {
    return errorCode.getHttpStatus();
  }
}

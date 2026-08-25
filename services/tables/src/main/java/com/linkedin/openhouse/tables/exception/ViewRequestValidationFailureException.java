package com.linkedin.openhouse.tables.exception;

import java.util.List;
import org.springframework.http.HttpStatus;

/**
 * Structural validation failure of a /v2 views request. Accumulated reasons are joined with {@code
 * "; "} exactly like {@link
 * com.linkedin.openhouse.common.exception.RequestValidationFailureException} does for tables, so
 * the two APIs report multiple failures identically.
 *
 * <p>Only 400-mapped codes are accepted: a validation failure that is not a bad request would be a
 * programming error, not a client error.
 */
public final class ViewRequestValidationFailureException extends ViewApiException {

  public ViewRequestValidationFailureException(ViewErrorCode errorCode, List<String> reasons) {
    super(requireBadRequest(errorCode), String.join("; ", reasons));
  }

  public ViewRequestValidationFailureException(ViewErrorCode errorCode, String message) {
    super(requireBadRequest(errorCode), message);
  }

  private static ViewErrorCode requireBadRequest(ViewErrorCode errorCode) {
    if (errorCode == null || errorCode.getHttpStatus() != HttpStatus.BAD_REQUEST) {
      throw new IllegalArgumentException(
          "ViewRequestValidationFailureException requires a BAD_REQUEST view error code");
    }
    return errorCode;
  }
}

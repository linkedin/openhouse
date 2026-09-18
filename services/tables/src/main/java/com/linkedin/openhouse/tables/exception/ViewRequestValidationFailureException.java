package com.linkedin.openhouse.tables.exception;

import java.util.List;
import java.util.Objects;

/**
 * Structural validation failure of a /v1 views request. Accumulated reasons are joined with {@code
 * "; "} exactly like {@link
 * com.linkedin.openhouse.common.exception.RequestValidationFailureException} does for tables, so
 * the two APIs report multiple failures identically.
 *
 * <p>Only 400-mapped codes are accepted, and that is expressed in the type: the constructors take a
 * {@link ViewValidationErrorCode}, which can only name one of the three {@code BAD_REQUEST} codes.
 * A validation failure that is not a bad request is therefore not expressible, rather than
 * representable and rejected at runtime. {@link #getErrorCode()} still reports the corresponding
 * {@link ViewErrorCode}, so status selection is unchanged.
 */
public final class ViewRequestValidationFailureException extends ViewApiException {

  private static final String ERROR_CODE_REQUIRED =
      "ViewRequestValidationFailureException requires a non-null ViewValidationErrorCode";

  public ViewRequestValidationFailureException(
      ViewValidationErrorCode errorCode, List<String> reasons) {
    super(viewErrorCode(errorCode), String.join("; ", reasons));
  }

  public ViewRequestValidationFailureException(ViewValidationErrorCode errorCode, String message) {
    super(viewErrorCode(errorCode), message);
  }

  private static ViewErrorCode viewErrorCode(ViewValidationErrorCode errorCode) {
    return Objects.requireNonNull(errorCode, ERROR_CODE_REQUIRED).getViewErrorCode();
  }
}

package com.linkedin.openhouse.tables.exception;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.tables.controller.ViewsController;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * Narrow, highest-precedence exception advice scoped to {@link ViewsController} (plan &sect;7,
 * R5/F1). View failures never reach the global {@code OpenHouseExceptionHandler}: this advice
 * renders only status and a safe message, never a cause or stacktrace, so an engine/storage
 * exception embedding a metadata location, SQL, or schema never reaches the wire, audit, or
 * application logs. The Java cause chain is retained in-process on the thrown exception for
 * classification, but is never rendered here.
 *
 * <p>Declares only explicit, most-specific handlers &mdash; no {@code Throwable}/{@code Error}
 * handler, so a fatal {@link Error} always propagates and is never caught or swallowed.
 */
@RestControllerAdvice(assignableTypes = ViewsController.class)
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ViewExceptionHandler {

  private static final String ACCESS_DENIED_MESSAGE_FALLBACK = "Access denied";

  private static final String AUTHORIZATION_SERVICE_UNAVAILABLE_MESSAGE =
      "Authorization service unavailable";

  private static final String MALFORMED_REQUEST_BODY_MESSAGE = "Malformed request body";

  private static final String SIZE_MUST_BE_AN_INTEGER_MESSAGE = "size : must be an integer";

  private static final String UNEXPECTED_ERROR_MESSAGE =
      "An unexpected error occurred processing the view request";

  @ExceptionHandler(ViewApiException.class)
  public ResponseEntity<ErrorResponseBody> handleViewApiException(ViewApiException exception) {
    return respond(exception.getHttpStatus(), exception.getMessage());
  }

  @ExceptionHandler(AccessDeniedException.class)
  public ResponseEntity<ErrorResponseBody> handleAccessDenied(AccessDeniedException exception) {
    String message = exception.getMessage();
    return respond(
        HttpStatus.FORBIDDEN, message != null ? message : ACCESS_DENIED_MESSAGE_FALLBACK);
  }

  @ExceptionHandler(AuthorizationServiceException.class)
  public ResponseEntity<ErrorResponseBody> handleAuthorizationServiceUnavailable(
      AuthorizationServiceException exception) {
    return respond(HttpStatus.SERVICE_UNAVAILABLE, AUTHORIZATION_SERVICE_UNAVAILABLE_MESSAGE);
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<ErrorResponseBody> handleMalformedRequestBody(
      HttpMessageNotReadableException exception) {
    return respond(HttpStatus.BAD_REQUEST, MALFORMED_REQUEST_BODY_MESSAGE);
  }

  @ExceptionHandler(MethodArgumentTypeMismatchException.class)
  public ResponseEntity<ErrorResponseBody> handleTypeMismatch(
      MethodArgumentTypeMismatchException exception) {
    return respond(HttpStatus.BAD_REQUEST, SIZE_MUST_BE_AN_INTEGER_MESSAGE);
  }

  /**
   * Residual safety net only: every engine/repository {@code RuntimeException} is translated into a
   * {@link ViewApiException} at the {@code ViewsServiceImpl} boundary, so this handler only ever
   * sees a genuine unwrapped bug.
   */
  @ExceptionHandler(RuntimeException.class)
  public ResponseEntity<ErrorResponseBody> handleUnexpectedRuntimeException(
      RuntimeException exception) {
    return respond(HttpStatus.INTERNAL_SERVER_ERROR, UNEXPECTED_ERROR_MESSAGE);
  }

  private static ResponseEntity<ErrorResponseBody> respond(HttpStatus status, String message) {
    ErrorResponseBody body =
        ErrorResponseBody.builder()
            .status(status)
            .error(status.getReasonPhrase())
            .message(message)
            .build();
    return new ResponseEntity<>(body, status);
  }
}

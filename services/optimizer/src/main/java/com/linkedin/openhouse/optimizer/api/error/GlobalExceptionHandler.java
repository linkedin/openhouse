package com.linkedin.openhouse.optimizer.api.error;

import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.server.ResponseStatusException;

/**
 * Maps framework + service exceptions to {@link ApiError} bodies with consistent HTTP status codes
 * across every optimizer endpoint.
 *
 * <p>Codes used: {@code VALIDATION_ERROR}, {@code INVALID_PARAMETER}, {@code MISSING_PARAMETER},
 * {@code MALFORMED_REQUEST}, {@code OPERATION_NOT_FOUND}, {@code STATS_NOT_FOUND}, {@code
 * INTERNAL_ERROR}. Endpoint-specific 404 codes are passed through via {@link
 * ResponseStatusException}'s {@code reason} field.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<ApiError> handleValidation(
      MethodArgumentNotValidException e, HttpServletRequest req) {
    String message =
        e.getBindingResult().getFieldErrors().stream()
            .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
            .reduce((a, b) -> a + "; " + b)
            .orElse(e.getMessage());
    return error(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", message, req);
  }

  @ExceptionHandler(MethodArgumentTypeMismatchException.class)
  public ResponseEntity<ApiError> handleTypeMismatch(
      MethodArgumentTypeMismatchException e, HttpServletRequest req) {
    String type = e.getRequiredType() == null ? "?" : e.getRequiredType().getSimpleName();
    return error(
        HttpStatus.BAD_REQUEST,
        "INVALID_PARAMETER",
        "Parameter '"
            + e.getName()
            + "' has invalid value '"
            + e.getValue()
            + "' (expected "
            + type
            + ")",
        req);
  }

  @ExceptionHandler(MissingServletRequestParameterException.class)
  public ResponseEntity<ApiError> handleMissingParam(
      MissingServletRequestParameterException e, HttpServletRequest req) {
    return error(
        HttpStatus.BAD_REQUEST,
        "MISSING_PARAMETER",
        "Required parameter '" + e.getParameterName() + "' is missing",
        req);
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<ApiError> handleMalformedBody(
      HttpMessageNotReadableException e, HttpServletRequest req) {
    return error(
        HttpStatus.BAD_REQUEST, "MALFORMED_REQUEST", "Request body is missing or malformed", req);
  }

  @ExceptionHandler(ResponseStatusException.class)
  public ResponseEntity<ApiError> handleResponseStatus(
      ResponseStatusException e, HttpServletRequest req) {
    HttpStatus status = HttpStatus.resolve(e.getStatus().value());
    if (status == null) {
      status = HttpStatus.INTERNAL_SERVER_ERROR;
    }
    String reason = e.getReason() == null ? status.getReasonPhrase() : e.getReason();
    // Convention: when callers throw ResponseStatusException, they pack a "CODE: human message"
    // into the reason. If no colon is present, the whole reason becomes the message and the code
    // defaults to the status name (e.g. NOT_FOUND).
    int sep = reason.indexOf(':');
    String code = sep > 0 ? reason.substring(0, sep).trim() : status.name();
    String message = sep > 0 ? reason.substring(sep + 1).trim() : reason;
    return error(status, code, message, req);
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ApiError> handleUncaught(Exception e, HttpServletRequest req) {
    log.warn("Unhandled exception on {}: {}", req.getRequestURI(), e.toString(), e);
    return error(
        HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "An unexpected error occurred", req);
  }

  private static ResponseEntity<ApiError> error(
      HttpStatus status, String code, String message, HttpServletRequest req) {
    return ResponseEntity.status(status)
        .body(ApiError.builder().code(code).message(message).path(req.getRequestURI()).build());
  }
}

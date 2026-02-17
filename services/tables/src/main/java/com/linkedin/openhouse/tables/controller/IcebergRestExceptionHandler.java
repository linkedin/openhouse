package com.linkedin.openhouse.tables.controller;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Scoped exception mapper for Iceberg REST endpoints. */
@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice(assignableTypes = IcebergRestCatalogController.class)
public class IcebergRestExceptionHandler {

  @ExceptionHandler(NoSuchTableException.class)
  public ResponseEntity<String> handleNoSuchTable(NoSuchTableException e) {
    return errorResponse(404, e.getMessage(), NoSuchTableException.class.getSimpleName(), e);
  }

  @ExceptionHandler(NoSuchNamespaceException.class)
  public ResponseEntity<String> handleNoSuchNamespace(NoSuchNamespaceException e) {
    return errorResponse(
        404, e.getMessage(), NoSuchNamespaceException.class.getSimpleName(), e);
  }

  @ExceptionHandler({RequestValidationFailureException.class, IllegalArgumentException.class})
  public ResponseEntity<String> handleBadRequest(Exception e) {
    return errorResponse(
        400, e.getMessage(), IllegalArgumentException.class.getSimpleName(), e);
  }

  @ExceptionHandler(AccessDeniedException.class)
  public ResponseEntity<String> handleForbidden(AccessDeniedException e) {
    return errorResponse(403, e.getMessage(), ForbiddenException.class.getSimpleName(), e);
  }

  @ExceptionHandler(UnsupportedOperationException.class)
  public ResponseEntity<String> handleNotImplemented(UnsupportedOperationException e) {
    return errorResponse(
        501, e.getMessage(), UnsupportedOperationException.class.getSimpleName(), e);
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<String> handleDefault(Exception e) {
    return errorResponse(500, e.getMessage(), e.getClass().getSimpleName(), e);
  }

  private ResponseEntity<String> errorResponse(
      int statusCode, String message, String type, Throwable throwable) {
    ErrorResponse response =
        ErrorResponse.builder()
            .responseCode(statusCode)
            .withMessage(message)
            .withType(type)
            .withStackTrace(throwable)
            .build();
    return ResponseEntity.status(statusCode)
        .contentType(MediaType.APPLICATION_JSON)
        .body(IcebergRestSerde.toJson(response));
  }
}

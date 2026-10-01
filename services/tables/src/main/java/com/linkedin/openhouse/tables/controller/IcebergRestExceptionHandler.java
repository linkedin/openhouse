package com.linkedin.openhouse.tables.controller;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Scoped exception mapper for Iceberg REST endpoints. */
@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice(assignableTypes = IcebergRestCatalogController.class)
@ConditionalOnProperty(value = "cluster.tables.iceberg-rest.enabled", havingValue = "true")
@Slf4j
public class IcebergRestExceptionHandler {

  @ExceptionHandler(NoSuchTableException.class)
  public ResponseEntity<ErrorResponse> handleNoSuchTable(NoSuchTableException e) {
    return errorResponse(404, e.getMessage(), NoSuchTableException.class.getSimpleName());
  }

  @ExceptionHandler(NoSuchNamespaceException.class)
  public ResponseEntity<ErrorResponse> handleNoSuchNamespace(NoSuchNamespaceException e) {
    return errorResponse(404, e.getMessage(), NoSuchNamespaceException.class.getSimpleName());
  }

  @ExceptionHandler({RequestValidationFailureException.class, IllegalArgumentException.class})
  public ResponseEntity<ErrorResponse> handleBadRequest(Exception e) {
    return errorResponse(400, e.getMessage(), IllegalArgumentException.class.getSimpleName());
  }

  @ExceptionHandler(AccessDeniedException.class)
  public ResponseEntity<ErrorResponse> handleForbidden(AccessDeniedException e) {
    return errorResponse(403, "Access denied", ForbiddenException.class.getSimpleName());
  }

  @ExceptionHandler(UnsupportedOperationException.class)
  public ResponseEntity<ErrorResponse> handleNotImplemented(UnsupportedOperationException e) {
    return errorResponse(501, e.getMessage(), UnsupportedOperationException.class.getSimpleName());
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ErrorResponse> handleDefault(Exception e) {
    log.error("Unhandled Iceberg REST request failure", e);
    return errorResponse(500, "Internal server error", "InternalServerError");
  }

  private ResponseEntity<ErrorResponse> errorResponse(int statusCode, String message, String type) {
    ErrorResponse response =
        ErrorResponse.builder()
            .responseCode(statusCode)
            .withMessage(message)
            .withType(type)
            .build();
    return ResponseEntity.status(statusCode).body(response);
  }
}

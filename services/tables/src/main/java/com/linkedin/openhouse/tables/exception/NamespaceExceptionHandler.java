package com.linkedin.openhouse.tables.exception;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.tables.api.spec.namespace.NamespaceErrorResponse;
import com.linkedin.openhouse.tables.api.spec.namespace.NamespaceErrorResponse.NamespaceError;
import com.linkedin.openhouse.tables.controller.IcebergNamespacesController;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(assignableTypes = IcebergNamespacesController.class)
@Order(Ordered.HIGHEST_PRECEDENCE)
public class NamespaceExceptionHandler {
  @ExceptionHandler(InvalidPropertyUpdateException.class)
  public ResponseEntity<NamespaceErrorResponse> handleInvalidProperties() {
    return respond(
        HttpStatus.UNPROCESSABLE_ENTITY, "UnprocessableEntityException", "Invalid property update");
  }

  @ExceptionHandler(NotFoundException.class)
  public ResponseEntity<NamespaceErrorResponse> handleMissing() {
    return respond(HttpStatus.NOT_FOUND, "NoSuchNamespaceException", "Namespace does not exist");
  }

  @ExceptionHandler(DuplicateCreationException.class)
  public ResponseEntity<NamespaceErrorResponse> handleDuplicate() {
    return respond(HttpStatus.CONFLICT, "AlreadyExistsException", "Namespace already exists");
  }

  @ExceptionHandler(EntityConcurrentModificationException.class)
  public ResponseEntity<NamespaceErrorResponse> handleConcurrentModification() {
    return respond(
        HttpStatus.CONFLICT, "CommitFailedException", "Namespace was concurrently modified");
  }

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<NamespaceErrorResponse> handleInvalidFields(
      MethodArgumentNotValidException exception) {
    if (exception.getBindingResult().getFieldErrors().stream()
        .anyMatch(
            error ->
                error.getField().equals("namespace")
                    || error.getField().startsWith("namespace["))) {
      return respond(
          HttpStatus.BAD_REQUEST,
          "BadRequestException",
          String.format(
              "Namespace '%s' must contain exactly one name of 1 to 128 letters, digits, or underscores",
              exception.getBindingResult().getRawFieldValue("namespace")));
    }
    return respond(HttpStatus.BAD_REQUEST, "BadRequestException", "Invalid namespace request");
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<NamespaceErrorResponse> handleInvalidRequest() {
    return respond(HttpStatus.BAD_REQUEST, "BadRequestException", "Invalid namespace request");
  }

  @ExceptionHandler(AccessDeniedException.class)
  public ResponseEntity<NamespaceErrorResponse> handleForbidden() {
    return respond(HttpStatus.FORBIDDEN, "ForbiddenException", "Access denied");
  }

  @ExceptionHandler(AuthenticationException.class)
  public ResponseEntity<NamespaceErrorResponse> handleUnauthenticated() {
    return respond(HttpStatus.UNAUTHORIZED, "NotAuthorizedException", "Authentication required");
  }

  @ExceptionHandler({AuthorizationServiceException.class, RuntimeException.class})
  public ResponseEntity<NamespaceErrorResponse> handleNamespaceFailure() {
    return respond(HttpStatus.INTERNAL_SERVER_ERROR, "RESTException", "Namespace operation failed");
  }

  private ResponseEntity<NamespaceErrorResponse> respond(
      HttpStatus status, String type, String message) {
    return ResponseEntity.status(status)
        .body(new NamespaceErrorResponse(new NamespaceError(message, type, status.value())));
  }
}

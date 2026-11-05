package com.linkedin.openhouse.tables.exception;

import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException;
import com.linkedin.openhouse.tables.api.spec.namespace.NamespaceErrorResponse;
import com.linkedin.openhouse.tables.api.spec.namespace.NamespaceErrorResponse.NamespaceError;
import com.linkedin.openhouse.tables.controller.IcebergNamespacesController;
import javax.servlet.http.HttpServletRequest;
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
  public static final String RENDERED_ERROR_ATTRIBUTE =
      NamespaceExceptionHandler.class.getName() + ".rendered";

  @ExceptionHandler(NamespaceException.class)
  public ResponseEntity<NamespaceErrorResponse> handleNamespace(
      NamespaceException exception, HttpServletRequest request) {
    return respond(exception.getStatus(), exception.getType(), exception.getMessage(), request);
  }

  @ExceptionHandler(HouseDatabaseException.class)
  public ResponseEntity<NamespaceErrorResponse> handleRepository(
      HouseDatabaseException exception, HttpServletRequest request) {
    switch (exception.getReason()) {
      case NOT_FOUND:
        return respond(
            HttpStatus.NOT_FOUND, "NoSuchNamespaceException", "Namespace does not exist", request);
      case CONFLICT:
        return respond(
            HttpStatus.CONFLICT,
            request.getRequestURI().endsWith("/namespaces")
                ? "AlreadyExistsException"
                : "CommitFailedException",
            "Namespace already exists or was modified concurrently",
            request);
      case BAD_REQUEST:
        return respond(
            HttpStatus.BAD_REQUEST, "BadRequestException", "Invalid namespace request", request);
      case INVALID_PROPERTIES:
        return respond(
            HttpStatus.UNPROCESSABLE_ENTITY,
            "UnprocessableEntityException",
            "Invalid property update",
            request);
      case UNAVAILABLE:
        return respond(
            HttpStatus.SERVICE_UNAVAILABLE,
            "ServiceUnavailableException",
            "Database metadata service unavailable",
            request);
      default:
        return respond(
            HttpStatus.INTERNAL_SERVER_ERROR,
            "ServiceFailureException",
            "Database metadata operation failed",
            request);
    }
  }

  @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentNotValidException.class})
  public ResponseEntity<NamespaceErrorResponse> handleInvalidRequest(HttpServletRequest request) {
    return respond(
        HttpStatus.BAD_REQUEST, "BadRequestException", "Invalid namespace request", request);
  }

  @ExceptionHandler(AuthorizationServiceException.class)
  public ResponseEntity<NamespaceErrorResponse> handleAuthorizationService(
      HttpServletRequest request) {
    return respond(
        HttpStatus.SERVICE_UNAVAILABLE,
        "ServiceUnavailableException",
        "Authorization service unavailable",
        request);
  }

  @ExceptionHandler(AccessDeniedException.class)
  public ResponseEntity<NamespaceErrorResponse> handleForbidden(HttpServletRequest request) {
    return respond(HttpStatus.FORBIDDEN, "ForbiddenException", "Access denied", request);
  }

  @ExceptionHandler(AuthenticationException.class)
  public ResponseEntity<NamespaceErrorResponse> handleUnauthenticated(HttpServletRequest request) {
    return respond(
        HttpStatus.UNAUTHORIZED, "NotAuthorizedException", "Authentication required", request);
  }

  @ExceptionHandler(RuntimeException.class)
  public ResponseEntity<NamespaceErrorResponse> handleUnexpectedFailure(
      HttpServletRequest request) {
    return respond(
        HttpStatus.INTERNAL_SERVER_ERROR,
        "ServiceFailureException",
        "Namespace operation failed",
        request);
  }

  private ResponseEntity<NamespaceErrorResponse> respond(
      HttpStatus status, String type, String message, HttpServletRequest request) {
    request.setAttribute(RENDERED_ERROR_ATTRIBUTE, true);
    return ResponseEntity.status(status)
        .body(new NamespaceErrorResponse(new NamespaceError(message, type, status.value())));
  }
}

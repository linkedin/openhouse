package com.linkedin.openhouse.tables.exception;

import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.InvalidPropertyUpdateException;
import com.linkedin.openhouse.common.exception.checked.InvalidRequestException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
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

  @ExceptionHandler(InvalidRequestException.class)
  public ResponseEntity<NamespaceErrorResponse> handleInvalidNamespace(
      InvalidRequestException exception, HttpServletRequest request) {
    return respond(HttpStatus.BAD_REQUEST, "BadRequestException", exception.getMessage(), request);
  }

  @ExceptionHandler(InvalidPropertyUpdateException.class)
  public ResponseEntity<NamespaceErrorResponse> handleInvalidProperties(
      HttpServletRequest request) {
    return respond(
        HttpStatus.UNPROCESSABLE_ENTITY,
        "UnprocessableEntityException",
        "Invalid property update",
        request);
  }

  @ExceptionHandler(NotEmptyException.class)
  public ResponseEntity<NamespaceErrorResponse> handleNotEmpty(HttpServletRequest request) {
    return respond(
        HttpStatus.BAD_REQUEST,
        "NamespaceNotEmptyException",
        "Namespace contains tables or views",
        request);
  }

  @ExceptionHandler(NotFoundException.class)
  public ResponseEntity<NamespaceErrorResponse> handleMissing(HttpServletRequest request) {
    return respond(
        HttpStatus.NOT_FOUND, "NoSuchNamespaceException", "Namespace does not exist", request);
  }

  @ExceptionHandler(DuplicateCreationException.class)
  public ResponseEntity<NamespaceErrorResponse> handleDuplicate(HttpServletRequest request) {
    return respond(
        HttpStatus.CONFLICT, "AlreadyExistsException", "Namespace already exists", request);
  }

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<NamespaceErrorResponse> handleInvalidFields(
      MethodArgumentNotValidException exception, HttpServletRequest request) {
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
              exception.getBindingResult().getRawFieldValue("namespace")),
          request);
    }
    return respond(
        HttpStatus.BAD_REQUEST, "BadRequestException", "Invalid namespace request", request);
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<NamespaceErrorResponse> handleInvalidRequest(HttpServletRequest request) {
    return respond(
        HttpStatus.BAD_REQUEST, "BadRequestException", "Invalid namespace request", request);
  }

  @ExceptionHandler({AccessDeniedException.class, PermissionDeniedException.class})
  public ResponseEntity<NamespaceErrorResponse> handleForbidden(HttpServletRequest request) {
    return respond(HttpStatus.FORBIDDEN, "ForbiddenException", "Access denied", request);
  }

  @ExceptionHandler(AuthenticationException.class)
  public ResponseEntity<NamespaceErrorResponse> handleUnauthenticated(HttpServletRequest request) {
    return respond(
        HttpStatus.UNAUTHORIZED, "NotAuthorizedException", "Authentication required", request);
  }

  @ExceptionHandler({AuthorizationServiceException.class, RuntimeException.class})
  public ResponseEntity<NamespaceErrorResponse> handleNamespaceFailure(HttpServletRequest request) {
    return respond(
        HttpStatus.INTERNAL_SERVER_ERROR, "RESTException", "Namespace operation failed", request);
  }

  private ResponseEntity<NamespaceErrorResponse> respond(
      HttpStatus status, String type, String message, HttpServletRequest request) {
    request.setAttribute(RENDERED_ERROR_ATTRIBUTE, true);
    return ResponseEntity.status(status)
        .body(new NamespaceErrorResponse(new NamespaceError(message, type, status.value())));
  }
}

package com.linkedin.openhouse.tables.rest.controller;

import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import com.linkedin.openhouse.common.exception.InvalidTableMetadataException;
import com.linkedin.openhouse.common.exception.NoSuchEntityException;
import com.linkedin.openhouse.common.exception.NoSuchSoftDeletedUserTableException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

/**
 * Spring REST controller advice scoped to {@code com.linkedin.openhouse.tables.rest}: translates
 * OpenHouse internal exceptions and Iceberg exceptions to Iceberg's wire-format {@link
 * ErrorResponse} JSON ({@code { "error": { "message": "...", "type": "...", "code": N } } }).
 *
 * <p>The {@code basePackages} attribute is critical — it scopes this advice ONLY to the new REST
 * adapter controllers so we don't clobber the existing OpenHouse exception handler for the native
 * {@code /v1/databases/...} surface.
 */
@RestControllerAdvice(basePackages = "com.linkedin.openhouse.tables.rest")
@Slf4j
public class IcebergRestExceptionHandler {

  // --------------------------------------------------------------------------------------------
  // OpenHouse common exceptions
  // --------------------------------------------------------------------------------------------

  @ExceptionHandler(NoSuchEntityException.class)
  public ResponseEntity<String> handleNoSuchEntity(NoSuchEntityException ex, WebRequest req) {
    String type = isTablePath(req) ? "NoSuchTableException" : "NoSuchNamespaceException";
    return render(HttpStatus.NOT_FOUND, type, safeMessage(ex));
  }

  @ExceptionHandler(NoSuchUserTableException.class)
  public ResponseEntity<String> handleNoSuchUserTable(NoSuchUserTableException ex) {
    return render(HttpStatus.NOT_FOUND, "NoSuchTableException", safeMessage(ex));
  }

  @ExceptionHandler(NoSuchSoftDeletedUserTableException.class)
  public ResponseEntity<String> handleNoSuchSoftDeleted(NoSuchSoftDeletedUserTableException ex) {
    return render(HttpStatus.NOT_FOUND, "NoSuchTableException", safeMessage(ex));
  }

  /**
   * Catch-all for {@link java.util.NoSuchElementException} (parent of OpenHouse's
   * NoSuchUserTableException). Some OpenHouse code paths throw the raw parent class.
   */
  @ExceptionHandler(java.util.NoSuchElementException.class)
  public ResponseEntity<String> handleNoSuchElement(
      java.util.NoSuchElementException ex, WebRequest req) {
    String type = isTablePath(req) ? "NoSuchTableException" : "NoSuchNamespaceException";
    return render(HttpStatus.NOT_FOUND, type, safeMessage(ex));
  }

  @ExceptionHandler(AlreadyExistsException.class)
  public ResponseEntity<String> handleAlreadyExists(AlreadyExistsException ex) {
    return render(HttpStatus.CONFLICT, "AlreadyExistsException", safeMessage(ex));
  }

  @ExceptionHandler(EntityConcurrentModificationException.class)
  public ResponseEntity<String> handleConcurrentModification(
      EntityConcurrentModificationException ex) {
    return render(HttpStatus.CONFLICT, "CommitFailedException", safeMessage(ex));
  }

  @ExceptionHandler(RequestValidationFailureException.class)
  public ResponseEntity<String> handleRequestValidation(RequestValidationFailureException ex) {
    return render(HttpStatus.BAD_REQUEST, "BadRequestException", safeMessage(ex));
  }

  @ExceptionHandler(InvalidSchemaEvolutionException.class)
  public ResponseEntity<String> handleSchemaEvolution(InvalidSchemaEvolutionException ex) {
    return render(HttpStatus.BAD_REQUEST, "BadRequestException", safeMessage(ex));
  }

  @ExceptionHandler(InvalidTableMetadataException.class)
  public ResponseEntity<String> handleInvalidTableMetadata(InvalidTableMetadataException ex) {
    return render(HttpStatus.BAD_REQUEST, "BadRequestException", safeMessage(ex));
  }

  @ExceptionHandler(UnsupportedClientOperationException.class)
  public ResponseEntity<String> handleUnsupportedClient(UnsupportedClientOperationException ex) {
    return render(HttpStatus.BAD_REQUEST, "BadRequestException", safeMessage(ex));
  }

  // --------------------------------------------------------------------------------------------
  // Iceberg-native exceptions (in case a handler unwraps and rethrows raw)
  // --------------------------------------------------------------------------------------------

  @ExceptionHandler(CommitFailedException.class)
  public ResponseEntity<String> handleCommitFailed(CommitFailedException ex) {
    return render(HttpStatus.CONFLICT, "CommitFailedException", safeMessage(ex));
  }

  @ExceptionHandler(org.apache.iceberg.exceptions.NoSuchTableException.class)
  public ResponseEntity<String> handleIcebergNoSuchTable(
      org.apache.iceberg.exceptions.NoSuchTableException ex) {
    return render(HttpStatus.NOT_FOUND, "NoSuchTableException", safeMessage(ex));
  }

  @ExceptionHandler(org.apache.iceberg.exceptions.NoSuchNamespaceException.class)
  public ResponseEntity<String> handleIcebergNoSuchNamespace(
      org.apache.iceberg.exceptions.NoSuchNamespaceException ex) {
    return render(HttpStatus.NOT_FOUND, "NoSuchNamespaceException", safeMessage(ex));
  }

  @ExceptionHandler(org.apache.iceberg.exceptions.AlreadyExistsException.class)
  public ResponseEntity<String> handleIcebergAlreadyExists(
      org.apache.iceberg.exceptions.AlreadyExistsException ex) {
    return render(HttpStatus.CONFLICT, "AlreadyExistsException", safeMessage(ex));
  }

  // --------------------------------------------------------------------------------------------
  // Generic fallbacks
  // --------------------------------------------------------------------------------------------

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<String> handleIllegalArgument(IllegalArgumentException ex) {
    return render(HttpStatus.BAD_REQUEST, "BadRequestException", safeMessage(ex));
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<String> handleAny(Exception ex) {
    log.warn("Unhandled exception in Iceberg REST adapter", ex);
    return render(HttpStatus.INTERNAL_SERVER_ERROR, "InternalServerError", safeMessage(ex));
  }

  // --------------------------------------------------------------------------------------------
  // helpers
  // --------------------------------------------------------------------------------------------

  private static ResponseEntity<String> render(HttpStatus status, String type, String message) {
    ErrorResponse err =
        ErrorResponse.builder()
            .withMessage(message == null ? "" : message)
            .withType(type)
            .responseCode(status.value())
            .build();
    String json = ErrorResponseParser.toJson(err);
    return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(json);
  }

  private static String safeMessage(Throwable t) {
    return t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
  }

  private static boolean isTablePath(WebRequest req) {
    if (req == null) {
      return false;
    }
    String desc = req.getDescription(false);
    return desc != null && desc.contains("/tables/");
  }
}

package com.linkedin.openhouse.tables.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * Internal taxonomy of view failure modes. This enum is never serialized to the wire: it exists
 * only to select the HTTP status of the response, and the error body shape stays unchanged.
 *
 * <p>Admission and dependency-analysis codes are reserved for those capabilities.
 */
@AllArgsConstructor
@Getter
public enum ViewErrorCode {
  NO_SUCH_VIEW(HttpStatus.NOT_FOUND),
  VIEW_ALREADY_EXISTS(HttpStatus.CONFLICT),
  NAME_ALREADY_EXISTS_AS_TABLE(HttpStatus.CONFLICT),
  CONCURRENT_VIEW_MODIFICATION(HttpStatus.CONFLICT),
  DATABASE_NOT_FOUND(HttpStatus.NOT_FOUND),
  VIEWS_DISABLED(HttpStatus.NOT_FOUND),
  INVALID_VIEW_DEFINITION(HttpStatus.BAD_REQUEST),
  UNSUPPORTED_VIEW_DIALECT(HttpStatus.BAD_REQUEST),
  UNSUPPORTED_VIEW_SCHEMA(HttpStatus.BAD_REQUEST),
  VIEW_ADMISSION_FAILED(HttpStatus.UNPROCESSABLE_ENTITY),
  REQUIRED_REPRESENTATION_MISSING(HttpStatus.UNPROCESSABLE_ENTITY),
  DEPENDENCY_CYCLE(HttpStatus.UNPROCESSABLE_ENTITY),
  MAX_VIEW_DEPTH_EXCEEDED(HttpStatus.UNPROCESSABLE_ENTITY),
  VIEW_SERVICE_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE),

  /**
   * Unexpected or corrupt server-side failure: trusted-input validation failures from the engine,
   * corrupt persisted metadata, and caller-translated HTS 4xx on a trusted server call. Never a
   * caller-input error — the API validator owns every caller-input 400.
   */
  INTERNAL_VIEW_ERROR(HttpStatus.INTERNAL_SERVER_ERROR),

  /**
   * A write's publication outcome is unacknowledged: it may have succeeded or failed. Distinct from
   * {@link #VIEW_SERVICE_UNAVAILABLE}, which is a transient read-side dependency failure. No blind
   * retry or cleanup of a possibly-committed write is performed.
   */
  COMMIT_STATE_UNKNOWN(HttpStatus.SERVICE_UNAVAILABLE);

  private final HttpStatus httpStatus;
}

package com.linkedin.openhouse.tables.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * Internal taxonomy of view failure modes. This enum is never serialized to the wire: it exists
 * only to select the HTTP status of the response, and the error body shape stays unchanged.
 *
 * <p>The full set is declared up front, including codes M1 never emits, so later milestones (view
 * admission, dependency analysis) add behavior without a breaking change to this enum.
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
  ADMISSION_SERVICE_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE);

  private final HttpStatus httpStatus;
}

package com.linkedin.openhouse.tables.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The subset of {@link ViewErrorCode} a structural validation failure is allowed to carry: exactly
 * the three codes that map to {@link org.springframework.http.HttpStatus#BAD_REQUEST}.
 *
 * <p>This exists so {@link ViewRequestValidationFailureException} can take a type that cannot hold
 * a non-400 code, rather than accepting the full taxonomy and rejecting the illegal ones at
 * runtime. A validation failure carrying, say, {@code NO_SUCH_VIEW} is a programming error, and the
 * compiler is a better place to catch it than a constructor guard.
 *
 * <p>{@link ViewErrorCode} keeps all of its values and its status mapping: this enum narrows what a
 * validator may throw, it does not narrow the taxonomy itself. Every constant here must name a
 * {@code ViewErrorCode} whose status is {@code BAD_REQUEST}, which {@code ViewApiExceptionTest}
 * asserts.
 */
@AllArgsConstructor
@Getter
public enum ViewValidationErrorCode {
  INVALID_VIEW_DEFINITION(ViewErrorCode.INVALID_VIEW_DEFINITION),
  UNSUPPORTED_VIEW_DIALECT(ViewErrorCode.UNSUPPORTED_VIEW_DIALECT),
  UNSUPPORTED_VIEW_SCHEMA(ViewErrorCode.UNSUPPORTED_VIEW_SCHEMA);

  private final ViewErrorCode viewErrorCode;
}

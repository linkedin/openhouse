package com.linkedin.openhouse.tables.config;

import java.util.Optional;

/**
 * Per-request options and client identity that the service's checks honor, such as the pre-commit
 * checks and the column-default client gate. Each option defaults to off, and each identity to
 * absent.
 */
public interface CatalogRequestContext {

  /** Whether the request skips column-default policy checks. */
  default boolean columnDefaultPolicyBypass() {
    return false;
  }

  /** The client name the request declares ({@code X-Client-Name}), if any. */
  default Optional<String> clientName() {
    return Optional.empty();
  }

  /** The request's {@code User-Agent}, if any. */
  default Optional<String> userAgent() {
    return Optional.empty();
  }
}

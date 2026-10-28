package com.linkedin.openhouse.tables.config;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Per-request options and client identity that the service's checks honor, such as the pre-commit
 * checks and the client capability gate. Each option defaults to off, and each identity to absent.
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

  /**
   * The capabilities the request's client declares ({@code X-OpenHouse-Client-Capabilities}), in
   * lower case. Empty if it declares none.
   */
  default Set<String> clientCapabilities() {
    return Collections.emptySet();
  }
}

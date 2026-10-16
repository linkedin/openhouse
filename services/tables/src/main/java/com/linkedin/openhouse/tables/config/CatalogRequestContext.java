package com.linkedin.openhouse.tables.config;

import java.util.Optional;

/**
 * Per-request commit context for the service's checks, such as the commit policy: the options the
 * request sent, and what the service recorded while preparing its commit. Each option defaults to
 * off and nothing is recorded.
 */
public interface CatalogRequestContext {

  /** Whether the request skips column-default policy checks. */
  default boolean columnDefaultPolicyBypass() {
    return false;
  }

  /**
   * The column-default handshake recorded for the request's commit: a JSON object of field ids to
   * the {@code initial-default} values the request sent. Empty when none was recorded, so the
   * commit is not subject to the column-default rules.
   */
  default Optional<String> columnDefaultHandshake() {
    return Optional.empty();
  }

  /** Records {@code handshake} as {@link #columnDefaultHandshake} for the rest of the request. */
  default void recordColumnDefaultHandshake(String handshake) {}
}

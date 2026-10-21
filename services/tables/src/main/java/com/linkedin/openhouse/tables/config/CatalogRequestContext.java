package com.linkedin.openhouse.tables.config;

/**
 * Per-request options that the service's checks honor, such as the pre-commit checks. Each option
 * defaults to off.
 */
public interface CatalogRequestContext {

  /** Whether the request skips column-default policy checks. */
  default boolean columnDefaultPolicyBypass() {
    return false;
  }
}

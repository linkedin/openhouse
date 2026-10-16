package com.linkedin.openhouse.tables.config;

/**
 * Per-request options that the service's checks honor, such as the commit policy. Each option
 * defaults to off, so {@link #NONE} applies outside a request and wherever no implementation is
 * wired.
 */
public interface CatalogRequestContext {
  CatalogRequestContext NONE = new CatalogRequestContext() {};

  /** Whether the request skips column-default policy checks. */
  default boolean columnDefaultPolicyBypass() {
    return false;
  }
}

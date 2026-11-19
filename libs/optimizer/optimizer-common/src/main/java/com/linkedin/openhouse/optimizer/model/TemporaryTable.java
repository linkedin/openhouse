package com.linkedin.openhouse.optimizer.model;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Reads a temporary table's expiration time from its table properties. A table whose {@value
 * #EXPIRATION_TIME_PROPERTY} property holds epoch milliseconds is a temporary table. The Optimizer
 * runs no maintenance on it, and the expired-table deletion analyzer schedules a drop once that
 * time passes.
 */
public final class TemporaryTable {

  public static final String EXPIRATION_TIME_PROPERTY = "openhouse.leaseExpirationTime";

  private TemporaryTable() {}

  /** The expiration time, or empty when the property is missing or is not epoch milliseconds. */
  public static Optional<Instant> expirationTime(Map<String, String> tableProperties) {
    return Optional.ofNullable(tableProperties.get(EXPIRATION_TIME_PROPERTY))
        .flatMap(TemporaryTable::parseEpochMillis);
  }

  private static Optional<Instant> parseEpochMillis(String value) {
    try {
      return Optional.of(Instant.ofEpochMilli(Long.parseLong(value)));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }
}

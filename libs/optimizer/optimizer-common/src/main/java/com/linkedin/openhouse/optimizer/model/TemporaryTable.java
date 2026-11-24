package com.linkedin.openhouse.optimizer.model;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Map;
import java.util.Optional;

/**
 * Reads the temporary table properties from a table's properties. A table is temporary when {@value
 * #TEMPORARY_PROPERTY} is exactly {@code true}. The Optimizer skips orphan file deletion for
 * temporary tables, and the expired table deletion analyzer schedules the drop once {@value
 * #EXPIRATION_TIME_PROPERTY} has passed.
 *
 * <p>The property names and the time format match the tables service's {@code
 * TemporaryTableProperties}. They are repeated here because optimizer-common does not depend on the
 * internal catalog.
 */
public final class TemporaryTable {

  public static final String TEMPORARY_PROPERTY = "openhouse.temporary";

  public static final String EXPIRATION_TIME_PROPERTY = "openhouse.expirationTime";

  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss'Z'")
          .withResolverStyle(ResolverStyle.STRICT);

  /**
   * The length of {@code YYYY-MM-DDTHH:MM:SSZ}. The formatter also parses signed and expanded
   * years, which this length excludes.
   */
  private static final int FORMATTED_LENGTH = 20;

  private TemporaryTable() {}

  /** Returns whether the temporary flag is exactly {@code true}. */
  public static boolean isTemporary(Map<String, String> tableProperties) {
    return "true".equals(tableProperties.get(TEMPORARY_PROPERTY));
  }

  /**
   * Returns the expiration time of a temporary table, or empty when the table is not temporary or
   * the time is absent or not exactly in the form {@code YYYY-MM-DDTHH:MM:SSZ}.
   */
  public static Optional<Instant> expirationTime(Map<String, String> tableProperties) {
    if (!isTemporary(tableProperties)) {
      return Optional.empty();
    }
    return Optional.ofNullable(tableProperties.get(EXPIRATION_TIME_PROPERTY))
        .flatMap(TemporaryTable::parseExpirationTime);
  }

  private static Optional<Instant> parseExpirationTime(String value) {
    if (value.length() != FORMATTED_LENGTH) {
      return Optional.empty();
    }
    try {
      return Optional.of(LocalDateTime.parse(value, FORMATTER).toInstant(ZoneOffset.UTC));
    } catch (DateTimeParseException e) {
      return Optional.empty();
    }
  }
}

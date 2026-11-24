package com.linkedin.openhouse.internal.catalog;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Map;
import java.util.Optional;

/**
 * Reads and writes the temporary table properties {@link CatalogConstants#OPENHOUSE_TEMPORARY_KEY}
 * and {@link CatalogConstants#OPENHOUSE_EXPIRATION_TIME_KEY}.
 */
public final class TemporaryTableProperties {

  /** The latest expiration time a commit may set, measured from the commit. */
  public static final Duration MAX_LIFETIME = Duration.ofDays(7);

  /** The expiration time a table gets when it becomes temporary without one, from the commit. */
  public static final Duration DEFAULT_LIFETIME = Duration.ofDays(3);

  /** The only accepted form of an expiration time: a UTC time in whole seconds. */
  public static final String EXPIRATION_TIME_FORMAT = "YYYY-MM-DDTHH:MM:SSZ";

  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss'Z'")
          .withResolverStyle(ResolverStyle.STRICT);

  /** The formatter also parses signed and expanded years, which this length excludes. */
  private static final int FORMATTED_LENGTH = EXPIRATION_TIME_FORMAT.length();

  private TemporaryTableProperties() {}

  /** Returns whether the temporary flag is exactly {@code true}. */
  public static boolean isTemporary(Map<String, String> properties) {
    return "true".equals(properties.get(CatalogConstants.OPENHOUSE_TEMPORARY_KEY));
  }

  /**
   * Parses an expiration time in exactly the form {@value #EXPIRATION_TIME_FORMAT}.
   *
   * @return the time, or empty when {@code value} is in any other form
   */
  public static Optional<Instant> parseExpirationTime(String value) {
    if (value.length() != FORMATTED_LENGTH) {
      return Optional.empty();
    }
    try {
      return Optional.of(LocalDateTime.parse(value, FORMATTER).toInstant(ZoneOffset.UTC));
    } catch (DateTimeParseException e) {
      return Optional.empty();
    }
  }

  /** Formats {@code instant} as {@value #EXPIRATION_TIME_FORMAT}, dropping fractional seconds. */
  public static String formatExpirationTime(Instant instant) {
    return FORMATTER.format(instant.atOffset(ZoneOffset.UTC));
  }

  /** Returns the parsed expiration time, or empty when it is absent or malformed. */
  public static Optional<Instant> expirationTime(Map<String, String> properties) {
    return Optional.ofNullable(properties.get(CatalogConstants.OPENHOUSE_EXPIRATION_TIME_KEY))
        .flatMap(TemporaryTableProperties::parseExpirationTime);
  }

  /** Returns whether the table is temporary and its expiration time is at or before {@code now}. */
  public static boolean isExpired(Map<String, String> properties, Instant now) {
    return isTemporary(properties)
        && expirationTime(properties).filter(time -> !now.isBefore(time)).isPresent();
  }
}

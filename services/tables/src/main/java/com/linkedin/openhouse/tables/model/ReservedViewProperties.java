package com.linkedin.openhouse.tables.model;

import com.linkedin.openhouse.internal.catalog.view.ViewSystemProperties;
import java.util.Map;

/** Create-time exceptions to the server-owned {@code openhouse.} namespace. */
public final class ReservedViewProperties {

  private ReservedViewProperties() {}

  /** Authoring dialect: supplied on writes, immutable after creation. */
  public static final String SOURCE_DIALECT = ViewSystemProperties.SOURCE_DIALECT;

  /**
   * Whether a client may set this reserved key when creating a view. Every other key in the {@code
   * openhouse.} namespace is server-owned and is rejected on the create surface.
   */
  public static boolean isWritableAtCreate(String key) {
    return SOURCE_DIALECT.equalsIgnoreCase(key);
  }

  public static String sourceDialect(Map<String, String> properties) {
    if (properties == null) {
      return null;
    }
    return properties.entrySet().stream()
        .filter(entry -> SOURCE_DIALECT.equalsIgnoreCase(entry.getKey()))
        .findFirst()
        .map(Map.Entry::getValue)
        .orElse(null);
  }
}

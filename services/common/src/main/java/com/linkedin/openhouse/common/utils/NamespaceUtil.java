package com.linkedin.openhouse.common.utils;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;

/** Utility class for validating Iceberg {@link Namespace} objects in OpenHouse. */
public final class NamespaceUtil {
  private NamespaceUtil() {}

  /**
   * Returns whether {@code namespace} matches OpenHouse's {@code database.table} identifier shape,
   * i.e. has exactly one level. Used by {@code isValidIdentifier(...)} to gate which identifiers
   * are treated as base tables (vs. metadata-table fallbacks, longer-namespace lookups, etc.).
   */
  public static boolean isSingleLevel(Namespace namespace) {
    return namespace != null && namespace.levels().length == 1;
  }

  /**
   * Enforce OpenHouse's database-only namespace shape on callers that accept a {@link Namespace}
   * argument.
   *
   * <p>OpenHouse identifies base tables with {@code database.table}, so the namespace portion is at
   * most a single level. The empty namespace is permitted because several callers use it to mean
   * "across all databases" (e.g. {@code listTables(Namespace.empty())}).
   *
   * @param namespace the namespace to validate
   * @throws ValidationException if the namespace has more than one level
   */
  public static void checkAtMostSingleLevelNamespace(Namespace namespace) {
    if (namespace.levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels " + String.join(".", namespace.levels()));
    }
  }
}

package com.linkedin.openhouse.common.utils;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;

/**
 * Helpers that encode OpenHouse's namespace contract for {@link Namespace} arguments.
 *
 * <p>OpenHouse currently identifies base tables with {@code database.table}, so a "table namespace"
 * is exactly one level (the database) and an "operation namespace" — the {@code Namespace} argument
 * to a database-scoped catalog method — is at most one level (with the empty namespace acting as a
 * sentinel for "across all databases").
 *
 * <p>The two predicates are intentionally separate concepts, not stricter/looser flavors of the
 * same rule. If OpenHouse ever changes its namespace shape (e.g. to support {@code
 * catalog.database.table}), the rule bodies update here and the call-site names continue to read
 * correctly.
 */
public final class NamespaceUtil {
  private NamespaceUtil() {}

  /**
   * Returns whether {@code namespace} can host an OpenHouse base table.
   *
   * <p>Used by {@code isValidIdentifier(...)} to gate which {@link
   * org.apache.iceberg.catalog.TableIdentifier}s are treated as base tables (vs. metadata-table
   * fallbacks, longer-namespace lookups, etc.). The empty namespace is not a table namespace
   * because there is no database under which to place the table.
   */
  public static boolean isTableNamespace(Namespace namespace) {
    return namespace != null && namespace.levels().length == 1;
  }

  /**
   * Validate that {@code namespace} is a legal argument to a database-scoped catalog operation
   * (e.g. {@code listTables}, {@code searchSoftDeletedTables}).
   *
   * <p>The empty namespace is permitted because callers use it as a sentinel for "across all
   * databases" (e.g. {@code listTables(Namespace.empty())}).
   *
   * @throws ValidationException if {@code namespace} cannot be used as an operation argument
   */
  public static void validateOperationNamespace(Namespace namespace) {
    if (namespace.levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels " + String.join(".", namespace.levels()));
    }
  }
}

package com.linkedin.openhouse.housetables.model;

import java.util.Locale;

/**
 * Type of the catalog object occupying a (databaseId, tableId) key. A null column value is a legacy
 * row and means {@link #TABLE}; {@link EntityTypeConverter} is where that resolution happens.
 *
 * <p>Constant names are the exact text stored in the {@code entity_type} column and exchanged on
 * the wire, so renaming one rewrites persisted data.
 */
public enum EntityType {
  TABLE,
  VIEW;

  /**
   * Resolves any accepted spelling, case-insensitively. Null is rejected rather than defaulted,
   * because the two callers mean incompatible things by it: from the column it means "written
   * before the discriminator existed" (a table), from the wire it means "the caller did not say"
   * (unknown). Only {@link EntityTypeConverter} knows it is reading a column, so only it resolves
   * the former.
   */
  public static EntityType fromName(String name) {
    if (name == null) {
      throw new IllegalArgumentException("entityType cannot be null");
    }
    return valueOf(name.toUpperCase(Locale.ROOT));
  }
}

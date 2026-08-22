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
   * Matches a constant name case-insensitively; anything else is corrupt, including an accented or
   * padded spelling. Null is rejected rather than defaulted, because it means "legacy row, hence a
   * table" from the column but "unstated" from the wire, and only {@link EntityTypeConverter} knows
   * which one it is reading.
   */
  public static EntityType fromName(String name) {
    if (name == null) {
      throw new IllegalArgumentException("entityType cannot be null");
    }
    return valueOf(name.toUpperCase(Locale.ROOT));
  }
}

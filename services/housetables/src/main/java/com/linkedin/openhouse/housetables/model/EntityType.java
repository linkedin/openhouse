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
   * Resolves any accepted spelling, case-insensitively; accents are not stripped, since {@code
   * 'TÁBLE'} is likelier corruption than intent. Null is rejected rather than defaulted, because it
   * means "legacy row, hence a table" from the column but "unstated" from the wire, and only {@link
   * EntityTypeConverter} knows which one it is reading.
   */
  public static EntityType fromName(String name) {
    if (name == null) {
      throw new IllegalArgumentException("entityType cannot be null");
    }
    // Only trailing U+0020 is insignificant, matching MySQL's PAD SPACE collations. Leading or
    // other whitespace stays corrupt: SQL treats it as significant, so such a row matches nothing.
    int end = name.length();
    while (end > 0 && name.charAt(end - 1) == ' ') {
      end--;
    }
    return valueOf(name.substring(0, end).toUpperCase(Locale.ROOT));
  }
}

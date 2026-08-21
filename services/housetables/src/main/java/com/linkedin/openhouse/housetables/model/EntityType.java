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
   * Resolves any accepted spelling, case-insensitively. Accents are deliberately not stripped:
   * {@code 'TÁBLE'} is likelier corruption than intent, and guessing at it would undercut failing
   * closed.
   *
   * <p>Null is rejected rather than defaulted, because the two callers mean incompatible things by
   * it: from the column it means "written before the discriminator existed" (a table), from the
   * wire it means "the caller did not say" (unknown). Only {@link EntityTypeConverter} knows it is
   * reading a column, so only it resolves the former.
   */
  public static EntityType fromName(String name) {
    if (name == null) {
      throw new IllegalArgumentException("entityType cannot be null");
    }
    // Only trailing U+0020 is insignificant, matching MySQL's PAD SPACE collations, which call
    // 'TABLE ' and 'TABLE' the same value. A leading space or any other whitespace stays corrupt:
    // SQL treats it as significant, so such a row would match no predicate.
    int end = name.length();
    while (end > 0 && name.charAt(end - 1) == ' ') {
      end--;
    }
    return valueOf(name.substring(0, end).toUpperCase(Locale.ROOT));
  }
}

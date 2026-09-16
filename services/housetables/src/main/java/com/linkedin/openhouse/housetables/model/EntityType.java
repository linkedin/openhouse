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
   * Resolves the spellings the transport model accepts, which is case-insensitive, so a value that
   * passes validation can never fail to resolve here.
   *
   * @return null for a null input
   * @throws IllegalArgumentException if the value is not a recognized entity type
   */
  public static EntityType fromName(String name) {
    return name == null ? null : valueOf(name.toUpperCase(Locale.ROOT));
  }
}

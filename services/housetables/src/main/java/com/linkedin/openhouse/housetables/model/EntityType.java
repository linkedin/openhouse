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
   * <p>A null is rejected rather than defaulted. The two callers give it incompatible meanings:
   * from the column it means "written before the discriminator existed", which is definitively a
   * table; from the wire it means "the caller did not say", which must not be guessed. Resolving
   * the former belongs to {@link EntityTypeConverter}, which is the only place that knows it is
   * reading a column.
   *
   * @throws IllegalArgumentException if the value is null or not a recognized entity type
   */
  public static EntityType fromName(String name) {
    if (name == null) {
      throw new IllegalArgumentException("entityType cannot be null");
    }
    return valueOf(name.toUpperCase(Locale.ROOT));
  }
}

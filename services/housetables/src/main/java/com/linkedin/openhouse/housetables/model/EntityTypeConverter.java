package com.linkedin.openhouse.housetables.model;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

/**
 * Keeps {@code entity_type} nullable in the column while {@link EntityType} stays total in Java: a
 * legacy row, written before the discriminator existed, hydrates as {@link EntityType#TABLE}.
 *
 * <p>Only the read side defaults. Stamping a type onto a write belongs to the endpoint that knows
 * which one it is, so a null attribute is still stored as a null column.
 *
 * <p>The read parses case-insensitively to agree with the equally case-insensitive table predicate
 * in the repository queries, so a row those queries matched can never then fail to hydrate.
 */
@Converter
public class EntityTypeConverter implements AttributeConverter<EntityType, String> {

  @Override
  public String convertToDatabaseColumn(EntityType entityType) {
    return entityType == null ? null : entityType.name();
  }

  @Override
  public EntityType convertToEntityAttribute(String columnValue) {
    // A legacy row predates the column, so its NULL means TABLE.
    if (columnValue == null) {
      return EntityType.TABLE;
    }
    try {
      return EntityType.fromName(columnValue);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Column user_table_row.entity_type holds unrecognized value [%s]; "
                  + "only TABLE, VIEW (in any case) and NULL are valid",
              columnValue),
          e);
    }
  }
}

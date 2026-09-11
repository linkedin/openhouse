package com.linkedin.openhouse.housetables.model;

import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

/**
 * Keeps {@code entity_type} nullable in the column while {@link EntityType} stays total in Java: a
 * legacy row, written before the discriminator existed, hydrates as {@link EntityType#TABLE}.
 *
 * <p>Only the read side defaults. The write side is strict: the endpoint stamps the type at
 * ingress, so a null reaching storage means an ingress path was missed, and failing here keeps the
 * legacy-null population that the {@code IS NULL} predicate arm carries from growing.
 *
 * <p>The read parses case-insensitively to agree with the equally case-insensitive table predicate
 * in the repository queries, so a row those queries matched can never then fail to hydrate.
 */
@Converter
public class EntityTypeConverter implements AttributeConverter<EntityType, String> {

  @Override
  public String convertToDatabaseColumn(EntityType entityType) {
    if (entityType == null) {
      throw new IllegalArgumentException(
          "Column user_table_row.entity_type cannot be written as null; "
              + "the endpoint serving the request is responsible for stamping the type");
    }
    return entityType.name();
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
      // Not an IllegalArgumentException: the shared advice answers 400 for those, and stored
      // corruption is a server-state failure whatever wrote it.
      throw new CorruptEntityTypeException(
          String.format(
              "Column user_table_row.entity_type holds unrecognized value ['%s']; "
                  + "only TABLE, VIEW (in any case) and NULL are valid",
              columnValue),
          e);
    }
  }
}

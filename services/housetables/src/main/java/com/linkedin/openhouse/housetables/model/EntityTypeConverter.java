package com.linkedin.openhouse.housetables.model;

import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

/**
 * Keeps {@code entity_type} nullable in the column while {@link EntityType} stays total in Java: a
 * legacy row hydrates as {@link EntityType#TABLE}, and this is the only place that happens.
 *
 * <p>The write side is strict: the endpoint stamps the type at ingress, so a null reaching storage
 * means an ingress path was missed, and failing here keeps the legacy-null population from growing.
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
    if (columnValue == null) {
      return EntityType.TABLE;
    }
    try {
      return EntityType.fromName(columnValue);
    } catch (IllegalArgumentException e) {
      throw new CorruptEntityTypeException(
          String.format(
              "Column user_table_row.entity_type holds unrecognized value [%s]; "
                  + "only TABLE, VIEW (in any case) and NULL are valid",
              columnValue),
          e);
    }
  }
}

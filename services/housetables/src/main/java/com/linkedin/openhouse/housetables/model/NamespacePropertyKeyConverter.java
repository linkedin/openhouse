package com.linkedin.openhouse.housetables.model;

import java.nio.charset.StandardCharsets;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

/** UTF-8 bytes preserve exact property keys across database collations. */
@Converter
public class NamespacePropertyKeyConverter implements AttributeConverter<String, byte[]> {
  @Override
  public byte[] convertToDatabaseColumn(String propertyKey) {
    return propertyKey.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public String convertToEntityAttribute(byte[] propertyKey) {
    return new String(propertyKey, StandardCharsets.UTF_8);
  }
}

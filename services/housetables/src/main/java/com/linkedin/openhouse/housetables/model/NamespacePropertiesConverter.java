package com.linkedin.openhouse.housetables.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import javax.persistence.PersistenceException;

/**
 * Persists the property map as one JSON document so updates share the namespace row's
 * optimistic-lock version. JSON object keys retain their exact spelling independently of database
 * collation.
 */
@Converter
public class NamespacePropertiesConverter
    implements AttributeConverter<Map<String, String>, String> {
  // ASCII JSON escapes preserve every accepted Java string through database character encoding.
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(JsonGenerator.Feature.ESCAPE_NON_ASCII);
  private static final TypeReference<Map<String, String>> PROPERTIES_TYPE =
      new TypeReference<Map<String, String>>() {};

  @Override
  public String convertToDatabaseColumn(Map<String, String> properties) {
    try {
      return MAPPER.writeValueAsString(properties);
    } catch (JsonProcessingException exception) {
      throw new PersistenceException("Could not encode namespace properties", exception);
    }
  }

  @Override
  public Map<String, String> convertToEntityAttribute(String document) {
    try {
      return MAPPER.readValue(document, PROPERTIES_TYPE);
    } catch (JsonProcessingException exception) {
      throw new PersistenceException("Could not decode namespace properties", exception);
    }
  }
}

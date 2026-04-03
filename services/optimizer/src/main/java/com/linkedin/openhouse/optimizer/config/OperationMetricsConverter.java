package com.linkedin.openhouse.optimizer.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.optimizer.api.model.OperationMetrics;
import java.io.IOException;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

/**
 * JPA {@link AttributeConverter} that serializes {@link OperationMetrics} to/from a JSON string.
 */
@Converter
public class OperationMetricsConverter implements AttributeConverter<OperationMetrics, String> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public String convertToDatabaseColumn(OperationMetrics attribute) {
    // Null metrics are valid for PENDING operations that have not yet produced output.
    if (attribute == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(attribute);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize OperationMetrics to JSON", e);
    }
  }

  @Override
  public OperationMetrics convertToEntityAttribute(String dbData) {
    // Null is stored for PENDING rows; return null so the entity reflects that state.
    if (dbData == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.readValue(dbData, OperationMetrics.class);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to deserialize OperationMetrics from JSON: " + dbData, e);
    }
  }
}

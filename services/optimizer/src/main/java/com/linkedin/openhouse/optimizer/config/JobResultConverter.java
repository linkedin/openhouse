package com.linkedin.openhouse.optimizer.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.optimizer.api.model.JobResult;
import java.io.IOException;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

/** JPA {@link AttributeConverter} that serializes {@link JobResult} to/from a JSON string. */
@Converter
public class JobResultConverter implements AttributeConverter<JobResult, String> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public String convertToDatabaseColumn(JobResult attribute) {
    if (attribute == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(attribute);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize JobResult to JSON", e);
    }
  }

  @Override
  public JobResult convertToEntityAttribute(String dbData) {
    if (dbData == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.readValue(dbData, JobResult.class);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize JobResult from JSON: " + dbData, e);
    }
  }
}

package com.linkedin.openhouse.optimizer.db;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import jakarta.persistence.PersistenceException;
import java.util.Map;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/** JPA converters for JSON payloads stored in the optimizer's MySQL {@code TEXT} columns. */
public final class JsonColumns {

  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {};

  private JsonColumns() {}

  private static String write(Object value) {
    if (value == null) {
      return null;
    }
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JacksonException e) {
      throw new PersistenceException("Could not serialize optimizer JSON column", e);
    }
  }

  private static <T> T read(String value, Class<T> type) {
    if (value == null) {
      return null;
    }
    try {
      return MAPPER.readValue(value, type);
    } catch (JacksonException e) {
      throw new PersistenceException(
          "Could not deserialize optimizer JSON column as " + type.getSimpleName(), e);
    }
  }

  private static Map<String, String> readStringMap(String value) {
    if (value == null) {
      return null;
    }
    try {
      return MAPPER.readValue(value, STRING_MAP);
    } catch (JacksonException e) {
      throw new PersistenceException("Could not deserialize optimizer JSON column as a map", e);
    }
  }

  @Converter
  public static final class SnapshotMetricsConverter
      implements AttributeConverter<SnapshotMetrics, String> {

    @Override
    public String convertToDatabaseColumn(SnapshotMetrics value) {
      return write(value);
    }

    @Override
    public SnapshotMetrics convertToEntityAttribute(String value) {
      return read(value, SnapshotMetrics.class);
    }
  }

  @Converter
  public static final class CommitDeltaMetricsConverter
      implements AttributeConverter<CommitDeltaMetrics, String> {

    @Override
    public String convertToDatabaseColumn(CommitDeltaMetrics value) {
      return write(value);
    }

    @Override
    public CommitDeltaMetrics convertToEntityAttribute(String value) {
      return read(value, CommitDeltaMetrics.class);
    }
  }

  @Converter
  public static final class StringMapConverter
      implements AttributeConverter<Map<String, String>, String> {

    @Override
    public String convertToDatabaseColumn(Map<String, String> value) {
      return write(value);
    }

    @Override
    public Map<String, String> convertToEntityAttribute(String value) {
      return readStringMap(value);
    }
  }
}

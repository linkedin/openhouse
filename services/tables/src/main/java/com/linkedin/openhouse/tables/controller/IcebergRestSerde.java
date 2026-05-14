package com.linkedin.openhouse.tables.controller;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.iceberg.rest.RESTSerializers;

/** Serde helper for Iceberg REST payloads that require kebab-case and Iceberg serializers. */
final class IcebergRestSerde {

  private static final ObjectMapper MAPPER = new ObjectMapper(new JsonFactory());

  static {
    MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    MAPPER.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(MAPPER);
  }

  private IcebergRestSerde() {}

  static String toJson(Object payload) {
    try {
      return MAPPER.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to serialize Iceberg REST response payload", e);
    }
  }
}

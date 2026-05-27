package com.linkedin.openhouse.tables.rest.adapter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.iceberg.rest.RESTSerializers;

/**
 * Project-local replacement for Iceberg's package-private {@code RESTObjectMapper}. Builds a
 * Jackson {@link ObjectMapper} configured with Iceberg REST request/response (de)serializers via
 * {@link RESTSerializers#registerAll(ObjectMapper)}. Use this for parsing inbound bodies and
 * rendering outbound bodies on all {@code /iceberg/v1/*} endpoints.
 */
public final class IcebergRestJson {

  private static final ObjectMapper MAPPER;

  static {
    ObjectMapper m = new ObjectMapper();
    // Iceberg's REST response types (ListNamespacesResponse, LoadTableResponse, ...) have
    // private fields and method-style accessors like `namespaces()` — not getters.
    // Without FIELD visibility ANY, Jackson sees no properties and emits {}.
    m.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    m.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
    m.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    m.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    m.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL);
    RESTSerializers.registerAll(m);
    MAPPER = m;
  }

  private IcebergRestJson() {}

  public static ObjectMapper mapper() {
    return MAPPER;
  }
}

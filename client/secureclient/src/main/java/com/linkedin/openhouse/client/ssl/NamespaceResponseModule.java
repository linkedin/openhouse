package com.linkedin.openhouse.client.ssl;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.tables.client.model.NamespaceResponse;
import com.linkedin.openhouse.tables.client.model.UpdateNamespacePropertiesResponse;
import java.io.IOException;
import java.util.List;

/** Applies strict scalar strings only to generated namespace responses. */
final class NamespaceResponseModule extends SimpleModule {
  NamespaceResponseModule() {
    ObjectMapper namespaceMapper =
        ApiClient.createDefaultObjectMapper(ApiClient.createDefaultDateFormat())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .registerModule(
                new SimpleModule()
                    .addDeserializer(String.class, new NamespaceStringDeserializer()));
    namespaceMapper
        .configOverride(List.class)
        .setSetterInfo(JsonSetter.Value.forValueNulls(Nulls.FAIL));
    addDeserializer(
        NamespaceResponse.class,
        new GeneratedResponseDeserializer<>(namespaceMapper.readerFor(NamespaceResponse.class)));
    addDeserializer(
        UpdateNamespacePropertiesResponse.class,
        new GeneratedResponseDeserializer<>(
            namespaceMapper.readerFor(UpdateNamespacePropertiesResponse.class)));
  }

  private static class GeneratedResponseDeserializer<T> extends JsonDeserializer<T> {
    private final ObjectReader reader;

    GeneratedResponseDeserializer(ObjectReader reader) {
      this.reader = reader;
    }

    @Override
    public T deserialize(JsonParser parser, DeserializationContext context) throws IOException {
      return reader.readValue(parser);
    }
  }

  private static class NamespaceStringDeserializer extends JsonDeserializer<String> {
    @Override
    public String deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      if (parser.hasToken(JsonToken.VALUE_STRING)) {
        return parser.getText();
      }
      return context.reportInputMismatch(String.class, "Expected a JSON string");
    }

    @Override
    public String getNullValue(DeserializationContext context) throws JsonMappingException {
      return context.reportInputMismatch(String.class, "Expected a JSON string");
    }
  }
}

package com.linkedin.openhouse.internal.catalog.repository;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;

/** Strict scalar strings for the namespace client's generated response decoder. */
public class HouseNamespaceResponseModule extends SimpleModule {
  public HouseNamespaceResponseModule() {
    addDeserializer(String.class, new NamespaceStringDeserializer());
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

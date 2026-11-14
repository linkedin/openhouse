package com.linkedin.openhouse.internal.catalog.repository;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Strict scalar strings for the namespace client's generated response decoder. */
public class HouseNamespaceResponseModule extends SimpleModule {
  public HouseNamespaceResponseModule() {
    addDeserializer(String.class, new NamespaceStringDeserializer());
  }

  @Override
  public void setupModule(SetupContext context) {
    super.setupModule(context);
    List.of(List.class, Map.class, Set.class)
        .forEach(
            collection ->
                context
                    .configOverride(collection)
                    .setSetterInfo(JsonSetter.Value.forValueNulls(Nulls.FAIL)));
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

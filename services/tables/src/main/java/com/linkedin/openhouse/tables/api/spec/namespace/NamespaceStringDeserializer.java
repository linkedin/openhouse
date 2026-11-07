package com.linkedin.openhouse.tables.api.spec.namespace;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;

/** Iceberg namespace fields require JSON strings, including property values. */
public class NamespaceStringDeserializer extends JsonDeserializer<String> {
  @Override
  public String deserialize(JsonParser parser, DeserializationContext context) throws IOException {
    if (parser.hasToken(JsonToken.VALUE_STRING)) {
      return parser.getText();
    }
    return context.reportInputMismatch(String.class, "Expected a JSON string");
  }
}

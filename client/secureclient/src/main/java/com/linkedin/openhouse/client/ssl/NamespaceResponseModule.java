package com.linkedin.openhouse.client.ssl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.linkedin.openhouse.tables.client.model.NamespaceResponse;
import com.linkedin.openhouse.tables.client.model.UpdateNamespacePropertiesResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Enforces the namespace wire contract before generated DTO defaults can hide missing fields. */
final class NamespaceResponseModule extends SimpleModule {
  NamespaceResponseModule() {
    addDeserializer(
        NamespaceResponse.class,
        new JsonDeserializer<NamespaceResponse>() {
          @Override
          public NamespaceResponse deserialize(JsonParser parser, DeserializationContext context)
              throws IOException {
            JsonNode response = parser.readValueAsTree();
            List<String> namespace = strings(response.path("namespace"), parser);
            JsonNode properties = response.path("properties");
            if (properties.isMissingNode() || properties.isNull()) {
              return new NamespaceResponse()
                  .namespace(namespace)
                  .properties(Collections.emptyMap());
            }
            if (!properties.isObject()
                || !StreamSupport.stream(properties.spliterator(), false)
                    .allMatch(JsonNode::isTextual)) {
              throw JsonMappingException.from(parser, "Expected string namespace properties");
            }
            Iterable<Map.Entry<String, JsonNode>> entries = properties::fields;
            return new NamespaceResponse()
                .namespace(namespace)
                .properties(
                    StreamSupport.stream(entries.spliterator(), false)
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey, entry -> entry.getValue().textValue())));
          }
        });
    addDeserializer(
        UpdateNamespacePropertiesResponse.class,
        new JsonDeserializer<UpdateNamespacePropertiesResponse>() {
          @Override
          public UpdateNamespacePropertiesResponse deserialize(
              JsonParser parser, DeserializationContext context) throws IOException {
            JsonNode response = parser.readValueAsTree();
            JsonNode missing = response.path("missing");
            return new UpdateNamespacePropertiesResponse()
                .updated(strings(response.path("updated"), parser))
                .removed(strings(response.path("removed"), parser))
                .missing(
                    missing.isMissingNode() || missing.isNull()
                        ? Collections.emptyList()
                        : strings(missing, parser));
          }
        });
  }

  private static List<String> strings(JsonNode values, JsonParser parser)
      throws JsonMappingException {
    if (!values.isArray()
        || !StreamSupport.stream(values.spliterator(), false).allMatch(JsonNode::isTextual)) {
      throw JsonMappingException.from(parser, "Expected a namespace string array");
    }
    return StreamSupport.stream(values.spliterator(), false)
        .map(JsonNode::textValue)
        .collect(Collectors.toList());
  }
}

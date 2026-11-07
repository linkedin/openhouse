package com.linkedin.openhouse.internal.catalog.repository;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.std.DelegatingDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.linkedin.openhouse.housetables.client.model.DatabaseMetadata;
import com.linkedin.openhouse.housetables.client.model.UpdateDatabasePropertiesResponseBody;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Checks the internal response contract before generated collection defaults are applied. */
public class DatabaseResponseModule extends SimpleModule {
  public DatabaseResponseModule() {
    Map<Class<?>, List<String>> requiredFields =
        Map.of(
            DatabaseMetadata.class, List.of("databaseUuid", "databaseName", "properties"),
            UpdateDatabasePropertiesResponseBody.class, List.of("updated", "removed", "missing"));
    setDeserializerModifier(
        new BeanDeserializerModifier() {
          @Override
          public JsonDeserializer<?> modifyDeserializer(
              DeserializationConfig configuration,
              BeanDescription description,
              JsonDeserializer<?> deserializer) {
            return requiredFields.containsKey(description.getBeanClass())
                ? new RequiredFieldsDeserializer(
                    deserializer, requiredFields.get(description.getBeanClass()))
                : deserializer;
          }
        });
  }

  private static class RequiredFieldsDeserializer extends DelegatingDeserializer {
    private final List<String> requiredFields;

    RequiredFieldsDeserializer(JsonDeserializer<?> delegate, List<String> requiredFields) {
      super(delegate);
      this.requiredFields = requiredFields;
    }

    @Override
    protected JsonDeserializer<?> newDelegatingInstance(JsonDeserializer<?> delegate) {
      return new RequiredFieldsDeserializer(delegate, requiredFields);
    }

    @Override
    public Object deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      JsonNode document = parser.readValueAsTree();
      if (!document.isObject()
          || requiredFields.stream().anyMatch(field -> !document.hasNonNull(field))) {
        return context.reportInputMismatch(
            handledType(), "Required database response fields are missing");
      }
      try (JsonParser validated = document.traverse(parser.getCodec())) {
        validated.nextToken();
        return super.deserialize(validated, context);
      }
    }
  }
}

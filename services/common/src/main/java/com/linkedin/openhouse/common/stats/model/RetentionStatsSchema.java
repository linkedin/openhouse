package com.linkedin.openhouse.common.stats.model;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/** Data Model for capturing retention stats about a table. */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder(toBuilder = true)
public class RetentionStatsSchema {

  private Integer count;

  private String granularity;

  private String columnName;

  private String columnPattern;

  public static class RetentionPolicyDeserializer
      implements JsonDeserializer<RetentionStatsSchema> {
    @Override
    public RetentionStatsSchema deserialize(
        JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      RetentionStatsSchema.RetentionStatsSchemaBuilder builder =
          RetentionStatsSchema.builder()
              .granularity(jsonObject.get("granularity").getAsString())
              .count(jsonObject.get("count").getAsInt());
      if (jsonObject.has("columnPattern")) {
        builder
            .columnName(
                jsonObject.get("columnPattern").getAsJsonObject().get("columnName").getAsString())
            .columnPattern(
                jsonObject.get("columnPattern").getAsJsonObject().get("pattern").getAsString());
      }
      return builder.build();
    }
  }
}

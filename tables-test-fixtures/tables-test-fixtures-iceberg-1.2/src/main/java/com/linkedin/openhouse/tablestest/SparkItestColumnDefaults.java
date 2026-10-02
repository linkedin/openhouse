package com.linkedin.openhouse.tablestest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultsSource;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Local-server stand-in for a deployment {@link ColumnDefaultsSource}. OSS OpenHouse has no
 * encoder; Spark catalog itests need one so ReadBridge can stamp {@code initial-default} on load.
 * Scoped to {@link #DATABASE} so other catalog tests stay on empty maps.
 */
@Configuration
public class SparkItestColumnDefaults {

  public static final String DATABASE = "d1_column_default";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Bean
  ColumnDefaultsSource sparkItestColumnDefaultsSource() {
    return SparkItestColumnDefaults::defaults;
  }

  static Map<Integer, JsonNode> defaults(TableDto tableDto) {
    if (!DATABASE.equals(tableDto.getDatabaseId())) {
      return Collections.emptyMap();
    }
    String schemaJson = tableDto.getSchema();
    if (schemaJson == null || schemaJson.isEmpty()) {
      return Collections.emptyMap();
    }
    JsonNode fields;
    try {
      fields = MAPPER.readTree(schemaJson).path("fields");
    } catch (Exception e) {
      return Collections.emptyMap();
    }
    if (!fields.isArray()) {
      return Collections.emptyMap();
    }
    Map<Integer, JsonNode> out = new LinkedHashMap<>();
    for (JsonNode field : fields) {
      String name = field.path("name").asText();
      if (!field.has("id")) {
        continue;
      }
      int id = field.get("id").asInt();
      if ("country".equals(name)) {
        out.put(id, TextNode.valueOf("US"));
      } else if ("tier".equals(name)) {
        out.put(id, IntNode.valueOf(1));
      }
    }
    return out;
  }
}

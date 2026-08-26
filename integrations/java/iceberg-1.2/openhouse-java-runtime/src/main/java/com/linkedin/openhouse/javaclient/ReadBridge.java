package com.linkedin.openhouse.javaclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

/**
 * Overlays server-stamped read-time behavior from table {@code config} onto loaded Iceberg
 * metadata.
 *
 * <p>Keys: {@code openhouse.read-bridge.column-default.<fieldId> = <single-value-json>}. {@link
 * #from} decodes; {@link #apply} overlays. Unknown keys are ignored. A malformed known entry throws
 * — that is an encoder or transport bug, not a missing default.
 *
 * <p>Overlays stay on the wire so the server can treat {@code initial-default} as the default-aware
 * signal; the server drops them before persist.
 */
final class ReadBridge {

  /** Same prefix the server encoder stamps. */
  static final String COLUMN_DEFAULT_PREFIX = "openhouse.read-bridge.column-default.";

  /** Nothing to overlay. */
  static final ReadBridge INERT = new ReadBridge(Collections.emptyMap());

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String SCHEMAS = "schemas";
  private static final String ID = "id";
  private static final String INITIAL_DEFAULT = "initial-default";

  private final Map<Integer, JsonNode> columnDefaults;

  private ReadBridge(Map<Integer, JsonNode> columnDefaults) {
    this.columnDefaults = columnDefaults;
  }

  /**
   * Decode stamped config. Returns {@link #INERT} when there is nothing to apply.
   *
   * @throws ReadBridgeException if a key this client owns is malformed
   */
  static ReadBridge from(Map<String, String> config) throws ReadBridgeException {
    Map<Integer, JsonNode> columnDefaults = decodeColumnDefaults(config);
    return columnDefaults.isEmpty() ? INERT : new ReadBridge(columnDefaults);
  }

  /** Overlay onto {@code raw}, or return it unchanged. */
  TableMetadata apply(TableMetadata raw) throws ReadBridgeException {
    if (columnDefaults.isEmpty()) {
      return raw;
    }
    ObjectNode root = metadataJson(raw);
    boolean changed = false;
    for (JsonNode field : fieldObjects(root)) {
      JsonNode defaultValue = columnDefaults.get(field.get(ID).asInt());
      if (defaultValue != null) {
        ((ObjectNode) field).set(INITIAL_DEFAULT, defaultValue);
        changed = true;
      }
    }
    return changed ? fromMetadataJson(raw, root) : raw;
  }

  Map<Integer, JsonNode> columnDefaults() {
    return columnDefaults;
  }

  /** Decode {@code column-default.<fieldId>} entries. Unknown keys are ignored. */
  private static Map<Integer, JsonNode> decodeColumnDefaults(Map<String, String> config)
      throws ReadBridgeException {
    if (config == null) {
      return Collections.emptyMap();
    }
    Map<Integer, JsonNode> byFieldId = new HashMap<>();
    for (Map.Entry<String, String> entry : config.entrySet()) {
      if (!entry.getKey().startsWith(COLUMN_DEFAULT_PREFIX)) {
        continue;
      }
      try {
        int fieldId = Integer.parseInt(entry.getKey().substring(COLUMN_DEFAULT_PREFIX.length()));
        byFieldId.put(fieldId, MAPPER.readTree(entry.getValue()));
      } catch (NumberFormatException | JsonProcessingException e) {
        // Known keys are stamped as int field-id + JSON; anything else is a bug.
        throw new ReadBridgeException(
            "read-bridge: unusable "
                + COLUMN_DEFAULT_PREFIX
                + " entry "
                + entry.getKey()
                + "="
                + entry.getValue(),
            e);
      }
    }
    return byFieldId;
  }

  /** Schema field objects are the JSON nodes that carry {@code id}. */
  private static JsonNode fieldObjects(ObjectNode metadata) {
    ArrayNode fields = MAPPER.createArrayNode();
    StreamSupport.stream(metadata.get(SCHEMAS).spliterator(), false)
        .map(schema -> schema.findParents(ID))
        .flatMap(List::stream)
        .forEach(fields::add);
    return fields;
  }

  private static ObjectNode metadataJson(TableMetadata metadata) throws ReadBridgeException {
    try {
      return (ObjectNode) readTree(TableMetadataParser.toJson(metadata));
    } catch (RuntimeException e) {
      throw new ReadBridgeException("read-bridge: failed to parse table metadata JSON", e);
    }
  }

  private static TableMetadata fromMetadataJson(TableMetadata metadata, ObjectNode root)
      throws ReadBridgeException {
    try {
      return TableMetadataParser.fromJson(
          metadata.metadataFileLocation(), MAPPER.writeValueAsString(root));
    } catch (JsonProcessingException | RuntimeException e) {
      throw new ReadBridgeException("read-bridge: failed to rebuild table metadata schemas", e);
    }
  }

  private static JsonNode readTree(String json) throws ReadBridgeException {
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new ReadBridgeException("read-bridge: invalid json", e);
    }
  }
}

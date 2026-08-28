package com.linkedin.openhouse.javaclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private static final String FIELDS = "fields";
  private static final String ID = "id";
  private static final String TYPE = "type";
  private static final String STRUCT = "struct";
  private static final String LIST = "list";
  private static final String MAP = "map";
  private static final String ELEMENT = "element";
  private static final String KEY = "key";
  private static final String VALUE = "value";
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
    for (ObjectNode field : fieldObjects(root).collect(Collectors.toList())) {
      JsonNode defaultValue = columnDefaults.get(field.get(ID).asInt());
      if (defaultValue != null) {
        field.set(INITIAL_DEFAULT, defaultValue);
        changed = true;
      }
    }
    if (!changed) {
      return raw;
    }
    final String json;
    try {
      json = MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw ReadBridgeException.unusableMetadata(
          "read-bridge: failed to serialize patched metadata", e);
    }
    try {
      return TableMetadataParser.fromJson(raw.metadataFileLocation(), json);
    } catch (RuntimeException e) {
      throw ReadBridgeException.cannotBind(e);
    }
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
        throw ReadBridgeException.unusableConfig(
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

  /** NestedField objects under every schema-id: {@code schemas[].fields}, then nested types. */
  private static Stream<ObjectNode> fieldObjects(ObjectNode metadata) {
    return StreamSupport.stream(metadata.get(SCHEMAS).spliterator(), false)
        .flatMap(ReadBridge::nestedFields);
  }

  private static Stream<ObjectNode> nestedFields(JsonNode type) {
    if (!type.isObject()) {
      return Stream.empty();
    }
    switch (type.get(TYPE).asText()) {
      case STRUCT:
        return fieldsOf(type.get(FIELDS));
      case LIST:
        return nestedFields(type.get(ELEMENT));
      case MAP:
        return Stream.concat(nestedFields(type.get(KEY)), nestedFields(type.get(VALUE)));
      default:
        return Stream.empty();
    }
  }

  private static Stream<ObjectNode> fieldsOf(JsonNode fields) {
    return StreamSupport.stream(fields.spliterator(), false)
        .map(field -> (ObjectNode) field)
        .flatMap(field -> Stream.concat(Stream.of(field), nestedFields(field.get(TYPE))));
  }

  private static ObjectNode metadataJson(TableMetadata metadata) throws ReadBridgeException {
    try {
      return (ObjectNode) readTree(TableMetadataParser.toJson(metadata));
    } catch (RuntimeException e) {
      throw ReadBridgeException.unusableMetadata(
          "read-bridge: failed to parse table metadata JSON", e);
    }
  }

  private static JsonNode readTree(String json) throws ReadBridgeException {
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw ReadBridgeException.unusableMetadata("read-bridge: invalid json", e);
    }
  }
}

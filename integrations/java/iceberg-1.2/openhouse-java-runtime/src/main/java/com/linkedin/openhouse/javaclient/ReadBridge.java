package com.linkedin.openhouse.javaclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * <p>{@link #sanitize} strips {@code initial-default} on stamped field-ids so overlays cannot
 * persist. Unstamped ids keep writer defaults.
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

  /** JSON strings, not JsonNodes — Jackson is relocated in the shaded client. */
  private final Map<Integer, String> columnDefaults;

  private ReadBridge(Map<Integer, String> columnDefaults) {
    this.columnDefaults = columnDefaults;
  }

  /**
   * Decode stamped config. Returns {@link #INERT} when there is nothing to apply.
   *
   * @throws IllegalStateException if a key this client owns is malformed
   */
  static ReadBridge from(Map<String, String> config) {
    Map<Integer, String> columnDefaults = decodeColumnDefaults(config);
    return columnDefaults.isEmpty() ? INERT : new ReadBridge(columnDefaults);
  }

  /** Overlay onto {@code raw}, or return it unchanged. */
  TableMetadata apply(TableMetadata raw) {
    if (columnDefaults.isEmpty()) {
      return raw;
    }
    ObjectNode root = metadataJson(raw);
    boolean changed = false;
    for (JsonNode field : fieldObjects(root)) {
      String defaultJson = columnDefaults.get(field.get(ID).asInt());
      if (defaultJson != null) {
        ((ObjectNode) field).set(INITIAL_DEFAULT, readTree(defaultJson));
        changed = true;
      }
    }
    return changed ? fromMetadataJson(raw, root) : raw;
  }

  /**
   * Strip {@code initial-default} on field-ids this bridge stamped. Name, type, nullability, doc,
   * order, write-default, and unstamped ids stay on {@code metadata}.
   */
  TableMetadata sanitize(TableMetadata metadata) {
    if (columnDefaults.isEmpty() || metadata == null) {
      return metadata;
    }
    ObjectNode root = metadataJson(metadata);
    boolean changed = false;
    for (JsonNode field : fieldObjects(root)) {
      if (columnDefaults.containsKey(field.get(ID).asInt())
          && ((ObjectNode) field).remove(INITIAL_DEFAULT) != null) {
        changed = true;
      }
    }
    return changed ? fromMetadataJson(metadata, root) : metadata;
  }

  Map<Integer, String> columnDefaults() {
    return columnDefaults;
  }

  /** Decode {@code column-default.<fieldId>} entries. Unknown keys are ignored. */
  private static Map<Integer, String> decodeColumnDefaults(Map<String, String> config) {
    if (config == null) {
      return Collections.emptyMap();
    }
    Map<Integer, String> byFieldId = new HashMap<>();
    for (Map.Entry<String, String> entry : config.entrySet()) {
      if (!entry.getKey().startsWith(COLUMN_DEFAULT_PREFIX)) {
        continue;
      }
      try {
        int fieldId = Integer.parseInt(entry.getKey().substring(COLUMN_DEFAULT_PREFIX.length()));
        // Validate JSON; keep the original string so apply can bind without a relocated JsonNode.
        MAPPER.readTree(entry.getValue());
        byFieldId.put(fieldId, entry.getValue());
      } catch (RuntimeException | JsonProcessingException e) {
        // Known keys are stamped as int field-id + JSON; anything else is a bug.
        throw new IllegalStateException(
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
  private static List<JsonNode> fieldObjects(ObjectNode metadata) {
    JsonNode schemas = metadata.get(SCHEMAS);
    if (schemas == null || !schemas.isArray()) {
      throw new IllegalStateException(
          "read-bridge: metadata JSON missing required '" + SCHEMAS + "' array");
    }
    List<JsonNode> fields = new ArrayList<>();
    for (JsonNode schema : schemas) {
      List<JsonNode> found = schema.findParents(ID);
      if (found != null) {
        fields.addAll(found);
      }
    }
    return fields;
  }

  private static ObjectNode metadataJson(TableMetadata metadata) {
    JsonNode root = readTree(TableMetadataParser.toJson(metadata));
    if (root == null || !root.isObject()) {
      throw new IllegalStateException("read-bridge: table metadata JSON is not an object");
    }
    return (ObjectNode) root;
  }

  private static TableMetadata fromMetadataJson(TableMetadata metadata, ObjectNode root) {
    try {
      return TableMetadataParser.fromJson(
          metadata.metadataFileLocation(), MAPPER.writeValueAsString(root));
    } catch (IllegalStateException e) {
      throw e;
    } catch (RuntimeException | JsonProcessingException e) {
      throw new IllegalStateException("read-bridge: failed to rebuild table metadata schemas", e);
    }
  }

  private static JsonNode readTree(String json) {
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("read-bridge: invalid json", e);
    }
  }
}

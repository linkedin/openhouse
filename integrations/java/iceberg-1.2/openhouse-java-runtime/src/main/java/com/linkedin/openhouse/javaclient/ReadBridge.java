package com.linkedin.openhouse.javaclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Overlays server-stamped read-time behavior from table {@code config} onto loaded Iceberg
 * metadata.
 *
 * <p>Keys: {@code openhouse.read-bridge.column-default.<fieldId> = <single-value-json>}. {@link
 * #from} decodes; {@link #apply} overlays. Unknown keys are ignored. A malformed known entry throws
 * — that is an encoder or transport bug, not a missing default.
 *
 * <p>Apply rebuilds schemas with {@code NestedField.withInitialDefault} and puts them back through
 * {@link TableMetadataParser} JSON. Missing field-id on a schema is a gap (NULL); a default that
 * cannot bind throws.
 *
 * <p>{@link #sanitize} restores default slots on field-ids that existed in the last on-disk
 * metadata so an overlay cannot persist. New field-ids keep the writer's defaults.
 */
final class ReadBridge {

  /** Same prefix the server encoder stamps. */
  static final String COLUMN_DEFAULT_PREFIX = "openhouse.read-bridge.column-default.";

  /** {@link #apply} is a no-op. */
  static final ReadBridge INERT = new ReadBridge(Collections.emptyMap());

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String FORMAT_VERSION = "format-version";
  private static final String SCHEMA = "schema";
  private static final String SCHEMAS = "schemas";
  private static final String SCHEMA_ID = "schema-id";

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

    Map<Integer, Schema> overlaidById = new HashMap<>();
    for (Schema schema : raw.schemas()) {
      Schema overlaid = overlaySchema(schema, columnDefaults);
      if (overlaid != schema) {
        overlaidById.put(schema.schemaId(), overlaid);
      }
    }
    if (overlaidById.isEmpty()) {
      // Every stamped field-id is missing from every schema — leave metadata as-is.
      return raw;
    }
    return replaceSchemas(raw, overlaidById);
  }

  /**
   * Restore {@code initialDefault} / {@code writeDefault} on field-ids that existed in {@code raw}.
   * Name, type, nullability, doc, order, and new field-ids stay on {@code metadata}.
   *
   * <p>A field-id is overlay iff it was on disk at load. Apply only stamps existing ids; V2
   * evolution does not set defaults on those ids. A field-id absent from {@code raw} was added in
   * this commit — keep the writer's defaults.
   */
  static TableMetadata sanitize(TableMetadata raw, TableMetadata metadata) {
    if (raw == null || metadata == null || raw == metadata) {
      return metadata;
    }
    Map<Integer, NestedField> rawById = indexFields(raw);
    Map<Integer, Schema> restoredById = new HashMap<>();
    for (Schema schema : metadata.schemas()) {
      Schema restored = restoreSchema(schema, rawById);
      if (restored != schema) {
        restoredById.put(schema.schemaId(), restored);
      }
    }
    if (restoredById.isEmpty()) {
      return metadata;
    }
    return replaceSchemas(metadata, restoredById);
  }

  Map<Integer, String> columnDefaults() {
    return columnDefaults;
  }

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

  private static Schema overlaySchema(Schema schema, Map<Integer, String> columnDefaults) {
    List<NestedField> columns = schema.columns();
    List<NestedField> overlaid = new ArrayList<>(columns.size());
    boolean changed = false;
    for (NestedField column : columns) {
      NestedField next = overlayField(column, columnDefaults);
      overlaid.add(next);
      if (next != column) {
        changed = true;
      }
    }
    if (!changed) {
      return schema;
    }
    return new Schema(
        schema.schemaId(), overlaid, schema.getAliases(), schema.identifierFieldIds());
  }

  private static NestedField overlayField(NestedField field, Map<Integer, String> columnDefaults) {
    Type type = field.type();
    Type overlaidType = overlayType(type, columnDefaults);
    String defaultJson = columnDefaults.get(field.fieldId());

    if (defaultJson == null && overlaidType == type) {
      return field;
    }

    NestedField.Builder builder = NestedField.from(field);
    if (overlaidType != type) {
      builder.ofType(overlaidType);
    }
    if (defaultJson != null) {
      try {
        Object value = SingleValueParser.fromJson(overlaidType, defaultJson);
        builder.withInitialDefault(Expressions.lit(value));
      } catch (RuntimeException e) {
        throw new IllegalStateException(
            "read-bridge: cannot bind "
                + COLUMN_DEFAULT_PREFIX
                + field.fieldId()
                + "="
                + defaultJson
                + " to "
                + field,
            e);
      }
    }
    return builder.build();
  }

  private static Type overlayType(Type type, Map<Integer, String> columnDefaults) {
    if (type.isStructType()) {
      List<NestedField> fields = type.asStructType().fields();
      List<NestedField> overlaid = new ArrayList<>(fields.size());
      boolean changed = false;
      for (NestedField field : fields) {
        NestedField next = overlayField(field, columnDefaults);
        overlaid.add(next);
        if (next != field) {
          changed = true;
        }
      }
      return changed ? Types.StructType.of(overlaid) : type;
    }
    if (type.isListType()) {
      Types.ListType list = type.asListType();
      Type element = overlayType(list.elementType(), columnDefaults);
      if (element == list.elementType()) {
        return type;
      }
      return list.isElementRequired()
          ? Types.ListType.ofRequired(list.elementId(), element)
          : Types.ListType.ofOptional(list.elementId(), element);
    }
    if (type.isMapType()) {
      Types.MapType map = type.asMapType();
      Type key = overlayType(map.keyType(), columnDefaults);
      Type value = overlayType(map.valueType(), columnDefaults);
      if (key == map.keyType() && value == map.valueType()) {
        return type;
      }
      return map.isValueRequired()
          ? Types.MapType.ofRequired(map.keyId(), map.valueId(), key, value)
          : Types.MapType.ofOptional(map.keyId(), map.valueId(), key, value);
    }
    return type;
  }

  private static Map<Integer, NestedField> indexFields(TableMetadata raw) {
    Map<Integer, NestedField> byId = new HashMap<>();
    for (Schema schema : raw.schemas()) {
      collectFields(schema.asStruct(), byId);
    }
    Schema current = raw.schema();
    if (current != null) {
      collectFields(current.asStruct(), byId);
    }
    return byId;
  }

  private static void collectFields(Type type, Map<Integer, NestedField> byId) {
    if (type.isStructType()) {
      for (NestedField field : type.asStructType().fields()) {
        byId.put(field.fieldId(), field);
        collectFields(field.type(), byId);
      }
    } else if (type.isListType()) {
      collectFields(type.asListType().elementType(), byId);
    } else if (type.isMapType()) {
      Types.MapType map = type.asMapType();
      collectFields(map.keyType(), byId);
      collectFields(map.valueType(), byId);
    }
  }

  private static Schema restoreSchema(Schema schema, Map<Integer, NestedField> rawById) {
    List<NestedField> columns = schema.columns();
    List<NestedField> restored = new ArrayList<>(columns.size());
    boolean changed = false;
    for (NestedField column : columns) {
      NestedField next = restoreField(column, rawById);
      restored.add(next);
      if (next != column) {
        changed = true;
      }
    }
    if (!changed) {
      return schema;
    }
    return new Schema(
        schema.schemaId(), restored, schema.getAliases(), schema.identifierFieldIds());
  }

  private static NestedField restoreField(NestedField field, Map<Integer, NestedField> rawById) {
    Type type = field.type();
    Type restoredType = restoreType(type, rawById);
    NestedField rawField = rawById.get(field.fieldId());
    if (rawField == null) {
      if (restoredType == type) {
        return field;
      }
      return NestedField.from(field).ofType(restoredType).build();
    }
    boolean defaultsSame =
        Objects.equals(field.initialDefaultLiteral(), rawField.initialDefaultLiteral())
            && Objects.equals(field.writeDefaultLiteral(), rawField.writeDefaultLiteral());
    if (defaultsSame && restoredType == type) {
      return field;
    }
    NestedField.Builder builder = NestedField.from(field);
    if (restoredType != type) {
      builder.ofType(restoredType);
    }
    if (!defaultsSame) {
      builder.withInitialDefault(rawField.initialDefaultLiteral());
      builder.withWriteDefault(rawField.writeDefaultLiteral());
    }
    return builder.build();
  }

  private static Type restoreType(Type type, Map<Integer, NestedField> rawById) {
    if (type.isStructType()) {
      List<NestedField> fields = type.asStructType().fields();
      List<NestedField> restored = new ArrayList<>(fields.size());
      boolean changed = false;
      for (NestedField field : fields) {
        NestedField next = restoreField(field, rawById);
        restored.add(next);
        if (next != field) {
          changed = true;
        }
      }
      return changed ? Types.StructType.of(restored) : type;
    }
    if (type.isListType()) {
      Types.ListType list = type.asListType();
      Type element = restoreType(list.elementType(), rawById);
      if (element == list.elementType()) {
        return type;
      }
      return list.isElementRequired()
          ? Types.ListType.ofRequired(list.elementId(), element)
          : Types.ListType.ofOptional(list.elementId(), element);
    }
    if (type.isMapType()) {
      Types.MapType map = type.asMapType();
      Type key = restoreType(map.keyType(), rawById);
      Type value = restoreType(map.valueType(), rawById);
      if (key == map.keyType() && value == map.valueType()) {
        return type;
      }
      return map.isValueRequired()
          ? Types.MapType.ofRequired(map.keyId(), map.valueId(), key, value)
          : Types.MapType.ofOptional(map.keyId(), map.valueId(), key, value);
    }
    return type;
  }

  /**
   * Swap rebuilt schemas in by rewriting metadata JSON. The public builder will not replace an
   * existing schema-id.
   */
  static TableMetadata replaceSchemas(
      TableMetadata metadata, Map<Integer, Schema> replacementById) {
    try {
      ObjectNode root = (ObjectNode) MAPPER.readTree(TableMetadataParser.toJson(metadata));
      JsonNode schemasNode = root.get(SCHEMAS);
      if (schemasNode == null || !schemasNode.isArray()) {
        throw new IllegalStateException(
            "read-bridge: metadata JSON missing required '" + SCHEMAS + "' array");
      }
      ArrayNode schemas = (ArrayNode) schemasNode;
      for (int i = 0; i < schemas.size(); i++) {
        JsonNode schemaNode = schemas.get(i);
        if (schemaNode == null || !schemaNode.has(SCHEMA_ID)) {
          throw new IllegalStateException(
              "read-bridge: schemas[" + i + "] missing '" + SCHEMA_ID + "'");
        }
        int schemaId = schemaNode.get(SCHEMA_ID).asInt();
        Schema replacement = replacementById.get(schemaId);
        if (replacement != null) {
          schemas.set(i, MAPPER.readTree(SchemaParser.toJson(replacement)));
        }
      }
      // v1 also writes the current schema under "schema"; keep it in sync.
      if (root.path(FORMAT_VERSION).asInt(/* default= */ 2) == 1) {
        Schema currentReplacement = replacementById.get(metadata.currentSchemaId());
        if (currentReplacement != null) {
          root.set(SCHEMA, MAPPER.readTree(SchemaParser.toJson(currentReplacement)));
        }
      }
      return TableMetadataParser.fromJson(
          metadata.metadataFileLocation(), MAPPER.writeValueAsString(root));
    } catch (IllegalStateException e) {
      throw e;
    } catch (RuntimeException | JsonProcessingException e) {
      throw new IllegalStateException("read-bridge: failed to rebuild table metadata schemas", e);
    }
  }
}

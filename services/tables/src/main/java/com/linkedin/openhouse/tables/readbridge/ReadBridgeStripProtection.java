package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;

/**
 * Type 1 / Type 2 strip protection for column defaults. Default-aware clients send {@code
 * initial-default} on the commit schema; this class uses that as the handshake, then drops those
 * keys before persist so overlays cannot land in Iceberg metadata.
 *
 * <p>The handshake is trust, not proof that data files were rewritten correctly. Do not ramp a
 * table until OpenHouse rewrites are trusted or default-aware compaction exists. A lift/compaction
 * flag is a follow-up, not this class.
 *
 * <p>The PUT includes the table's full snapshot list, so rewrite detection uses the main-branch
 * snapshot (the commit being applied), not historical overwrite snapshots still in the list.
 *
 * <p>Schema field objects are the JSON nodes that carry {@code id} — the same walk as the client
 * overlay. On Iceberg schema JSON that is NestedField; {@code element-id} / {@code schema-id} are
 * different keys.
 */
public class ReadBridgeStripProtection {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ID = "id";
  private static final String INITIAL_DEFAULT = "initial-default";

  private final ReadBridgeConfigResolver resolver;

  public ReadBridgeStripProtection(ReadBridgeConfigResolver resolver) {
    this.resolver = resolver;
  }

  /**
   * Reject Type 1 / Type 2 violations, then strip stamped {@code initial-default} from {@code
   * incoming} so they are not persisted. Returns {@code incoming} unchanged when there is nothing
   * to check or drop.
   */
  public TableDto prepare(TableDto existing, TableDto incoming) {
    if (incoming == null) {
      return incoming;
    }
    try {
      Map<Integer, String> previous =
          existing == null ? Collections.emptyMap() : resolver.stampedColumnDefaults(existing);
      Map<Integer, String> incomingStamped = resolver.stampedColumnDefaults(incoming);
      if (existing != null) {
        rejectRemovedDefaults(previous, incomingStamped, incoming);
        rejectUnawareRewrite(previous, incoming);
      }
      return stripStampedDefaults(previous, incomingStamped, incoming);
    } catch (UnsupportedClientOperationException e) {
      throw e;
    } catch (IllegalStateException e) {
      // Source/toggle/parse failures are ISE and must 400. A bug (NPE) must stay 500.
      throw unusable(incoming, e);
    }
  }

  private void rejectRemovedDefaults(
      Map<Integer, String> previous, Map<Integer, String> incomingStamped, TableDto incoming) {
    if (previous.isEmpty() || !resolver.isRampedForCommit(incoming)) {
      return;
    }
    Set<Integer> remaining = fieldIds(tree(incoming.getSchema()));
    for (Integer fieldId : previous.keySet()) {
      if (remaining.contains(fieldId) && !incomingStamped.containsKey(fieldId)) {
        throw new UnsupportedClientOperationException(
            UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_REMOVED,
            String.format(
                "Table %s.%s cannot drop the column default on field-id %s while the column remains",
                incoming.getDatabaseId(), incoming.getTableId(), fieldId));
      }
    }
  }

  private void rejectUnawareRewrite(Map<Integer, String> previous, TableDto incoming) {
    if (previous.isEmpty() || !isRewrite(incoming)) {
      return;
    }
    JsonNode schema = tree(incoming.getSchema());
    Set<Integer> remaining = fieldIds(schema);
    for (Map.Entry<Integer, String> stamp : previous.entrySet()) {
      if (!remaining.contains(stamp.getKey())) {
        continue;
      }
      JsonNode actual = initialDefault(schema, stamp.getKey());
      if (!tree(stamp.getValue()).equals(actual)) {
        throw new UnsupportedClientOperationException(
            UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_REWRITE,
            String.format(
                "Table %s.%s declares column defaults; overwrite/replace requires matching"
                    + " initial-default on field-ids %s",
                incoming.getDatabaseId(), incoming.getTableId(), previous.keySet()));
      }
    }
  }

  private TableDto stripStampedDefaults(
      Map<Integer, String> previous, Map<Integer, String> incomingStamped, TableDto incoming) {
    Set<Integer> stripIds = new HashSet<>();
    stripIds.addAll(previous.keySet());
    stripIds.addAll(incomingStamped.keySet());
    if (stripIds.isEmpty()) {
      return incoming;
    }
    String schema = strip(incoming.getSchema(), stripIds);
    List<String> intermediates = strip(incoming.getNewIntermediateSchemas(), stripIds);
    if (Objects.equals(schema, incoming.getSchema())
        && Objects.equals(intermediates, incoming.getNewIntermediateSchemas())) {
      return incoming;
    }
    return incoming.toBuilder().schema(schema).newIntermediateSchemas(intermediates).build();
  }

  private boolean isRewrite(TableDto incoming) {
    if (incoming.isReplaceCommit() || incoming.isStageReplace()) {
      return true;
    }
    List<String> jsonSnapshots = incoming.getJsonSnapshots();
    if (jsonSnapshots == null || jsonSnapshots.isEmpty()) {
      return false;
    }
    String operation = currentSnapshot(incoming, jsonSnapshots).operation();
    return DataOperations.OVERWRITE.equals(operation) || DataOperations.REPLACE.equals(operation);
  }

  private static Snapshot currentSnapshot(TableDto incoming, List<String> jsonSnapshots) {
    Long mainId = mainSnapshotId(incoming.getSnapshotRefs());
    if (mainId != null) {
      for (String json : jsonSnapshots) {
        Snapshot snapshot = snapshot(json);
        if (snapshot.snapshotId() == mainId) {
          return snapshot;
        }
      }
      throw new IllegalStateException("main-branch snapshot is missing from the request");
    }
    return snapshot(jsonSnapshots.get(jsonSnapshots.size() - 1));
  }

  private static Long mainSnapshotId(Map<String, String> snapshotRefs) {
    if (snapshotRefs == null) {
      return null;
    }
    String main = snapshotRefs.get(SnapshotRef.MAIN_BRANCH);
    if (main == null) {
      return null;
    }
    try {
      return SnapshotRefParser.fromJson(main).snapshotId();
    } catch (RuntimeException e) {
      throw new IllegalStateException("unreadable snapshot ref", e);
    }
  }

  private static Snapshot snapshot(String json) {
    try {
      return SnapshotParser.fromJson(json);
    } catch (RuntimeException e) {
      throw new IllegalStateException("unreadable snapshot", e);
    }
  }

  private static String strip(String schemaJson, Set<Integer> fieldIds) {
    JsonNode root = tree(schemaJson);
    boolean changed = false;
    for (JsonNode field : fieldObjects(root)) {
      if (field instanceof ObjectNode
          && field.has(ID)
          && fieldIds.contains(field.get(ID).asInt())
          && ((ObjectNode) field).remove(INITIAL_DEFAULT) != null) {
        changed = true;
      }
    }
    if (!changed) {
      return schemaJson;
    }
    try {
      return MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "read-bridge: failed to strip initial-default from schema", e);
    }
  }

  private static List<String> strip(List<String> schemas, Set<Integer> fieldIds) {
    if (schemas == null || schemas.isEmpty() || fieldIds.isEmpty()) {
      return schemas;
    }
    List<String> stripped = new ArrayList<>(schemas.size());
    boolean changed = false;
    for (String schema : schemas) {
      String next = strip(schema, fieldIds);
      stripped.add(next);
      changed |= !Objects.equals(next, schema);
    }
    return changed ? stripped : schemas;
  }

  private static Set<Integer> fieldIds(JsonNode schema) {
    Set<Integer> ids = new HashSet<>();
    for (JsonNode field : fieldObjects(schema)) {
      if (field.has(ID)) {
        ids.add(field.get(ID).asInt());
      }
    }
    return ids;
  }

  private static JsonNode initialDefault(JsonNode schema, int fieldId) {
    for (JsonNode field : fieldObjects(schema)) {
      if (field.has(ID) && field.get(ID).asInt() == fieldId) {
        return field.get(INITIAL_DEFAULT);
      }
    }
    return null;
  }

  private static List<JsonNode> fieldObjects(JsonNode schema) {
    List<JsonNode> found = schema.findParents(ID);
    return found == null ? Collections.emptyList() : found;
  }

  private static JsonNode tree(String json) {
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("unreadable json", e);
    }
  }

  private static UnsupportedClientOperationException unusable(
      TableDto incoming, IllegalStateException cause) {
    String reason = cause.getMessage() == null ? cause.toString() : cause.getMessage();
    return new UnsupportedClientOperationException(
        UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_UNUSABLE,
        String.format(
            "Table %s.%s cannot apply column-default protection: %s",
            incoming.getDatabaseId(), incoming.getTableId(), reason));
  }
}

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
 * initial-default} on the commit schema; this class uses that as the handshake, then drops every
 * {@code initial-default} before persist so overlays cannot land in Iceberg metadata.
 *
 * <p>The handshake is trust, not proof that data files were rewritten correctly. Do not ramp a
 * table until OpenHouse rewrites are trusted or default-aware compaction exists. A lift/compaction
 * flag is a follow-up, not this class.
 *
 * <p>The PUT includes the table's full snapshot list, so rewrite detection uses the main-branch
 * snapshot, not a historical overwrite still in the list. A WAP / named-branch overwrite leaves
 * {@code main} unchanged and is not gated. Do not infer the written ref by diffing ref maps — use
 * commit deltas from #669 once they reach this path. See
 * https://github.com/linkedin/openhouse/issues/693.
 *
 * <p>Schema field objects are the JSON nodes that carry {@code id} — the same walk as the client
 * overlay. On Iceberg schema JSON that is NestedField; {@code element-id} / {@code schema-id} are
 * different keys.
 */
public class ReadBridgeStripProtection {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final class SchemaKeys {
    private static final String ID = "id";
    private static final String NAME = "name";
    private static final String INITIAL_DEFAULT = "initial-default";

    private SchemaKeys() {}
  }

  private final ReadBridgeConfigResolver resolver;

  public ReadBridgeStripProtection(ReadBridgeConfigResolver resolver) {
    this.resolver = Objects.requireNonNull(resolver, "resolver");
  }

  /**
   * Reject Type 1 / Type 2 violations, then drop every {@code initial-default} from {@code
   * incoming} so overlays cannot land in Iceberg metadata. Ramp-off still strips. Returns {@code
   * incoming} unchanged when there is nothing to check or drop.
   */
  public TableDto prepare(TableDto existing, TableDto incoming) {
    if (incoming == null) {
      return incoming;
    }
    Map<Integer, String> previousStamped =
        existing == null ? Collections.emptyMap() : resolver.stampedColumnDefaults(existing);
    Map<Integer, String> incomingStamped = resolver.stampedColumnDefaults(incoming);
    if (existing != null) {
      rejectRemovedDefaults(previousStamped, incomingStamped, existing, incoming);
      rejectUnawareRewrite(previousStamped, existing, incoming);
    }
    return stripInitialDefaults(existing, incoming);
  }

  /**
   * Type 1: a still-present stamped column must keep its default. Unstamped or unramped tables skip
   * — there is no default to protect.
   */
  private void rejectRemovedDefaults(
      Map<Integer, String> previousStamped,
      Map<Integer, String> incomingStamped,
      TableDto existing,
      TableDto incoming) {
    if (previousStamped.isEmpty() || !resolver.isRampedForCommit(incoming)) {
      return;
    }
    JsonNode schema = tree(incoming.getSchema(), existing, incoming);
    Set<Integer> remaining = fieldIds(schema);
    for (Integer fieldId : previousStamped.keySet()) {
      if (remaining.contains(fieldId) && !incomingStamped.containsKey(fieldId)) {
        throw new UnsupportedClientOperationException(
            UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_REMOVED,
            String.format(
                "COLUMN_DEFAULT_REMOVED: %s.%s still has a column default on %s. This commit"
                    + " omitted it. Retry from Spark 3.1 or Spark 3.5 using the jars on the"
                    + " standard client image. Column defaults cannot be removed or changed.",
                incoming.getDatabaseId(), incoming.getTableId(), fieldLabel(schema, fieldId)));
      }
    }
  }

  /**
   * Type 2: overwrite/replace must send {@code initial-default} equal to the stamp. That handshake
   * is trust, not proof the files were rewritten. Appends are not rewrites.
   */
  private void rejectUnawareRewrite(
      Map<Integer, String> previousStamped, TableDto existing, TableDto incoming) {
    if (previousStamped.isEmpty() || !isRewrite(existing, incoming)) {
      return;
    }
    JsonNode schema = tree(incoming.getSchema(), existing, incoming);
    Set<Integer> remaining = fieldIds(schema);
    for (Map.Entry<Integer, String> stamp : previousStamped.entrySet()) {
      if (!remaining.contains(stamp.getKey())) {
        continue;
      }
      JsonNode actual = initialDefault(schema, stamp.getKey());
      if (!tree(stamp.getValue(), existing, incoming).equals(actual)) {
        throw new UnsupportedClientOperationException(
            UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_REWRITE,
            String.format(
                "COLUMN_DEFAULT_REWRITE: %s.%s still has a column default on %s. This"
                    + " overwrite/replace did not send a matching initial-default. Retry from"
                    + " Spark 3.1 or Spark 3.5 using the jars on the standard client image."
                    + " Unaware clients cannot overwrite or replace a table that has column"
                    + " defaults.",
                incoming.getDatabaseId(),
                incoming.getTableId(),
                fieldLabel(schema, stamp.getKey())));
      }
    }
  }

  private TableDto stripInitialDefaults(TableDto existing, TableDto incoming) {
    String schema = strip(incoming.getSchema(), existing, incoming);
    List<String> intermediates = strip(incoming.getNewIntermediateSchemas(), existing, incoming);
    if (Objects.equals(schema, incoming.getSchema())
        && Objects.equals(intermediates, incoming.getNewIntermediateSchemas())) {
      return incoming;
    }
    return incoming.toBuilder().schema(schema).newIntermediateSchemas(intermediates).build();
  }

  /**
   * Main-branch snapshot only, so history in {@code jsonSnapshots} is not "this commit." WAP
   * overwrite is therefore missed. Fix with #669 deltas, not a ref-map diff:
   * https://github.com/linkedin/openhouse/issues/693
   */
  private boolean isRewrite(TableDto existing, TableDto incoming) {
    if (incoming.isReplaceCommit() || incoming.isStageReplace()) {
      return true;
    }
    List<String> jsonSnapshots = incoming.getJsonSnapshots();
    if (jsonSnapshots == null || jsonSnapshots.isEmpty()) {
      return false;
    }
    String operation = currentSnapshot(existing, incoming, jsonSnapshots).operation();
    return DataOperations.OVERWRITE.equals(operation) || DataOperations.REPLACE.equals(operation);
  }

  private static Snapshot currentSnapshot(
      TableDto existing, TableDto incoming, List<String> jsonSnapshots) {
    Long mainId = mainSnapshotId(existing, incoming);
    if (mainId != null) {
      for (String json : jsonSnapshots) {
        Snapshot snapshot = snapshot(json, existing, incoming);
        if (snapshot.snapshotId() == mainId) {
          return snapshot;
        }
      }
      throw ReadBridgeConfigResolver.unusable(
          incoming, existing, "main-branch snapshot is missing from the request", null);
    }
    return snapshot(jsonSnapshots.get(jsonSnapshots.size() - 1), existing, incoming);
  }

  private static Long mainSnapshotId(TableDto existing, TableDto incoming) {
    Map<String, String> snapshotRefs = incoming.getSnapshotRefs();
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
      throw ReadBridgeConfigResolver.unusable(incoming, existing, "unreadable snapshot ref", e);
    }
  }

  private static Snapshot snapshot(String json, TableDto existing, TableDto incoming) {
    if (json == null || json.isEmpty()) {
      throw ReadBridgeConfigResolver.unusable(incoming, existing, "unreadable snapshot", null);
    }
    try {
      return SnapshotParser.fromJson(json);
    } catch (RuntimeException e) {
      throw ReadBridgeConfigResolver.unusable(incoming, existing, "unreadable snapshot", e);
    }
  }

  /**
   * Drop every {@code initial-default} so a handshake, a ramp-off leftover, or an unstamped writer
   * default cannot land in Iceberg metadata.
   */
  private static String strip(String schemaJson, TableDto existing, TableDto incoming) {
    if (schemaJson == null || schemaJson.isEmpty()) {
      return schemaJson;
    }
    JsonNode root = tree(schemaJson, existing, incoming);
    boolean changed = false;
    for (JsonNode field : fieldObjects(root)) {
      if (field instanceof ObjectNode
          && ((ObjectNode) field).remove(SchemaKeys.INITIAL_DEFAULT) != null) {
        changed = true;
      }
    }
    if (!changed) {
      return schemaJson;
    }
    try {
      return MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw ReadBridgeConfigResolver.unusable(
          incoming, existing, "read-bridge: failed to strip initial-default from schema", e);
    }
  }

  private static List<String> strip(List<String> schemas, TableDto existing, TableDto incoming) {
    if (schemas == null || schemas.isEmpty()) {
      return schemas;
    }
    List<String> stripped = new ArrayList<>(schemas.size());
    boolean changed = false;
    for (String schema : schemas) {
      String next = strip(schema, existing, incoming);
      stripped.add(next);
      changed |= !Objects.equals(next, schema);
    }
    return changed ? stripped : schemas;
  }

  private static Set<Integer> fieldIds(JsonNode schema) {
    Set<Integer> ids = new HashSet<>();
    for (JsonNode field : fieldObjects(schema)) {
      if (field.has(SchemaKeys.ID)) {
        ids.add(field.get(SchemaKeys.ID).asInt());
      }
    }
    return ids;
  }

  private static String fieldLabel(JsonNode schema, int fieldId) {
    for (JsonNode field : fieldObjects(schema)) {
      if (field.has(SchemaKeys.ID) && field.get(SchemaKeys.ID).asInt() == fieldId) {
        JsonNode name = field.get(SchemaKeys.NAME);
        if (name != null && name.isTextual() && !name.asText().isEmpty()) {
          return name.asText() + " (field-id " + fieldId + ")";
        }
        break;
      }
    }
    return "field-id " + fieldId;
  }

  private static JsonNode initialDefault(JsonNode schema, int fieldId) {
    for (JsonNode field : fieldObjects(schema)) {
      if (field.has(SchemaKeys.ID) && field.get(SchemaKeys.ID).asInt() == fieldId) {
        return field.get(SchemaKeys.INITIAL_DEFAULT);
      }
    }
    return null;
  }

  private static List<JsonNode> fieldObjects(JsonNode schema) {
    List<JsonNode> found = schema.findParents(SchemaKeys.ID);
    return found == null ? Collections.emptyList() : found;
  }

  private static JsonNode tree(String json, TableDto existing, TableDto incoming) {
    if (json == null || json.isEmpty()) {
      throw ReadBridgeConfigResolver.unusable(incoming, existing, "unreadable json", null);
    }
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw ReadBridgeConfigResolver.unusable(incoming, existing, "unreadable json", e);
    }
  }
}

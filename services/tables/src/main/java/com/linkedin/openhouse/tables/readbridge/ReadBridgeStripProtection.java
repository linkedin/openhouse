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
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Type 1 / Type 2 strip protection for column defaults. Default-aware clients send {@code
 * initial-default} on the commit schema; this class uses that as the handshake, then drops those
 * keys before persist so overlays cannot land in Iceberg metadata.
 *
 * <p>The PUT includes the table's full snapshot list, so rewrite detection uses the main-branch
 * snapshot (the commit being applied), not historical overwrite snapshots still in the list.
 */
@Slf4j
@Component
public class ReadBridgeStripProtection {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ID = "id";
  private static final String INITIAL_DEFAULT = "initial-default";

  private final ReadBridgeConfigResolver resolver;
  private final ColumnDefaultsSource columnDefaultsSource;

  public ReadBridgeStripProtection(
      ReadBridgeConfigResolver resolver, ColumnDefaultsSource columnDefaultsSource) {
    this.resolver = resolver;
    this.columnDefaultsSource = columnDefaultsSource;
  }

  @Autowired
  ReadBridgeStripProtection(
      ObjectProvider<ReadBridgeConfigResolver> resolver,
      ObjectProvider<ColumnDefaultsSource> source) {
    this(
        resolver.getIfAvailable(() -> null),
        source.getIfAvailable(() -> ColumnDefaultsSource.NONE));
  }

  /**
   * Reject Type 1 / Type 2 violations, then strip stamped {@code initial-default} from {@code
   * incoming} so they are not persisted. Returns {@code incoming} unchanged when there is nothing
   * to check or drop.
   */
  public TableDto prepare(TableDto existing, TableDto incoming) {
    if (incoming == null || resolver == null || columnDefaultsSource == ColumnDefaultsSource.NONE) {
      return incoming;
    }
    if (existing != null) {
      rejectRemovedDefaults(existing, incoming);
      rejectUnawareRewrite(existing, incoming);
    }
    return stripStampedDefaults(existing, incoming);
  }

  private void rejectRemovedDefaults(TableDto existing, TableDto incoming) {
    Set<Integer> previousIds = defaultsOrEmpty(existing).keySet();
    if (previousIds.isEmpty()) {
      return;
    }
    Set<Integer> incomingIds = defaultsOrEmpty(incoming).keySet();
    Set<Integer> incomingFieldIds = fieldIds(incoming.getSchema());
    for (Integer fieldId : previousIds) {
      if (incomingFieldIds.contains(fieldId) && !incomingIds.contains(fieldId)) {
        throw new UnsupportedClientOperationException(
            UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_REMOVED,
            String.format(
                "Table %s.%s cannot drop the column default on field-id %s while the column remains",
                incoming.getDatabaseId(), incoming.getTableId(), fieldId));
      }
    }
  }

  private void rejectUnawareRewrite(TableDto existing, TableDto incoming) {
    Set<Integer> stampedIds = stampedFieldIds(resolver.resolve(existing));
    if (stampedIds.isEmpty() || !isRewrite(incoming)) {
      return;
    }
    if (!fieldIdsWithInitialDefault(incoming.getSchema()).containsAll(stampedIds)) {
      throw new UnsupportedClientOperationException(
          UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_REWRITE,
          String.format(
              "Table %s.%s declares column defaults; overwrite/replace requires initial-default on field-ids %s",
              incoming.getDatabaseId(), incoming.getTableId(), stampedIds));
    }
  }

  private TableDto stripStampedDefaults(TableDto existing, TableDto incoming) {
    Set<Integer> stripIds = new HashSet<>();
    if (existing != null) {
      stripIds.addAll(stampedFieldIds(resolver.resolve(existing)));
    }
    stripIds.addAll(stampedFieldIds(resolver.resolve(incoming)));
    if (stripIds.isEmpty()) {
      return incoming;
    }
    String schema = stripInitialDefaults(incoming.getSchema(), stripIds);
    List<String> intermediates =
        stripInitialDefaults(incoming.getNewIntermediateSchemas(), stripIds);
    if (Objects.equals(schema, incoming.getSchema())
        && intermediatesEquals(intermediates, incoming.getNewIntermediateSchemas())) {
      return incoming;
    }
    return incoming.toBuilder().schema(schema).newIntermediateSchemas(intermediates).build();
  }

  private static boolean isRewrite(TableDto incoming) {
    if (incoming.isReplaceCommit() || incoming.isStageReplace()) {
      return true;
    }
    String operation = currentSnapshotOperation(incoming);
    return DataOperations.OVERWRITE.equals(operation) || DataOperations.REPLACE.equals(operation);
  }

  /**
   * The PUT repeats snapshot history. Only the main-branch snapshot is the commit being applied;
   * fall back to the last JSON snapshot when refs are absent.
   */
  private static String currentSnapshotOperation(TableDto incoming) {
    List<String> jsonSnapshots = incoming.getJsonSnapshots();
    if (jsonSnapshots == null || jsonSnapshots.isEmpty()) {
      return null;
    }
    Long currentSnapshotId = mainSnapshotId(incoming.getSnapshotRefs());
    if (currentSnapshotId != null) {
      for (String json : jsonSnapshots) {
        Snapshot snapshot = parseSnapshot(json);
        if (snapshot != null && snapshot.snapshotId() == currentSnapshotId) {
          return snapshot.operation();
        }
      }
    }
    Snapshot last = parseSnapshot(jsonSnapshots.get(jsonSnapshots.size() - 1));
    return last == null ? null : last.operation();
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
      return null;
    }
  }

  private static Snapshot parseSnapshot(String json) {
    try {
      return SnapshotParser.fromJson(json);
    } catch (RuntimeException e) {
      return null;
    }
  }

  private Map<Integer, JsonNode> defaultsOrEmpty(TableDto tableDto) {
    try {
      Map<Integer, JsonNode> defaults = columnDefaultsSource.defaults(tableDto);
      return defaults == null ? Collections.emptyMap() : defaults;
    } catch (RuntimeException e) {
      log.warn(
          "read-bridge: column-defaults source failed for {}.{}; skipping strip protection",
          tableDto.getDatabaseId(),
          tableDto.getTableId(),
          e);
      return Collections.emptyMap();
    }
  }

  static Set<Integer> stampedFieldIds(Map<String, String> config) {
    if (config == null || config.isEmpty()) {
      return Collections.emptySet();
    }
    Set<Integer> ids = new HashSet<>();
    String prefix = ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX;
    for (String key : config.keySet()) {
      if (!key.startsWith(prefix)) {
        continue;
      }
      try {
        ids.add(Integer.parseInt(key.substring(prefix.length())));
      } catch (NumberFormatException ignored) {
        // Encoder only stamps int field-ids; ignore anything else.
      }
    }
    return ids;
  }

  static Set<Integer> fieldIds(String schemaJson) {
    return collectFieldIds(schemaJson, false);
  }

  static Set<Integer> fieldIdsWithInitialDefault(String schemaJson) {
    return collectFieldIds(schemaJson, true);
  }

  private static Set<Integer> collectFieldIds(String schemaJson, boolean requireInitialDefault) {
    JsonNode root = readTree(schemaJson);
    if (root == null) {
      return Collections.emptySet();
    }
    Set<Integer> ids = new HashSet<>();
    for (JsonNode field : fieldObjects(root)) {
      if (!field.has(ID) || (requireInitialDefault && !field.has(INITIAL_DEFAULT))) {
        continue;
      }
      ids.add(field.get(ID).asInt());
    }
    return ids;
  }

  static String stripInitialDefaults(String schemaJson, Set<Integer> fieldIds) {
    if (schemaJson == null || fieldIds == null || fieldIds.isEmpty()) {
      return schemaJson;
    }
    JsonNode root = readTree(schemaJson);
    if (root == null) {
      return schemaJson;
    }
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

  private static List<String> stripInitialDefaults(List<String> schemas, Set<Integer> fieldIds) {
    if (schemas == null || schemas.isEmpty() || fieldIds.isEmpty()) {
      return schemas;
    }
    List<String> stripped = new ArrayList<>(schemas.size());
    boolean changed = false;
    for (String schema : schemas) {
      String next = stripInitialDefaults(schema, fieldIds);
      stripped.add(next);
      changed |= next != schema;
    }
    return changed ? stripped : schemas;
  }

  private static List<JsonNode> fieldObjects(JsonNode schema) {
    List<JsonNode> found = schema.findParents(ID);
    return found == null ? Collections.emptyList() : found;
  }

  private static JsonNode readTree(String json) {
    if (json == null || json.isEmpty()) {
      return null;
    }
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  private static boolean intermediatesEquals(List<String> left, List<String> right) {
    if (left == right) {
      return true;
    }
    if (left == null || right == null) {
      return false;
    }
    return left.equals(right);
  }
}

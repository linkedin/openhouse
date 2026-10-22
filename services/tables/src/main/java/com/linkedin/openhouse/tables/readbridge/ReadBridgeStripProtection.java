package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.openhouse.common.exception.CommitRejectedException;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.internal.catalog.PendingCommit;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

/**
 * Type 1 / Type 2 strip protection for column defaults. Default-aware clients send {@code
 * initial-default} on the commit schema; this class uses that as the handshake, then drops every
 * {@code initial-default} before persist so overlays cannot land in Iceberg metadata.
 *
 * <p>The catalog applies the rules only after its stale-base check, so they always judge a commit
 * against the table's current version. A commit built on an older version gets the retryable
 * version conflict instead, and the client reloads the table, defaults included, before it retries:
 * a default-aware writer that loaded the table before it opted in succeeds on the retry.
 *
 * <p>Type 2 counts a client as default-aware when it declares the {@code column-defaults}
 * capability ({@link ClientCapabilityGate#declares}). That is trust, not proof that data files were
 * rewritten correctly. Do not opt a table in until OpenHouse rewrites are trusted or default-aware
 * compaction exists. A lift/compaction flag is a follow-up, not this class.
 *
 * <p>Type 2 applies to every write that commits snapshots to a table with column defaults: a
 * snapshots PUT on any branch, or a replace. It does not matter whether a snapshot in the PUT
 * rewrites data or is new at all, so the check needs nothing from the table's existing snapshots. A
 * client that is not default-aware cannot append, overwrite, move refs or expire snapshots on such
 * a table. See https://github.com/linkedin/openhouse/issues/693.
 *
 * <p>A request with the column-default policy bypass ({@link
 * TablesMvcConstants#HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS}) skips the rules and the default
 * lookups they need. {@code initial-default} is still dropped.
 *
 * <p>Schema field objects are the JSON nodes that carry {@code id} — the same walk as the client
 * overlay. On Iceberg schema JSON that is NestedField; {@code element-id} / {@code schema-id} are
 * different keys.
 */
public class ReadBridgeStripProtection {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String COLUMN_DEFAULTS = ClientCapabilityGate.COLUMN_DEFAULTS;

  private static final class SchemaKeys {
    private static final String ID = "id";
    private static final String INITIAL_DEFAULT = "initial-default";

    private SchemaKeys() {}
  }

  private final ReadBridgeConfigResolver resolver;

  public ReadBridgeStripProtection(ReadBridgeConfigResolver resolver) {
    this.resolver = resolver;
  }

  /**
   * Drops every {@code initial-default} from {@code incoming} so overlays cannot land in Iceberg
   * metadata. Returns {@code incoming} unchanged when there is nothing to drop.
   *
   * @throws ColumnDefaultException if {@code incoming} has a schema this protection cannot read
   */
  public TableDto prepare(TableDto incoming) throws ColumnDefaultException {
    if (incoming == null) {
      return incoming;
    }
    return stripInitialDefaults(incoming);
  }

  /**
   * Checks {@code commit} against the table's current version: the defaults the base declares, then
   * the ones the commit declares, must be usable, and the commit must pass Type 1 and Type 2. A
   * commit that creates the table only has its declared defaults checked.
   *
   * @param clients the gate that says whether the commit's client declares the {@code
   *     column-defaults} capability and words the rejection
   * @throws TableConfigUnavailableException if a default the base declares cannot be applied
   * @throws CommitRejectedException if a default the commit declares cannot be applied, or the
   *     commit breaks Type 1 or Type 2
   */
  public void enforce(PendingCommit commit, ClientCapabilityGate clients) {
    TableIdentifier table = commit.getTableIdentifier();
    TableMetadata base = commit.getBase().orElse(null);
    Schema schema = commit.getMetadata().schema();
    String location = base == null ? null : base.metadataFileLocation();
    Map<Integer, String> previousStamped =
        base == null || !resolver.isBridged(base.properties())
            ? Collections.emptyMap()
            : resolver.storedColumnDefaults(
                tableDto(table, base.schema(), base.properties(), location));
    boolean bridged = resolver.isBridged(commit.getProperties());
    Map<Integer, String> declared =
        bridged
            ? declaredColumnDefaults(tableDto(table, schema, commit.getProperties(), location))
            : Collections.emptyMap();
    if (base != null) {
      rejectRemovedDefaults(table, previousStamped, declared, bridged, schema);
      rejectUnawareCommit(table, previousStamped, schema, clients, commit.isSnapshotCommit());
    }
  }

  /** The defaults {@code committed} declares; one that cannot be applied rejects the commit. */
  private Map<Integer, String> declaredColumnDefaults(TableDto committed) {
    try {
      return resolver.incomingColumnDefaults(committed);
    } catch (ColumnDefaultException e) {
      throw new CommitRejectedException(e.getMessage(), e);
    }
  }

  /**
   * Type 1: a still-present stamped column must keep its default. Tables that are unstamped or not
   * opted in skip — there is no default to protect.
   */
  private static void rejectRemovedDefaults(
      TableIdentifier table,
      Map<Integer, String> previousStamped,
      Map<Integer, String> declared,
      boolean bridged,
      Schema schema) {
    if (previousStamped.isEmpty() || !bridged) {
      return;
    }
    for (Integer fieldId : previousStamped.keySet()) {
      Types.NestedField field = schema.findField(fieldId);
      if (field != null && !declared.containsKey(fieldId)) {
        throw new CommitRejectedException(
            String.format(
                "COLUMN_DEFAULT_REMOVED: %s still has a column default on %s, and this commit"
                    + " omitted it. %s",
                table, fieldLabel(field), ClientCapabilityGate.remedy(COLUMN_DEFAULTS)));
      }
    }
  }

  /**
   * Type 2: a write that commits snapshots must come from a default-aware client. That is trust,
   * not proof the files were rewritten.
   */
  private static void rejectUnawareCommit(
      TableIdentifier table,
      Map<Integer, String> previousStamped,
      Schema schema,
      ClientCapabilityGate clients,
      boolean snapshotCommit) {
    if (previousStamped.isEmpty() || !snapshotCommit || clients.declares(COLUMN_DEFAULTS)) {
      return;
    }
    if (previousStamped.keySet().stream().map(schema::findField).anyMatch(Objects::nonNull)) {
      throw new CommitRejectedException(
          clients.missingCapability(table.toString(), COLUMN_DEFAULTS));
    }
  }

  private TableDto stripInitialDefaults(TableDto incoming) throws ColumnDefaultException {
    String schema = strip(incoming.getSchema(), incoming);
    List<String> intermediates = strip(incoming.getNewIntermediateSchemas(), incoming);
    if (Objects.equals(schema, incoming.getSchema())
        && Objects.equals(intermediates, incoming.getNewIntermediateSchemas())) {
      return incoming;
    }
    return incoming.toBuilder().schema(schema).newIntermediateSchemas(intermediates).build();
  }

  /** The table as a {@link ColumnDefaultsSource} reads it. */
  private static TableDto tableDto(
      TableIdentifier table, Schema schema, Map<String, String> properties, String location) {
    return TableDto.builder()
        .databaseId(table.namespace().toString())
        .tableId(table.name())
        .tableLocation(location)
        .schema(SchemaParser.toJson(schema))
        .tableProperties(properties)
        .build();
  }

  /**
   * Drops every {@code initial-default} from {@code json}, a schema, so an overlay, a leftover on a
   * table that is not opted in, or an unstamped writer default cannot land in Iceberg metadata.
   * Returns {@code json} when there was nothing to drop. Writes require a schema, and every
   * intermediate schema is one, so a missing schema is metadata this protection cannot read.
   */
  private static String strip(String json, TableDto incoming) throws ColumnDefaultException {
    if (json == null || json.isEmpty()) {
      throw unusable(incoming, "missing schema", null);
    }
    JsonNode root = tree(json, incoming);
    boolean changed = false;
    for (JsonNode field : fieldObjects(root)) {
      if (field instanceof ObjectNode
          && ((ObjectNode) field).remove(SchemaKeys.INITIAL_DEFAULT) != null) {
        changed = true;
      }
    }
    if (!changed) {
      return json;
    }
    try {
      return MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw unusable(incoming, "failed to strip initial-default from schema", e);
    }
  }

  private static List<String> strip(List<String> schemas, TableDto incoming)
      throws ColumnDefaultException {
    if (schemas == null || schemas.isEmpty()) {
      return schemas;
    }
    List<String> stripped = new ArrayList<>(schemas.size());
    boolean changed = false;
    for (String schema : schemas) {
      String next = strip(schema, incoming);
      stripped.add(next);
      changed |= !Objects.equals(next, schema);
    }
    return changed ? stripped : schemas;
  }

  private static String fieldLabel(Types.NestedField field) {
    return field.name() == null || field.name().isEmpty()
        ? "field-id " + field.fieldId()
        : field.name() + " (field-id " + field.fieldId() + ")";
  }

  private static List<JsonNode> fieldObjects(JsonNode schema) {
    List<JsonNode> found = schema.findParents(SchemaKeys.ID);
    return found == null ? Collections.emptyList() : found;
  }

  private static JsonNode tree(String json, TableDto incoming) throws ColumnDefaultException {
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw unusable(incoming, "unreadable json", e);
    }
  }

  /** {@code incoming} data this protection cannot read. */
  private static ColumnDefaultException unusable(
      TableDto incoming, String reason, Throwable cause) {
    return new ColumnDefaultException(
        String.format(
            "COLUMN_DEFAULT_UNUSABLE: OpenHouse could not validate column defaults on %s.%s"
                + " (metadata %s). Cause: %s. Contact the OpenHouse team with the Spark"
                + " application logs.",
            incoming.getDatabaseId(), incoming.getTableId(), incoming.getTableLocation(), reason),
        cause);
  }
}

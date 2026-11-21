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
import java.util.Set;
import java.util.TreeSet;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Type 1 / Type 2 strip protection for column defaults. Type 1 holds a table that has opted in to
 * Iceberg's rules for {@code initial-default}: a commit declares a default only on a column it
 * adds, and keeps every stamped column's default, unchanged. Type 2 asks {@link
 * ClientCapabilityGate} whether the commit's client is default-aware. Every {@code initial-default}
 * is dropped before persist so overlays cannot land in Iceberg metadata.
 *
 * <p>The commit that opts a table in, or creates it opted in, may declare a default on any column
 * the table has. Type 1 applies to every commit after it.
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
 * <p>A declared default must also fit its column's type in every schema version of the table, the
 * versions time travel, branches and tags read with: clients overlay it on all of them, and one it
 * does not fit fails every load of the table.
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
   * the ones the commit declares, must be usable, the commit must pass Type 1, its defaults must
   * fit every schema version, and it must pass Type 2. Type 1 comes first, so a changed default is
   * refused as such even when it also does not fit. A commit that creates the table or opts it in
   * only has its declared defaults checked.
   *
   * @param clients the gate that says whether the commit's client declares the {@code
   *     column-defaults} capability and words the rejection
   * @throws TableConfigUnavailableException if a default the base declares cannot be applied
   * @throws CommitRejectedException if a default the commit declares cannot be applied or does not
   *     fit a schema version, or the commit breaks Type 1 or Type 2
   */
  public void enforce(PendingCommit commit, ClientCapabilityGate clients) {
    TableIdentifier table = commit.getTableIdentifier();
    TableMetadata base = commit.getBase().orElse(null);
    Schema schema = commit.getMetadata().schema();
    String location = base == null ? null : base.metadataFileLocation();
    boolean optedInBefore = base != null && resolver.isBridged(base.properties());
    Map<Integer, String> previousStamped =
        optedInBefore
            ? resolver.storedColumnDefaults(
                tableDto(table, base.schema(), base.properties(), location))
            : Collections.emptyMap();
    boolean optedInAfter = resolver.isBridged(commit.getProperties());
    Map<Integer, String> declared =
        optedInAfter
            ? declaredColumnDefaults(tableDto(table, schema, commit.getProperties(), location))
            : Collections.emptyMap();
    if (optedInBefore) {
      rejectChangedDefaults(table, previousStamped, base.schema(), declared, schema);
    }
    rejectDefaultsThatDoNotFit(table, declared, commit.getMetadata().schemas());
    rejectUnawareCommit(table, previousStamped, schema, clients, commit.isSnapshotCommit());
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
   * Each declared default must fit its column's type in every schema version that has the column,
   * parsed as clients parse it when they overlay it.
   */
  private static void rejectDefaultsThatDoNotFit(
      TableIdentifier table, Map<Integer, String> declared, List<Schema> versions) {
    for (Schema version : versions) {
      for (Map.Entry<Integer, String> entry : declared.entrySet()) {
        Types.NestedField field = version.findField(entry.getKey());
        if (field == null) {
          continue;
        }
        try {
          SingleValueParser.fromJson(field.type(), entry.getValue());
        } catch (RuntimeException e) {
          throw new CommitRejectedException(
              String.format(
                  "COLUMN_DEFAULT_UNUSABLE: OpenHouse cannot apply the column defaults declared on"
                      + " %s (TYPE_MISMATCH, column %s, field ID %d). The default does not fit %s,"
                      + " the column's type in schema version %d. Defaults apply to every schema"
                      + " version, so declare a default that fits every type the column has had.",
                  table,
                  version.findColumnName(field.fieldId()),
                  field.fieldId(),
                  field.type(),
                  version.schemaId()),
              e);
        }
      }
    }
  }

  /**
   * Type 1: a column the table already has keeps its default as it is, as Iceberg sets {@code
   * initial-default} only when it adds a field and never changes it. Rewrites store the default as
   * a value in the files they write, so a default that came later, changed or went away would read
   * differently in rewritten and untouched files.
   */
  private static void rejectChangedDefaults(
      TableIdentifier table,
      Map<Integer, String> previousStamped,
      Schema previousSchema,
      Map<Integer, String> declared,
      Schema schema) {
    Set<Integer> fieldIds = new TreeSet<>(previousStamped.keySet());
    fieldIds.addAll(declared.keySet());
    for (Integer fieldId : fieldIds) {
      Types.NestedField previous = previousSchema.findField(fieldId);
      Types.NestedField field = schema.findField(fieldId);
      if (previous == null || field == null) {
        continue;
      }
      String stamped = previousStamped.get(fieldId);
      String value = declared.get(fieldId);
      if (value == null) {
        throw new CommitRejectedException(
            String.format(
                "COLUMN_DEFAULT_REMOVED: %s still has a column default on %s, and this commit"
                    + " omitted it. %s",
                table, fieldLabel(field), ClientCapabilityGate.remedy(COLUMN_DEFAULTS)));
      }
      if (stamped == null || !sameDefault(previous.type(), stamped, value)) {
        throw new CommitRejectedException(
            String.format(
                "COLUMN_DEFAULT_CHANGED: this commit to %s changes the column default on %s. A"
                    + " column gets a default only in the commit that adds the column, and keeps"
                    + " it unchanged.",
                table, fieldLabel(field)));
      }
    }
  }

  /**
   * Whether two defaults are the same value of {@code type}, the column's type before the commit. A
   * promotion can render an unchanged default anew for the wider type: a float 0.1 becomes the
   * double 0.10000000149011612.
   */
  private static boolean sameDefault(Type type, String previous, String declared) {
    if (previous.equals(declared)) {
      return true;
    }
    try {
      return SingleValueParser.toJson(type, SingleValueParser.fromJson(type, previous))
          .equals(SingleValueParser.toJson(type, SingleValueParser.fromJson(type, declared)));
    } catch (RuntimeException e) {
      // One of them is not a value of the type, such as 8000000000 for an int.
      return false;
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

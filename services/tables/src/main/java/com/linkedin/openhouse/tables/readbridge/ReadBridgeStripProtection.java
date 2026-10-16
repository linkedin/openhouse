package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.internal.catalog.TableCommit;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
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
 * initial-default} on the commit schema. {@link #prepare} drops every {@code initial-default}
 * before persist, so overlays cannot land in Iceberg metadata, and records the values in the
 * request's {@link CatalogRequestContext} as the column-default handshake. {@link
 * ColumnDefaultCommitPolicy} reads it there and has {@link #enforce} check the commit inside the
 * catalog's {@code doCommit}.
 *
 * <p>The catalog applies the rules only after its stale-base check, so they always judge a commit
 * against the table's current version. A commit built on an older version gets the retryable
 * version conflict instead, and the client reloads the table, defaults included, before it retries:
 * a default-aware writer that loaded the table before it opted in succeeds on the retry.
 *
 * <p>The handshake is trust, not proof that data files were rewritten correctly. Do not opt a table
 * in until OpenHouse rewrites are trusted or default-aware compaction exists. A lift/compaction
 * flag is a follow-up, not this class.
 *
 * <p>Type 2 applies to every write that commits snapshots to a table with column defaults: a
 * snapshots PUT on any branch, or a replace. It does not matter whether a snapshot in the PUT
 * rewrites data or is new at all, so the check needs nothing from the table's existing snapshots. A
 * client without the handshake cannot append, overwrite, move refs or expire snapshots on such a
 * table. See https://github.com/linkedin/openhouse/issues/693.
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

  private static final class SchemaKeys {
    private static final String ID = "id";
    private static final String INITIAL_DEFAULT = "initial-default";

    private SchemaKeys() {}
  }

  private final ReadBridgeConfigResolver resolver;

  private final CatalogRequestContext requestContext;

  public ReadBridgeStripProtection(
      ReadBridgeConfigResolver resolver, CatalogRequestContext requestContext) {
    this.resolver = Objects.requireNonNull(resolver, "resolver");
    this.requestContext = Objects.requireNonNull(requestContext, "requestContext");
  }

  /**
   * Drops every {@code initial-default} from {@code incoming} so overlays cannot land in Iceberg
   * metadata, and records the values its schema sent as the request's column-default handshake,
   * which subjects the commit to {@link #enforce}.
   *
   * @throws ColumnDefaultException if {@code incoming} has a schema this protection cannot read
   */
  public TableDto prepare(TableDto incoming) throws ColumnDefaultException {
    if (incoming == null) {
      return incoming;
    }
    ObjectNode handshake = MAPPER.createObjectNode();
    String schema = strip(incoming.getSchema(), incoming, handshake);
    List<String> intermediates = strip(incoming.getNewIntermediateSchemas(), incoming);
    requestContext.recordColumnDefaultHandshake(handshake.toString());
    return incoming.toBuilder().schema(schema).newIntermediateSchemas(intermediates).build();
  }

  /**
   * Checks a commit that carries the column-default {@code handshake} against the table's current
   * version: the defaults the base declares, then the ones the commit declares, must be usable, and
   * the commit must pass Type 1 and Type 2. A commit that creates the table only has its declared
   * defaults checked.
   *
   * @throws TableConfigUnavailableException if a default the base declares cannot be applied
   * @throws ColumnDefaultException Type 1, Type 2, or a default the commit declares that cannot be
   *     applied
   */
  public void enforce(TableCommit commit, String handshake) throws ColumnDefaultException {
    TableIdentifier table = commit.getTableIdentifier();
    TableMetadata base = commit.getBase().orElse(null);
    Schema schema = commit.getSchema();
    String location = base == null ? null : base.metadataFileLocation();
    Map<Integer, String> previousStamped =
        base == null || !resolver.isBridged(base.properties())
            ? Collections.emptyMap()
            : resolver.storedColumnDefaults(
                tableDto(table, base.schema(), base.properties(), location));
    boolean bridged = resolver.isBridged(commit.getProperties());
    Map<Integer, String> declared =
        bridged
            ? resolver.incomingColumnDefaults(
                tableDto(table, schema, commit.getProperties(), location))
            : Collections.emptyMap();
    if (base != null) {
      rejectRemovedDefaults(table, previousStamped, declared, bridged, schema);
      rejectUnawareCommit(
          table, location, previousStamped, schema, handshake, commit.isSnapshotCommit());
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
      Schema schema)
      throws ColumnDefaultException {
    if (previousStamped.isEmpty() || !bridged) {
      return;
    }
    for (Integer fieldId : previousStamped.keySet()) {
      Types.NestedField field = schema.findField(fieldId);
      if (field != null && !declared.containsKey(fieldId)) {
        throw new ColumnDefaultException(
            String.format(
                "COLUMN_DEFAULT_REMOVED: %s.%s still has a column default on %s. This commit"
                    + " omitted it. Retry from Spark 3.1 or Spark 3.5 using the jars on the"
                    + " standard client image. Column defaults cannot be removed or changed."
                    + TablesMvcConstants.COLUMN_DEFAULT_POLICY_BYPASS_HINT,
                table.namespace(),
                table.name(),
                fieldLabel(field)));
      }
    }
  }

  /**
   * Type 2: a write that commits snapshots must send {@code initial-default} equal to the stamp.
   * That handshake is trust, not proof the files were rewritten.
   */
  private static void rejectUnawareCommit(
      TableIdentifier table,
      String location,
      Map<Integer, String> previousStamped,
      Schema schema,
      String handshake,
      boolean snapshotCommit)
      throws ColumnDefaultException {
    if (previousStamped.isEmpty() || !snapshotCommit) {
      return;
    }
    JsonNode sent =
        handshake == null || handshake.isEmpty()
            ? MAPPER.createObjectNode()
            : tree(handshake, table, location);
    for (Map.Entry<Integer, String> stamp : previousStamped.entrySet()) {
      Types.NestedField field = schema.findField(stamp.getKey());
      if (field != null
          && !tree(stamp.getValue(), table, location)
              .equals(sent.get(String.valueOf(stamp.getKey())))) {
        throw new ColumnDefaultException(
            String.format(
                "COLUMN_DEFAULT_UNAWARE_COMMIT: %s.%s has a column default on %s, and this commit"
                    + " did not send it as a matching initial-default. Commit from Spark 3.1 or"
                    + " Spark 3.5 using the jars on the standard client image."
                    + TablesMvcConstants.COLUMN_DEFAULT_POLICY_BYPASS_HINT,
                table.namespace(),
                table.name(),
                fieldLabel(field)));
      }
    }
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
   * Drops every {@code initial-default} from {@code json}, a schema, so a handshake, a leftover on
   * a table that is not opted in, or an unstamped writer default cannot land in Iceberg metadata,
   * and records each dropped value in {@code handshake} by field id unless it is null. Returns
   * {@code json} when there was nothing to drop.
   */
  private static String strip(String json, TableDto incoming, ObjectNode handshake)
      throws ColumnDefaultException {
    if (json == null || json.isEmpty()) {
      return json;
    }
    JsonNode root = tree(json, incoming);
    boolean changed = false;
    for (JsonNode field : fieldObjects(root)) {
      JsonNode dropped =
          field instanceof ObjectNode
              ? ((ObjectNode) field).remove(SchemaKeys.INITIAL_DEFAULT)
              : null;
      if (dropped == null) {
        continue;
      }
      changed = true;
      String fieldId = field.get(SchemaKeys.ID).asText();
      if (handshake != null && !handshake.has(fieldId)) {
        handshake.set(fieldId, dropped);
      }
    }
    if (!changed) {
      return json;
    }
    try {
      return MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw unusable(
          incoming.getDatabaseId(),
          incoming.getTableId(),
          incoming.getTableLocation(),
          "failed to strip initial-default from schema",
          e);
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
      String next = strip(schema, incoming, null);
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
    return tree(json, incoming.getDatabaseId(), incoming.getTableId(), incoming.getTableLocation());
  }

  private static JsonNode tree(String json, TableIdentifier table, String location)
      throws ColumnDefaultException {
    return tree(json, table.namespace().toString(), table.name(), location);
  }

  private static JsonNode tree(String json, String databaseId, String tableId, String location)
      throws ColumnDefaultException {
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw unusable(databaseId, tableId, location, "unreadable json", e);
    }
  }

  /** Data this protection cannot read. */
  private static ColumnDefaultException unusable(
      String databaseId, String tableId, String location, String reason, Throwable cause) {
    return new ColumnDefaultException(
        String.format(
            "COLUMN_DEFAULT_UNUSABLE: OpenHouse could not validate column defaults on %s.%s"
                + " (metadata %s). Cause: %s. Contact the OpenHouse team with the Spark"
                + " application logs.",
            databaseId, tableId, location, reason),
        cause);
  }
}

package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Type 1 / Type 2 strip protection for column defaults. Default-aware clients send {@code
 * initial-default} on the commit schema; this class uses that as the handshake, then drops every
 * {@code initial-default} before persist so overlays cannot land in Iceberg metadata.
 *
 * <p>The handshake is trust, not proof that data files were rewritten correctly. Do not ramp a
 * table until OpenHouse rewrites are trusted or default-aware compaction exists. A lift/compaction
 * flag is a follow-up, not this class.
 *
 * <p>Type 2 applies to every write that commits snapshots to a table with column defaults: a
 * snapshots PUT on any branch, or a replace. It does not matter whether a snapshot in the PUT
 * rewrites data or is new at all, so the check needs nothing from the table's existing snapshots. A
 * client without the handshake cannot append, overwrite, move refs or expire snapshots on such a
 * table. See https://github.com/linkedin/openhouse/issues/693.
 *
 * <p>A request with the column-default policy bypass ({@link
 * TablesMvcConstants#HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS}) skips Type 1, Type 2 and the
 * default lookups they need. {@code initial-default} is still dropped.
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

  private final CatalogRequestContext requestContext;

  /** Outside a request: the column-default policy bypass is never set. */
  public ReadBridgeStripProtection(ReadBridgeConfigResolver resolver) {
    this(resolver, new CatalogRequestContext() {});
  }

  public ReadBridgeStripProtection(
      ReadBridgeConfigResolver resolver, CatalogRequestContext requestContext) {
    this.resolver = Objects.requireNonNull(resolver, "resolver");
    this.requestContext = Objects.requireNonNull(requestContext, "requestContext");
  }

  /**
   * Reject Type 1 / Type 2 violations, then drop every {@code initial-default} from {@code
   * incoming} so overlays cannot land in Iceberg metadata. Ramp-off and the policy bypass still
   * strip. Returns {@code incoming} unchanged when there is nothing to check or drop.
   *
   * @throws ColumnDefaultException Type 1, Type 2, a default {@code incoming} declares that cannot
   *     be applied, or {@code incoming} data this protection cannot read
   * @throws TableConfigUnavailableException if a default {@code existing} declares cannot be
   *     applied
   */
  public TableDto prepare(TableDto existing, TableDto incoming) throws ColumnDefaultException {
    if (incoming == null) {
      return incoming;
    }
    JsonNode schema = parse(incoming.getSchema(), incoming);
    if (!requestContext.columnDefaultPolicyBypass()) {
      Map<Integer, String> previousStamped =
          existing == null ? Collections.emptyMap() : resolver.storedColumnDefaults(existing);
      Map<Integer, String> incomingStamped = resolver.incomingColumnDefaults(incoming);
      if (existing != null) {
        rejectRemovedDefaults(previousStamped, incomingStamped, schema, incoming);
        rejectUnawareCommit(previousStamped, schema, incoming);
      }
    }
    return stripInitialDefaults(schema, incoming);
  }

  /**
   * Type 1: a still-present stamped column must keep its default. Unstamped or unramped tables skip
   * — there is no default to protect.
   */
  private void rejectRemovedDefaults(
      Map<Integer, String> previousStamped,
      Map<Integer, String> incomingStamped,
      JsonNode schema,
      TableDto incoming)
      throws ColumnDefaultException {
    if (previousStamped.isEmpty() || !resolver.isBridged(incoming)) {
      return;
    }
    Map<Integer, JsonNode> fields = fieldsById(required(schema, incoming));
    for (Integer fieldId : previousStamped.keySet()) {
      JsonNode field = fields.get(fieldId);
      if (field != null && !incomingStamped.containsKey(fieldId)) {
        throw new ColumnDefaultException(
            String.format(
                "COLUMN_DEFAULT_REMOVED: %s.%s still has a column default on %s. This commit"
                    + " omitted it. Retry from Spark 3.1 or Spark 3.5 using the jars on the"
                    + " standard client image. Column defaults cannot be removed or changed."
                    + TablesMvcConstants.COLUMN_DEFAULT_POLICY_BYPASS_HINT,
                incoming.getDatabaseId(),
                incoming.getTableId(),
                fieldLabel(field, fieldId)));
      }
    }
  }

  /**
   * Type 2: a write that commits snapshots must send {@code initial-default} equal to the stamp.
   * That handshake is trust, not proof the files were rewritten.
   */
  private void rejectUnawareCommit(
      Map<Integer, String> previousStamped, JsonNode schema, TableDto incoming)
      throws ColumnDefaultException {
    if (previousStamped.isEmpty() || !commitsSnapshots(incoming)) {
      return;
    }
    Map<Integer, JsonNode> fields = fieldsById(required(schema, incoming));
    for (Map.Entry<Integer, String> stamp : previousStamped.entrySet()) {
      JsonNode field = fields.get(stamp.getKey());
      if (field != null
          && !tree(stamp.getValue(), incoming).equals(field.get(SchemaKeys.INITIAL_DEFAULT))) {
        throw new ColumnDefaultException(
            String.format(
                "COLUMN_DEFAULT_UNAWARE_COMMIT: %s.%s has a column default on %s, and this commit"
                    + " did not send it as a matching initial-default. Commit from Spark 3.1 or"
                    + " Spark 3.5 using the jars on the standard client image; if the job already"
                    + " does, reload the table and retry, because it loaded the table before the"
                    + " default applied."
                    + TablesMvcConstants.COLUMN_DEFAULT_POLICY_BYPASS_HINT,
                incoming.getDatabaseId(),
                incoming.getTableId(),
                fieldLabel(field, stamp.getKey())));
      }
    }
  }

  private TableDto stripInitialDefaults(JsonNode schema, TableDto incoming)
      throws ColumnDefaultException {
    String stripped =
        schema == null ? incoming.getSchema() : strip(schema, incoming.getSchema(), incoming);
    List<String> intermediates = strip(incoming.getNewIntermediateSchemas(), incoming);
    if (Objects.equals(stripped, incoming.getSchema())
        && Objects.equals(intermediates, incoming.getNewIntermediateSchemas())) {
      return incoming;
    }
    return incoming.toBuilder().schema(stripped).newIntermediateSchemas(intermediates).build();
  }

  /** A replace, or a snapshots PUT, whatever refs it moves and whatever its snapshots do. */
  private static boolean commitsSnapshots(TableDto incoming) {
    return incoming.isReplaceCommit()
        || incoming.isStageReplace()
        || (incoming.getJsonSnapshots() != null && !incoming.getJsonSnapshots().isEmpty());
  }

  /**
   * Drop every {@code initial-default} from {@code root}, the parsed {@code json}, so a handshake,
   * a ramp-off leftover, or an unstamped writer default cannot land in Iceberg metadata. Returns
   * {@code json} when there was nothing to drop.
   */
  private static String strip(JsonNode root, String json, TableDto incoming)
      throws ColumnDefaultException {
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
      String next =
          schema == null || schema.isEmpty()
              ? schema
              : strip(tree(schema, incoming), schema, incoming);
      stripped.add(next);
      changed |= !Objects.equals(next, schema);
    }
    return changed ? stripped : schemas;
  }

  /** Field objects by id; the first wins if a malformed schema repeats an id. */
  private static Map<Integer, JsonNode> fieldsById(JsonNode schema) {
    Map<Integer, JsonNode> byId = new HashMap<>();
    for (JsonNode field : fieldObjects(schema)) {
      byId.putIfAbsent(field.get(SchemaKeys.ID).asInt(), field);
    }
    return byId;
  }

  private static String fieldLabel(JsonNode field, int fieldId) {
    JsonNode name = field.get(SchemaKeys.NAME);
    return name != null && name.isTextual() && !name.asText().isEmpty()
        ? name.asText() + " (field-id " + fieldId + ")"
        : "field-id " + fieldId;
  }

  private static List<JsonNode> fieldObjects(JsonNode schema) {
    List<JsonNode> found = schema.findParents(SchemaKeys.ID);
    return found == null ? Collections.emptyList() : found;
  }

  /** The request's JSON parsed once for the checks and the strip; null when there is none. */
  private static JsonNode parse(String json, TableDto incoming) throws ColumnDefaultException {
    return json == null || json.isEmpty() ? null : tree(json, incoming);
  }

  /** A check that needs the request schema treats a missing one as unreadable. */
  private static JsonNode required(JsonNode schema, TableDto incoming)
      throws ColumnDefaultException {
    if (schema == null) {
      throw unusable(incoming, "unreadable json", null);
    }
    return schema;
  }

  private static JsonNode tree(String json, TableDto incoming) throws ColumnDefaultException {
    if (json == null || json.isEmpty()) {
      throw unusable(incoming, "unreadable json", null);
    }
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

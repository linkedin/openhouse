package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class OpenHouseTransactionsTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "table");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testStagedContextDoesNotLeakIntoLaterCommits(boolean replace) throws Exception {
    TableMetadata base = replace ? persist(tableWithSnapshot(), "context-base") : null;
    TableMetadata staged =
        persist(
            replace
                ? base.buildReplacement(
                    SCHEMA,
                    PartitionSpec.unpartitioned(),
                    SortOrder.unsorted(),
                    base.location(),
                    Collections.singletonMap("server-default", "retained"))
                : emptyMetadata(),
            "context-stage");
    try (CatalogServer server = new CatalogServer(base, staged)) {
      Transaction transaction =
          replace
              ? server.catalog.buildTable(IDENTIFIER, SCHEMA).replaceTransaction()
              : server.catalog.buildTable(IDENTIFIER, SCHEMA).createTransaction();
      append(transaction);
      transaction.commitTransaction();
      TableMetadata committed = persist(replay(base, server.commit.get()), "context-created");
      server.current.set(committed);
      Table table =
          new BaseTable(((BaseTransaction) transaction).underlyingOps(), IDENTIFIER.toString());
      table.updateProperties().set("server-default", "changed").commit();
      TableMetadata changed = persist(replay(committed, server.commit.get()), "context-updated");
      server.current.set(changed);
      table.updateProperties().set("after-staging", "committed").commit();
      TableMetadata finalState = replay(changed, server.commit.get());
      Assertions.assertEquals("changed", finalState.properties().get("server-default"));
      Assertions.assertEquals("committed", finalState.properties().get("after-staging"));
      Assertions.assertFalse(
          server.commit.get().get("createUpdateTableRequestBody").get("replaceCommit").asBoolean());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testStagedCreateInitializesEmptyBaseBeforeSnapshot(int formatVersion)
      throws Exception {
    TableMetadata staged = persist(stagedMetadata(formatVersion), "create-stage");
    try (CatalogServer server = new CatalogServer(null, staged)) {
      Transaction transaction =
          server.catalog.buildTable(IDENTIFIER, staged.schema()).createTransaction();
      Assertions.assertEquals(staged.currentSchemaId(), transaction.table().schema().schemaId());
      Assertions.assertEquals(staged.defaultSpecId(), transaction.table().spec().specId());
      Assertions.assertEquals(
          staged.defaultSortOrderId(), transaction.table().sortOrder().orderId());
      transaction.updateSchema().addColumn("added", Types.LongType.get()).commit();
      transaction
          .updateProperties()
          .set("server-default", "temporary")
          .set("client.table.schema", "user-schema")
          .commit();
      transaction.updateProperties().set("server-default", "retained").commit();
      append(transaction);
      transaction.commitTransaction();

      JsonNode request = server.commit.get();
      Assertions.assertEquals("INITIAL_VERSION", request.get("baseTableVersion").asText());
      Assertions.assertFalse(
          request.get("createUpdateTableRequestBody").get("replaceCommit").asBoolean());
      Assertions.assertEquals(
          "PRIMARY_TABLE", request.get("createUpdateTableRequestBody").get("tableType").asText());
      Assertions.assertFalse(
          request
              .get("createUpdateTableRequestBody")
              .get("policies")
              .get("sharingEnabled")
              .asBoolean());
      assertStagedPropertyUpdates(request, false);
      Assertions.assertEquals(
          "upgrade-format-version", request.get("updates").get(0).get("action").asText());
      Assertions.assertEquals(
          formatVersion, request.get("updates").get(0).get("format-version").asInt());
      TableMetadata applied = replay(null, request);
      Assertions.assertEquals(formatVersion, applied.formatVersion());
      Assertions.assertEquals(staged.uuid(), applied.uuid());
      Assertions.assertEquals(staged.location(), applied.location());
      staged
          .schemas()
          .forEach(
              schema ->
                  Assertions.assertEquals(
                      SchemaParser.toJson(schema),
                      SchemaParser.toJson(applied.schemasById().get(schema.schemaId()))));
      staged
          .specs()
          .forEach(
              spec ->
                  Assertions.assertEquals(
                      PartitionSpecParser.toJson(spec),
                      PartitionSpecParser.toJson(applied.specsById().get(spec.specId()))));
      staged
          .sortOrders()
          .forEach(
              order ->
                  Assertions.assertEquals(
                      SortOrderParser.toJson(order),
                      SortOrderParser.toJson(applied.sortOrdersById().get(order.orderId()))));
      Assertions.assertEquals(staged.defaultSpecId(), applied.defaultSpecId());
      Assertions.assertEquals(staged.defaultSortOrderId(), applied.defaultSortOrderId());
      Assertions.assertEquals(17, applied.schema().findField("id").fieldId());
      Assertions.assertEquals(19, applied.schema().findField("added").fieldId());
      Assertions.assertEquals("user-schema", applied.properties().get("client.table.schema"));
      List<String> propertyHistory = new ArrayList<>();
      for (JsonNode update : request.get("updates")) {
        if ("set-properties".equals(update.path("action").asText())
            && update.get("updates").has("server-default")) {
          propertyHistory.add(update.get("updates").get("server-default").asText());
        }
      }
      Assertions.assertEquals(
          java.util.Arrays.asList("retained", "temporary", "retained"), propertyHistory);
      Assertions.assertEquals("retained", applied.properties().get("server-default"));
      Assertions.assertEquals(1, applied.snapshots().size());
      Assertions.assertEquals(
          formatVersion == 1 ? 0 : 1, applied.currentSnapshot().sequenceNumber());
      Assertions.assertEquals(applied.currentSchemaId(), applied.currentSnapshot().schemaId());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testStagedCreateWithoutUserChangesStillInitializesMetadata(int formatVersion)
      throws Exception {
    TableMetadata staged = persist(emptyMetadata(formatVersion), "empty-create-stage");
    try (CatalogServer server = new CatalogServer(null, staged)) {
      server.catalog.buildTable(IDENTIFIER, SCHEMA).createTransaction().commitTransaction();

      TableMetadata applied = replay(null, server.commit.get());
      Assertions.assertEquals(formatVersion, applied.formatVersion());
      Assertions.assertEquals(staged.uuid(), applied.uuid());
      Assertions.assertEquals(staged.location(), applied.location());
      Assertions.assertTrue(staged.schema().sameSchema(applied.schema()));
      Assertions.assertTrue(applied.snapshots().isEmpty());
      Assertions.assertFalse(applied.properties().containsKey("client.table.schema"));
    }
  }

  @Test
  public void testStagedReplaceAppliesToPersistedBaseAndRetainsOtherRefs() throws Exception {
    TableMetadata base = persist(tableWithSnapshot(), "replace-base");
    Schema replacementSchema =
        new Schema(Types.NestedField.optional(8, "replacement", Types.StringType.get()));
    Map<String, String> stagedProperties = new HashMap<>();
    stagedProperties.put("replaced", "true");
    stagedProperties.put("openhouse.tableLocation", "staged-location");
    stagedProperties.put("openhouse.tableVersion", "staged-version");
    stagedProperties.put("openhouse.lastModifiedTime", "200");
    stagedProperties.put("client.table.schema", "staged-schema");
    stagedProperties.put("policies", "{\"sharingEnabled\":true}");
    TableMetadata staged =
        persist(
            base.buildReplacement(
                replacementSchema,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                base.location(),
                stagedProperties),
            "replace-stage");
    try (CatalogServer server = new CatalogServer(base, staged)) {
      Transaction transaction =
          server.catalog.buildTable(IDENTIFIER, replacementSchema).replaceTransaction();
      append(transaction);
      transaction.commitTransaction();

      JsonNode request = server.commit.get();
      Assertions.assertEquals(
          base.metadataFileLocation(), request.get("baseTableVersion").asText());
      Assertions.assertTrue(
          request.get("createUpdateTableRequestBody").get("replaceCommit").asBoolean());
      assertStagedPropertyUpdates(request, true);
      Assertions.assertTrue(
          request
              .get("createUpdateTableRequestBody")
              .get("policies")
              .get("sharingEnabled")
              .asBoolean());
      TableMetadata applied = replay(base, request);
      Assertions.assertTrue(staged.schema().sameSchema(applied.schema()));
      Assertions.assertEquals(staged.currentSchemaId(), applied.currentSchemaId());
      Assertions.assertEquals(42L, applied.refs().get("saved").snapshotId());
      Assertions.assertEquals(2, applied.snapshots().size());
      Assertions.assertNotEquals(42L, applied.currentSnapshot().snapshotId());
      Assertions.assertEquals(applied.currentSchemaId(), applied.currentSnapshot().schemaId());
      Assertions.assertEquals("true", applied.properties().get("replaced"));
      for (Map.Entry<String, String> entry : base.properties().entrySet()) {
        if (entry.getKey().startsWith("openhouse.")
            || "policies".equals(entry.getKey())
            || "client.table.schema".equals(entry.getKey())) {
          Assertions.assertEquals(entry.getValue(), applied.properties().get(entry.getKey()));
        }
      }
      List<JsonNode> mainChanges = refChanges(request, SnapshotRef.MAIN_BRANCH);
      Assertions.assertEquals("remove-snapshot-ref", mainChanges.get(0).get("action").asText());
      Assertions.assertEquals("set-snapshot-ref", mainChanges.get(1).get("action").asText());
    }
  }

  @Test
  public void testCompoundSchemaSnapshotAndRefTransactionIsNotReplace() throws Exception {
    TableMetadata base = persist(tableWithSnapshot(), "compound-base");
    try (CatalogServer server = new CatalogServer(base, null)) {
      Transaction transaction = server.catalog.loadTable(IDENTIFIER).newTransaction();
      transaction.updateSchema().addColumn("added", Types.LongType.get()).commit();
      transaction.updateProperties().set("compound", "true").commit();
      append(transaction);
      long appendedId = transaction.table().currentSnapshot().snapshotId();
      transaction.manageSnapshots().createBranch("feature", 42L).commit();
      transaction.manageSnapshots().replaceBranch("feature", appendedId).commit();
      transaction.manageSnapshots().replaceBranch("feature", 42L).commit();
      transaction.manageSnapshots().removeBranch("feature").commit();
      transaction.commitTransaction();

      JsonNode request = server.commit.get();
      Assertions.assertFalse(
          request.get("createUpdateTableRequestBody").get("replaceCommit").asBoolean());
      TableMetadata applied = replay(base, request);
      Assertions.assertEquals(Types.LongType.get(), applied.schema().findType("added"));
      Assertions.assertEquals("true", applied.properties().get("compound"));
      Assertions.assertEquals(appendedId, applied.currentSnapshot().snapshotId());
      Assertions.assertEquals(applied.currentSchemaId(), applied.currentSnapshot().schemaId());
      Assertions.assertFalse(applied.refs().containsKey("feature"));
      List<JsonNode> transitions = refChanges(request, "feature");
      Assertions.assertEquals(4, transitions.size());
      Assertions.assertEquals(42L, transitions.get(0).get("snapshot-id").asLong());
      Assertions.assertEquals(appendedId, transitions.get(1).get("snapshot-id").asLong());
      Assertions.assertEquals(42L, transitions.get(2).get("snapshot-id").asLong());
      Assertions.assertEquals("remove-snapshot-ref", transitions.get(3).get("action").asText());
    }
  }

  @Test
  public void testRefRoundTripWithNoNetChangeStillCommits() throws Exception {
    TableMetadata base = persist(tableWithSnapshot(), "ref-base");
    try (CatalogServer server = new CatalogServer(base, null)) {
      Transaction transaction = server.catalog.loadTable(IDENTIFIER).newTransaction();
      transaction.manageSnapshots().createBranch("temporary", 42L).commit();
      transaction.manageSnapshots().removeBranch("temporary").commit();
      transaction.commitTransaction();

      JsonNode request = server.commit.get();
      List<JsonNode> transitions = refChanges(request, "temporary");
      Assertions.assertEquals(2, transitions.size());
      Assertions.assertEquals("set-snapshot-ref", transitions.get(0).get("action").asText());
      Assertions.assertEquals("remove-snapshot-ref", transitions.get(1).get("action").asText());
      Assertions.assertEquals(base.refs(), replay(base, request).refs());
    }
  }

  @Test
  public void testStagedReplaceRejectsRefreshedBaseRatherThanRebasingOldActions() throws Exception {
    TableMetadata base = persist(tableWithSnapshot(), "stale-base");
    TableMetadata staged =
        persist(
            base.buildReplacement(
                SCHEMA,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                base.location(),
                Collections.emptyMap()),
            "stale-stage");
    try (CatalogServer server = new CatalogServer(base, staged)) {
      Transaction transaction = server.catalog.buildTable(IDENTIFIER, SCHEMA).replaceTransaction();
      append(transaction);
      server.current.set(
          persist(
              TableMetadata.buildFrom(base)
                  .setProperties(Collections.singletonMap("concurrent", "true"))
                  .build(),
              "concurrent-base"));

      Assertions.assertThrows(CommitFailedException.class, transaction::commitTransaction);
      Assertions.assertNull(server.commit.get());
    }
  }

  private TableMetadata emptyMetadata() {
    return emptyMetadata(2);
  }

  private TableMetadata emptyMetadata(int formatVersion) {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion));
    properties.put(TableProperties.COMMIT_NUM_RETRIES, "0");
    properties.put("server-default", "retained");
    properties.put("openhouse.tableType", "PRIMARY_TABLE");
    properties.put("openhouse.clusterId", "test");
    properties.put("openhouse.tableLocation", "original-location");
    properties.put("openhouse.tableVersion", "original-version");
    properties.put("openhouse.lastModifiedTime", "100");
    properties.put("client.table.schema", "server-schema");
    properties.put("policies", "{\"sharingEnabled\":false}");
    return TableMetadata.newTableMetadata(
        SCHEMA,
        PartitionSpec.unpartitioned(),
        SortOrder.unsorted(),
        directory.resolve("table").toString(),
        properties);
  }

  private TableMetadata stagedMetadata(int formatVersion) throws IOException {
    ObjectNode json =
        (ObjectNode) MAPPER.readTree(TableMetadataParser.toJson(emptyMetadata(formatVersion)));
    JsonNode originalSchema =
        MAPPER.readTree(
            "{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
                + "{\"id\":17,\"name\":\"id\",\"type\":\"int\",\"required\":true,"
                + "\"initial-default\":7,\"write-default\":7}]}");
    ObjectNode currentSchema = originalSchema.deepCopy();
    currentSchema.put("schema-id", 1);
    currentSchema
        .withArray("fields")
        .addObject()
        .put("id", 18)
        .put("name", "payload")
        .put("type", "string")
        .put("required", false);
    json.withArray("schemas").removeAll().add(originalSchema).add(currentSchema);
    json.set("schema", currentSchema);
    json.put("current-schema-id", 1);
    json.put("last-column-id", 18);
    TableMetadata metadata = TableMetadataParser.fromJson(json.toString());
    return TableMetadata.buildFrom(metadata)
        .setDefaultPartitionSpec(PartitionSpec.builderFor(metadata.schema()).identity("id").build())
        .setDefaultSortOrder(SortOrder.builderFor(metadata.schema()).asc("id").build())
        .build();
  }

  private TableMetadata tableWithSnapshot() {
    return TableMetadata.buildFrom(emptyMetadata())
        .setBranchSnapshot(
            SnapshotParser.fromJson(
                "{\"snapshot-id\":42,\"sequence-number\":1,\"timestamp-ms\":1669126937912,"
                    + "\"summary\":{\"operation\":\"append\"},"
                    + "\"manifests\":[],\"schema-id\":0}"),
            SnapshotRef.MAIN_BRANCH)
        .setRef("saved", SnapshotRef.branchBuilder(42L).build())
        .build();
  }

  private TableMetadata persist(TableMetadata metadata, String name) {
    HadoopFileIO io = new HadoopFileIO(new Configuration());
    String location = directory.resolve(name + ".metadata.json").toString();
    TableMetadataParser.write(metadata, io.newOutputFile(location));
    return TableMetadataParser.read(io, location);
  }

  private void append(Transaction transaction) {
    DataFiles.Builder file =
        DataFiles.builder(transaction.table().spec())
            .withPath(directory.resolve("data.parquet").toString())
            .withFileSizeInBytes(10)
            .withRecordCount(1);
    if (transaction.table().spec().isPartitioned()) {
      file.withPartitionPath("id=7");
    }
    transaction.newFastAppend().appendFile(file.build()).commit();
  }

  private static TableMetadata replay(TableMetadata base, JsonNode request)
      throws ReflectiveOperationException {
    TableMetadata.Builder builder;
    if (base == null) {
      MetadataUpdate first =
          MetadataUpdateParser.fromJson(request.get("updates").get(0).toString());
      Assertions.assertTrue(first instanceof MetadataUpdate.UpgradeFormatVersion);
      int formatVersion = ((MetadataUpdate.UpgradeFormatVersion) first).formatVersion();
      builder = TableMetadata.buildFromEmpty();
      try {
        // Test-only bridge to the server's native initial-version semantics, without server deps.
        Method initialize =
            TableMetadata.Builder.class.getDeclaredMethod("setInitialFormatVersion", int.class);
        initialize.setAccessible(true);
        initialize.invoke(builder, formatVersion);
      } catch (NoSuchMethodException e) {
        // Iceberg 1.2 has no initial setter and starts at v1; only that missing API falls back.
        builder.upgradeFormatVersion(formatVersion);
      }
    } else {
      builder = TableMetadata.buildFrom(base);
    }
    for (JsonNode update : request.get("updates")) {
      MetadataUpdateParser.fromJson(update.toString()).applyTo(builder);
    }
    return builder.build();
  }

  private static List<JsonNode> refChanges(JsonNode request, String ref) {
    List<JsonNode> changes = new ArrayList<>();
    for (JsonNode update : request.get("updates")) {
      if (ref.equals(update.path("ref-name").asText())) {
        changes.add(update);
      }
    }
    return changes;
  }

  private static void assertStagedPropertyUpdates(JsonNode request, boolean replace) {
    for (JsonNode update : request.get("updates")) {
      if ("set-properties".equals(update.path("action").asText())) {
        update
            .get("updates")
            .fieldNames()
            .forEachRemaining(
                key -> {
                  if (replace) {
                    Assertions.assertFalse(key.startsWith("openhouse."));
                    Assertions.assertNotEquals("policies", key);
                  }
                  Assertions.assertNotEquals("client.table.schema", key);
                });
        if (!replace) {
          // Only the bootstrap properties are filtered; subsequent user deltas are authoritative.
          return;
        }
      }
    }
  }

  private static final class CatalogServer implements AutoCloseable {
    private final MockWebServer server = new MockWebServer();
    private final AtomicReference<TableMetadata> current;
    private final AtomicReference<JsonNode> commit = new AtomicReference<>();
    private final OpenHouseCatalog catalog = new OpenHouseCatalog();

    private CatalogServer(TableMetadata base, TableMetadata staged) throws IOException {
      current = new AtomicReference<>(base);
      server.setDispatcher(
          new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
              if ("GET".equals(request.getMethod())) {
                TableMetadata metadata = current.get();
                return metadata == null
                    ? new MockResponse().setResponseCode(404)
                    : metadataResponse(metadata);
              }
              if ("POST".equals(request.getMethod()) && staged != null) {
                return metadataResponse(staged);
              }
              if ("PUT".equals(request.getMethod()) && request.getPath().contains("snapshots")) {
                try {
                  commit.set(MAPPER.readTree(request.getBody().readUtf8()));
                } catch (IOException e) {
                  throw new IllegalArgumentException(e);
                }
                return new MockResponse()
                    .addHeader("Content-Type", "application/json")
                    .setBody("{}");
              }
              return new MockResponse().setResponseCode(400);
            }
          });
      server.start();
      catalog.setConf(new Configuration());
      catalog.initialize(
          "openhouse", Collections.singletonMap(CatalogProperties.URI, server.url("/").toString()));
    }

    private static MockResponse metadataResponse(TableMetadata metadata) {
      return new MockResponse()
          .addHeader("Content-Type", "application/json")
          .setBody("{\"tableLocation\":\"" + metadata.metadataFileLocation() + "\"}");
    }

    @Override
    public void close() throws IOException {
      server.close();
    }
  }
}

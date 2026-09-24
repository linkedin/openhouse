package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.tables.e2e.h2.RequestAndValidateHelper.createTableAndValidateResponse;
import static com.linkedin.openhouse.tables.e2e.h2.RequestAndValidateHelper.putSnapshotsAndValidateResponse;
import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX;
import static com.linkedin.openhouse.tables.model.IcebergSnapshotsModelTestUtilities.createDummyDataFile;
import static com.linkedin.openhouse.tables.model.IcebergSnapshotsModelTestUtilities.obtainSnapshotRefsFromSnapshot;
import static com.linkedin.openhouse.tables.model.IcebergSnapshotsModelTestUtilities.preparePutSnapshotsWithAppendRequest;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildGetTableResponseBody;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildGetTableResponseBodyWithDbTbl;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalTableOperations;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.SnapshotRefChange;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.utils.MetadataUpdateCommit;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.RetentionColumnPattern;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.OperationType;
import com.linkedin.openhouse.tables.audit.model.TableAuditEvent;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.util.FileSystemUtils;

/** Exercises the real snapshot handler, service, repository and catalog publication boundary. */
@SpringBootTest(classes = SpringH2Application.class)
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@WithMockUser(username = "testUser")
public class OrderedMetadataCommitTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Autowired private MockMvc mvc;
  @Autowired private Catalog catalog;
  @Autowired private StorageManager storageManager;
  @Autowired private FileIOManager fileIOManager;
  @Autowired private OpenHouseInternalRepository repository;
  @Autowired private AuditHandler<TableAuditEvent> tableAuditHandler;

  @SpyBean(name = "houseTablesH2Repository")
  private HouseTableRepository houseTableRepository;

  @TempDir Path dataDirectory;

  private TableIdentifier identifier;
  private Path tableDirectory;
  private MvcResult latestResponse;
  private int nextDataFile;

  @Test
  void compoundTransactionPreservesIntermediateMainTransitionsAndIgnoresLegacyState()
      throws Exception {
    seed("ordered_compound", 2);
    TableMetadata base = metadata();
    long startingHead = base.currentSnapshot().snapshotId();
    long first = base.currentSnapshot().parentId();
    BaseTransaction transaction = transaction();
    transaction.updateSchema().addColumn("ordered_column", Types.LongType.get()).commit();
    transaction.updateProperties().set("ordered-property", "from-updates").commit();
    transaction.manageSnapshots().createBranch("feature", startingHead).commit();
    transaction.manageSnapshots().createBranch("saved", first).commit();
    transaction.newAppend().appendFile(dataFile(transaction.table())).toBranch("feature").commit();
    long featureHead = transaction.table().refs().get("feature").snapshotId();
    transaction.manageSnapshots().rollbackTo(first).commit();
    transaction.manageSnapshots().cherrypick(startingHead).commit();
    transaction.manageSnapshots().removeBranch("feature").commit();
    List<Map<String, Object>> updates = updates(transaction.currentMetadata());

    CreateUpdateTableRequestBody envelope = envelope(base);
    Map<String, String> legacyProperties = new HashMap<>(envelope.getTableProperties());
    legacyProperties.put("ordered-property", "from-legacy");
    legacyProperties.put("legacy-only", "must-not-be-applied");
    String legacySnapshot = SnapshotParser.toJson(base.snapshot(first));
    IcebergSnapshotsRequestBody request =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(base.metadataFileLocation())
            .createUpdateTableRequestBody(
                envelope.toBuilder().tableProperties(legacyProperties).build())
            .jsonSnapshots(Collections.singletonList(legacySnapshot))
            .snapshotRefs(obtainSnapshotRefsFromSnapshot(legacySnapshot))
            .updates(updates)
            .build();

    clearInvocations(tableAuditHandler);
    MvcResult response = put(request).andExpect(status().isOk()).andReturn();
    TableMetadata committed = metadata();
    assertNotEquals(base.metadataFileLocation(), committed.metadataFileLocation());
    assertEquals(startingHead, committed.currentSnapshot().snapshotId());
    assertEquals(first, committed.refs().get("saved").snapshotId());
    assertEquals(2, committed.refs().size());
    assertFalse(committed.refs().containsKey("feature"));
    assertNotNull(committed.snapshot(featureHead));
    assertEquals(3, committed.snapshots().size());
    assertEquals("from-updates", committed.properties().get("ordered-property"));
    assertFalse(committed.properties().containsKey("legacy-only"));
    assertEquals(Types.LongType.get(), committed.schema().findType("ordered_column"));

    TableAuditEvent event = snapshotAudit(OperationStatus.SUCCESS);
    assertEquals(
        URI.create(committed.metadataFileLocation()).getPath(),
        URI.create(event.getCurrentTableRoot()).getPath());
    assertEquals(startingHead, event.getCurrentSnapshotId().longValue());
    assertEquals(
        committed.currentSnapshot().timestampMillis(),
        event.getCurrentSnapshotTimestampMs().longValue());
    List<SnapshotRefChange> changes = event.getRefChanges();
    List<Integer> indices = refUpdateIndices(updates);
    assertEquals(6, changes.size());
    assertTransition(
        changes.get(0), indices.get(0), "set-snapshot-ref", "feature", null, startingHead);
    assertTransition(changes.get(1), indices.get(1), "set-snapshot-ref", "saved", null, first);
    assertTransition(
        changes.get(2), indices.get(2), "set-snapshot-ref", "feature", startingHead, featureHead);
    assertTransition(
        changes.get(3), indices.get(3), "set-snapshot-ref", "main", startingHead, first);
    assertTransition(
        changes.get(4), indices.get(4), "set-snapshot-ref", "main", first, startingHead);
    assertTransition(
        changes.get(5), indices.get(5), "remove-snapshot-ref", "feature", featureHead, null);
    assertFalse(MAPPER.readTree(response.getResponse().getContentAsString()).has("commitResult"));
  }

  @Test
  void unknownMiddleActionRejectsEntireTransaction() throws Exception {
    assertRejectedMiddleUpdate(Collections.singletonMap("action", "unknown-table-update"));
  }

  @Test
  void invalidMiddleRefRejectsAlreadyAppliedPrefix() throws Exception {
    Map<String, Object> invalid = new HashMap<>();
    invalid.put("action", "set-snapshot-ref");
    invalid.put("ref-name", "missing-snapshot");
    invalid.put("type", "branch");
    invalid.put("snapshot-id", Long.MAX_VALUE);
    assertRejectedMiddleUpdate(invalid);
  }

  @Test
  void staleTransactionCannotRebaseOntoAnotherWritersCommit() throws Exception {
    seed("ordered_stale", 1);
    TableMetadata base = metadata();
    BaseTransaction stale = transaction();
    stale.manageSnapshots().createBranch("loser", base.currentSnapshot().snapshotId()).commit();
    stale.updateSchema().addColumn("loser_column", Types.StringType.get()).commit();
    stale.updateProperties().set("loser-property", "never").commit();
    IcebergSnapshotsRequestBody staleRequest = request(base, stale);

    BaseTransaction winner = transaction();
    winner.manageSnapshots().createBranch("winner", base.currentSnapshot().snapshotId()).commit();
    winner.updateProperties().set("winner-property", "retained").commit();
    put(request(base, winner)).andExpect(status().isOk());
    TableMetadata winning = metadata();

    clearInvocations(tableAuditHandler);
    put(staleRequest).andExpect(status().isConflict());
    assertWinnerUnchanged(winning);
    assertNull(snapshotAudit(OperationStatus.FAILED).getRefChanges());
  }

  @Test
  void lateCasConflictDoesNotPublishOrRetryAnyPartOfTheBatch() throws Exception {
    seed("ordered_late_conflict", 1);
    TableMetadata base = metadata();
    BaseTransaction loser = transaction();
    loser.manageSnapshots().createBranch("loser", base.currentSnapshot().snapshotId()).commit();
    loser.updateSchema().addColumn("loser_column", Types.StringType.get()).commit();
    loser.updateProperties().set("loser-property", "never").commit();
    IcebergSnapshotsRequestBody losingRequest = request(base, loser);
    BaseTransaction winner = transaction();
    winner.manageSnapshots().createBranch("winner", base.currentSnapshot().snapshotId()).commit();
    winner.updateProperties().set("winner-property", "retained").commit();
    List<Map<String, Object>> winningUpdates = updates(winner.currentMetadata());
    AtomicReference<TableMetadata> winning = new AtomicReference<>();

    // The competing catalog writer publishes at the losing HTTP writer's save boundary. It does
    // not share the HTTP replay guard, as another service instance would not. H2 has no CAS, so
    // inject the conflict real HTS would return after the competing publication.
    doAnswer(
            invocation -> {
              reset(houseTableRepository);
              OpenHouseInternalTableOperations competing =
                  ((OpenHouseInternalCatalog) catalog).newCommitOperations(identifier);
              TableMetadata competingBase = competing.current();
              competing.commitAndGetMetadata(
                  competingBase,
                  MetadataUpdateCommit.apply(competingBase, winningUpdates).getMetadata());
              winning.set(metadata());
              throw new HouseTableConcurrentUpdateException("Competing commit won the CAS", null);
            })
        .when(houseTableRepository)
        .save(any(HouseTable.class));
    clearInvocations(tableAuditHandler);
    try {
      put(losingRequest).andExpect(status().isConflict());
      assertNotNull(winning.get());
      assertWinnerUnchanged(winning.get());
      assertNull(snapshotAudit(OperationStatus.FAILED).getRefChanges());
    } finally {
      reset(houseTableRepository);
    }
  }

  @Test
  void explicitEmptyUpdatesDoNotApplyLegacyAppendOrProperties() throws Exception {
    seed("ordered_empty", 1);
    TableMetadata base = metadata();
    GetTableResponseBody response = buildGetTableResponseBody(latestResponse);
    IcebergSnapshotsRequestBody legacyAppend =
        preparePutSnapshotsWithAppendRequest(
            latestResponse,
            response,
            base.metadataFileLocation(),
            catalog,
            Collections.singletonList(dataFile(catalog.loadTable(identifier))));
    Map<String, String> legacyProperties = new HashMap<>(envelope(base).getTableProperties());
    legacyProperties.put("legacy-only", "must-not-be-applied");
    IcebergSnapshotsRequestBody empty =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(base.metadataFileLocation())
            .createUpdateTableRequestBody(
                envelope(base).toBuilder().tableProperties(legacyProperties).build())
            .jsonSnapshots(legacyAppend.getJsonSnapshots())
            .snapshotRefs(legacyAppend.getSnapshotRefs())
            .updates(Collections.emptyList())
            .build();

    clearInvocations(tableAuditHandler);
    put(empty).andExpect(status().isOk());
    TableMetadata committed = metadata();
    assertEquals(base.metadataFileLocation(), committed.metadataFileLocation());
    assertEquals(base.refs(), committed.refs());
    assertEquals(snapshotIds(base), snapshotIds(committed));
    assertTrue(base.schema().sameSchema(committed.schema()));
    assertFalse(committed.properties().containsKey("legacy-only"));
    TableAuditEvent event = snapshotAudit(OperationStatus.SUCCESS);
    assertTrue(event.getRefChanges().isEmpty());
    assertEquals(base.currentSnapshot().snapshotId(), event.getCurrentSnapshotId().longValue());
  }

  @Test
  void stagedCreateInitializesEmptyServerBaseAndPublishesItsFirstSnapshot() throws Exception {
    GetTableResponseBody fixture = fixture("ordered_create");
    latestResponse = createTableAndValidateResponse(fixture, mvc, storageManager, true);
    GetTableResponseBody stagedResponse = buildGetTableResponseBody(latestResponse);
    FileIO io = fileIOManager.getFileIO(storageManager.getDefaultStorage().getType());
    TableMetadata staged =
        new StaticTableOperations(stagedResponse.getTableLocation(), io).current();
    trackDirectory(staged.location());
    assertFalse(repository.existsById(key()));
    Map<String, String> properties = new HashMap<>(staged.properties());
    Arrays.asList(
            "client.table.schema",
            "evolved.table.schema",
            "newIntermediateSchemas",
            "snapshotsJsonToBePut",
            "snapshotsRefs",
            "sortOrder",
            "isStageCreate",
            "isStageReplace",
            "isReplaceCommit",
            "commitKey")
        .forEach(properties::remove);
    properties.put("ordered-create", "retained");
    TableMetadata initialized =
        TableMetadata.buildFromEmpty()
            .assignUUID(staged.uuid())
            .upgradeFormatVersion(staged.formatVersion())
            .setCurrentSchema(staged.schema(), staged.lastColumnId())
            .setDefaultPartitionSpec(staged.spec())
            .setDefaultSortOrder(staged.sortOrder())
            .setLocation(staged.location())
            .setProperties(properties)
            .build();
    BaseTransaction transaction =
        (BaseTransaction)
            Transactions.createTableTransaction(
                identifier.toString(),
                ((OpenHouseInternalCatalog) catalog).newCommitOperations(identifier),
                initialized);
    transaction.newAppend().appendFile(dataFile(transaction.table())).commit();
    long head = transaction.table().currentSnapshot().snapshotId();
    IcebergSnapshotsRequestBody create =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(INITIAL_TABLE_VERSION)
            .createUpdateTableRequestBody(envelope(null))
            .updates(updates(transaction.currentMetadata()))
            .build();

    clearInvocations(tableAuditHandler);
    MvcResult response = put(create).andExpect(status().isCreated()).andReturn();
    TableMetadata committed = metadata();
    assertEquals(staged.uuid(), committed.uuid());
    assertEquals(staged.location(), committed.location());
    assertTrue(staged.schema().sameSchema(committed.schema()));
    assertEquals(head, committed.currentSnapshot().snapshotId());
    assertEquals(committed.currentSchemaId(), committed.currentSnapshot().schemaId().intValue());
    assertEquals("retained", committed.properties().get("ordered-create"));
    assertEquals(stagedResponse.getTableUUID(), buildGetTableResponseBody(response).getTableUUID());
    TableAuditEvent event = snapshotAudit(OperationStatus.SUCCESS);
    assertEquals(OperationType.STAGED_COMMIT, event.getOperationType());
    assertEquals(head, event.getCurrentSnapshotId().longValue());
    assertEquals(1, event.getRefChanges().size());
    assertNull(event.getRefChanges().get(0).getBefore());
    assertEquals(head, event.getRefChanges().get(0).getAfter().getSnapshotId());
  }

  @Test
  void identityAndTransportMutationsRejectTheEntireBatch() throws Exception {
    seed("ordered_identity", 1);
    // A replicated table must not make canonical identity mutations eligible for publication.
    catalog
        .loadTable(identifier)
        .updateProperties()
        .set("openhouse.isTableReplicated", "true")
        .commit();
    TableMetadata base = metadata();
    SnapshotRef prefix = SnapshotRef.branchBuilder(base.currentSnapshot().snapshotId()).build();
    Map<String, String> identity = new HashMap<>();
    identity.put("openhouse.clusterId", "another-cluster");
    identity.put("openhouse.tableCreator", "another-owner");
    List<TableMetadata> invalid =
        Arrays.asList(
            TableMetadata.buildFrom(base)
                .setRef("prefix", prefix)
                .assignUUID(UUID.randomUUID().toString())
                .build(),
            TableMetadata.buildFrom(base)
                .setRef("prefix", prefix)
                .setLocation("hdfs://another-authority" + URI.create(base.location()).getPath())
                .build(),
            TableMetadata.buildFrom(base).setRef("prefix", prefix).setProperties(identity).build(),
            TableMetadata.buildFrom(base)
                .setRef("prefix", prefix)
                .setProperties(Collections.singletonMap("snapshotsJsonToBePut", "[]"))
                .build());
    for (TableMetadata candidate : invalid) {
      clearInvocations(tableAuditHandler);
      put(IcebergSnapshotsRequestBody.builder()
              .baseTableVersion(base.metadataFileLocation())
              .createUpdateTableRequestBody(envelope(base))
              .updates(updates(candidate))
              .build())
          .andExpect(status().isBadRequest());
      assertEquals(base.metadataFileLocation(), metadata().metadataFileLocation());
      assertEquals(base.refs(), metadata().refs());
      assertNull(snapshotAudit(OperationStatus.FAILED).getRefChanges());
    }
  }

  @Test
  void policiesAreValidatedAgainstTheAuthoritativeSchema() throws Exception {
    GetTableResponseBody fixture = fixture("ordered_policy");
    fixture =
        fixture
            .toBuilder()
            .timePartitioning(null)
            .policies(fixture.getPolicies().toBuilder().retention(null).build())
            .build();
    latestResponse =
        mvc.perform(
                MockMvcRequestBuilders.post(
                        CURRENT_MAJOR_VERSION_PREFIX + "/databases/{database}/tables",
                        fixture.getDatabaseId())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(buildCreateUpdateTableRequestBody(fixture).toJson()))
            .andExpect(status().isCreated())
            .andReturn();
    trackDirectory(catalog.loadTable(identifier).location());
    TableMetadata base = metadata();
    List<Types.NestedField> fields = new ArrayList<>(base.schema().columns());
    fields.add(
        Types.NestedField.optional(base.lastColumnId() + 1, "expiry", Types.StringType.get()));
    Schema withExpiry = new Schema(fields);
    CreateUpdateTableRequestBody body = envelope(base);
    Retention retention =
        Retention.builder()
            .count(3)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .columnPattern(
                RetentionColumnPattern.builder().columnName("expiry").pattern("yyyy-MM-dd").build())
            .build();
    body =
        body.toBuilder()
            .schema(SchemaParser.toJson(withExpiry))
            .policies(body.getPolicies().toBuilder().retention(retention).build())
            .build();
    put(IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(base.metadataFileLocation())
            .createUpdateTableRequestBody(body)
            .updates(Collections.emptyList())
            .build())
        .andExpect(status().isBadRequest());
    assertEquals(base.metadataFileLocation(), metadata().metadataFileLocation());

    TableMetadata candidate =
        TableMetadata.buildFrom(base).setCurrentSchema(withExpiry, base.lastColumnId() + 1).build();
    MvcResult result =
        put(IcebergSnapshotsRequestBody.builder()
                .baseTableVersion(base.metadataFileLocation())
                .createUpdateTableRequestBody(body)
                .updates(updates(candidate))
                .build())
            .andExpect(status().isOk())
            .andReturn();
    assertNotNull(metadata().schema().findField("expiry"));
    assertEquals(
        "expiry",
        MAPPER
            .readTree(result.getResponse().getContentAsString())
            .path("policies")
            .path("retention")
            .path("columnPattern")
            .path("columnName")
            .asText());
  }

  @Test
  void invalidHistoricalSchemaCannotHideBehindAnUnchangedCurrentSchema() throws Exception {
    seed("ordered_schema", 1);
    TableMetadata base = metadata();
    List<Types.NestedField> fields = new ArrayList<>(base.schema().columns());
    fields.add(Types.NestedField.optional(base.lastColumnId() + 1, "ID", Types.StringType.get()));
    TableMetadata candidate =
        TableMetadata.buildFrom(base)
            .addSchema(new Schema(fields), base.lastColumnId() + 1)
            .build();
    put(IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(base.metadataFileLocation())
            .createUpdateTableRequestBody(envelope(base))
            .updates(updates(candidate))
            .build())
        .andExpect(status().isBadRequest());
    assertEquals(base.metadataFileLocation(), metadata().metadataFileLocation());
    assertEquals(base.schemas().size(), metadata().schemas().size());
  }

  private void assertRejectedMiddleUpdate(Map<String, Object> invalid) throws Exception {
    seed("ordered_invalid", 1);
    TableMetadata base = metadata();
    BaseTransaction transaction = transaction();
    transaction
        .manageSnapshots()
        .createBranch("prefix", base.currentSnapshot().snapshotId())
        .commit();
    transaction.updateProperties().set("suffix-property", "never").commit();
    List<Map<String, Object>> updates = updates(transaction.currentMetadata());
    updates.add(1, invalid);
    clearInvocations(tableAuditHandler);
    put(IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(base.metadataFileLocation())
            .createUpdateTableRequestBody(envelope(base))
            .updates(updates)
            .build())
        .andExpect(status().isBadRequest());
    TableMetadata unchanged = metadata();
    assertEquals(base.metadataFileLocation(), unchanged.metadataFileLocation());
    assertEquals(base.refs(), unchanged.refs());
    assertEquals(base.properties(), unchanged.properties());
    assertEquals(snapshotIds(base), snapshotIds(unchanged));
    assertNull(snapshotAudit(OperationStatus.FAILED).getRefChanges());
  }

  private void assertWinnerUnchanged(TableMetadata winning) {
    TableMetadata committed = metadata();
    assertEquals(winning.metadataFileLocation(), committed.metadataFileLocation());
    assertEquals(winning.refs(), committed.refs());
    assertEquals(winning.properties(), committed.properties());
    assertEquals("retained", committed.properties().get("winner-property"));
    assertFalse(committed.refs().containsKey("loser"));
    assertFalse(committed.properties().containsKey("loser-property"));
    assertNull(committed.schema().findField("loser_column"));
  }

  private GetTableResponseBody fixture(String tableName) {
    GetTableResponseBody fixture = buildGetTableResponseBodyWithDbTbl("d1", tableName);
    identifier = TableIdentifier.of(fixture.getDatabaseId(), fixture.getTableId());
    return fixture;
  }

  private void seed(String tableName, int snapshots) throws Exception {
    latestResponse = createTableAndValidateResponse(fixture(tableName), mvc, storageManager);
    trackDirectory(catalog.loadTable(identifier).location());
    for (int i = 0; i < snapshots; i++) {
      GetTableResponseBody current = buildGetTableResponseBody(latestResponse);
      IcebergSnapshotsRequestBody legacy =
          preparePutSnapshotsWithAppendRequest(
              latestResponse,
              current,
              current.getTableLocation(),
              catalog,
              Collections.singletonList(dataFile(catalog.loadTable(identifier))));
      latestResponse = putSnapshotsAndValidateResponse(catalog, mvc, legacy, false);
    }
  }

  private org.apache.iceberg.DataFile dataFile(Table table) throws Exception {
    return createDummyDataFile(
        dataDirectory.resolve("data-" + nextDataFile++ + ".orc").toString(), table.spec());
  }

  private void trackDirectory(String location) {
    tableDirectory =
        location.startsWith("file:") ? Paths.get(URI.create(location)) : Paths.get(location);
  }

  private BaseTransaction transaction() {
    return (BaseTransaction) catalog.loadTable(identifier).newTransaction();
  }

  private TableMetadata metadata() {
    Table table = catalog.loadTable(identifier);
    table.refresh();
    return ((BaseTable) table).operations().current();
  }

  private CreateUpdateTableRequestBody envelope(TableMetadata base) throws Exception {
    GetTableResponseBody response =
        new Gson()
            .fromJson(
                latestResponse.getResponse().getContentAsString(), GetTableResponseBody.class);
    return buildCreateUpdateTableRequestBody(response)
        .toBuilder()
        .baseTableVersion(base == null ? INITIAL_TABLE_VERSION : base.metadataFileLocation())
        .build();
  }

  private IcebergSnapshotsRequestBody request(TableMetadata base, BaseTransaction transaction)
      throws Exception {
    return IcebergSnapshotsRequestBody.builder()
        .baseTableVersion(base.metadataFileLocation())
        .createUpdateTableRequestBody(envelope(base))
        .updates(updates(transaction.currentMetadata()))
        .build();
  }

  private ResultActions put(IcebergSnapshotsRequestBody request) throws Exception {
    return mvc.perform(
            MockMvcRequestBuilders.put(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/{database}/tables/{table}/iceberg/v2/snapshots",
                    identifier.namespace().toString(),
                    identifier.name())
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .content(request.toJson()))
        .andExpect(jsonPath("$.commitResult").doesNotExist());
  }

  private static List<Map<String, Object>> updates(TableMetadata metadata) throws Exception {
    List<Map<String, Object>> updates = new ArrayList<>();
    for (MetadataUpdate update : metadata.changes()) {
      updates.add(
          MAPPER.readValue(
              MetadataUpdateParser.toJson(update), new TypeReference<Map<String, Object>>() {}));
    }
    return updates;
  }

  private static List<Long> snapshotIds(TableMetadata metadata) {
    return metadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toList());
  }

  private static List<Integer> refUpdateIndices(List<Map<String, Object>> updates) {
    List<Integer> indices = new ArrayList<>();
    for (int index = 0; index < updates.size(); index++) {
      Object action = updates.get(index).get("action");
      if ("set-snapshot-ref".equals(action) || "remove-snapshot-ref".equals(action)) {
        indices.add(index);
      }
    }
    return indices;
  }

  private TableAuditEvent snapshotAudit(OperationStatus status) {
    ArgumentCaptor<TableAuditEvent> events = ArgumentCaptor.forClass(TableAuditEvent.class);
    verify(tableAuditHandler, atLeastOnce()).audit(events.capture());
    List<TableAuditEvent> matching =
        events.getAllValues().stream()
            .filter(event -> identifier.name().equals(event.getTableName()))
            .filter(
                event ->
                    event.getOperationType() == OperationType.COMMIT
                        || event.getOperationType() == OperationType.STAGED_COMMIT)
            .filter(event -> event.getOperationStatus() == status)
            .collect(Collectors.toList());
    assertEquals(1, matching.size());
    return matching.get(0);
  }

  private static void assertTransition(
      SnapshotRefChange change, int index, String action, String ref, Long before, Long after) {
    assertEquals(index, change.getUpdateIndex());
    assertEquals(action, change.getAction());
    assertEquals(ref, change.getRefName());
    assertEquals(before, change.getBefore() == null ? null : change.getBefore().getSnapshotId());
    assertEquals(after, change.getAfter() == null ? null : change.getAfter().getSnapshotId());
    if (change.getAfter() != null) {
      assertEquals("branch", change.getAfter().getType());
    }
  }

  private TableDtoPrimaryKey key() {
    return TableDtoPrimaryKey.builder()
        .databaseId(identifier.namespace().toString())
        .tableId(identifier.name())
        .build();
  }

  @AfterEach
  void cleanup() throws Exception {
    reset(houseTableRepository);
    if (identifier != null) {
      repository.findById(key()).ifPresent(repository::delete);
    }
    if (tableDirectory != null) {
      FileSystemUtils.deleteRecursively(tableDirectory);
    }
  }
}

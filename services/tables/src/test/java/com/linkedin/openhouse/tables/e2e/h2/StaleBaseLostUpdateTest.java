package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.tables.model.TableModelConstants.TABLE_DTO;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalTableOperations;
import com.linkedin.openhouse.internal.catalog.SnapshotsUtil;
import com.linkedin.openhouse.tables.model.IcebergSnapshotsModelTestUtilities;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

/**
 * Deterministic H2 reproduction of the incident-12185 stale-base silent snapshot drop. Single-JVM,
 * no threads, no docker.
 *
 * <p>A stale writer opens an Iceberg transaction at base L1 and stages its real L1 snapshot view
 * (the writer's {@code SNAPSHOTS_JSON} payload) plus {@code COMMIT_KEY=L1}, but holds it
 * uncommitted. A racing writer appends a new snapshot via the repository path, advancing the
 * catalog L1 -&gt; L2. When the held transaction is committed, {@code BaseTransaction.applyUpdates}
 * rebases the stale payload + {@code COMMIT_KEY=L1} onto L2, so {@code doCommit} would subtract the
 * racing snapshot. The invariant asserted is that the racing snapshot must survive (PR #612's
 * stale-base CAS aborts the commit; without it the racing snapshot is silently dropped).
 */
@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class StaleBaseLostUpdateTest {

  private static final Logger LOG = LoggerFactory.getLogger(StaleBaseLostUpdateTest.class);

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired Catalog catalog;

  /** Base table is freshly created with zero committed data snapshots when the race begins. */
  @Test
  void testLostUpdateViaStagedTransactionConflictWithNoPriorData() throws Exception {
    TableDto l1 = createTableWithCommittedDataSnapshots("conflict_staged_no_data", 0);
    assertRacingSnapshotSurvivesStaleStagedCommit(l1);
  }

  /** Base table holds a single committed data snapshot when the race begins. */
  @Test
  void testLostUpdateViaStagedTransactionConflict() throws Exception {
    TableDto l1 = createTableWithCommittedDataSnapshots("conflict_staged", 1);
    assertRacingSnapshotSurvivesStaleStagedCommit(l1);
  }

  /**
   * Same scenario, but the table already has prior committed data history (two snapshots) before
   * the race begins — the stale writer's base is a non-trivial, data-bearing table.
   */
  @Test
  void testLostUpdateViaStagedTransactionConflictWithPriorDataSnapshot() throws Exception {
    TableDto l1 = createTableWithCommittedDataSnapshots("conflict_staged_prior_data", 2);
    assertRacingSnapshotSurvivesStaleStagedCommit(l1);
  }

  /**
   * Core reproduction. Given a table whose latest committed version is {@code l1}, stages a stale
   * commit at L1, lands a racing append (L1 -&gt; L2), then commits the stale transaction and
   * asserts the racing snapshot is not dropped.
   */
  private void assertRacingSnapshotSurvivesStaleStagedCommit(TableDto l1) throws Exception {
    TableIdentifier id = idOf(l1);

    // Stale writer opens a transaction at L1 and performs its own insert: a new data snapshot
    // appended at base L1. Its staged payload contains the existing snapshots + its own append but
    // NOT the racing snapshot (which it cannot see), with COMMIT_KEY=L1 — exactly what
    // OpenHouseInternalRepositoryImpl.save stamps. Held uncommitted so a racing commit lands first.
    Table staleHandle = catalog.loadTable(id);
    List<Snapshot> staleView = Lists.newArrayList(staleHandle.snapshots());
    int priorSnapshotCount = staleView.size();
    Snapshot staleInsert = staleHandle.newAppend().appendFile(dummyDataFile()).apply();
    List<Snapshot> stalePayload = new ArrayList<>(staleView);
    stalePayload.add(staleInsert);
    Transaction staleTxn = staleHandle.newTransaction();
    staleTxn
        .updateProperties()
        .set(CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(stalePayload))
        .set(CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(refs(staleInsert)))
        .set(CatalogConstants.COMMIT_KEY, l1.getTableLocation())
        .commit();

    // Racing writer appends S2 via the repository path -> catalog advances L1 -> L2.
    Snapshot s2 = catalog.loadTable(id).newAppend().appendFile(dummyDataFile()).apply();
    List<String> racingSnapshots = new ArrayList<>();
    for (Snapshot existing : staleView) {
      racingSnapshots.add(SnapshotParser.toJson(existing));
    }
    racingSnapshots.add(SnapshotParser.toJson(s2));
    openHouseInternalRepository.save(
        l1.toBuilder()
            .tableVersion(l1.getTableLocation())
            .jsonSnapshots(racingSnapshots)
            .snapshotRefs(refs(s2))
            .build());

    clearPerJvmRetryCache();

    // Commit the held transaction: applyUpdates sees base L1 != catalog L2, rebases, and re-stamps
    // the stale view + COMMIT_KEY=L1 onto L2. A clean rejection is acceptable and version-dependent
    // (CommitFailedException from PR #612's CAS, or BadRequestException from the pre-#406 guard);
    // the invariant below is the same either way: the racing snapshot must survive.
    try {
      staleTxn.commitTransaction();
    } catch (Exception expected) {
      LOG.info("commitTransaction rejected the stale commit: {}", expected.toString());
    }

    List<Snapshot> remaining = Lists.newArrayList(catalog.loadTable(id).snapshots());
    LOG.info(
        "staged-conflict result: remaining snapshots={}",
        remaining.stream().map(Snapshot::snapshotId).collect(Collectors.toList()));
    Assertions.assertTrue(
        remaining.stream().anyMatch(x -> x.snapshotId() == s2.snapshotId()),
        "racing snapshot S2 (" + s2.snapshotId() + ") must not be silently dropped from H2");
    Assertions.assertTrue(
        remaining.stream().noneMatch(x -> x.snapshotId() == staleInsert.snapshotId()),
        "stale writer's conflicting insert ("
            + staleInsert.snapshotId()
            + ") must be rejected wholesale, not merged on top of the racing commit");
    Assertions.assertEquals(
        priorSnapshotCount + 1,
        remaining.size(),
        "table must hold only the prior snapshots + the racing snapshot S2");

    cleanup(id);
  }

  /**
   * Creates a table and commits {@code count} data-bearing snapshots through the repository path,
   * returning the latest committed {@link TableDto} (the base the stale writer will load as L1).
   */
  private TableDto createTableWithCommittedDataSnapshots(String tableId, int count) throws Exception {
    Schema schema =
        new Schema(
            required(1, "id", Types.StringType.get()), optional(2, "data", Types.StringType.get()));
    TableDto dto =
        openHouseInternalRepository.save(
            TABLE_DTO
                .toBuilder()
                .tableId(tableId)
                .schema(SchemaParser.toJson(schema, false))
                .timePartitioning(null)
                .clustering(null)
                .tableVersion(INITIAL_TABLE_VERSION)
                .build());
    TableIdentifier id = idOf(dto);
    List<Snapshot> committed = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Snapshot snapshot = catalog.loadTable(id).newAppend().appendFile(dummyDataFile()).apply();
      committed.add(snapshot);
      List<String> jsonSnapshots =
          committed.stream().map(SnapshotParser::toJson).collect(Collectors.toList());
      dto =
          openHouseInternalRepository.save(
              dto.toBuilder()
                  .tableVersion(dto.getTableLocation())
                  .jsonSnapshots(jsonSnapshots)
                  .snapshotRefs(refs(snapshot))
                  .build());
    }
    return dto;
  }

  private static TableIdentifier idOf(TableDto dto) {
    return TableIdentifier.of(dto.getDatabaseId(), dto.getTableId());
  }

  private DataFile dummyDataFile() throws Exception {
    return IcebergSnapshotsModelTestUtilities.createDummyDataFile(
        Files.createTempFile("incident-12185-", ".orc").toString(), PartitionSpec.unpartitioned());
  }

  private static Map<String, String> refs(Snapshot snapshot) {
    return IcebergSnapshotsModelTestUtilities.obtainSnapshotRefsFromSnapshot(
        SnapshotParser.toJson(snapshot));
  }

  private void cleanup(TableIdentifier id) {
    openHouseInternalRepository.deleteById(
        TableDtoPrimaryKey.builder()
            .databaseId(id.namespace().toString())
            .tableId(id.name())
            .build());
  }

  /**
   * Invalidates the per-JVM static retry cache used by {@code failIfRetryUpdate} so the test models
   * a stale commit reaching a replica that never cached the prior {@code COMMIT_KEY}. Without this,
   * the single-JVM cache hit would short-circuit the second committer before the abort is
   * exercised.
   */
  private static void clearPerJvmRetryCache() throws Exception {
    Field cacheField = OpenHouseInternalTableOperations.class.getDeclaredField("CACHE");
    cacheField.setAccessible(true);
    ((com.google.common.cache.Cache<?, ?>) cacheField.get(null)).invalidateAll();
  }
}

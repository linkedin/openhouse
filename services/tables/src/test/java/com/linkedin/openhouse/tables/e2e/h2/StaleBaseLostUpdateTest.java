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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
 * Deterministic H2 reproduction of the incident-12185 stale-base silent snapshot drop. A stale
 * writer holds an uncommitted transaction opened at base L1; a racing writer advances the catalog
 * L1 -> L2 (adding S2); the stale transaction then commits, and {@code BaseTransaction.applyUpdates}
 * silently rebases the stale {S1} view + COMMIT_KEY=L1 onto L2. Without the PR #612 abort, S2 is
 * silently dropped.
 */
@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class StaleBaseLostUpdateTest {

  private static final Logger LOG = LoggerFactory.getLogger(StaleBaseLostUpdateTest.class);

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired Catalog catalog;

  @Test
  void testLostUpdateViaStagedTransactionConflict() throws Exception {
    Setup s = createTableWithOneSnapshot("conflict_staged");

    // Stale writer opens a transaction at L1 and stages its real L1 view ({S1}) -- exactly what
    // OpenHouseInternalRepositoryImpl.save does internally, but left uncommitted so a racing commit
    // can land first.
    Table staleHandle = catalog.loadTable(s.id);
    List<Snapshot> staleView = Lists.newArrayList(staleHandle.snapshots());
    Assertions.assertEquals(1, staleView.size(), "stale writer's base L1 must hold exactly {S1}");
    Transaction staleTxn = staleHandle.newTransaction();
    staleTxn
        .updateProperties()
        .set(CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(staleView))
        .set(CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(refs(s.s1)))
        .set(CatalogConstants.COMMIT_KEY, s.l1.getTableLocation())
        .set("foo", "bar")
        .commit();

    // Racing writer appends S2 via the repository path -> catalog advances L1 -> L2.
    Snapshot s2 = catalog.loadTable(s.id).newAppend().appendFile(dummyDataFile()).apply();
    openHouseInternalRepository.save(
        s.l1
            .toBuilder()
            .tableVersion(s.l1.getTableLocation())
            .jsonSnapshots(Arrays.asList(SnapshotParser.toJson(s.s1), SnapshotParser.toJson(s2)))
            .snapshotRefs(refs(s2))
            .build());

    clearPerJvmRetryCache();

    // Commit the held transaction: applyUpdates sees base L1 != catalog L2, rebases, and re-stamps
    // the now-stale {S1} list + COMMIT_KEY=L1 onto L2. With #612 this aborts; without it, S2 is
    // silently dropped.
    try {
      staleTxn.commitTransaction();
    } catch (Exception expected) {
      // A clean rejection is acceptable and version-dependent:
      //  - D (#612): CommitFailedException from the stale-base CAS.
      //  - A (pre-#406): BadRequestException from SnapshotInspector.validateSnapshotsUpdate
      //    ("Cannot delete the latest snapshot").
      // The invariant asserted below is the same either way: racing S2 must survive.
      LOG.info("commitTransaction rejected the stale commit: {}", expected.toString());
    }

    List<Snapshot> remaining = Lists.newArrayList(catalog.loadTable(s.id).snapshots());
    LOG.info(
        "staged-conflict result: remaining snapshots={}",
        remaining.stream()
            .map(Snapshot::snapshotId)
            .collect(java.util.stream.Collectors.toList()));
    Assertions.assertEquals(
        2, remaining.size(), "S2 (" + s2.snapshotId() + ") must not be silently dropped from H2");
    Assertions.assertTrue(
        remaining.stream().anyMatch(x -> x.snapshotId() == s2.snapshotId()),
        "racing snapshot S2 must still be present after the conflicting stale commit");

    cleanup(s.id);
  }

  /** Holds the post-setup state of a freshly created table with a single snapshot S1 at L1. */
  private static final class Setup {
    final TableIdentifier id;
    final TableDto l1;
    final Snapshot s1;

    Setup(TableIdentifier id, TableDto l1, Snapshot s1) {
      this.id = id;
      this.l1 = l1;
      this.s1 = s1;
    }
  }

  private Setup createTableWithOneSnapshot(String tableId) throws Exception {
    Schema schema =
        new Schema(
            required(1, "id", Types.StringType.get()), optional(2, "data", Types.StringType.get()));
    TableDto l0 =
        openHouseInternalRepository.save(
            TABLE_DTO
                .toBuilder()
                .tableId(tableId)
                .schema(SchemaParser.toJson(schema, false))
                .timePartitioning(null)
                .clustering(null)
                .tableVersion(INITIAL_TABLE_VERSION)
                .build());
    TableIdentifier id = TableIdentifier.of(l0.getDatabaseId(), tableId);
    Snapshot s1 = catalog.loadTable(id).newAppend().appendFile(dummyDataFile()).apply();
    TableDto l1 =
        openHouseInternalRepository.save(
            l0.toBuilder()
                .tableVersion(l0.getTableLocation())
                .jsonSnapshots(Collections.singletonList(SnapshotParser.toJson(s1)))
                .snapshotRefs(refs(s1))
                .build());
    return new Setup(id, l1, s1);
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
   * the single-JVM cache hit would short-circuit the second committer before #612 is exercised.
   */
  private static void clearPerJvmRetryCache() throws Exception {
    Field cacheField = OpenHouseInternalTableOperations.class.getDeclaredField("CACHE");
    cacheField.setAccessible(true);
    ((com.google.common.cache.Cache<?, ?>) cacheField.get(null)).invalidateAll();
  }
}

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
import java.util.Set;
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
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

/**
 * Concurrency tests for committing a snapshot set against a stale base version.
 *
 * <p>In each test the table is at version L1, a second writer commits a new snapshot (advancing the
 * catalog to L2), and then a writer that still declares L1 as its base commits a snapshot set
 * computed at L1 — which therefore omits the snapshot the second writer just added. The catalog
 * must not drop that concurrently added snapshot: the stale commit is rejected and the
 * concurrently added snapshot remains in the table.
 */
@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class StaleBaseLostUpdateTest {

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired Catalog catalog;

  /**
   * An expiration commit declaring a stale base, racing a concurrent insert. The expiring writer
   * keeps only the current head and drops older snapshots; its kept set, computed at the stale
   * base, does not include the concurrently inserted snapshot. The concurrent insert must remain
   * and the expiration must be rejected, leaving the prior snapshots plus the concurrent insert.
   */
  @Test
  void testExpireSnapshotsDropsConcurrentDataCommit() throws Exception {
    TableDto l1 = createTableWithCommittedDataSnapshots("expire_race", 2);
    TableIdentifier id = TableIdentifier.of(l1.getDatabaseId(), l1.getTableId());

    Table staleHandle = catalog.loadTable(id);
    List<Snapshot> base = Lists.newArrayList(staleHandle.snapshots());
    Snapshot head = staleHandle.currentSnapshot();
    List<Snapshot> keepOnlyHead = Lists.newArrayList(head);

    assertRacingDataCommitSurvivesStaleCommit(l1, base, keepOnlyHead, head);
  }

  /**
   * Two writers each perform the first insert into a freshly created table. One commits first; the
   * other then commits its own first snapshot still declaring the create version as its base. The
   * first writer's snapshot must remain.
   */
  @Test
  void testStaleInsertDropsConcurrentDataCommitOnFreshTable() throws Exception {
    TableDto l1 = createTableWithCommittedDataSnapshots("insert_race_fresh", 0);
    TableIdentifier id = TableIdentifier.of(l1.getDatabaseId(), l1.getTableId());

    Table staleHandle = catalog.loadTable(id);
    List<Snapshot> base = Lists.newArrayList(staleHandle.snapshots()); // empty
    Snapshot staleInsert = staleHandle.newAppend().appendFile(dummyDataFile()).apply();
    List<Snapshot> stalePayload = new ArrayList<>(base);
    stalePayload.add(staleInsert);

    assertRacingDataCommitSurvivesStaleCommit(l1, base, stalePayload, staleInsert);
  }

  /**
   * Two writers insert into a table that already holds data, both based on the same version. The
   * stale writer's appended snapshot shares a sequence number with the concurrent insert, so the
   * catalog rejects the stale commit; the concurrent insert remains.
   */
  @Test
  void testConcurrentInsertOnPopulatedTableIsRejected() throws Exception {
    TableDto l1 = createTableWithCommittedDataSnapshots("insert_race_populated", 2);
    TableIdentifier id = TableIdentifier.of(l1.getDatabaseId(), l1.getTableId());

    Table staleHandle = catalog.loadTable(id);
    List<Snapshot> base = Lists.newArrayList(staleHandle.snapshots());
    Snapshot staleInsert = staleHandle.newAppend().appendFile(dummyDataFile()).apply();
    List<Snapshot> stalePayload = new ArrayList<>(base);
    stalePayload.add(staleInsert);

    assertRacingDataCommitSurvivesStaleCommit(l1, base, stalePayload, staleInsert);
  }

  /**
   * A stale commit that updates metadata without adding a snapshot, racing a concurrent insert. Its
   * declared snapshot set is its base view, which omits the concurrent insert, so the diff is a
   * pure deletion of the concurrent snapshot. The concurrent insert must remain and the stale
   * commit must be rejected.
   */
  @Test
  void testStaleMetadataUpdateDropsConcurrentDataCommit() throws Exception {
    TableDto l1 = createTableWithCommittedDataSnapshots("metadata_update_race", 2);
    TableIdentifier id = TableIdentifier.of(l1.getDatabaseId(), l1.getTableId());

    Table staleHandle = catalog.loadTable(id);
    List<Snapshot> base = Lists.newArrayList(staleHandle.snapshots());
    Snapshot head = staleHandle.currentSnapshot();

    // The stale writer adds no snapshot; its declared set is its base view and main stays at head.
    assertRacingDataCommitSurvivesStaleCommit(l1, base, base, head);
  }

  /**
   * Loads the table at version {@code l1}, has a second writer commit a fresh data snapshot
   * (advancing the catalog), then commits a writer that still declares {@code l1} as its base with
   * {@code stalePayload} (pointing main at {@code staleHead}) — a snapshot set that omits the second
   * writer's snapshot. After the stale commit, the table must hold exactly the prior snapshots plus
   * the second writer's snapshot: the stale commit applies none of its payload.
   *
   * @param base the snapshots present at {@code l1}
   * @param stalePayload the snapshot set the stale writer commits (omits the concurrent snapshot)
   * @param staleHead the snapshot the stale writer points main at
   */
  private void assertRacingDataCommitSurvivesStaleCommit(
      TableDto l1, List<Snapshot> base, List<Snapshot> stalePayload, Snapshot staleHead)
      throws Exception {
    TableIdentifier id = TableIdentifier.of(l1.getDatabaseId(), l1.getTableId());

    // Stale writer holds a transaction at L1 staging its payload, left uncommitted.
    Transaction staleTxn = catalog.loadTable(id).newTransaction();
    staleTxn
        .updateProperties()
        .set(CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(stalePayload))
        .set(
            CatalogConstants.SNAPSHOTS_REFS_KEY,
            SnapshotsUtil.serializeMap(
                IcebergSnapshotsModelTestUtilities.obtainSnapshotRefsFromSnapshot(
                    SnapshotParser.toJson(staleHead))))
        .set(CatalogConstants.COMMIT_KEY, l1.getTableLocation())
        .commit();

    // Second writer, also based on L1, commits a fresh data snapshot, advancing the catalog to L2.
    Snapshot racing = catalog.loadTable(id).newAppend().appendFile(dummyDataFile()).apply();
    List<Snapshot> snapshotsAfterRace = new ArrayList<>(base);
    snapshotsAfterRace.add(racing);
    openHouseInternalRepository.save(
        l1.toBuilder()
            .tableVersion(l1.getTableLocation())
            .jsonSnapshots(
                snapshotsAfterRace.stream()
                    .map(SnapshotParser::toJson)
                    .collect(Collectors.toList()))
            .snapshotRefs(
                IcebergSnapshotsModelTestUtilities.obtainSnapshotRefsFromSnapshot(
                    SnapshotParser.toJson(racing)))
            .build());

    // Evaluate the stale commit on its base version rather than short-circuit it as a duplicate.
    clearRetryCache();

    // The stale commit declares a base that no longer matches the catalog, so it must be rejected
    // (the rejection's exception type is not part of the contract).
    Assertions.assertThrows(
        Exception.class,
        staleTxn::commitTransaction,
        "the stale commit must be rejected, not applied against the advanced catalog");

    // The table must then hold exactly the prior snapshots plus the concurrently committed snapshot.
    Set<Long> expected = base.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    expected.add(racing.snapshotId());
    Set<Long> actual =
        Lists.newArrayList(catalog.loadTable(id).snapshots()).stream()
            .map(Snapshot::snapshotId)
            .collect(Collectors.toSet());
    Assertions.assertEquals(
        expected,
        actual,
        "table must hold exactly the prior snapshots plus the concurrently committed snapshot "
            + racing.snapshotId());

    cleanup(id);
  }

  /**
   * Creates a table and commits {@code count} data-bearing snapshots, returning the latest
   * committed {@link TableDto} (the version both writers load as their base).
   */
  private TableDto createTableWithCommittedDataSnapshots(String tableId, int count)
      throws Exception {
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
    TableIdentifier id = TableIdentifier.of(dto.getDatabaseId(), dto.getTableId());
    List<Snapshot> committed = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Snapshot snapshot = catalog.loadTable(id).newAppend().appendFile(dummyDataFile()).apply();
      committed.add(snapshot);
      dto =
          openHouseInternalRepository.save(
              dto.toBuilder()
                  .tableVersion(dto.getTableLocation())
                  .jsonSnapshots(
                      committed.stream().map(SnapshotParser::toJson).collect(Collectors.toList()))
                  .snapshotRefs(
                      IcebergSnapshotsModelTestUtilities.obtainSnapshotRefsFromSnapshot(
                          SnapshotParser.toJson(snapshot)))
                  .build());
    }
    return dto;
  }

  private DataFile dummyDataFile() throws Exception {
    return IcebergSnapshotsModelTestUtilities.createDummyDataFile(
        Files.createTempFile("stale-base-conflict-", ".orc").toString(),
        PartitionSpec.unpartitioned());
  }

  private void cleanup(TableIdentifier id) {
    openHouseInternalRepository.deleteById(
        TableDtoPrimaryKey.builder()
            .databaseId(id.namespace().toString())
            .tableId(id.name())
            .build());
  }

  /**
   * Invalidates the per-JVM retry cache so the stale commit is evaluated against its base version
   * rather than short-circuited as a duplicate retry of an already-seen commit.
   */
  private static void clearRetryCache() throws Exception {
    Field cacheField = OpenHouseInternalTableOperations.class.getDeclaredField("CACHE");
    cacheField.setAccessible(true);
    ((com.google.common.cache.Cache<?, ?>) cacheField.get(null)).invalidateAll();
  }
}

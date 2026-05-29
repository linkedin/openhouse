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
 * A stale writer opens an Iceberg transaction at base L1 and stages its L1 snapshot view +
 * {@code COMMIT_KEY=L1}, held uncommitted. A racing writer commits a new snapshot through the
 * repository path, advancing the catalog L1 -&gt; L2. Committing the held transaction drives {@code
 * BaseTransaction.applyUpdates} to silently refresh the in-flight base to L2 (passing the HTS
 * optimistic-version CAS) while keeping the stale payload + {@code COMMIT_KEY=L1}; {@code doCommit}
 * then subtracts the racing snapshot. On the buggy catalog the racing commit is silently dropped;
 * PR #612's stale-base CAS aborts instead. Invariant: the racing data commit always survives.
 *
 * <p>Both cases here are <b>subtractive</b> stale commits — the stale payload omits the racing
 * snapshot and does not add a new snapshot whose sequence number would collide with it. That is the
 * shape that actually reproduced incident-12185 (an expire/optimizer commit dropping a fresh data
 * commit). A stale writer that <i>adds</i> its own snapshot on a multi-snapshot base is instead
 * rejected by Iceberg's snapshot sequence-number validation, so that shape cannot lose data.
 */
@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class StaleBaseLostUpdateTest {

  private static final Logger LOG = LoggerFactory.getLogger(StaleBaseLostUpdateTest.class);

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired Catalog catalog;

  /**
   * Snapshot-expiration commit racing a concurrent data insert — the production maintenance shape,
   * on a table with prior committed data history. The stale expire keeps only the current head
   * (expiring older snapshots); its keep-subset cannot reference the concurrently-added data
   * snapshot, so the subtractive merge would expire the racing data commit. Asserts the data commit
   * survives and the stale expire is rejected wholesale.
   */
  @Test
  void testExpireSnapshotsDropsConcurrentDataCommit() throws Exception {
    TableDto l1 = createTableWithCommittedDataSnapshots("expire_race", 2);
    TableIdentifier id = idOf(l1);

    Table staleHandle = catalog.loadTable(id);
    List<Snapshot> base = Lists.newArrayList(staleHandle.snapshots());
    Snapshot head = staleHandle.currentSnapshot();
    List<Snapshot> keepOnlyHead = Lists.newArrayList(head);

    assertRacingDataCommitSurvivesStaleCommit(l1, base, keepOnlyHead, head);
  }

  /**
   * Two writers concurrently perform the first insert into a freshly created table. The racing
   * writer commits first; the stale writer then commits its own first snapshot still declaring the
   * create version as its base. The subtractive merge would drop the racing first commit.
   */
  @Test
  void testStaleInsertDropsConcurrentDataCommitOnFreshTable() throws Exception {
    TableDto l1 = createTableWithCommittedDataSnapshots("insert_race_fresh", 0);
    TableIdentifier id = idOf(l1);

    Table staleHandle = catalog.loadTable(id);
    List<Snapshot> base = Lists.newArrayList(staleHandle.snapshots()); // empty
    Snapshot staleInsert = staleHandle.newAppend().appendFile(dummyDataFile()).apply();

    assertRacingDataCommitSurvivesStaleCommit(l1, base, appended(base, staleInsert), staleInsert);
  }

  /**
   * Core reproduction. A stale writer holds a transaction at base {@code l1} staging {@code
   * stalePayload} (main -&gt; {@code staleHead}); a racing writer commits a fresh data snapshot
   * first (L1 -&gt; L2); the held transaction then commits, rebasing the stale payload (which omits
   * the racing snapshot) onto L2. Asserts the racing commit survives and the stale commit is
   * rejected wholesale (nothing from the stale payload is applied).
   *
   * @param base the snapshots present at L1 (the stale writer's view before its own staging)
   * @param stalePayload the full snapshot set the stale writer commits (omits the racing snapshot)
   * @param staleHead the snapshot the stale writer points main at
   */
  private void assertRacingDataCommitSurvivesStaleCommit(
      TableDto l1, List<Snapshot> base, List<Snapshot> stalePayload, Snapshot staleHead)
      throws Exception {
    TableIdentifier id = idOf(l1);
    int priorSnapshotCount = base.size();

    // Stale writer: held transaction at L1 staging its payload + COMMIT_KEY=L1, uncommitted.
    Transaction staleTxn = catalog.loadTable(id).newTransaction();
    staleTxn
        .updateProperties()
        .set(CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(stalePayload))
        .set(CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(refs(staleHead)))
        .set(CatalogConstants.COMMIT_KEY, l1.getTableLocation())
        .commit();

    // Racing writer (also based on L1) commits a fresh data snapshot first -> L1 -> L2.
    Snapshot racing = catalog.loadTable(id).newAppend().appendFile(dummyDataFile()).apply();
    commitThroughRepository(l1, snapshotJson(base, racing), racing);

    clearPerJvmRetryCache();

    try {
      staleTxn.commitTransaction();
    } catch (Exception expected) {
      LOG.info("stale commit rejected: {}", expected.toString());
    }

    List<Snapshot> remaining = Lists.newArrayList(catalog.loadTable(id).snapshots());
    LOG.info(
        "stale-conflict result: remaining={}",
        remaining.stream().map(Snapshot::snapshotId).collect(Collectors.toList()));
    Assertions.assertTrue(
        remaining.stream().anyMatch(s -> s.snapshotId() == racing.snapshotId()),
        "racing data commit (" + racing.snapshotId() + ") must not be silently dropped");
    Assertions.assertEquals(
        priorSnapshotCount + 1,
        remaining.size(),
        "stale commit must be rejected wholesale; table holds only the prior snapshots + the racing "
            + "data commit");

    cleanup(id);
  }

  /** Commits {@code jsonSnapshots} (main -&gt; {@code head}) declaring base {@code l1}'s version. */
  private void commitThroughRepository(TableDto l1, List<String> jsonSnapshots, Snapshot head) {
    openHouseInternalRepository.save(
        l1.toBuilder()
            .tableVersion(l1.getTableLocation())
            .jsonSnapshots(jsonSnapshots)
            .snapshotRefs(refs(head))
            .build());
  }

  /**
   * Creates a table and commits {@code count} data-bearing snapshots through the repository path,
   * returning the latest committed {@link TableDto} (the base L1 the racers will load).
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
    TableIdentifier id = idOf(dto);
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
                  .snapshotRefs(refs(snapshot))
                  .build());
    }
    return dto;
  }

  private static List<Snapshot> appended(List<Snapshot> existing, Snapshot extra) {
    List<Snapshot> all = new ArrayList<>(existing);
    all.add(extra);
    return all;
  }

  private static List<String> snapshotJson(List<Snapshot> existing, Snapshot extra) {
    return appended(existing, extra).stream()
        .map(SnapshotParser::toJson)
        .collect(Collectors.toList());
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
   * Invalidates the per-JVM static retry cache used by {@code failIfRetryUpdate} so the stale write
   * models a request reaching a replica that never cached the racing commit's {@code COMMIT_KEY}.
   * Without this, the single-JVM cache hit would reject the stale write before the subtractive
   * merge (or the PR #612 abort) is exercised.
   */
  private static void clearPerJvmRetryCache() throws Exception {
    Field cacheField = OpenHouseInternalTableOperations.class.getDeclaredField("CACHE");
    cacheField.setAccessible(true);
    ((com.google.common.cache.Cache<?, ?>) cacheField.get(null)).invalidateAll();
  }
}

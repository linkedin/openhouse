package com.linkedin.openhouse.jobs.spark;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Branch expiration in the snapshot expiration job. Ages sit between the 3-day history windows used
 * here and the 7-day default maximum reference age, so each test isolates branch expiration from
 * Iceberg's own reference expiration.
 */
class SnapshotsExpirationSparkAppTest {
  private static final String TABLE_NAME = "db.branch_expiration";
  private static final long DAY_MS = Duration.ofDays(1).toMillis();
  private static final long MINUTE_MS = Duration.ofMinutes(1).toMillis();

  @TempDir Path tempDir;

  @Test
  void expiresBranchesByWhenTheySplitFromMainNotByTheirHead() {
    long now = System.currentTimeMillis();
    Table table = createTable();
    addSnapshot(table, 1, null, now - 10 * DAY_MS, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("idle", 1).createBranch("active", 1).commit();
    addSnapshot(table, 2, 1L, now - 9 * DAY_MS, "active");
    addSnapshot(table, 3, 2L, now - DAY_MS, "active");
    addSnapshot(table, 4, 1L, now - 5 * DAY_MS, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createTag("release", 4).commit();
    addSnapshot(table, 5, 4L, now - DAY_MS, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("recent", 5).commit();
    addSnapshot(table, 6, 5L, now, "recent");
    addSnapshot(table, 7, 5L, now, SnapshotRef.MAIN_BRANCH);

    runApp(table, 3, "day", 0);

    Table reloaded = new HadoopTables().load(table.location());
    Assertions.assertEquals(
        new HashSet<>(Arrays.asList(SnapshotRef.MAIN_BRANCH, "recent", "release")),
        reloaded.refs().keySet(),
        "A branch with a fresh head is still removed when it split from main before the window");
    Assertions.assertEquals(7, reloaded.currentSnapshot().snapshotId());
    Assertions.assertEquals(6, reloaded.refs().get("recent").snapshotId());
    Assertions.assertEquals(4, reloaded.refs().get("release").snapshotId());
    Assertions.assertNull(reloaded.snapshot(1), "Removing the idle branch must unpin its snapshot");
    Assertions.assertNull(reloaded.snapshot(2), "Old branch history must expire in the same run");
    Assertions.assertNotNull(
        reloaded.snapshot(3), "Young unreferenced snapshots still obey maxAge");
    Assertions.assertNotNull(reloaded.snapshot(4), "Tags are not branch expiration candidates");
  }

  @Test
  void neverRemovesMainEvenWhenItsSnapshotIsOld() {
    Table table = createTable();
    addSnapshot(table, 1, null, System.currentTimeMillis() - 10 * DAY_MS, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("stale", 1).commit();

    runApp(table, 3, "day", 1);

    Assertions.assertEquals(
        new HashSet<>(Arrays.asList(SnapshotRef.MAIN_BRANCH)), table.refs().keySet());
    Assertions.assertEquals(1, table.currentSnapshot().snapshotId());
    Assertions.assertNotNull(table.snapshot(1));
  }

  @Test
  void usesTheHistoryGranularityForBranchSplits() {
    long now = System.currentTimeMillis();
    Table table = createTable();
    addSnapshot(table, 1, null, now - Duration.ofHours(3).toMillis(), SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("stale", 1).commit();
    addSnapshot(table, 2, 1L, now - 30 * MINUTE_MS, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("recent", 2).commit();
    addSnapshot(table, 3, 2L, now - 10 * MINUTE_MS, SnapshotRef.MAIN_BRANCH);

    runApp(table, 2, "hour", 1);

    Assertions.assertEquals(
        new HashSet<>(Arrays.asList(SnapshotRef.MAIN_BRANCH, "recent")), table.refs().keySet());
    Assertions.assertEquals(3, table.currentSnapshot().snapshotId());
    Assertions.assertNotNull(table.snapshot(2), "Versions must not drop a live branch's head");
    Assertions.assertNull(table.snapshot(1));
  }

  @Test
  void removesBranchesWhoseHistoryNoLongerReachesMain() {
    long now = System.currentTimeMillis();
    Table table = createTable();
    addSnapshot(table, 1, null, now - 10 * DAY_MS, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("cut_off", 1).commit();
    addSnapshot(table, 2, 1L, now - DAY_MS, "cut_off");
    addSnapshot(table, 3, 1L, now - DAY_MS, SnapshotRef.MAIN_BRANCH);
    table.expireSnapshots().expireSnapshotId(1).cleanExpiredFiles(false).commit();

    runApp(table, 3, "day", 0);

    Assertions.assertEquals(
        new HashSet<>(Arrays.asList(SnapshotRef.MAIN_BRANCH)),
        table.refs().keySet(),
        "A branch that shares no retained history with main is removed despite a fresh head");
    Assertions.assertEquals(3, table.currentSnapshot().snapshotId());
  }

  @Test
  void keepsRecentBranchesThatNeverSharedHistoryWithMain() {
    long now = System.currentTimeMillis();
    Table table = createTable();
    addSnapshot(table, 1, null, now - DAY_MS, SnapshotRef.MAIN_BRANCH);
    addSnapshot(table, 2, null, now - DAY_MS, "unrelated");

    runApp(table, 3, "day", 0);

    Assertions.assertEquals(
        new HashSet<>(Arrays.asList(SnapshotRef.MAIN_BRANCH, "unrelated")), table.refs().keySet());
    Assertions.assertEquals(2, table.refs().get("unrelated").snapshotId());
  }

  @Test
  void removesOldBranchesOnATableWithoutMain() {
    Table table = createTable();
    addSnapshot(table, 1, null, System.currentTimeMillis() - 5 * DAY_MS, "experiment");

    runApp(table, 3, "day", 0);

    Assertions.assertTrue(table.refs().isEmpty());
    Assertions.assertNull(table.snapshot(1));
    Assertions.assertNull(table.currentSnapshot());
  }

  @Test
  void removesBranchesWhenMainsHistoryBelowItsHeadExpired() {
    long now = System.currentTimeMillis();
    Table table = createTable();
    addSnapshot(table, 1, null, now - 5 * DAY_MS, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("forgotten", 1).commit();
    addSnapshot(table, 2, 1L, now - 4 * DAY_MS, SnapshotRef.MAIN_BRANCH);
    addSnapshot(table, 3, 2L, now, SnapshotRef.MAIN_BRANCH);
    table.expireSnapshots().expireSnapshotId(2).cleanExpiredFiles(false).commit();

    runApp(table, 3, "day", 0);

    Assertions.assertFalse(table.refs().containsKey("forgotten"));
    Assertions.assertNull(table.snapshot(1));
    Assertions.assertEquals(3, table.currentSnapshot().snapshotId());
  }

  @Test
  void appliesTheDefaultHistoryWindowWhenOnlyVersionsAreConfigured() {
    long now = System.currentTimeMillis();
    Table table = createTable();
    addSnapshot(table, 1, null, now - 4 * DAY_MS, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("stale", 1).commit();
    addSnapshot(table, 2, 1L, now - 2 * DAY_MS, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("recent", 2).commit();
    addSnapshot(table, 3, 2L, now - DAY_MS, SnapshotRef.MAIN_BRANCH);

    runApp(table, 0, "", 1);

    Assertions.assertEquals(
        new HashSet<>(Arrays.asList(SnapshotRef.MAIN_BRANCH, "recent")), table.refs().keySet());
    Assertions.assertNull(table.snapshot(1));
    Assertions.assertEquals(3, table.currentSnapshot().snapshotId());
  }

  @Test
  void removesBranchesOnceVersionsRetentionMovesMainPastTheirSplit() {
    long now = System.currentTimeMillis();
    Table table = createTable();
    addSnapshot(table, 1, null, now - 60 * MINUTE_MS, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("wap", 1).commit();
    addSnapshot(table, 2, 1L, now - 50 * MINUTE_MS, "wap");
    addSnapshot(table, 3, 1L, now - 40 * MINUTE_MS, SnapshotRef.MAIN_BRANCH);
    addSnapshot(table, 4, 3L, now - 30 * MINUTE_MS, SnapshotRef.MAIN_BRANCH);

    runApp(table, 3, "day", 1);

    Assertions.assertEquals(
        new HashSet<>(Arrays.asList(SnapshotRef.MAIN_BRANCH, "wap")),
        table.refs().keySet(),
        "The branch still split from a snapshot in main's retained history");
    Assertions.assertNull(table.snapshot(1), "Keeping one version expires the split snapshot");

    runApp(table, 3, "day", 1);

    Assertions.assertEquals(
        new HashSet<>(Arrays.asList(SnapshotRef.MAIN_BRANCH)),
        table.refs().keySet(),
        "Once main's retained history no longer includes the split, the branch is removed");
    Assertions.assertNull(table.snapshot(2), "The versions limit then expires its snapshots");
    Assertions.assertEquals(4, table.currentSnapshot().snapshotId());
  }

  @Test
  void keepsBranchesThatSplitExactlyAtTheCutoff() {
    long cutoff = System.currentTimeMillis() - 3 * DAY_MS;
    Table table = createTable();
    addSnapshot(table, 1, null, cutoff - 1, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("before", 1).commit();
    addSnapshot(table, 2, 1L, cutoff, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("at_cutoff", 2).commit();
    addSnapshot(table, 3, 2L, cutoff + 1, SnapshotRef.MAIN_BRANCH);
    table.manageSnapshots().createBranch("after", 3).commit();

    Operations.expireBranchesOlderThan(table, cutoff);

    Assertions.assertEquals(
        new HashSet<>(Arrays.asList(SnapshotRef.MAIN_BRANCH, "at_cutoff", "after")),
        table.refs().keySet());
    Assertions.assertEquals(2, table.refs().get("at_cutoff").snapshotId());
  }

  @Test
  void handlesAnEmptyTable() {
    Table table = createTable();

    runApp(table, 3, "day", 0);

    Assertions.assertTrue(table.refs().isEmpty());
    Assertions.assertNull(table.currentSnapshot());
  }

  private Table createTable() {
    return new HadoopTables()
        .create(
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
            tempDir.resolve("table").toString());
  }

  private void addSnapshot(Table table, long id, Long parentId, long timestampMs, String branch) {
    TableOperations tableOperations = ((HasTableOperations) table).operations();
    TableMetadata base = tableOperations.current();
    String parent = parentId == null ? "" : "\"parent-snapshot-id\":" + parentId + ",";
    Snapshot snapshot =
        SnapshotParser.fromJson(
            "{\"snapshot-id\":"
                + id
                + ",\"sequence-number\":"
                + (base.formatVersion() == 1 ? 0 : id)
                + ","
                + parent
                + "\"timestamp-ms\":"
                + System.currentTimeMillis()
                + ",\"summary\":{\"operation\":\"append\"},\"manifest-list\":\""
                + tempDir.resolve("snap-" + id + ".avro")
                + "\"}");
    JsonObject metadata =
        JsonParser.parseString(
                TableMetadataParser.toJson(
                    TableMetadata.buildFrom(base).setBranchSnapshot(snapshot, branch).build()))
            .getAsJsonObject();
    // Age the snapshot without moving Iceberg's metadata and snapshot-log clocks backwards.
    JsonArray snapshots = metadata.getAsJsonArray("snapshots");
    snapshots.get(snapshots.size() - 1).getAsJsonObject().addProperty("timestamp-ms", timestampMs);
    tableOperations.commit(base, TableMetadataParser.fromJson(metadata.toString()));
    table.refresh();
  }

  private void runApp(Table table, int maxAge, String granularity, int versions) {
    OtelEmitter emitter = Mockito.mock(OtelEmitter.class);
    Operations operations = Mockito.spy(Operations.of(null, emitter));
    Mockito.doReturn(table).when(operations).getTable(TABLE_NAME);
    new SnapshotsExpirationSparkApp(
            "branch-expiration",
            Mockito.mock(StateManager.class),
            TABLE_NAME,
            maxAge,
            granularity,
            versions,
            false,
            ".backup",
            emitter)
        .runInner(operations);
  }
}

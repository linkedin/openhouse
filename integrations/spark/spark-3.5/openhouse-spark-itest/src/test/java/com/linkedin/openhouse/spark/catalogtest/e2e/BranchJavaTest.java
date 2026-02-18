package com.linkedin.openhouse.spark.catalogtest.e2e;

import static org.apache.iceberg.types.Types.NestedField.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BranchJavaTest extends OpenHouseSparkITest {

  private static final String DATABASE = "d1_branch_java";

  private static final DataFile F1 =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/p/f1.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();
  private static final DataFile F2 =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/p/f2.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();

  @Test
  void testBranchOperations() throws Exception {
    // Test Scenarios:
    // 1. Linear History: Verifies table creation and sequential snapshot history.
    // 2. Divergent Branch & Cherry-Pick: Creates a branch, commits to it, and cherry-picks back to
    // main.
    // 3. Snapshot Expiration: Ensures reachable snapshots are retained.
    // 4. Independent Branching: Verifies a second branch can evolve independently.
    // 5. Cross-Branch Cherry-Pick: Cherry-picks a new snapshot from one branch to another
    // correctly.
    // 6. Staged Commits: Confirms stageOnly adds a snapshot without updating branch reference
    // history.
    try (SparkSession spark = getSparkSession()) {
      String name = DATABASE + ".branch_test";
      spark.sql("DROP TABLE IF EXISTS openhouse." + name);
      spark.sql("CREATE TABLE openhouse." + name + " (d string)");
      CatalogPlugin plugin = spark.sessionState().catalogManager().catalog("openhouse");
      Catalog catalog = ((HasIcebergCatalog) plugin).icebergCatalog();
      Table table = catalog.loadTable(TableIdentifier.parse(name));

      // 1. Start with no snapshots (newly created), add 2 snapshots
      table.newAppend().appendFile(F1).commit();
      long snap1 = table.currentSnapshot().snapshotId();

      table.newAppend().appendFile(F2).commit();
      long snap2 = table.currentSnapshot().snapshotId();

      table = catalog.loadTable(TableIdentifier.parse(name)); // Load table
      // History: snap1 -> snap2
      // Snapshots: snap1, snap2
      Assertions.assertEquals(2, table.history().size());
      Assertions.assertEquals(2, list(table.snapshots()).size());

      // 2. Existing table - add another snapshot
      table.newAppend().appendFile(F1).commit();
      table = catalog.loadTable(TableIdentifier.parse(name)); // Load table
      // History: snap1 -> snap2 -> snap3
      // Snapshots: snap1, snap2, snap3
      Assertions.assertEquals(3, table.history().size());
      Assertions.assertEquals(3, list(table.snapshots()).size());

      // 3. Cherry pick operations
      // Create featureA branch at snap2 (divergent point)
      table.manageSnapshots().createBranch("featureA", snap2).commit();

      // Append to featureA branch
      table.newAppend().appendFile(F2).toBranch("featureA").commit();
      Snapshot featureASnap = table.snapshot("featureA");

      // Cherry pick featureA snapshot to main
      table.manageSnapshots().cherrypick(featureASnap.snapshotId()).commit();
      table = catalog.loadTable(TableIdentifier.parse(name));
      // Cherry pick creates a new snapshot ID because it's not a fast-forward
      Assertions.assertNotEquals(featureASnap.snapshotId(), table.currentSnapshot().snapshotId());

      // History: snap1 -> snap2 -> snap3 -> snap4(cherry-pick)
      // Snapshots: snap1, snap2, snap3, featureASnap, snap4
      Assertions.assertEquals(4, table.history().size()); // 4 committed snapshots in history
      Assertions.assertEquals(
          5, list(table.snapshots()).size()); // 5 committed snapshots in snapshots list

      // 4. Expiration - use epoch timestamp to ensure no test snapshots are expired
      // This verifies that expiration doesn't break reachable snapshots without timing dependency
      table.expireSnapshots().expireOlderThan(1L).commit();
      table = catalog.loadTable(TableIdentifier.parse(name));
      // Should still have the latest snapshots on branches and main
      Assertions.assertNotNull(table.currentSnapshot());
      Assertions.assertNotNull(table.snapshot("featureA"));

      // 5. FeatureB branch
      long mainHead = table.currentSnapshot().snapshotId();
      table.manageSnapshots().createBranch("featureB", mainHead).commit();

      // Add 2 snapshots to featureB
      table.newAppend().appendFile(F1).toBranch("featureB").commit();
      table.newAppend().appendFile(F2).toBranch("featureB").commit();
      Snapshot featureBHead = table.snapshot("featureB");

      // Cherry pick from featureA (simulate another change on featureA and pick it)
      table.newAppend().appendFile(F1).toBranch("featureA").commit();
      Snapshot featureASnap2 = table.snapshot("featureA");

      table.manageSnapshots().cherrypick(featureASnap2.snapshotId()).commit();
      table = catalog.loadTable(TableIdentifier.parse(name));

      // Verify Main advanced
      Assertions.assertNotEquals(mainHead, table.currentSnapshot().snapshotId());
      // Verify Main is not FeatureB
      Assertions.assertNotEquals(featureBHead.snapshotId(), table.currentSnapshot().snapshotId());

      // Verify FeatureB is still at its head
      Assertions.assertEquals(featureBHead.snapshotId(), table.snapshot("featureB").snapshotId());

      // Verify sizes
      // Main history: snap1 -> snap2 -> snap3 -> snap4(cherry1) -> snap5(cherry2)
      if (table.history().size() != 5) {
        System.out.println("HISTORY SIZE MISMATCH: " + table.history().size());
        System.out.println("Refs: " + table.refs());
        table
            .snapshots()
            .forEach(
                s ->
                    System.out.println(
                        "Snapshot: "
                            + s.snapshotId()
                            + ", Parent: "
                            + s.parentId()
                            + ", Ts: "
                            + s.timestampMillis()));
      }
      Assertions.assertEquals(5, table.history().size());

      // Total Snapshots: 5 (Main) + 2 (featureA unique) + 2 (featureB unique) = 9
      Assertions.assertEquals(9, list(table.snapshots()).size());

      // Fast-forward / Staged
      table.newAppend().appendFile(F1).stageOnly().commit();
      Assertions.assertEquals(5, table.history().size());
      Assertions.assertEquals(10, list(table.snapshots()).size());

      spark.sql("DROP TABLE openhouse." + name);
    }
  }

  private List<Snapshot> list(Iterable<Snapshot> iter) {
    List<Snapshot> list = new ArrayList<>();
    iter.forEach(list::add);
    return list;
  }
}

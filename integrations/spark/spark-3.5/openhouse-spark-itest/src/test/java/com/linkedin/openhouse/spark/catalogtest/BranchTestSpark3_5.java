package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive tests for multi-branch WAP operations in Spark 3.5. Tests validate the enhanced
 * maybeAppendSnapshots functionality that supports: - Non-main branch operations (add/expire
 * snapshots from any branch) - WAP.id staging with multi-branch support - Cherry picking between
 * any branches - Fast forward merges for all branches - Backward compatibility with main-only
 * workflows - Forward compatibility for future wap.branch features
 */
public class BranchTestSpark3_5 extends OpenHouseSparkITest {

  // ===== BASIC BRANCH OPERATIONS =====

  @Test
  public void testBasicBranchOperations() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName);
      spark.sql("CREATE TABLE " + tableName + " (name string)");

      // Add initial data to main
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.initial')");

      // Create feature branch
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");

      // Write to feature branch
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature-a.data1')");
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature-a.data2')");

      // Verify branch isolation
      assertEquals(
          1, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size()); // main has 1 row
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size()); // feature-a has 3 rows

      // Verify refs exist for both branches
      List<Row> refs =
          spark.sql("SELECT name FROM " + tableName + ".refs ORDER BY name").collectAsList();
      assertEquals(2, refs.size());
      assertEquals("feature_a", refs.get(0).getString(0));
      assertEquals("main", refs.get(1).getString(0));

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  // ===== WAP STAGING WITH MULTI-BRANCH SUPPORT =====

  @Test
  public void testWapStagingWithBranches() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup main and feature branches
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.data')");
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature-a.data')");

      // Stage WAP snapshot (should not affect any branch)
      spark.conf().set("spark.wap.id", "multi-branch-wap");
      spark.sql("INSERT INTO " + tableName + " VALUES ('wap.staged.data')");
      spark.conf().unset("spark.wap.id");

      // Verify WAP staging doesn't affect branch visibility
      assertEquals(
          1, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size()); // main unchanged
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size()); // feature-a unchanged

      // Verify WAP snapshot exists but no new refs
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + ".snapshots")
              .collectAsList()
              .size()); // 1 main + 1 feature + 1 wap
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + ".refs")
              .collectAsList()
              .size()); // main + feature-a only

      // Verify WAP snapshot has correct properties
      List<Row> wapSnapshots =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'multi-branch-wap'")
              .collectAsList();
      assertEquals(1, wapSnapshots.size());

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  // ===== CHERRY PICKING BETWEEN BRANCHES =====

  @Test
  public void testCherryPickToMainWithFeatureBranch() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup branches
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.base')");
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");

      // Create WAP snapshot
      spark.conf().set("spark.wap.id", "feature-target-wap");
      spark.sql("INSERT INTO " + tableName + " VALUES ('wap.for.feature')");
      String wapSnapshotId =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'feature-target-wap'")
              .first()
              .mkString();
      spark.conf().unset("spark.wap.id");

      // CRITICAL: Advance main branch to force non-fast-forward cherry-pick
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.advance')");

      // Cherry-pick WAP to main branch (this tests our enhanced maybeAppendSnapshots)
      // Main should have 2 rows now (main.base + main.advance)
      assertEquals(2, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size());
      spark.sql(
          String.format(
              "CALL openhouse.system.cherrypick_snapshot('"
                  + tableName.replace("openhouse.", "")
                  + "', %s)",
              wapSnapshotId));

      // Verify cherry-pick worked - 3 rows of data should appear in main (main.base + main.advance
      // + wap.for.feature)
      assertEquals(3, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size());
      assertEquals(
          1,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size());

      // Verify published WAP snapshot properties
      List<Row> publishedSnapshots =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['published-wap-id'] = 'feature-target-wap'")
              .collectAsList();
      assertTrue(
          publishedSnapshots.size() >= 1,
          "Should find at least one snapshot with published-wap-id");

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  // ===== FAST FORWARD MERGES =====

  @Test
  public void testFastForwardMergeToMain() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");

      // Setup base data
      spark.sql("INSERT INTO " + tableName + " VALUES ('base.data')");

      // Create feature branch from main
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");

      // Advance feature branch
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature.data1')");
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature.data2')");

      // Verify initial state
      assertEquals(
          1, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size()); // main has 1 row
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size()); // feature has 3 rows

      // Fast-forward main to feature_a
      spark.sql("CALL openhouse.system.fast_forward('" + tableName + "', 'main', 'feature_a')");

      // Verify fast-forward worked - main should now have same data as feature_a
      assertEquals(3, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size());
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size());

      // Verify both branches point to same snapshot
      String mainSnapshot =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'main'")
              .first()
              .mkString();
      String featureSnapshot =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'feature_a'")
              .first()
              .mkString();
      assertEquals(mainSnapshot, featureSnapshot);

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  @Test
  public void testFastForwardMergeToFeature() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");

      // Setup base data
      spark.sql("INSERT INTO " + tableName + " VALUES ('base.data')");

      // Create feature branch from main
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");

      // Advance main branch (feature_a stays at base)
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.data1')");
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.data2')");

      // Verify initial state
      assertEquals(
          3,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size()); // main has 3 rows
      assertEquals(
          1,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size()); // feature has 1 row

      // Fast-forward feature_a to main
      spark.sql("CALL openhouse.system.fast_forward('" + tableName + "', 'feature_a', 'main')");

      // Verify fast-forward worked - feature_a should now have same data as main
      assertEquals(3, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size());
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size());

      // Verify both branches point to same snapshot
      String mainSnapshot =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'main'")
              .first()
              .mkString();
      String featureSnapshot =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'feature_a'")
              .first()
              .mkString();
      assertEquals(mainSnapshot, featureSnapshot);

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  @Test
  public void testFastForwardMergeWithWapId() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup base data
      spark.sql("INSERT INTO " + tableName + " VALUES ('base.data')");

      // Create feature branch
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");

      // Create WAP snapshot
      spark.conf().set("spark.wap.id", "test-wap");
      spark.sql("INSERT INTO " + tableName + " VALUES ('wap.data')");
      String wapSnapshotId =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'test-wap'")
              .first()
              .mkString();
      spark.conf().unset("spark.wap.id");

      // Advance feature branch normally (not using WAP)
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature.data')");

      // Verify WAP snapshot doesn't interfere with fast-forward
      assertEquals(
          1, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size()); // main unchanged
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size()); // feature advanced

      // Fast-forward main to feature_a should work despite WAP presence
      spark.sql("CALL openhouse.system.fast_forward('" + tableName + "', 'main', 'feature_a')");

      // Verify fast-forward worked
      assertEquals(2, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size());
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size());

      // Verify WAP snapshot is still available for cherry-pick
      List<Row> wapSnapshots =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'test-wap'")
              .collectAsList();
      assertEquals(1, wapSnapshots.size());
      assertEquals(wapSnapshotId, wapSnapshots.get(0).mkString());

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  @Test
  public void testFastForwardMergeBetweenTwoFeatureBranches() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");

      // Setup base data
      spark.sql("INSERT INTO " + tableName + " VALUES ('base.data')");

      // Create two feature branches from main
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_b");

      // Advance feature_a
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature_a.data1')");
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature_a.data2')");

      // Verify initial state
      assertEquals(
          1, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size()); // main has 1 row
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size()); // feature_a has 3 rows
      assertEquals(
          1,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_b'")
              .collectAsList()
              .size()); // feature_b has 1 row

      // Fast-forward feature_b to feature_a
      spark.sql(
          "CALL openhouse.system.fast_forward('" + tableName + "', 'feature_b', 'feature_a')");

      // Verify fast-forward worked
      assertEquals(
          1, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size()); // main unchanged
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size()); // feature_a unchanged
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_b'")
              .collectAsList()
              .size()); // feature_b now matches feature_a

      // Verify feature_a and feature_b point to same snapshot
      String featureASnapshot =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'feature_a'")
              .first()
              .mkString();
      String featureBSnapshot =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'feature_b'")
              .first()
              .mkString();
      assertEquals(featureASnapshot, featureBSnapshot);

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  @Test
  public void testFastForwardMergeIncompatibleLineage() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");

      // Setup base data
      spark.sql("INSERT INTO " + tableName + " VALUES ('base.data')");

      // Create feature branch
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");

      // Advance both branches independently (creating divergent history)
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.divergent')");
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature.divergent')");

      // Verify divergent state
      assertEquals(
          2,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size()); // main has 2 rows
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size()); // feature_a has 2 rows (different)

      // Attempt fast-forward should fail due to incompatible lineage
      assertThrows(
          Exception.class,
          () ->
              spark.sql(
                  "CALL openhouse.system.fast_forward('" + tableName + "', 'main', 'feature_a')"),
          "Fast-forward should fail when branches have divergent history");

      // Verify branches remain unchanged after failed fast-forward
      assertEquals(2, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size());
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size());

      // Verify snapshots are still different
      String mainSnapshot =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'main'")
              .first()
              .mkString();
      String featureSnapshot =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'feature_a'")
              .first()
              .mkString();
      assertNotEquals(mainSnapshot, featureSnapshot);

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  // ===== SNAPSHOT EXPIRATION FROM NON-MAIN BRANCHES =====

  @Test
  public void testSnapshotExpirationFromFeatureBranch() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup: Create multiple snapshots to have some that can be expired

      // 1. Create initial main data
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.initial')");

      // 2. Create feature branch from main
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");

      // 3. Add multiple snapshots to feature branch
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature.data1')");
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature.data2')");
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature.data3')");

      // 4. Query metadata tables to find snapshots that are NOT current branch heads

      // Get all snapshots
      List<Row> allSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".snapshots ORDER BY committed_at")
              .collectAsList();
      assertTrue(allSnapshots.size() >= 4, "Should have at least 4 snapshots");

      // Get current branch head snapshots from refs table
      List<Row> branchHeads =
          spark.sql("SELECT snapshot_id FROM " + tableName + ".refs").collectAsList();
      Set<String> referencedSnapshots =
          branchHeads.stream().map(row -> row.mkString()).collect(Collectors.toSet());

      System.out.println(
          "DEBUG: All snapshots: "
              + allSnapshots.stream().map(Row::mkString).collect(Collectors.toList()));
      System.out.println("DEBUG: Referenced snapshots (branch heads): " + referencedSnapshots);

      // Find snapshots that are NOT referenced by any branch head
      List<String> unreferencedSnapshots =
          allSnapshots.stream()
              .map(Row::mkString)
              .filter(snapshotId -> !referencedSnapshots.contains(snapshotId))
              .collect(Collectors.toList());

      System.out.println("DEBUG: Unreferenced snapshots: " + unreferencedSnapshots);

      // We should have at least one unreferenced snapshot (intermediate feature snapshots)
      assertFalse(
          unreferencedSnapshots.isEmpty(),
          "Should have at least one unreferenced snapshot to expire");

      // Select the first unreferenced snapshot to expire
      String snapshotToExpire = unreferencedSnapshots.get(0);

      // Verify this snapshot exists in the snapshots table
      List<Row> beforeExpiration =
          spark.sql("SELECT snapshot_id FROM " + tableName + ".snapshots").collectAsList();
      assertTrue(
          beforeExpiration.stream().anyMatch(row -> row.mkString().equals(snapshotToExpire)),
          "Snapshot to expire should exist before expiration");

      // Expire the unreferenced snapshot
      spark.sql(
          String.format(
              "CALL openhouse.system.expire_snapshots(table => '"
                  + tableName.replace("openhouse.", "")
                  + "', snapshot_ids => Array(%s))",
              snapshotToExpire));

      // Verify snapshot is gone
      List<Row> afterExpiration =
          spark.sql("SELECT snapshot_id FROM " + tableName + ".snapshots").collectAsList();
      assertFalse(
          afterExpiration.stream().anyMatch(row -> row.mkString().equals(snapshotToExpire)),
          "Expired snapshot should no longer exist");

      // Verify branches are still intact after expiration
      // Main should have: main.initial = 1 row
      assertEquals(1, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size());

      // Feature_a should have: main.initial + feature.data1 + feature.data2 + feature.data3 = 4
      // rows
      assertEquals(
          4,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size());

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  @Test
  public void testWapSnapshotExpirationWithMultipleBranches() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup multi-branch environment
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.base')");
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature.base')");

      // Create multiple WAP snapshots
      spark.conf().set("spark.wap.id", "wap-to-keep");
      spark.sql("INSERT INTO " + tableName + " VALUES ('wap.keep.data')");

      spark.conf().set("spark.wap.id", "wap-to-expire");
      spark.sql("INSERT INTO " + tableName + " VALUES ('wap.expire.data')");
      String expireWapId =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'wap-to-expire'")
              .first()
              .mkString();
      spark.conf().unset("spark.wap.id");

      // Expire specific WAP snapshot
      spark.sql(
          String.format(
              "CALL openhouse.system.expire_snapshots(table => '"
                  + tableName.replace("openhouse.", "")
                  + "', snapshot_ids => Array(%s))",
              expireWapId));

      // Verify selective WAP expiration
      List<Row> remainingWaps =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'wap-to-keep'")
              .collectAsList();
      assertEquals(1, remainingWaps.size());

      List<Row> expiredWaps =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'wap-to-expire'")
              .collectAsList();
      assertEquals(0, expiredWaps.size());

      // Verify branches unchanged
      assertEquals(1, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size());
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size());

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  // ===== BACKWARD COMPATIBILITY =====

  @Test
  public void testBackwardCompatibilityMainBranchOnly() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Traditional main-only workflow (should work exactly as before)
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.1')");
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.2')");

      // WAP staging (traditional)
      spark.conf().set("spark.wap.id", "compat-test-wap");
      spark.sql("INSERT INTO " + tableName + " VALUES ('compat.wap.data')");
      String wapSnapshotId =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'compat-test-wap'")
              .first()
              .mkString();
      spark.conf().unset("spark.wap.id");

      // Traditional cherry-pick to main
      spark.sql(
          String.format(
              "CALL openhouse.system.cherrypick_snapshot('"
                  + tableName.replace("openhouse.", "")
                  + "', %s)",
              wapSnapshotId));

      // Verify traditional behavior preserved
      assertEquals(3, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size());
      List<Row> refs = spark.sql("SELECT name FROM " + tableName + ".refs").collectAsList();
      assertEquals(1, refs.size());
      assertEquals("main", refs.get(0).getString(0));

      // Traditional snapshot queries should work
      assertTrue(
          spark.sql("SELECT * FROM " + tableName + ".snapshots").collectAsList().size() >= 3);

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  // ===== ERROR SCENARIOS =====

  @Test
  public void testErrorInsertToNonExistentBranch() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");

      // Setup base data
      spark.sql("INSERT INTO " + tableName + " VALUES ('base.data')");

      // Create one valid branch
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");

      // Verify valid branch works
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('valid.data')");
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size());

      // Attempt to insert into non-existent branch should fail
      assertThrows(
          Exception.class,
          () ->
              spark.sql("INSERT INTO " + tableName + ".branch_nonexistent VALUES ('invalid.data')"),
          "Insert to non-existent branch should fail");

      // Verify table state unchanged after failed insert
      assertEquals(
          1, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size()); // main unchanged
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size()); // feature_a unchanged

      // Verify only valid branches exist
      List<Row> refs =
          spark.sql("SELECT name FROM " + tableName + ".refs ORDER BY name").collectAsList();
      assertEquals(2, refs.size());
      assertEquals("feature_a", refs.get(0).getString(0));
      assertEquals("main", refs.get(1).getString(0));

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }

  @Test
  public void testErrorCherryPickNonExistentWapId() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
      spark.sql("CREATE TABLE " + tableName + " (name string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup base data and branch
      spark.sql("INSERT INTO " + tableName + " VALUES ('base.data')");
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");

      // Create a valid WAP snapshot
      spark.conf().set("spark.wap.id", "valid-wap");
      spark.sql("INSERT INTO " + tableName + " VALUES ('valid.wap.data')");
      String validWapId =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'valid-wap'")
              .first()
              .mkString();
      spark.conf().unset("spark.wap.id");

      // Verify valid WAP cherry-pick works
      spark.sql(
          String.format(
              "CALL openhouse.system.cherrypick_snapshot('"
                  + tableName.replace("openhouse.", "")
                  + "', %s)",
              validWapId));
      assertEquals(2, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size());

      // Attempt to cherry-pick non-existent snapshot ID should fail
      long nonExistentSnapshotId = 999999999L;
      assertThrows(
          Exception.class,
          () ->
              spark.sql(
                  String.format(
                      "CALL openhouse.system.cherrypick_snapshot('"
                          + tableName.replace("openhouse.", "")
                          + "', %s)",
                      nonExistentSnapshotId)),
          "Cherry-pick of non-existent snapshot should fail");

      // Attempt to cherry-pick with malformed snapshot ID should fail
      assertThrows(
          Exception.class,
          () ->
              spark.sql(
                  String.format(
                      "CALL openhouse.system.cherrypick_snapshot('"
                          + tableName.replace("openhouse.", "")
                          + "', %s)",
                      "invalid-id")),
          "Cherry-pick with invalid snapshot ID should fail");

      // Verify table state unchanged after failed cherry-picks
      assertEquals(
          2, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size()); // main unchanged
      assertEquals(
          1,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_a'")
              .collectAsList()
              .size()); // feature_a unchanged

      // Verify valid WAP snapshot still exists
      List<Row> validWaps =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'valid-wap'")
              .collectAsList();
      assertEquals(1, validWaps.size());

      spark.sql("DROP TABLE IF EXISTS " + tableName + "");
    }
  }
}

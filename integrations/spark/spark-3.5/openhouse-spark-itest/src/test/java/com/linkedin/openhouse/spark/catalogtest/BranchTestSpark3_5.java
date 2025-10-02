package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

/**
 * Comprehensive tests for multi-branch WAP operations in Spark 3.5. Tests validate the enhanced
 * applySnapshotOperations functionality that supports: - Non-main branch operations (add/expire
 * snapshots from any branch) - WAP.id staging with multi-branch support - Cherry picking between
 * any branches - Fast forward merges for all branches - Backward compatibility with main-only
 * workflows - Forward compatibility for future wap.branch features
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@Execution(ExecutionMode.SAME_THREAD)
public class BranchTestSpark3_5 extends OpenHouseSparkITest {

  /**
   * Comprehensive cleanup method to prevent configuration and table bleed-over between tests. This
   * ensures WAP configurations are properly reset and all test tables are dropped.
   */
  @AfterEach
  public void cleanupAfterTest() {
    try (SparkSession spark = getSparkSession()) {
      // Clear WAP configurations to prevent bleed-over between tests
      spark.conf().unset("spark.wap.id");
      spark.conf().unset("spark.wap.branch");

      // Drop all test tables to ensure clean state for next test
      // Get all tables in the d1 database that start with branch_test_ or similar patterns
      try {
        List<Row> tables = spark.sql("SHOW TABLES IN openhouse.d1").collectAsList();
        for (Row table : tables) {
          String tableName = table.getString(1); // table name is in second column
          if (tableName.startsWith("branch_test_") || tableName.startsWith("test_")) {
            String fullTableName = "openhouse.d1." + tableName;
            spark.sql("DROP TABLE IF EXISTS " + fullTableName);
          }
        }
      } catch (Exception e) {
        // If SHOW TABLES fails, try to drop common test table patterns
        // This is a fallback in case the database doesn't exist yet
        for (String pattern : new String[] {"branch_test_", "test_"}) {
          for (int i = 0; i < 10; i++) { // Try a few recent timestamps
            long timestamp = System.currentTimeMillis() - (i * 1000);
            String tableName = "openhouse.d1." + pattern + timestamp;
            try {
              spark.sql("DROP TABLE IF EXISTS " + tableName);
            } catch (Exception ignored) {
              // Ignore failures for non-existent tables
            }
          }
        }
      }
    } catch (Exception e) {
      // Log but don't fail the test for cleanup issues
      System.err.println("Warning: Failed to cleanup after test: " + e.getMessage());
    }
  }

  // ===== BASIC BRANCH OPERATIONS =====

  @Test
  public void testBasicBranchOperations() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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
    }
  }

  // ===== WAP STAGING WITH MULTI-BRANCH SUPPORT =====

  @Test
  public void testWapStagingWithBranches() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("CREATE TABLE " + tableName + " (name string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup main and feature branches
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.data')");
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES ('feature-a.data')");

      // Stage WAP snapshot (should not affect any branch)
      spark.conf().set("spark.wap.id", "multi-branch-wap");
      spark.sql("INSERT INTO " + tableName + " VALUES ('wap.staged.data')");

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
    }
  }

  @Test
  public void testWapIdAfterCreateTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "wap_id_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      // Create table without any data (no snapshots exist)
      spark.sql("CREATE TABLE " + tableName + " (name string)");

      // Enable WAP on the table
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Verify no snapshots exist yet
      List<Row> initialSnapshots =
          spark.sql("SELECT * FROM " + tableName + ".snapshots").collectAsList();
      assertEquals(0, initialSnapshots.size(), "Newly created table should have no snapshots");

      // Verify no branches exist yet (empty table has no branches)
      List<Row> initialRefs = spark.sql("SELECT name FROM " + tableName + ".refs").collectAsList();
      assertEquals(0, initialRefs.size(), "Empty table should have no branches initially");

      // ===== WAP STAGING ON EMPTY TABLE =====

      // 1. Create WAP staged data on empty table (should create staging snapshot)
      spark.conf().set("spark.wap.id", "wap-stage-1");
      spark.sql("INSERT INTO " + tableName + " VALUES ('wap_staged_data_1')");
      spark.conf().unset("spark.wap.id");

      // Verify WAP snapshot was created
      List<Row> wapSnapshots =
          spark
              .sql(
                  "SELECT snapshot_id, summary FROM "
                      + tableName
                      + ".snapshots "
                      + "WHERE summary['wap.id'] = 'wap-stage-1'")
              .collectAsList();
      assertEquals(1, wapSnapshots.size(), "Should have 1 WAP staged snapshot");

      // Verify no branches exist yet (WAP staging doesn't create branches)
      List<Row> refsAfterWapStaging =
          spark.sql("SELECT name FROM " + tableName + ".refs").collectAsList();
      assertEquals(0, refsAfterWapStaging.size(), "WAP staging should not create branches");

      // Verify WAP data is not visible in main queries (no branch exists)
      assertEquals(
          0,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Should see 0 rows - no branches exist, WAP data is staged");

      // ===== WAP PUBLISHING TO CREATE MAIN BRANCH =====

      // 2. Publish WAP data to create main branch
      String wapSnapshotId = String.valueOf(wapSnapshots.get(0).getLong(0));
      spark.sql(
          "CALL openhouse.system.cherrypick_snapshot('"
              + tableName.replace("openhouse.", "")
              + "', "
              + wapSnapshotId
              + ")");

      // Verify main branch now exists
      List<Row> refsAfterPublishing =
          spark.sql("SELECT name FROM " + tableName + ".refs ORDER BY name").collectAsList();
      assertEquals(
          1, refsAfterPublishing.size(), "Should have main branch after publishing WAP data");
      assertEquals("main", refsAfterPublishing.get(0).getString(0), "Should have main branch");

      // Verify WAP data is now visible in main branch
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should have 1 row after WAP publishing");

      List<Row> mainData = spark.sql("SELECT name FROM " + tableName + "").collectAsList();
      assertEquals(
          "wap_staged_data_1", mainData.get(0).getString(0), "Should see published WAP data");

      // ===== MULTI-WAP OPERATIONS =====

      // 3. Create multiple WAP staged data sets
      spark.conf().set("spark.wap.id", "wap-stage-2");
      spark.sql("INSERT INTO " + tableName + " VALUES ('wap_staged_data_2')");
      spark.conf().unset("spark.wap.id");

      spark.conf().set("spark.wap.id", "wap-stage-3");
      spark.sql("INSERT INTO " + tableName + " VALUES ('wap_staged_data_3')");
      spark.conf().unset("spark.wap.id");

      // Verify multiple WAP snapshots exist
      List<Row> allWapSnapshots =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots "
                      + "WHERE summary['wap.id'] IS NOT NULL")
              .collectAsList();
      assertEquals(3, allWapSnapshots.size(), "Should have 3 WAP staged snapshots");

      // Verify main branch is unchanged (WAP data is staged)
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should still have 1 row (staged WAP not visible)");

      // ===== SELECTIVE WAP PUBLISHING =====

      // 4. Publish second WAP data set only
      List<Row> wap2Snapshots =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots "
                      + "WHERE summary['wap.id'] = 'wap-stage-2'")
              .collectAsList();
      String wap2SnapshotId = String.valueOf(wap2Snapshots.get(0).getLong(0));
      spark.sql(
          "CALL openhouse.system.cherrypick_snapshot('"
              + tableName.replace("openhouse.", "")
              + "', "
              + wap2SnapshotId
              + ")");

      // Verify main branch now has both published datasets
      assertEquals(
          2,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should have 2 rows after second WAP publishing");

      List<Row> publishedData =
          spark.sql("SELECT name FROM " + tableName + " ORDER BY name").collectAsList();
      assertEquals(
          "wap_staged_data_1",
          publishedData.get(0).getString(0),
          "First row should be first WAP data");
      assertEquals(
          "wap_staged_data_2",
          publishedData.get(1).getString(0),
          "Second row should be second WAP data");

      // ===== UNPUBLISHED WAP DATA VERIFICATION =====

      // 5. Verify third WAP data remains unpublished
      List<Row> wap3Snapshots =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots "
                      + "WHERE summary['wap.id'] = 'wap-stage-3'")
              .collectAsList();
      assertEquals(1, wap3Snapshots.size(), "Third WAP snapshot should still exist");

      // Verify unpublished WAP data is not visible
      List<Row> currentData =
          spark.sql("SELECT name FROM " + tableName + " ORDER BY name").collectAsList();
      assertFalse(
          currentData.stream().anyMatch(row -> "wap_staged_data_3".equals(row.getString(0))),
          "Unpublished WAP data should not be visible in main branch");

      // ===== REGULAR DATA VS WAP DATA =====

      // 6. Add regular (non-WAP) data to main branch
      spark.sql("INSERT INTO " + tableName + " VALUES ('regular_data')");

      // Verify main branch now has mixed data
      assertEquals(
          3,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should have 3 rows (2 published WAP + 1 regular)");

      List<Row> finalData =
          spark.sql("SELECT name FROM " + tableName + " ORDER BY name").collectAsList();
      assertEquals("regular_data", finalData.get(0).getString(0), "Should contain regular data");
      assertEquals(
          "wap_staged_data_1", finalData.get(1).getString(0), "Should contain first WAP data");
      assertEquals(
          "wap_staged_data_2", finalData.get(2).getString(0), "Should contain second WAP data");

      // ===== SNAPSHOT HISTORY VERIFICATION =====

      // 7. Verify snapshot counts and types
      List<Row> totalSnapshots =
          spark.sql("SELECT * FROM " + tableName + ".snapshots").collectAsList();
      assertTrue(
          totalSnapshots.size() >= 4, "Should have at least 4 snapshots (3 WAP + 1 regular)");

      // Verify WAP snapshots still exist in metadata
      List<Row> remainingWapSnapshots =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots "
                      + "WHERE summary['wap.id'] IS NOT NULL")
              .collectAsList();
      assertEquals(
          3, remainingWapSnapshots.size(), "All 3 WAP snapshots should still exist in metadata");

      // Verify main branch has the latest published snapshot (points to regular INSERT snapshot)
      List<Row> mainSnapshotRef =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'main'")
              .collectAsList();
      assertEquals(1, mainSnapshotRef.size(), "Main branch should exist and point to a snapshot");
    }
  }

  @Test
  public void testBranchAfterCreateTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      // Create table without any data (no snapshots exist)
      spark.sql("CREATE TABLE " + tableName + " (name string)");

      // Verify no snapshots exist yet
      List<Row> initialSnapshots =
          spark.sql("SELECT * FROM " + tableName + ".snapshots").collectAsList();
      assertEquals(0, initialSnapshots.size(), "Newly created table should have no snapshots");

      // Create branch on table with no existing snapshots
      // According to Iceberg specification, this should succeed and create an empty snapshot
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_on_empty");

      // Verify that an empty snapshot was created for the branch
      List<Row> snapshotsAfterBranchCreation =
          spark.sql("SELECT * FROM " + tableName + ".snapshots").collectAsList();
      assertEquals(
          1,
          snapshotsAfterBranchCreation.size(),
          "Should have 1 empty snapshot after branch creation");

      // Verify the empty snapshot properties
      Row emptySnapshot = snapshotsAfterBranchCreation.get(0);
      // The parent_id should be null for the empty snapshot
      assertNull(
          emptySnapshot.get(emptySnapshot.fieldIndex("parent_id")),
          "Empty snapshot should have no parent");

      // Verify the branch was created successfully
      List<Row> refsAfterBranchCreation =
          spark.sql("SELECT name FROM " + tableName + ".refs ORDER BY name").collectAsList();
      assertEquals(
          1,
          refsAfterBranchCreation.size(),
          "Should have feature_on_empty branch (main doesn't exist yet)");
      assertEquals(
          "feature_on_empty",
          refsAfterBranchCreation.get(0).getString(0),
          "Should have feature_on_empty branch");

      // Verify that main branch still doesn't exist (as expected)
      boolean hasMainBranch =
          refsAfterBranchCreation.stream().anyMatch(row -> "main".equals(row.getString(0)));
      assertFalse(hasMainBranch, "Main branch should not exist on empty table");

      // Now insert data to create a data snapshot
      spark.sql("INSERT INTO " + tableName + " VALUES ('initial.data')");

      // Verify we now have 2 snapshots (empty + data)
      List<Row> snapshotsAfterInsert =
          spark.sql("SELECT * FROM " + tableName + ".snapshots").collectAsList();
      assertEquals(
          2, snapshotsAfterInsert.size(), "Should have 2 snapshots after insert (empty + data)");

      // Now we should have main branch as well
      List<Row> refsAfterInsert =
          spark.sql("SELECT name FROM " + tableName + ".refs ORDER BY name").collectAsList();
      assertEquals(2, refsAfterInsert.size(), "Should have feature_on_empty and main branches");

      // Create another branch after data exists - this should also succeed
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_after_snapshot");

      // Verify we now have 3 branches (feature_on_empty, main, feature_after_snapshot)
      List<Row> refs =
          spark.sql("SELECT name FROM " + tableName + ".refs ORDER BY name").collectAsList();
      assertEquals(3, refs.size(), "Should have 3 branches total");

      // Verify all expected branches exist
      Set<String> branchNames =
          refs.stream().map(row -> row.getString(0)).collect(Collectors.toSet());
      assertTrue(branchNames.contains("feature_on_empty"), "feature_on_empty branch should exist");
      assertTrue(branchNames.contains("main"), "main branch should exist");
      assertTrue(
          branchNames.contains("feature_after_snapshot"),
          "feature_after_snapshot branch should exist");

      // ===== BRANCH ISOLATION TESTING =====

      // 1. Test initial state: main and feature_after_snapshot should have the same data
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should have 1 row");
      assertEquals(
          1,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_after_snapshot'")
              .collectAsList()
              .size(),
          "feature_after_snapshot branch should have 1 row");

      // 2. Test feature_on_empty branch should be empty (points to empty snapshot)
      assertEquals(
          0,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_on_empty'")
              .collectAsList()
              .size(),
          "feature_on_empty branch should have 0 rows (points to empty snapshot)");

      // 3. Add data to feature_on_empty branch only
      spark.sql(
          "INSERT INTO " + tableName + ".branch_feature_on_empty VALUES ('empty_branch_data')");

      // Verify isolation: feature_on_empty now has data, others unchanged
      assertEquals(
          1,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_on_empty'")
              .collectAsList()
              .size(),
          "feature_on_empty branch should now have 1 row");
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should still have 1 row (unchanged)");
      assertEquals(
          1,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_after_snapshot'")
              .collectAsList()
              .size(),
          "feature_after_snapshot branch should still have 1 row (unchanged)");

      // 4. Add different data to feature_after_snapshot branch
      spark.sql(
          "INSERT INTO "
              + tableName
              + ".branch_feature_after_snapshot VALUES ('snapshot_branch_data')");

      // Verify isolation: each branch has its own data
      assertEquals(
          1,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_on_empty'")
              .collectAsList()
              .size(),
          "feature_on_empty branch should still have 1 row");
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should still have 1 row (unchanged)");
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_after_snapshot'")
              .collectAsList()
              .size(),
          "feature_after_snapshot branch should now have 2 rows");

      // 5. Add data to main branch
      spark.sql("INSERT INTO " + tableName + " VALUES ('main_branch_data')");

      // Verify complete isolation: each branch maintains its own data
      assertEquals(
          1,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_on_empty'")
              .collectAsList()
              .size(),
          "feature_on_empty branch should still have 1 row");
      assertEquals(
          2,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should now have 2 rows");
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_after_snapshot'")
              .collectAsList()
              .size(),
          "feature_after_snapshot branch should still have 2 rows (unchanged)");

      // 6. Verify data content isolation
      List<Row> featureOnEmptyData =
          spark
              .sql(
                  "SELECT name FROM "
                      + tableName
                      + " VERSION AS OF 'feature_on_empty' ORDER BY name")
              .collectAsList();
      assertEquals(
          "empty_branch_data",
          featureOnEmptyData.get(0).getString(0),
          "feature_on_empty should contain its specific data");

      List<Row> mainData =
          spark.sql("SELECT name FROM " + tableName + " ORDER BY name").collectAsList();
      assertEquals(
          "initial.data", mainData.get(0).getString(0), "main should contain initial data");
      assertEquals(
          "main_branch_data",
          mainData.get(1).getString(0),
          "main should contain its specific data");

      List<Row> featureAfterSnapshotData =
          spark
              .sql(
                  "SELECT name FROM "
                      + tableName
                      + " VERSION AS OF 'feature_after_snapshot' ORDER BY name")
              .collectAsList();
      assertEquals(
          "initial.data",
          featureAfterSnapshotData.get(0).getString(0),
          "feature_after_snapshot should contain initial data");
      assertEquals(
          "snapshot_branch_data",
          featureAfterSnapshotData.get(1).getString(0),
          "feature_after_snapshot should contain its specific data");

      // 7. Verify snapshot isolation: each branch should have different snapshot histories
      List<Row> mainSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'main'")
              .collectAsList();
      List<Row> featureOnEmptySnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'feature_on_empty'")
              .collectAsList();
      List<Row> featureAfterSnapshotSnapshots =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".refs WHERE name = 'feature_after_snapshot'")
              .collectAsList();

      assertNotEquals(
          mainSnapshots.get(0).getLong(0),
          featureOnEmptySnapshots.get(0).getLong(0),
          "main and feature_on_empty should point to different snapshots");
      assertNotEquals(
          mainSnapshots.get(0).getLong(0),
          featureAfterSnapshotSnapshots.get(0).getLong(0),
          "main and feature_after_snapshot should point to different snapshots");
      assertNotEquals(
          featureOnEmptySnapshots.get(0).getLong(0),
          featureAfterSnapshotSnapshots.get(0).getLong(0),
          "feature_on_empty and feature_after_snapshot should point to different snapshots");
    }
  }

  @Test
  public void testWapBranchAfterCreateTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "wap_branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      // Create table without any data (no snapshots exist)
      spark.sql("CREATE TABLE " + tableName + " (name string)");

      // Enable WAP on the table
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Verify no snapshots exist yet
      List<Row> initialSnapshots =
          spark.sql("SELECT * FROM " + tableName + ".snapshots").collectAsList();
      assertEquals(0, initialSnapshots.size(), "Newly created table should have no snapshots");

      // Create branch on table with no existing snapshots
      // According to Iceberg specification, this should succeed and create an empty snapshot
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_empty");

      // Verify that an empty snapshot was created for the branch
      List<Row> snapshotsAfterBranchCreation =
          spark.sql("SELECT * FROM " + tableName + ".snapshots").collectAsList();
      assertEquals(
          1,
          snapshotsAfterBranchCreation.size(),
          "Should have 1 empty snapshot after branch creation");

      // Verify the branch was created successfully
      List<Row> refsAfterBranchCreation =
          spark.sql("SELECT name FROM " + tableName + ".refs ORDER BY name").collectAsList();
      assertEquals(
          1,
          refsAfterBranchCreation.size(),
          "Should have feature_empty branch (main doesn't exist yet)");
      assertEquals(
          "feature_empty",
          refsAfterBranchCreation.get(0).getString(0),
          "Should have feature_empty branch");

      // ===== WAP BRANCH TESTING =====

      // 1. Set WAP branch and insert data - should go to the feature_empty branch
      spark.conf().set("spark.wap.branch", "feature_empty");
      spark.sql("INSERT INTO " + tableName + " VALUES ('wap_branch_data_1')");

      // Verify WAP branch data is visible when spark.wap.branch is set
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Should see 1 row when spark.wap.branch=feature_empty");

      List<Row> wapBranchData = spark.sql("SELECT name FROM " + tableName + "").collectAsList();
      assertEquals(
          "wap_branch_data_1", wapBranchData.get(0).getString(0), "Should see WAP branch data");

      // Verify feature_empty branch directly
      assertEquals(
          1,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_empty'")
              .collectAsList()
              .size(),
          "feature_empty branch should have 1 row");

      // Unset WAP branch - queries should now see main branch (which doesn't exist yet, so empty)
      spark.conf().unset("spark.wap.branch");
      assertEquals(
          0,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Should see 0 rows when spark.wap.branch is unset (main doesn't exist)");

      // ===== MULTI-BRANCH WAP TESTING =====

      // 2. Create main branch with regular data
      spark.sql("INSERT INTO " + tableName + " VALUES ('main_data')");

      // Now we should have main branch
      List<Row> refs =
          spark.sql("SELECT name FROM " + tableName + ".refs ORDER BY name").collectAsList();
      assertEquals(2, refs.size(), "Should have feature_empty and main branches");

      // Verify main branch data when spark.wap.branch is unset
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should have 1 row");
      List<Row> mainData = spark.sql("SELECT name FROM " + tableName + "").collectAsList();
      assertEquals("main_data", mainData.get(0).getString(0), "Should see main branch data");

      // 3. Create another branch and test WAP branch functionality
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_wap_test");

      // Set WAP branch to feature_wap_test and add data
      spark.conf().set("spark.wap.branch", "feature_wap_test");
      spark.sql("INSERT INTO " + tableName + " VALUES ('wap_branch_data_2')");

      // Verify WAP branch data is visible when spark.wap.branch=feature_wap_test
      assertEquals(
          2,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Should see 2 rows when spark.wap.branch=feature_wap_test (main_data + wap_branch_data_2)");

      // ===== COMPREHENSIVE WAP BRANCH ISOLATION VERIFICATION =====

      // Verify each branch has independent data
      spark.conf().unset("spark.wap.branch");
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should have 1 row when WAP branch is unset");

      assertEquals(
          1,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_empty'")
              .collectAsList()
              .size(),
          "feature_empty branch should have 1 row");

      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_wap_test'")
              .collectAsList()
              .size(),
          "feature_wap_test branch should have 2 rows");

      // Verify data content isolation
      List<Row> finalMainData =
          spark.sql("SELECT name FROM " + tableName + " ORDER BY name").collectAsList();
      assertEquals("main_data", finalMainData.get(0).getString(0), "main should contain main_data");

      List<Row> finalFeatureEmptyData =
          spark
              .sql("SELECT name FROM " + tableName + " VERSION AS OF 'feature_empty' ORDER BY name")
              .collectAsList();
      assertEquals(
          "wap_branch_data_1",
          finalFeatureEmptyData.get(0).getString(0),
          "feature_empty should contain wap_branch_data_1");

      List<Row> finalFeatureWapTestData =
          spark
              .sql(
                  "SELECT name FROM "
                      + tableName
                      + " VERSION AS OF 'feature_wap_test' ORDER BY name")
              .collectAsList();
      assertEquals(
          "main_data",
          finalFeatureWapTestData.get(0).getString(0),
          "feature_wap_test should contain main_data");
      assertEquals(
          "wap_branch_data_2",
          finalFeatureWapTestData.get(1).getString(0),
          "feature_wap_test should contain wap_branch_data_2");

      // ===== WAP BRANCH SWITCHING BEHAVIOR =====

      // 4. Test switching between WAP branches
      spark.conf().set("spark.wap.branch", "feature_empty");
      List<Row> switchToFeatureEmpty =
          spark.sql("SELECT name FROM " + tableName + " ORDER BY name").collectAsList();
      assertEquals(
          "wap_branch_data_1",
          switchToFeatureEmpty.get(0).getString(0),
          "Should see feature_empty data when switched");

      spark.conf().set("spark.wap.branch", "feature_wap_test");
      List<Row> switchToFeatureWapTest =
          spark.sql("SELECT name FROM " + tableName + " ORDER BY name").collectAsList();
      assertEquals(
          2, switchToFeatureWapTest.size(), "Should see 2 rows when switched to feature_wap_test");
      assertEquals(
          "main_data", switchToFeatureWapTest.get(0).getString(0), "First row should be main_data");
      assertEquals(
          "wap_branch_data_2",
          switchToFeatureWapTest.get(1).getString(0),
          "Second row should be wap_branch_data_2");

      // 5. Test INSERT behavior with WAP branch set
      spark.conf().set("spark.wap.branch", "feature_empty");
      spark.sql("INSERT INTO " + tableName + " VALUES ('additional_wap_data')");

      // Verify the insert went to the WAP branch
      assertEquals(
          2,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Should see 2 rows in feature_empty after additional insert");

      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_empty'")
              .collectAsList()
              .size(),
          "feature_empty branch should have 2 rows after additional insert");

      // Verify other branches are unchanged
      spark.conf().unset("spark.wap.branch");
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should still have 1 row (unchanged)");

      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'feature_wap_test'")
              .collectAsList()
              .size(),
          "feature_wap_test branch should still have 2 rows (unchanged)");

      // ===== SNAPSHOT HISTORY VERIFICATION =====

      // 6. Verify that each branch points to different snapshots
      List<Row> finalMainSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'main'")
              .collectAsList();
      List<Row> finalFeatureEmptySnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'feature_empty'")
              .collectAsList();
      List<Row> finalFeatureWapTestSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'feature_wap_test'")
              .collectAsList();

      assertNotEquals(
          finalMainSnapshots.get(0).getLong(0),
          finalFeatureEmptySnapshots.get(0).getLong(0),
          "main and feature_empty should point to different snapshots");
      assertNotEquals(
          finalMainSnapshots.get(0).getLong(0),
          finalFeatureWapTestSnapshots.get(0).getLong(0),
          "main and feature_wap_test should point to different snapshots");
      assertNotEquals(
          finalFeatureEmptySnapshots.get(0).getLong(0),
          finalFeatureWapTestSnapshots.get(0).getLong(0),
          "feature_empty and feature_wap_test should point to different snapshots");

      // Clean up WAP branch configuration
      spark.conf().unset("spark.wap.branch");
    }
  }

  @Test
  public void testWapBranchCommitWithMultipleBranches() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "wap_multi_branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      // Create table and enable WAP
      spark.sql("CREATE TABLE " + tableName + " (name string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Step 1: Start with main at snapshotX
      spark.sql("INSERT INTO " + tableName + " VALUES ('main_data')");

      // Verify main branch exists and get its snapshot
      List<Row> mainSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'main'")
              .collectAsList();
      assertEquals(1, mainSnapshots.size(), "Main branch should exist");
      long snapshotX = mainSnapshots.get(0).getLong(0);
      System.out.println("SnapshotX (main): " + snapshotX);

      // Step 2: Create branchA from main → branchA also points to snapshotX
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH branchA");

      // Verify branchA points to same snapshot as main
      List<Row> branchASnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchA'")
              .collectAsList();
      assertEquals(1, branchASnapshots.size(), "BranchA should exist");
      long branchASnapshotAfterCreation = branchASnapshots.get(0).getLong(0);
      assertEquals(
          snapshotX, branchASnapshotAfterCreation, "BranchA should point to same snapshot as main");

      // Step 3: Set branchA as the WAP branch and commit data
      spark.conf().set("spark.wap.branch", "branchA");
      spark.sql("INSERT INTO " + tableName + " VALUES ('branchA_data')");

      // Step 4: Verify branchA now points to snapshotY (child of snapshotX)
      List<Row> branchASnapshotsAfterCommit =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchA'")
              .collectAsList();
      long snapshotY = branchASnapshotsAfterCommit.get(0).getLong(0);
      assertNotEquals(
          snapshotX, snapshotY, "BranchA should now point to a new snapshot (snapshotY)");
      System.out.println("SnapshotY (branchA after commit): " + snapshotY);

      // Verify branchA has both main_data and branchA_data
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'branchA'")
              .collectAsList()
              .size(),
          "BranchA should have 2 rows after commit");

      // Verify main still points to snapshotX and has only main_data
      spark.conf().unset("spark.wap.branch");
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should still have 1 row");

      // Step 5: Create branchB from branchA → branchB points to snapshotY
      // First create the branch, then set it to point to the same snapshot as branchA
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH branchB");
      spark.sql("CALL openhouse.system.fast_forward('" + tableName + "', 'branchB', 'branchA')");

      // Verify branchB points to snapshotY
      List<Row> branchBSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchB'")
              .collectAsList();
      long branchBSnapshotAfterCreation = branchBSnapshots.get(0).getLong(0);
      assertEquals(
          snapshotY,
          branchBSnapshotAfterCreation,
          "BranchB should point to snapshotY (same as branchA)");

      // Step 6: Make a commit on branchB → branchB now points to snapshotZ (child of snapshotY)
      // Use direct branch syntax to target branchB specifically
      spark.sql("INSERT INTO " + tableName + ".branch_branchB VALUES ('branchB_data')");

      // Verify branchB now points to snapshotZ
      List<Row> branchBSnapshotsAfterCommit =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchB'")
              .collectAsList();
      long snapshotZ = branchBSnapshotsAfterCommit.get(0).getLong(0);
      assertNotEquals(
          snapshotY, snapshotZ, "BranchB should now point to a new snapshot (snapshotZ)");
      System.out.println("SnapshotZ (branchB after commit): " + snapshotZ);

      // ===== VERIFICATION OF FINAL STATE =====

      // Verify all three branches exist and point to different snapshots
      List<Row> allRefs =
          spark
              .sql("SELECT name, snapshot_id FROM " + tableName + ".refs ORDER BY name")
              .collectAsList();
      assertEquals(3, allRefs.size(), "Should have 3 branches: main, branchA, branchB");

      // Verify snapshot relationships
      List<Row> mainFinalSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'main'")
              .collectAsList();
      List<Row> branchAFinalSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchA'")
              .collectAsList();
      List<Row> branchBFinalSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchB'")
              .collectAsList();

      long finalSnapshotX = mainFinalSnapshots.get(0).getLong(0);
      long finalSnapshotY = branchAFinalSnapshots.get(0).getLong(0);
      long finalSnapshotZ = branchBFinalSnapshots.get(0).getLong(0);

      assertEquals(snapshotX, finalSnapshotX, "Main should still point to snapshotX");
      assertEquals(snapshotY, finalSnapshotY, "BranchA should still point to snapshotY");
      assertEquals(snapshotZ, finalSnapshotZ, "BranchB should point to snapshotZ");

      // Verify data isolation between branches
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should have 1 row");
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'branchA'")
              .collectAsList()
              .size(),
          "BranchA should have 2 rows");
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'branchB'")
              .collectAsList()
              .size(),
          "BranchB should have 3 rows");

      // Verify content
      List<Row> mainData =
          spark.sql("SELECT name FROM " + tableName + " ORDER BY name").collectAsList();
      assertEquals("main_data", mainData.get(0).getString(0), "Main should contain main_data");

      List<Row> branchAData =
          spark
              .sql("SELECT name FROM " + tableName + " VERSION AS OF 'branchA' ORDER BY name")
              .collectAsList();
      assertEquals(
          "branchA_data", branchAData.get(0).getString(0), "BranchA should contain branchA_data");
      assertEquals(
          "main_data", branchAData.get(1).getString(0), "BranchA should contain main_data");

      List<Row> branchBData =
          spark
              .sql("SELECT name FROM " + tableName + " VERSION AS OF 'branchB' ORDER BY name")
              .collectAsList();
      assertEquals(
          "branchA_data", branchBData.get(0).getString(0), "BranchB should contain branchA_data");
      assertEquals(
          "branchB_data", branchBData.get(1).getString(0), "BranchB should contain branchB_data");
      assertEquals(
          "main_data", branchBData.get(2).getString(0), "BranchB should contain main_data");

      // Verify parent-child relationships in snapshot metadata
      List<Row> allSnapshots =
          spark
              .sql(
                  "SELECT snapshot_id, parent_id FROM "
                      + tableName
                      + ".snapshots ORDER BY committed_at")
              .collectAsList();
      assertTrue(allSnapshots.size() >= 3, "Should have at least 3 snapshots");

      // Clean up WAP configuration
      spark.conf().unset("spark.wap.branch");
    }
  }

  @Test
  public void testRegularCommitWithMultipleBranches() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "regular_multi_branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      // Create table (no WAP needed for this test)
      spark.sql("CREATE TABLE " + tableName + " (name string)");

      // Step 1: Start with main at snapshotX
      spark.sql("INSERT INTO " + tableName + " VALUES ('main_data')");

      // Verify main branch exists and get its snapshot
      List<Row> mainSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'main'")
              .collectAsList();
      assertEquals(1, mainSnapshots.size(), "Main branch should exist");
      long snapshotX = mainSnapshots.get(0).getLong(0);
      System.out.println("SnapshotX (main): " + snapshotX);

      // Step 2: Create branchA from main → branchA also points to snapshotX
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH branchA");

      // Verify branchA points to same snapshot as main
      List<Row> branchASnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchA'")
              .collectAsList();
      assertEquals(1, branchASnapshots.size(), "BranchA should exist");
      long branchASnapshotAfterCreation = branchASnapshots.get(0).getLong(0);
      assertEquals(
          snapshotX, branchASnapshotAfterCreation, "BranchA should point to same snapshot as main");

      // Step 3: Commit some data on branchA → branchA now points to snapshotY (child of snapshotX)
      spark.sql("INSERT INTO " + tableName + ".branch_branchA VALUES ('branchA_data')");

      // Verify branchA now points to snapshotY (child of snapshotX)
      List<Row> branchASnapshotsAfterCommit =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchA'")
              .collectAsList();
      long snapshotY = branchASnapshotsAfterCommit.get(0).getLong(0);
      assertNotEquals(
          snapshotX, snapshotY, "BranchA should now point to a new snapshot (snapshotY)");
      System.out.println("SnapshotY (branchA after commit): " + snapshotY);

      // Verify branchA has both main_data and branchA_data
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'branchA'")
              .collectAsList()
              .size(),
          "BranchA should have 2 rows after commit");

      // Verify main still points to snapshotX and has only main_data
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should still have 1 row");

      // Step 4: Create branchB from branchA → branchB points to snapshotY
      // First create the branch, then set it to point to the same snapshot as branchA
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH branchB");
      spark.sql("CALL openhouse.system.fast_forward('" + tableName + "', 'branchB', 'branchA')");

      // Verify branchB points to snapshotY
      List<Row> branchBSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchB'")
              .collectAsList();
      long branchBSnapshotAfterCreation = branchBSnapshots.get(0).getLong(0);
      assertEquals(
          snapshotY,
          branchBSnapshotAfterCreation,
          "BranchB should point to snapshotY (same as branchA)");

      // Step 5: Make a commit on branchB → branchB now points to snapshotZ (child of snapshotY)
      spark.sql("INSERT INTO " + tableName + ".branch_branchB VALUES ('branchB_data')");

      // Verify branchB now points to snapshotZ
      List<Row> branchBSnapshotsAfterCommit =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchB'")
              .collectAsList();
      long snapshotZ = branchBSnapshotsAfterCommit.get(0).getLong(0);
      assertNotEquals(
          snapshotY, snapshotZ, "BranchB should now point to a new snapshot (snapshotZ)");
      System.out.println("SnapshotZ (branchB after commit): " + snapshotZ);

      // ===== VERIFICATION OF FINAL STATE =====

      // Verify all three branches exist and point to different snapshots
      List<Row> allRefs =
          spark
              .sql("SELECT name, snapshot_id FROM " + tableName + ".refs ORDER BY name")
              .collectAsList();
      assertEquals(3, allRefs.size(), "Should have 3 branches: main, branchA, branchB");

      // Verify snapshot relationships
      List<Row> mainFinalSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'main'")
              .collectAsList();
      List<Row> branchAFinalSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchA'")
              .collectAsList();
      List<Row> branchBFinalSnapshots =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchB'")
              .collectAsList();

      long finalSnapshotX = mainFinalSnapshots.get(0).getLong(0);
      long finalSnapshotY = branchAFinalSnapshots.get(0).getLong(0);
      long finalSnapshotZ = branchBFinalSnapshots.get(0).getLong(0);

      assertEquals(snapshotX, finalSnapshotX, "Main should still point to snapshotX");
      assertEquals(snapshotY, finalSnapshotY, "BranchA should still point to snapshotY");
      assertEquals(snapshotZ, finalSnapshotZ, "BranchB should point to snapshotZ");

      // Verify all snapshots are different
      assertNotEquals(
          finalSnapshotX, finalSnapshotY, "SnapshotX and snapshotY should be different");
      assertNotEquals(
          finalSnapshotY, finalSnapshotZ, "SnapshotY and snapshotZ should be different");
      assertNotEquals(
          finalSnapshotX, finalSnapshotZ, "SnapshotX and snapshotZ should be different");

      // Verify data isolation between branches
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main branch should have 1 row");
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'branchA'")
              .collectAsList()
              .size(),
          "BranchA should have 2 rows");
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'branchB'")
              .collectAsList()
              .size(),
          "BranchB should have 3 rows");

      // Verify content
      List<Row> mainData =
          spark.sql("SELECT name FROM " + tableName + " ORDER BY name").collectAsList();
      assertEquals("main_data", mainData.get(0).getString(0), "Main should contain main_data");

      List<Row> branchAData =
          spark
              .sql("SELECT name FROM " + tableName + " VERSION AS OF 'branchA' ORDER BY name")
              .collectAsList();
      assertEquals(
          "branchA_data", branchAData.get(0).getString(0), "BranchA should contain branchA_data");
      assertEquals(
          "main_data", branchAData.get(1).getString(0), "BranchA should contain main_data");

      List<Row> branchBData =
          spark
              .sql("SELECT name FROM " + tableName + " VERSION AS OF 'branchB' ORDER BY name")
              .collectAsList();
      assertEquals(
          "branchA_data", branchBData.get(0).getString(0), "BranchB should contain branchA_data");
      assertEquals(
          "branchB_data", branchBData.get(1).getString(0), "BranchB should contain branchB_data");
      assertEquals(
          "main_data", branchBData.get(2).getString(0), "BranchB should contain main_data");

      // ===== TEST THE SPECIFIC SCENARIO THAT WOULD HAVE BEEN AMBIGUOUS =====

      // At this point, we have:
      // - main points to snapshotX
      // - branchA points to snapshotY
      // - branchB points to snapshotZ
      //
      // If we were to commit a new snapshot as child of snapshotY, our fixed logic should work
      // because only the explicitly targeted branch (via branch-specific insert syntax) should be
      // considered

      // Verify that we can still commit to branchA even though multiple branches exist
      spark.sql("INSERT INTO " + tableName + ".branch_branchA VALUES ('additional_branchA_data')");

      // Verify branchA advanced but branchB didn't
      List<Row> branchAFinalSnapshots2 =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchA'")
              .collectAsList();
      List<Row> branchBFinalSnapshots2 =
          spark
              .sql("SELECT snapshot_id FROM " + tableName + ".refs WHERE name = 'branchB'")
              .collectAsList();

      long finalSnapshotY2 = branchAFinalSnapshots2.get(0).getLong(0);
      long finalSnapshotZ2 = branchBFinalSnapshots2.get(0).getLong(0);

      assertNotEquals(snapshotY, finalSnapshotY2, "BranchA should have advanced to a new snapshot");
      assertEquals(snapshotZ, finalSnapshotZ2, "BranchB should remain at the same snapshot");

      // Verify data counts
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'branchA'")
              .collectAsList()
              .size(),
          "BranchA should now have 3 rows");
      assertEquals(
          3,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'branchB'")
              .collectAsList()
              .size(),
          "BranchB should still have 3 rows (unchanged)");
    }
  }

  // ===== CHERRY PICKING BETWEEN BRANCHES =====

  @Test
  public void testCherryPickToMainWithFeatureBranch() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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

      // CRITICAL: Unset WAP ID before advancing main branch to force non-fast-forward cherry-pick
      spark.conf().unset("spark.wap.id");
      spark.sql("INSERT INTO " + tableName + " VALUES ('main.advance')");

      // Cherry-pick WAP to main branch (this tests our enhanced applySnapshotOperations)
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
    }
  }

  // ===== FAST FORWARD MERGES =====

  @Test
  public void testFastForwardMergeToMain() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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
    }
  }

  @Test
  public void testFastForwardMergeToFeature() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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
    }
  }

  @Test
  public void testFastForwardFeatureToMainAndWapId() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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

      // Unset WAP ID before advancing feature branch normally (not using WAP - else WAP staged
      // snapshot will apply to feature branch)
      spark.conf().unset("spark.wap.id");
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
    }
  }

  @Test
  public void testFastForwardMergeBetweenTwoFeatureBranches() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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
    }
  }

  @Test
  public void testFastForwardMergeIncompatibleLineage() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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
    }
  }

  // ===== SNAPSHOT EXPIRATION FROM NON-MAIN BRANCHES =====

  @Test
  public void testSnapshotExpirationFromFeatureBranch() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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
    }
  }

  @Test
  public void testWapSnapshotExpirationWithMultipleBranches() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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
    }
  }

  // ===== BACKWARD COMPATIBILITY =====

  @Test
  public void testWapIdOnFeatureBranchAndMainBranch() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("CREATE TABLE " + tableName + " (id int, data string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup base data in main branch
      spark.sql("INSERT INTO " + tableName + " VALUES (0, 'main_base')");

      // Create feature branch and add base data to it
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature_a");
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES (10, 'feature_base')");

      // Verify initial state - main has 1 row, feature has 2 rows
      assertEquals(1, spark.sql("SELECT * FROM " + tableName + "").collectAsList().size());
      assertEquals(
          2, spark.sql("SELECT * FROM " + tableName + ".branch_feature_a").collectAsList().size());

      // Create WAP staged snapshot (invisible to normal reads)
      spark.conf().set("spark.wap.id", "shared-wap-snapshot");
      spark.sql("INSERT INTO " + tableName + " VALUES (99, 'wap_staged_data')");

      // Get the WAP snapshot ID
      String wapSnapshotId =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'shared-wap-snapshot'")
              .first()
              .mkString();

      // Verify WAP staging doesn't affect normal reads (principle 2: invisible until published)
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main should not see WAP staged data");
      assertEquals(
          2,
          spark.sql("SELECT * FROM " + tableName + ".branch_feature_a").collectAsList().size(),
          "Feature should not see WAP staged data");

      // Clear WAP ID to avoid contamination
      spark.conf().unset("spark.wap.id");

      // Cherry-pick the same WAP snapshot to MAIN branch
      spark.sql(
          String.format(
              "CALL openhouse.system.cherrypick_snapshot('"
                  + tableName.replace("openhouse.", "")
                  + "', %s)",
              wapSnapshotId));

      // Verify cherry-pick to main worked - main should now have the WAP data
      List<Row> mainAfterCherryPick = spark.sql("SELECT * FROM " + tableName + "").collectAsList();
      assertEquals(2, mainAfterCherryPick.size(), "Main should have base + cherry-picked WAP data");
      boolean mainHasWapData =
          mainAfterCherryPick.stream().anyMatch(row -> "wap_staged_data".equals(row.getString(1)));
      assertTrue(mainHasWapData, "Main should contain cherry-picked WAP data");

      // Verify feature branch is still unaffected
      assertEquals(
          2,
          spark.sql("SELECT * FROM " + tableName + ".branch_feature_a").collectAsList().size(),
          "Feature branch should be unchanged");

      // Demonstrate that WAP snapshots work independently on different branches by
      // creating a separate WAP snapshot while on the feature branch context

      // Create another WAP snapshot that could be applied to feature branch
      spark.conf().set("spark.wap.id", "feature-specific-wap");
      spark.sql("INSERT INTO " + tableName + ".branch_feature_a VALUES (50, 'feature_wap_data')");

      String featureWapSnapshotId =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'feature-specific-wap'")
              .first()
              .mkString();

      // Clear WAP ID again
      spark.conf().unset("spark.wap.id");

      // Verify that both WAP snapshots exist but are invisible to normal reads
      assertEquals(
          2,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "Main should still only show cherry-picked data");
      assertEquals(
          2,
          spark.sql("SELECT * FROM " + tableName + ".branch_feature_a").collectAsList().size(),
          "Feature should not show new WAP data yet");

      // Show that we can cherry-pick the feature WAP to main as well (demonstrating cross-branch
      // capability)
      spark.sql(
          String.format(
              "CALL openhouse.system.cherrypick_snapshot('"
                  + tableName.replace("openhouse.", "")
                  + "', %s)",
              featureWapSnapshotId));

      // Verify main now has both cherry-picked WAP snapshots
      List<Row> finalMain = spark.sql("SELECT * FROM " + tableName + "").collectAsList();
      assertEquals(3, finalMain.size(), "Main should have base + first WAP + second WAP data");

      boolean hasOriginalWap =
          finalMain.stream().anyMatch(row -> "wap_staged_data".equals(row.getString(1)));
      boolean hasFeatureWap =
          finalMain.stream().anyMatch(row -> "feature_wap_data".equals(row.getString(1)));
      assertTrue(hasOriginalWap, "Main should contain first cherry-picked WAP data");
      assertTrue(hasFeatureWap, "Main should contain second cherry-picked WAP data");

      // Verify feature branch is still independent and unchanged by main's cherry-picks
      List<Row> finalFeature =
          spark.sql("SELECT * FROM " + tableName + ".branch_feature_a").collectAsList();
      assertEquals(
          2, finalFeature.size(), "Feature should still only have base + feature_base data");

      // Verify that both original WAP snapshots are still available in metadata
      List<Row> originalWapSnapshots =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'shared-wap-snapshot'")
              .collectAsList();
      List<Row> featureWapSnapshots =
          spark
              .sql(
                  "SELECT snapshot_id FROM "
                      + tableName
                      + ".snapshots WHERE summary['wap.id'] = 'feature-specific-wap'")
              .collectAsList();
      assertEquals(1, originalWapSnapshots.size(), "Original WAP snapshot should still exist");
      assertEquals(1, featureWapSnapshots.size(), "Feature WAP snapshot should still exist");
    }
  }

  @Test
  public void testBackwardCompatibilityMainBranchOnly() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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
    }
  }

  // ===== WAP BRANCH TESTING =====
  // These tests validate the intended WAP branch functionality.
  // WAP branch should stage writes to a specific branch without affecting main.

  @Test
  public void testStagedChangesVisibleViaConf() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "wap_branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("CREATE TABLE " + tableName + " (id int, data string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup base data
      spark.sql("INSERT INTO " + tableName + " VALUES (1, 'base_data')");

      // Create WAP branch and insert staged data
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH wap_branch");
      spark.conf().set("spark.wap.branch", "wap_branch");
      spark.sql("INSERT INTO " + tableName + " VALUES (2, 'staged_data')");

      // When spark.wap.branch is set, SELECT should see WAP branch data (2 rows)
      List<Row> wapVisible = spark.sql("SELECT * FROM " + tableName).collectAsList();
      assertEquals(
          2, wapVisible.size(), "Should see both base and staged data when wap.branch is set");

      // When spark.wap.branch is unset, SELECT should see only main data (1 row)
      spark.conf().unset("spark.wap.branch");
      List<Row> mainOnly = spark.sql("SELECT * FROM " + tableName).collectAsList();
      assertEquals(1, mainOnly.size(), "Should see only base data when wap.branch is unset");
    }
  }

  @Test
  public void testStagedChangesHidden() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "wap_branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("CREATE TABLE " + tableName + " (id int, data string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup base data
      spark.sql("INSERT INTO " + tableName + " VALUES (0, 'base')");

      // Create WAP branch for staged operations
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH wap");

      // Set WAP branch for staged testing
      spark.conf().set("spark.wap.branch", "wap");

      // INSERT INTO table -> inserts to the WAP branch
      spark.sql("INSERT INTO " + tableName + " VALUES (1, 'staged_data')");

      // When spark.wap.branch is set:
      // ✅ SELECT * FROM table → reads from the WAP branch
      List<Row> tableData = spark.sql("SELECT * FROM " + tableName + "").collectAsList();
      assertEquals(
          2,
          tableData.size(),
          "SELECT * FROM table should read from WAP branch when spark.wap.branch is set");
      boolean hasBase = tableData.stream().anyMatch(row -> "base".equals(row.getString(1)));
      boolean hasStaged =
          tableData.stream().anyMatch(row -> "staged_data".equals(row.getString(1)));
      assertTrue(hasBase, "WAP branch should contain base data");
      assertTrue(hasStaged, "WAP branch should contain staged data");

      // ✅ SELECT * FROM table.branch_wap → explicitly reads from WAP branch
      List<Row> wapBranchData =
          spark.sql("SELECT * FROM " + tableName + ".branch_wap").collectAsList();
      assertEquals(2, wapBranchData.size(), "Explicit WAP branch select should show staged data");

      // ✅ SELECT * FROM table.branch_main → explicitly reads from main branch
      List<Row> mainBranchData =
          spark.sql("SELECT * FROM " + tableName + ".branch_main").collectAsList();
      assertEquals(
          1, mainBranchData.size(), "Explicit main branch select should only show base data");
      assertEquals(
          "base", mainBranchData.get(0).getString(1), "Main branch should only contain base data");

      // Now unset spark.wap.branch and ensure main branch is the referenced data
      spark.conf().unset("spark.wap.branch");

      // When spark.wap.branch is unset, SELECT * FROM table should read from main branch
      List<Row> afterUnsetData = spark.sql("SELECT * FROM " + tableName + "").collectAsList();
      assertEquals(
          1,
          afterUnsetData.size(),
          "SELECT * FROM table should read from main branch when spark.wap.branch is unset");
      assertEquals(
          "base",
          afterUnsetData.get(0).getString(1),
          "After unsetting wap.branch, should read from main");

      // INSERT INTO table should go to main branch when spark.wap.branch is unset
      spark.sql("INSERT INTO " + tableName + " VALUES (2, 'main_data')");
      List<Row> finalMainData = spark.sql("SELECT * FROM " + tableName + "").collectAsList();
      assertEquals(
          2, finalMainData.size(), "Main branch should now have 2 rows after unsetting wap.branch");
      boolean hasMainData =
          finalMainData.stream().anyMatch(row -> "main_data".equals(row.getString(1)));
      assertTrue(hasMainData, "Main branch should contain the newly inserted data");

      // WAP branch should remain unchanged
      List<Row> finalWapData =
          spark.sql("SELECT * FROM " + tableName + ".branch_wap").collectAsList();
      assertEquals(
          2, finalWapData.size(), "WAP branch should remain unchanged with base + staged data");
    }
  }

  @Test
  public void testPublishWapBranch() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "wap_branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("CREATE TABLE " + tableName + " (id int, data string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup base data
      spark.sql("INSERT INTO " + tableName + " VALUES (0, 'base')");

      // Create staging branch
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH staging");

      // Stage changes to WAP branch
      spark.conf().set("spark.wap.branch", "staging");
      spark.sql("INSERT INTO " + tableName + " VALUES (1, 'staged_for_publish')");

      // When spark.wap.branch is set, SELECT * FROM table should read from WAP branch
      assertEquals(
          2,
          spark.sql("SELECT * FROM " + tableName + "").collectAsList().size(),
          "SELECT * FROM table should read from WAP branch when spark.wap.branch is set");
      assertEquals(
          2,
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF 'staging'")
              .collectAsList()
              .size(),
          "Staging should have staged data");

      // Verify main branch still only has base data
      assertEquals(
          1,
          spark.sql("SELECT * FROM " + tableName + ".branch_main").collectAsList().size(),
          "Main branch should not have staged data");

      // Fast-forward main branch to staging branch to publish the staged changes
      spark.sql("CALL openhouse.system.fast_forward('" + tableName + "', 'main', 'staging')");

      // Verify data is now published to main branch (need to explicitly check main branch)
      List<Row> publishedData =
          spark.sql("SELECT * FROM " + tableName + ".branch_main").collectAsList();
      assertEquals(2, publishedData.size(), "Main branch should now have published data");

      boolean hasPublished =
          publishedData.stream().anyMatch(row -> "staged_for_publish".equals(row.getString(1)));
      assertTrue(hasPublished, "Main branch should contain the published staged data");

      // Verify that with wap.branch still set, SELECT * FROM table still reads from WAP branch
      List<Row> wapData = spark.sql("SELECT * FROM " + tableName + "").collectAsList();
      assertEquals(2, wapData.size(), "SELECT * FROM table should still read from WAP branch");
    }
  }

  @Test
  public void testWapIdAndWapBranchIncompatible() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "wap_branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("CREATE TABLE " + tableName + " (id int, data string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup base data
      spark.sql("INSERT INTO " + tableName + " VALUES (0, 'base')");

      // Create staging branch
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH staging");

      // Set both WAP ID and WAP branch - this should be invalid
      spark.conf().set("spark.wap.id", "test-wap-id");
      spark.conf().set("spark.wap.branch", "staging");

      // Attempt to write with both configurations should fail
      assertThrows(
          Exception.class,
          () -> spark.sql("INSERT INTO " + tableName + " VALUES (1, 'invalid')"),
          "Cannot use both wap.id and wap.branch simultaneously");
    }
  }

  @Test
  public void testCannotWriteToBothBranches() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "wap_branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

      spark.sql("CREATE TABLE " + tableName + " (id int, data string)");
      spark.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('write.wap.enabled'='true')");

      // Setup base data
      spark.sql("INSERT INTO " + tableName + " VALUES (0, 'base')");

      // Create branches
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH feature");
      spark.sql("ALTER TABLE " + tableName + " CREATE BRANCH staging");

      // Set WAP branch
      spark.conf().set("spark.wap.branch", "staging");

      // ❌ INVALID: Cannot write to both normal branch and WAP branch
      assertThrows(
          Exception.class,
          () -> spark.sql("INSERT INTO " + tableName + ".branch_feature VALUES (1, 'invalid')"),
          "Cannot write to explicit branch when wap.branch is set");
    }
  }

  // ===== ERROR SCENARIOS =====

  @Test
  public void testErrorInsertToNonExistentBranch() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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
    }
  }

  @Test
  public void testErrorCherryPickNonExistentWapId() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = "branch_test_" + System.currentTimeMillis();
      String tableName = "openhouse.d1." + tableId;

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
    }
  }
}

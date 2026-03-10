package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
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
 * Tests that snapshot expiration does NOT remove snapshots reachable from tags or branches. This
 * demonstrates that member data can remain queryable through refs even after expire_snapshots runs,
 * which is the compliance gap motivating the RFC for automatic ref-aware purging.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@Execution(ExecutionMode.SAME_THREAD)
public class SnapshotExpirationRefsTest extends OpenHouseSparkITest {

  private static final String DATABASE = "d1_expiration_refs";
  private static final String TEST_PREFIX = "exp_refs_";

  @AfterEach
  public void cleanupAfterTest() {
    try (SparkSession spark = getSparkSession()) {
      List<Row> tables = spark.sql("SHOW TABLES IN openhouse." + DATABASE).collectAsList();
      for (Row table : tables) {
        String name = table.getString(1);
        spark.sql("DROP TABLE IF EXISTS openhouse." + DATABASE + "." + name);
      }
    } catch (Exception e) {
      System.err.println("Warning: cleanup failed: " + e.getMessage());
    }
  }

  /**
   * A tag pointing at a snapshot should prevent expire_snapshots from removing that snapshot's
   * data. After expiration, querying the table at the tagged snapshot should still return the
   * original rows.
   */
  @Test
  public void testTagPreservesSnapshotThroughExpiration() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = TEST_PREFIX + System.currentTimeMillis();
      String tableName = "openhouse." + DATABASE + "." + tableId;

      // Create table and insert Wave 1 member data
      spark.sql("CREATE TABLE " + tableName + " (member_id bigint, event_type string)");
      spark.sql(
          "INSERT INTO " + tableName + " VALUES (1001, 'login'), (1002, 'click'), (1003, 'view')");

      // Capture the Wave 1 snapshot ID
      long wave1SnapshotId = getLatestSnapshotId(spark, tableName);

      // Tag the Wave 1 snapshot
      spark.sql(
          "ALTER TABLE "
              + tableName
              + " CREATE TAG wave1_members AS OF VERSION "
              + wave1SnapshotId);

      // Insert Wave 2 and Wave 3 to push Wave 1 into history
      spark.sql("INSERT INTO " + tableName + " VALUES (2001, 'login'), (2002, 'click')");
      spark.sql("INSERT INTO " + tableName + " VALUES (3001, 'view')");

      int snapshotsBeforeExpiry = getSnapshotIds(spark, tableName).size();
      assertEquals(3, snapshotsBeforeExpiry, "Should have 3 snapshots before expiration");

      // Expire all snapshots aggressively, keeping only 1 on the main lineage
      spark.sql(
          "CALL openhouse.system.expire_snapshots(table => '"
              + tableName
              + "', older_than => TIMESTAMP '2099-01-01 00:00:00', retain_last => 1)");

      // The tag should still be present in refs
      List<String> refNames = getRefNames(spark, tableName);
      assertTrue(refNames.contains("wave1_members"), "Tag wave1_members should still exist");

      // KEY ASSERTION: Wave 1 data should still be queryable through the tagged snapshot
      List<Row> taggedData =
          spark
              .sql("SELECT * FROM " + tableName + " VERSION AS OF " + wave1SnapshotId)
              .collectAsList();
      assertEquals(
          3,
          taggedData.size(),
          "Tag should preserve all 3 Wave 1 rows through snapshot expiration");
    }
  }

  /**
   * A branch pointing at a snapshot should prevent expire_snapshots from removing that snapshot's
   * data. After expiration, querying the table at the branch's snapshot should still return the
   * original rows.
   */
  @Test
  public void testBranchPreservesSnapshotThroughExpiration() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = TEST_PREFIX + System.currentTimeMillis();
      String tableName = "openhouse." + DATABASE + "." + tableId;

      // Create table and insert Wave 1 member data
      spark.sql("CREATE TABLE " + tableName + " (member_id bigint, event_type string)");
      spark.sql(
          "INSERT INTO " + tableName + " VALUES (4001, 'login'), (4002, 'click'), (4003, 'view')");

      // Capture the Wave 1 snapshot ID
      long wave1SnapshotId = getLatestSnapshotId(spark, tableName);

      // Create a branch at the Wave 1 snapshot
      spark.sql(
          "ALTER TABLE "
              + tableName
              + " CREATE BRANCH audit_branch AS OF VERSION "
              + wave1SnapshotId);

      // Insert Wave 2 and Wave 3 on main to push Wave 1 into history
      spark.sql("INSERT INTO " + tableName + " VALUES (5001, 'login'), (5002, 'click')");
      spark.sql("INSERT INTO " + tableName + " VALUES (6001, 'view')");

      int snapshotsBeforeExpiry = getSnapshotIds(spark, tableName).size();
      assertEquals(3, snapshotsBeforeExpiry, "Should have 3 snapshots before expiration");

      // Expire all snapshots aggressively, keeping only 1 on the main lineage
      spark.sql(
          "CALL openhouse.system.expire_snapshots(table => '"
              + tableName
              + "', older_than => TIMESTAMP '2099-01-01 00:00:00', retain_last => 1)");

      // The branch should still be present in refs
      List<String> refNames = getRefNames(spark, tableName);
      assertTrue(refNames.contains("audit_branch"), "Branch audit_branch should still exist");

      // KEY ASSERTION: Wave 1 data should still be queryable through the branch
      List<Row> branchData =
          spark.sql("SELECT * FROM " + tableName + " VERSION AS OF 'audit_branch'").collectAsList();
      assertEquals(
          3,
          branchData.size(),
          "Branch should preserve all 3 Wave 1 rows through snapshot expiration");
    }
  }

  /**
   * Option B validation: Setting the table property history.expire.max-ref-age-ms to a small value
   * should cause expire_snapshots to drop refs older than the threshold, making their snapshots
   * eligible for expiration. This is the zero-syntax-change solution for compliance.
   */
  @Test
  public void testMaxRefAgeMsPropertyDropsExpiredTagAndBranch() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableId = TEST_PREFIX + System.currentTimeMillis();
      String tableName = "openhouse." + DATABASE + "." + tableId;

      // Create table and insert Wave 1 member data
      spark.sql("CREATE TABLE " + tableName + " (member_id bigint, event_type string)");
      spark.sql(
          "INSERT INTO " + tableName + " VALUES (1001, 'login'), (1002, 'click'), (1003, 'view')");

      long wave1SnapshotId = getLatestSnapshotId(spark, tableName);

      // Create a tag and a branch at the Wave 1 snapshot
      spark.sql(
          "ALTER TABLE " + tableName + " CREATE TAG old_tag AS OF VERSION " + wave1SnapshotId);
      spark.sql(
          "ALTER TABLE "
              + tableName
              + " CREATE BRANCH old_branch AS OF VERSION "
              + wave1SnapshotId);

      // Insert Wave 2 on main so main has a newer snapshot
      spark.sql("INSERT INTO " + tableName + " VALUES (2001, 'login'), (2002, 'click')");

      // Verify both refs exist before expiration
      List<String> refsBefore = getRefNames(spark, tableName);
      assertTrue(refsBefore.contains("old_tag"), "Tag should exist before expiration");
      assertTrue(refsBefore.contains("old_branch"), "Branch should exist before expiration");
      assertEquals(2, getSnapshotIds(spark, tableName).size(), "Should have 2 snapshots");

      // Set max-ref-age-ms to 1ms — any ref older than 1ms will be dropped on next expiration
      spark.sql(
          "ALTER TABLE "
              + tableName
              + " SET TBLPROPERTIES ('history.expire.max-ref-age-ms' = '1')");

      // Small delay to ensure refs are older than 1ms
      Thread.sleep(10);

      // Run expire_snapshots — should drop the tag and branch, then expire Wave 1 snapshot
      spark.sql(
          "CALL openhouse.system.expire_snapshots(table => '"
              + tableName
              + "', older_than => TIMESTAMP '2099-01-01 00:00:00', retain_last => 1)");

      // KEY ASSERTIONS: refs should be gone
      List<String> refsAfter = getRefNames(spark, tableName);
      assertFalse(refsAfter.contains("old_tag"), "Tag should be dropped by max-ref-age-ms");
      assertFalse(refsAfter.contains("old_branch"), "Branch should be dropped by max-ref-age-ms");
      assertTrue(refsAfter.contains("main"), "Main branch should always be retained");

      // Wave 1 snapshot should be expired since no ref protects it anymore
      List<Long> remainingSnapshots = getSnapshotIds(spark, tableName);
      assertEquals(1, remainingSnapshots.size(), "Only the latest main snapshot should remain");
      assertFalse(
          remainingSnapshots.contains(wave1SnapshotId),
          "Wave 1 snapshot should be expired after ref was dropped");
    }
  }

  private static long getLatestSnapshotId(SparkSession spark, String tableName) {
    List<Row> snapshots =
        spark
            .sql("SELECT snapshot_id FROM " + tableName + ".snapshots ORDER BY committed_at")
            .collectAsList();
    return snapshots.get(snapshots.size() - 1).getLong(0);
  }

  private static List<Long> getSnapshotIds(SparkSession spark, String tableName) {
    return spark.sql("SELECT snapshot_id FROM " + tableName + ".snapshots ORDER BY committed_at")
        .collectAsList().stream()
        .map(r -> r.getLong(0))
        .collect(Collectors.toList());
  }

  private static List<String> getRefNames(SparkSession spark, String tableName) {
    return spark.sql("SELECT name FROM " + tableName + ".refs").collectAsList().stream()
        .map(r -> r.getString(0))
        .collect(Collectors.toList());
  }
}

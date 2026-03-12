package com.linkedin.openhouse.spark.catalogtest.e2e;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class ChangelogViewTest extends OpenHouseSparkITest {

  private static final String DATABASE = "d1_changelog";

  @Test
  void testChangelogViewForAppends() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String name = DATABASE + ".changelog_appends";
      spark.sql("DROP TABLE IF EXISTS openhouse." + name);
      spark.sql("CREATE TABLE openhouse." + name + " (id int, data string)");

      spark.sql("INSERT INTO openhouse." + name + " VALUES (1, 'a'), (2, 'b')");
      spark.sql("INSERT INTO openhouse." + name + " VALUES (3, 'c')");

      // Get snapshot IDs
      List<Row> snapshots =
          spark
              .sql("SELECT snapshot_id FROM openhouse." + name + ".snapshots ORDER BY committed_at")
              .collectAsList();
      assertEquals(2, snapshots.size());
      long snap1 = snapshots.get(0).getLong(0);
      long snap2 = snapshots.get(1).getLong(0);

      // Create changelog view between the two snapshots
      spark.sql(
          String.format(
              "CALL openhouse.system.create_changelog_view("
                  + "table => '%s', "
                  + "options => map('start-snapshot-id', '%d', 'end-snapshot-id', '%d'))",
              name, snap1, snap2));

      // The default view name is <table>_changes
      List<Row> changes = spark.sql("SELECT * FROM changelog_appends_changes").collectAsList();
      assertEquals(1, changes.size(), "Should have 1 change (the appended row)");

      // Verify change type and data
      Row change = changes.get(0);
      assertEquals("INSERT", change.getAs("_change_type"));
      assertEquals(3, (int) change.getAs("id"));
      assertEquals("c", change.getAs("data"));

      spark.sql("DROP TABLE openhouse." + name);
    }
  }

  @Test
  void testChangelogViewForOverwrite() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String name = DATABASE + ".changelog_overwrite";
      spark.sql("DROP TABLE IF EXISTS openhouse." + name);
      spark.sql("CREATE TABLE openhouse." + name + " (id int, data string)");

      spark.sql("INSERT INTO openhouse." + name + " VALUES (1, 'a'), (2, 'b')");
      spark.sql("INSERT OVERWRITE openhouse." + name + " VALUES (3, 'c')");

      List<Row> snapshots =
          spark
              .sql("SELECT snapshot_id FROM openhouse." + name + ".snapshots ORDER BY committed_at")
              .collectAsList();
      assertEquals(2, snapshots.size());
      long snap1 = snapshots.get(0).getLong(0);
      long snap2 = snapshots.get(1).getLong(0);

      spark.sql(
          String.format(
              "CALL openhouse.system.create_changelog_view("
                  + "table => '%s', "
                  + "options => map('start-snapshot-id', '%d', 'end-snapshot-id', '%d'))",
              name, snap1, snap2));

      List<Row> changes = spark.sql("SELECT * FROM changelog_overwrite_changes").collectAsList();

      // Overwrite should produce DELETEs for old rows and INSERTs for new rows
      Set<String> changeTypes =
          changes.stream().map(r -> r.getAs("_change_type").toString()).collect(Collectors.toSet());
      assertTrue(changeTypes.contains("DELETE"), "Should have DELETE changes for overwritten rows");
      assertTrue(changeTypes.contains("INSERT"), "Should have INSERT changes for new rows");

      // Verify the deletes are for the original rows
      List<Row> deletes =
          changes.stream()
              .filter(r -> "DELETE".equals(r.getAs("_change_type")))
              .collect(Collectors.toList());
      assertEquals(2, deletes.size(), "Should delete both original rows");
      Set<Integer> deletedIds =
          deletes.stream().map(r -> (int) r.getAs("id")).collect(Collectors.toSet());
      assertTrue(deletedIds.containsAll(Set.of(1, 2)));

      // Verify the insert is the new row
      List<Row> inserts =
          changes.stream()
              .filter(r -> "INSERT".equals(r.getAs("_change_type")))
              .collect(Collectors.toList());
      assertEquals(1, inserts.size(), "Should insert one new row");
      assertEquals(3, (int) inserts.get(0).getAs("id"));
      assertEquals("c", inserts.get(0).getAs("data"));

      spark.sql("DROP TABLE openhouse." + name);
    }
  }

  @Test
  void testChangelogViewForDelete() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String name = DATABASE + ".changelog_delete";
      spark.sql("DROP TABLE IF EXISTS openhouse." + name);
      spark.sql(
          "CREATE TABLE openhouse."
              + name
              + " (id int, data string) TBLPROPERTIES ('format-version'='2')");

      spark.sql("INSERT INTO openhouse." + name + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
      spark.sql("DELETE FROM openhouse." + name + " WHERE id = 2");

      List<Row> snapshots =
          spark
              .sql("SELECT snapshot_id FROM openhouse." + name + ".snapshots ORDER BY committed_at")
              .collectAsList();
      assertEquals(2, snapshots.size());
      long snap1 = snapshots.get(0).getLong(0);
      long snap2 = snapshots.get(1).getLong(0);

      spark.sql(
          String.format(
              "CALL openhouse.system.create_changelog_view("
                  + "table => '%s', "
                  + "options => map('start-snapshot-id', '%d', 'end-snapshot-id', '%d'))",
              name, snap1, snap2));

      List<Row> changes = spark.sql("SELECT * FROM changelog_delete_changes").collectAsList();

      // DELETE should produce a DELETE change for the removed row
      assertEquals(1, changes.size(), "Should have 1 change for the deleted row");
      Row change = changes.get(0);
      assertEquals("DELETE", change.getAs("_change_type"));
      assertEquals(2, (int) change.getAs("id"));
      assertEquals("b", change.getAs("data"));

      spark.sql("DROP TABLE openhouse." + name);
    }
  }

  @Test
  void testChangelogViewWithNetChanges() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String name = DATABASE + ".changelog_net";
      spark.sql("DROP TABLE IF EXISTS openhouse." + name);
      spark.sql(
          "CREATE TABLE openhouse."
              + name
              + " (id int, data string) TBLPROPERTIES ('format-version'='2')");

      // Insert, then delete, then re-insert the same id across multiple snapshots
      spark.sql("INSERT INTO openhouse." + name + " VALUES (1, 'a'), (2, 'b')");
      spark.sql("DELETE FROM openhouse." + name + " WHERE id = 1");
      spark.sql("INSERT INTO openhouse." + name + " VALUES (1, 'a_updated'), (3, 'c')");

      List<Row> snapshots =
          spark
              .sql("SELECT snapshot_id FROM openhouse." + name + ".snapshots ORDER BY committed_at")
              .collectAsList();
      assertEquals(3, snapshots.size());
      long snap1 = snapshots.get(0).getLong(0);
      long snap3 = snapshots.get(2).getLong(0);

      // Use net_changes to collapse intermediate changes.
      // compute_updates defaults to true when identifier_columns is set, but
      // net_changes and compute_updates cannot both be true, so disable compute_updates.
      spark.sql(
          String.format(
              "CALL openhouse.system.create_changelog_view("
                  + "table => '%s', "
                  + "options => map('start-snapshot-id', '%d', 'end-snapshot-id', '%d'), "
                  + "net_changes => true, "
                  + "compute_updates => false, "
                  + "identifier_columns => array('id'))",
              name, snap1, snap3));

      List<Row> changes =
          spark
              .sql("SELECT * FROM changelog_net_changes ORDER BY _change_type, id")
              .collectAsList();

      // With compute_updates=false, net changes for each id:
      //   id=1: deleted then re-inserted → separate DELETE + INSERT (not collapsed into UPDATE)
      //   id=2: unchanged → no change
      //   id=3: inserted → INSERT
      assertEquals(3, changes.size(), "Should have 3 net changes");

      // id=1 DELETE (old value)
      Row delete1 = changes.get(0);
      assertEquals("DELETE", delete1.getAs("_change_type"));
      assertEquals(1, (int) delete1.getAs("id"));
      assertEquals("a", delete1.getAs("data"));

      // id=1 INSERT (new value)
      Row insert1 = changes.get(1);
      assertEquals("INSERT", insert1.getAs("_change_type"));
      assertEquals(1, (int) insert1.getAs("id"));
      assertEquals("a_updated", insert1.getAs("data"));

      // id=3 INSERT
      Row insert3 = changes.get(2);
      assertEquals("INSERT", insert3.getAs("_change_type"));
      assertEquals(3, (int) insert3.getAs("id"));
      assertEquals("c", insert3.getAs("data"));

      // id=2 should not appear (unchanged across the range)
      assertTrue(
          changes.stream().noneMatch(r -> (int) r.getAs("id") == 2),
          "id=2 should not appear in net changes (unchanged)");

      spark.sql("DROP TABLE openhouse." + name);
    }
  }

  @Test
  void testChangelogViewMultipleSnapshotsSpan() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String name = DATABASE + ".changelog_multi";
      spark.sql("DROP TABLE IF EXISTS openhouse." + name);
      spark.sql("CREATE TABLE openhouse." + name + " (id int, data string)");

      spark.sql("INSERT INTO openhouse." + name + " VALUES (1, 'a')");
      spark.sql("INSERT INTO openhouse." + name + " VALUES (2, 'b')");
      spark.sql("INSERT INTO openhouse." + name + " VALUES (3, 'c')");

      List<Row> snapshots =
          spark
              .sql("SELECT snapshot_id FROM openhouse." + name + ".snapshots ORDER BY committed_at")
              .collectAsList();
      assertEquals(3, snapshots.size());
      long snap1 = snapshots.get(0).getLong(0);
      long snap3 = snapshots.get(2).getLong(0);

      spark.sql(
          String.format(
              "CALL openhouse.system.create_changelog_view("
                  + "table => '%s', "
                  + "options => map('start-snapshot-id', '%d', 'end-snapshot-id', '%d'))",
              name, snap1, snap3));

      List<Row> changes =
          spark.sql("SELECT * FROM changelog_multi_changes ORDER BY id").collectAsList();

      // Should see INSERTs for rows added in snapshots 2 and 3
      assertEquals(2, changes.size(), "Should see 2 inserts spanning snapshots 2 and 3");
      assertTrue(changes.stream().allMatch(r -> "INSERT".equals(r.getAs("_change_type"))));

      Set<Integer> insertedIds =
          changes.stream().map(r -> (int) r.getAs("id")).collect(Collectors.toSet());
      assertEquals(Set.of(2, 3), insertedIds);

      spark.sql("DROP TABLE openhouse." + name);
    }
  }
}

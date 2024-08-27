package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.DuplicateWAPCommitException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class WapIdTest extends OpenHouseSparkITest {
  @Test
  public void testWriteWapToEmptyTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)");
      spark.sql("ALTER TABLE openhouse.d1.t1 SET TBLPROPERTIES ('write.wap.enabled'='true')");

      spark.conf().set("spark.wap.id", "wap1");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('wap1.a')");

      // snapshot added but no data inserted
      assertEquals(0, spark.sql("SELECT * FROM openhouse.d1.t1").collectAsList().size());
      assertEquals(1, spark.sql("SELECT * FROM openhouse.d1.t1.snapshots").collectAsList().size());
      assertEquals(0, spark.sql("SELECT * FROM openhouse.d1.t1.refs").collectAsList().size());

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }

  @Test
  public void testWriteWapToNonEmptyTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)");
      spark.sql("ALTER TABLE openhouse.d1.t1 SET TBLPROPERTIES ('write.wap.enabled'='true')");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('main.a')");

      spark.conf().set("spark.wap.id", "wap1");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('wap1.a')");

      // snapshot added but no data inserted
      assertEquals(1, spark.sql("SELECT * FROM openhouse.d1.t1").collectAsList().size());
      assertEquals(2, spark.sql("SELECT * FROM openhouse.d1.t1.snapshots").collectAsList().size());
      assertEquals(1, spark.sql("SELECT * FROM openhouse.d1.t1.refs").collectAsList().size());

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }

  @Test
  public void testCherryPickBaseUnchanged() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)");
      spark.sql("ALTER TABLE openhouse.d1.t1 SET TBLPROPERTIES ('write.wap.enabled'='true')");

      spark.conf().set("spark.wap.id", "wap1");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('wap1.a')");
      String snapshotIdWap1 =
          spark
              .sql(
                  "SELECT snapshot_id FROM openhouse.d1.t1.snapshots WHERE summary['wap.id'] = 'wap1'")
              .first()
              .mkString();
      spark.sql(
          String.format("CALL openhouse.system.cherrypick_snapshot('d1.t1', %s)", snapshotIdWap1));

      // snapshot committed and data inserted
      assertEquals(1, spark.sql("SELECT * FROM openhouse.d1.t1").collectAsList().size());
      assertEquals(1, spark.sql("SELECT * FROM openhouse.d1.t1.snapshots").collectAsList().size());
      assertEquals(
          snapshotIdWap1,
          spark
              .sql("SELECT snapshot_id FROM openhouse.d1.t1.refs WHERE name == 'main'")
              .first()
              .mkString());

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }

  @Test
  public void testCherryPickBaseChanged() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)");
      spark.sql("ALTER TABLE openhouse.d1.t1 SET TBLPROPERTIES ('write.wap.enabled'='true')");

      spark.conf().set("spark.wap.id", "wap1");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('wap1.a')");
      spark.conf().unset("spark.wap.id");

      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('main.a')");
      String snapshotIdWap1 =
          spark
              .sql(
                  "SELECT snapshot_id FROM openhouse.d1.t1.snapshots WHERE summary['wap.id'] = 'wap1'")
              .first()
              .mkString();
      spark.sql(
          String.format("CALL openhouse.system.cherrypick_snapshot('d1.t1', %s)", snapshotIdWap1));
      String snapshotIdPublished =
          spark
              .sql(
                  "SELECT snapshot_id FROM openhouse.d1.t1.snapshots WHERE summary['published-wap-id'] = 'wap1'")
              .first()
              .mkString();

      // a new snapshot is added and committed
      assertEquals(2, spark.sql("SELECT * FROM openhouse.d1.t1").collectAsList().size());
      assertEquals(3, spark.sql("SELECT * FROM openhouse.d1.t1.snapshots").collectAsList().size());
      assertEquals(
          snapshotIdPublished,
          spark
              .sql("SELECT snapshot_id FROM openhouse.d1.t1.refs WHERE name == 'main'")
              .first()
              .mkString());

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }

  @Test
  public void testCherryPickPublishedWap() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)");
      spark.sql("ALTER TABLE openhouse.d1.t1 SET TBLPROPERTIES ('write.wap.enabled'='true')");

      spark.conf().set("spark.wap.id", "wap1");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('wap1.a')");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('wap1.b')");
      List<String> snapshotList =
          spark
              .sql(
                  "SELECT snapshot_id FROM openhouse.d1.t1.snapshots WHERE summary['wap.id'] = 'wap1'")
              .collectAsList().stream()
              .map(Row::mkString)
              .collect(Collectors.toList());
      spark.sql(
          String.format(
              "CALL openhouse.system.cherrypick_snapshot('d1.t1', %s)", snapshotList.get(0)));

      // a wap id cannot be picked up more than once
      assertThrows(
          DuplicateWAPCommitException.class,
          () ->
              spark.sql(
                  String.format(
                      "CALL openhouse.system.cherrypick_snapshot('d1.t1', %s)",
                      snapshotList.get(1))));

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }

  @Test
  public void testExpireSnapshotReferenced() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)");
      spark.sql("ALTER TABLE openhouse.d1.t1 SET TBLPROPERTIES ('write.wap.enabled'='true')");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('main.a')");

      spark.conf().set("spark.wap.id", "wap1");
      String snapshotId =
          spark.sql("SELECT snapshot_id FROM openhouse.d1.t1.snapshots").first().mkString();

      // cannot expire snapshot that is still referenced
      assertThrows(
          IllegalArgumentException.class,
          () ->
              spark.sql(
                  String.format(
                      "CALL openhouse.system.expire_snapshots(table => 'd1.t1', snapshot_ids => Array(%s))",
                      snapshotId)));

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }

  @Test
  public void testExpireSnapshotUnreferenced() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)");
      spark.sql("ALTER TABLE openhouse.d1.t1 SET TBLPROPERTIES ('write.wap.enabled'='true')");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('main.a')");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('main.b')");

      spark.conf().set("spark.wap.id", "wap1");
      String snapshotIdHead =
          spark
              .sql("SELECT snapshot_id FROM openhouse.d1.t1.refs WHERE name == 'main'")
              .first()
              .mkString();
      String snapshotIdToExpire =
          spark
              .sql(
                  String.format(
                      "SELECT snapshot_id FROM openhouse.d1.t1.snapshots WHERE 'snapshot-id' != '%s'",
                      snapshotIdHead))
              .first()
              .mkString();
      spark.sql(
          String.format(
              "CALL openhouse.system.expire_snapshots(table => 'd1.t1', snapshot_ids => Array(%s))",
              snapshotIdToExpire));

      // can expire snapshot that is not referenced in wap mode
      assertEquals(2, spark.sql("SELECT * FROM openhouse.d1.t1").collectAsList().size());
      assertEquals(1, spark.sql("SELECT * FROM openhouse.d1.t1.snapshots").collectAsList().size());

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }

  @Test
  public void testExpireSnapshotsWithUnpublishedWap() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)");
      spark.sql("ALTER TABLE openhouse.d1.t1 SET TBLPROPERTIES ('write.wap.enabled'='true')");

      spark.conf().set("spark.wap.id", "wap1");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('wap1.a')");
      spark.conf().unset("spark.wap.id");

      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('main.a')");
      String snapshotIdToExpire =
          spark
              .sql("SELECT snapshot_id FROM openhouse.d1.t1.refs WHERE name == 'main'")
              .first()
              .mkString();
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('main.b')");
      spark.sql(
          String.format(
              "CALL openhouse.system.expire_snapshots(table => 'd1.t1', snapshot_ids => Array(%s))",
              snapshotIdToExpire));

      // expiring a specific snapshot won't affect its ancestor or unpublished wap snapshots
      assertEquals(2, spark.sql("SELECT * FROM openhouse.d1.t1").collectAsList().size());
      assertEquals(2, spark.sql("SELECT * FROM openhouse.d1.t1.snapshots").collectAsList().size());

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }

  @Test
  public void testExpireSnapshotsWithEmptyRefs() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sparkContext().setLogLevel("WARN");
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)");
      spark.sql("ALTER TABLE openhouse.d1.t1 SET TBLPROPERTIES ('write.wap.enabled'='true')");

      spark.conf().set("spark.wap.id", "wap1");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('wap1.a')");

      String snapshotIdToExpire =
          spark.sql("SELECT snapshot_id FROM openhouse.d1.t1.snapshots").first().mkString();
      spark.sql(
          String.format(
              "CALL openhouse.system.expire_snapshots(table => 'd1.t1', snapshot_ids => Array(%s))",
              snapshotIdToExpire));

      // We don't support this uses case yet, so the expire_snapshots will do nothing
      assertEquals(0, spark.sql("SELECT * FROM openhouse.d1.t1").collectAsList().size());
      assertEquals(1, spark.sql("SELECT * FROM openhouse.d1.t1.snapshots").collectAsList().size());

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }

  @Test
  public void testWapWorkflowWithVariousOperations() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sparkContext().setLogLevel("WARN");
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)"); // create
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('main.a')"); // insert
      spark.sql("ALTER TABLE openhouse.d1.t1 SET TBLPROPERTIES ('write.wap.enabled'='true')");
      spark.sql("ALTER TABLE openhouse.d1.t1 SET POLICY (SHARING=TRUE)"); // set policy
      spark.sql("GRANT SELECT ON TABLE openhouse.d1.t1 TO lejiang").show(); // grant
      spark.conf().set("spark.wap.id", "wap1");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('wap1.a')"); // wap insert
      spark.conf().unset("spark.wap.id");
      String snapshotId =
          spark
              .sql(
                  "SELECT snapshot_id FROM openhouse.d1.t1.snapshots WHERE summary['wap.id'] = 'wap1'")
              .first()
              .mkString(); // select
      spark.sql(
          String.format(
              "CALL openhouse.system.cherrypick_snapshot('d1.t1', %s)", snapshotId)); // cherry-pick
      spark.sql("DELETE FROM openhouse.d1.t1 WHERE name = 'wap1.a'"); // delete
      spark.sql(
          String.format(
              "CALL openhouse.system.expire_snapshots(table => 'd1.t1', snapshot_ids => Array(%s))",
              snapshotId)); // expire

      assertEquals(1, spark.sql("SELECT * FROM openhouse.d1.t1").collectAsList().size());
      assertEquals(2, spark.sql("SELECT * FROM openhouse.d1.t1.snapshots").collectAsList().size());

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }
}

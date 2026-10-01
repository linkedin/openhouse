package com.linkedin.openhouse.spark.catalogtest.e2e;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class IncrementalReadTest extends OpenHouseSparkITest {

  private static final String DATABASE = "d1_incremental";

  @Test
  void testIncrementalReadWithOverwriteSkipsNonAppends() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String name = DATABASE + ".incr_overwrite";
      spark.sql("DROP TABLE IF EXISTS openhouse." + name);
      spark.sql("CREATE TABLE openhouse." + name + " (id string, data string)");

      // Batch 1: append → snapshot 1
      spark.sql("INSERT INTO openhouse." + name + " VALUES ('1', 'a'), ('2', 'b')");
      // Overwrite all data → overwrite snapshot 2
      spark.sql("INSERT OVERWRITE openhouse." + name + " VALUES ('3', 'c')");
      // Batch 3: append → snapshot 3
      spark.sql("INSERT INTO openhouse." + name + " VALUES ('4', 'd')");

      List<Row> snapshots =
          spark
              .sql(
                  "SELECT snapshot_id, operation FROM openhouse."
                      + name
                      + ".snapshots ORDER BY committed_at")
              .collectAsList();
      assertEquals(3, snapshots.size(), "Should have exactly 3 snapshots");
      assertEquals(
          "overwrite", snapshots.get(1).getString(1), "Middle snapshot should be an overwrite");

      long snap1 = snapshots.get(0).getLong(0);
      long snap3 = snapshots.get(2).getLong(0);

      // Iceberg 1.5 (Spark 3.5): IncrementalAppendScan silently skips non-append snapshots
      // and returns only data from append operations.
      List<String> incrementalRows =
          spark.read().format("iceberg").option("start-snapshot-id", String.valueOf(snap1))
              .option("end-snapshot-id", String.valueOf(snap3)).load("openhouse." + name)
              .collectAsList().stream()
              .map(row -> row.mkString("."))
              .collect(Collectors.toList());

      assertEquals(
          1, incrementalRows.size(), "Should return only appended rows, skipping the overwrite");
      assertTrue(
          incrementalRows.contains("4.d"), "Should contain only the row from the append snapshot");

      spark.sql("DROP TABLE openhouse." + name);
    }
  }
}

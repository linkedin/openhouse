package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Pin tests (real embedded OpenHouse server + real Spark SQL) documenting that RTAS ({@code CREATE
 * OR REPLACE TABLE ... AS SELECT}) is intentionally allowed to change the partition spec and drop
 * columns — evolutions that the incremental {@code ALTER TABLE} path forbids. RTAS defines a brand
 * new table body, so these are by design; the tests guard against a future change that would
 * incorrectly apply the update-path schema/partition guards to the replace path.
 */
public class RtasSchemaEvolutionPinTest extends OpenHouseSparkITest {

  @Test
  public void testRtasMayDropColumnAndChangePartitionSpec() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasPin.dropAndReSpec";
      spark.sql("DROP TABLE IF EXISTS " + table);
      spark.sql(
          "CREATE TABLE "
              + table
              + " (id bigint, data string, keep string) USING iceberg PARTITIONED BY (id)");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a', 'x'), (2, 'b', 'y')");
      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('replace.enabled'='true')");

      // RTAS that drops the 'data' column and repartitions by 'keep' — both forbidden by ALTER,
      // allowed by RTAS.
      spark.sql(
          "REPLACE TABLE "
              + table
              + " USING iceberg PARTITIONED BY (keep) AS SELECT id, keep FROM "
              + table);

      List<String> columns =
          spark.sql("DESCRIBE TABLE " + table).collectAsList().stream()
              .map(r -> r.getString(0))
              .collect(Collectors.toList());
      assertTrue(columns.contains("id"), "id should remain");
      assertTrue(columns.contains("keep"), "keep should remain");
      assertFalse(columns.contains("data"), "RTAS should have dropped the 'data' column");

      // Partition spec is now on 'keep'.
      List<Row> parts = spark.sql("SELECT * FROM " + table + ".partitions").collectAsList();
      assertEquals(2, parts.size(), "expected two partitions after re-partitioning by keep");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }
}

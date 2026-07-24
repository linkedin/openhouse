package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Black-box behavior tests for RTAS ({@code CREATE OR REPLACE TABLE ... AS SELECT}) driven entirely
 * through Spark SQL against a real embedded OpenHouse server. RTAS defines a brand new table body,
 * so it is intentionally allowed schema/partition evolutions that the incremental {@code ALTER
 * TABLE} path forbids, while still preserving the table's identity. These tests pin that contract
 * so a future change can't silently start applying the update-path guards to the replace path (or
 * lose the table identity across a replace).
 */
public class RtasBehaviorTest extends OpenHouseSparkITest {

  private static void createSeededReplaceEnabledTable(
      SparkSession spark, String table, String schema, String partitionClause) {
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql("CREATE TABLE " + table + " " + schema + " USING iceberg " + partitionClause);
    spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('replace.enabled'='true')");
  }

  private static List<String> columnsOf(SparkSession spark, String table) {
    return spark.sql("DESCRIBE TABLE " + table).collectAsList().stream()
        .map(r -> r.getString(0))
        .collect(Collectors.toList());
  }

  private static Map<String, String> tableProperties(SparkSession spark, String table) {
    return spark.sql("SHOW TBLPROPERTIES " + table).collectAsList().stream()
        .collect(Collectors.toMap(r -> r.getString(0), r -> r.getString(1)));
  }

  @Test
  public void testRtasMayDropColumn() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.dropColumn";
      createSeededReplaceEnabledTable(spark, table, "(id bigint, data string, keep string)", "");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a', 'x'), (2, 'b', 'y')");

      // Dropping a column is forbidden by ALTER TABLE but allowed by RTAS.
      spark.sql("REPLACE TABLE " + table + " USING iceberg AS SELECT id, keep FROM " + table);

      List<String> columns = columnsOf(spark, table);
      assertTrue(columns.contains("id") && columns.contains("keep"), "kept columns should remain");
      assertFalse(columns.contains("data"), "RTAS should have dropped the 'data' column");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasMayAddColumn() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.addColumn";
      createSeededReplaceEnabledTable(spark, table, "(id bigint, data string)", "");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')");

      spark.sql(
          "REPLACE TABLE "
              + table
              + " USING iceberg AS SELECT id, data, CAST(id * 10 AS bigint) AS derived FROM "
              + table);

      assertTrue(columnsOf(spark, table).contains("derived"), "RTAS should have added 'derived'");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasMayChangePartitionSpec() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.changeSpec";
      createSeededReplaceEnabledTable(
          spark, table, "(id bigint, part string)", "PARTITIONED BY (id)");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'x'), (2, 'y')");

      // Repartition by a different column — forbidden by ALTER, allowed by RTAS.
      spark.sql(
          "REPLACE TABLE "
              + table
              + " USING iceberg PARTITIONED BY (part) AS SELECT id, part FROM "
              + table);

      List<Row> parts = spark.sql("SELECT * FROM " + table + ".partitions").collectAsList();
      assertEquals(2, parts.size(), "expected two partitions after re-partitioning by part");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasMayRemovePartitioning() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.removePartitioning";
      createSeededReplaceEnabledTable(
          spark, table, "(id bigint, part string)", "PARTITIONED BY (part)");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'x'), (2, 'y')");

      // Replace with an unpartitioned table body.
      spark.sql("REPLACE TABLE " + table + " USING iceberg AS SELECT id, part FROM " + table);

      List<Row> specFields = spark.sql("SELECT * FROM " + table + ".partitions").collectAsList();
      // An unpartitioned Iceberg table reports a single (empty) partition row.
      assertEquals(1, specFields.size(), "expected an unpartitioned table after RTAS");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasReplacesData() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.replacesData";
      createSeededReplaceEnabledTable(spark, table, "(id bigint, data string)", "");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");

      // Replace the body with a filtered projection; the row set must reflect the SELECT.
      spark.sql(
          "REPLACE TABLE "
              + table
              + " USING iceberg AS SELECT id, data FROM "
              + table
              + " WHERE id <= 2");

      long count = spark.sql("SELECT count(*) FROM " + table).collectAsList().get(0).getLong(0);
      assertEquals(2L, count, "RTAS should have replaced the data with the filtered row set");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasPreservesTableIdentity() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.identity";
      createSeededReplaceEnabledTable(spark, table, "(id bigint, data string)", "");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a')");

      String uuidBefore = tableProperties(spark, table).get("openhouse.tableUUID");
      assertNotNull(uuidBefore, "table should have an openhouse.tableUUID before RTAS");

      spark.sql("REPLACE TABLE " + table + " USING iceberg AS SELECT id, data FROM " + table);

      String uuidAfter = tableProperties(spark, table).get("openhouse.tableUUID");
      assertEquals(
          uuidBefore, uuidAfter, "RTAS must preserve the table identity (openhouse.tableUUID)");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasRejectedWhenReplaceNotEnabled() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.replaceDisabled";
      spark.sql("DROP TABLE IF EXISTS " + table);
      // Create WITHOUT opting into RTAS.
      spark.sql("CREATE TABLE " + table + " (id bigint, data string) USING iceberg");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a')");

      Exception e =
          org.junit.jupiter.api.Assertions.assertThrows(
              Exception.class,
              () ->
                  spark.sql(
                      "REPLACE TABLE "
                          + table
                          + " USING iceberg AS SELECT id, data FROM "
                          + table));
      assertTrue(
          e.getMessage() != null
              && e.getMessage().contains("REPLACE TABLE AS SELECT is not enabled"),
          "expected an RTAS-disabled error but got: " + e.getMessage());

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }
}

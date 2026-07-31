package com.linkedin.openhouse.spark.statementtest;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OptimizeStatementTest {

  private static SparkSession spark = null;

  private Map<String, String> optimize(String sql) {
    Map<String, String> metrics = new HashMap<>();
    for (Row r : spark.sql(sql).collectAsList()) {
      metrics.put(r.getString(0), r.getString(1));
    }
    return metrics;
  }

  private long rowCount(String table) {
    return spark.sql("SELECT * FROM " + table).count();
  }

  private String tableProperty(String table, String key) {
    List<Row> rows = spark.sql("SHOW TBLPROPERTIES " + table + " ('" + key + "')").collectAsList();
    return rows.isEmpty() ? null : rows.get(0).getString(1);
  }

  @Test
  public void testOptimizeBinPackReturnsMetricsAndPreservesRows() {
    Map<String, String> m = optimize("OPTIMIZE openhouse.db.table");
    // Output surface: the four reduction metrics.
    Assertions.assertTrue(m.containsKey("files_before"));
    Assertions.assertTrue(m.containsKey("files_after"));
    Assertions.assertTrue(m.containsKey("files_removed"));
    Assertions.assertTrue(m.containsKey("snapshots_committed"));
    // Bin-pack compaction never loses or duplicates rows.
    Assertions.assertEquals(6, rowCount("openhouse.db.table"));
    // Six single-row files are above the default min-input-files, so they compact down.
    Assertions.assertTrue(
        Long.parseLong(m.get("files_after")) < Long.parseLong(m.get("files_before")));
  }

  @Test
  public void testOptimizeClusteringWritesDurableStateAndPreservesRows() {
    spark
        .sql(
            "ALTER TABLE openhouse.db.table SET TBLPROPERTIES ("
                + "'optimize.cluster.keys' = 'id', "
                + "'optimize.cluster.sort-mode' = 'sort', "
                + "'optimize.cluster.min-snapshot-age-minutes' = '0')")
        .show();

    Map<String, String> m = optimize("OPTIMIZE openhouse.db.table");
    Assertions.assertEquals(6, rowCount("openhouse.db.table"));
    // A clustered run committed a rewrite snapshot and advanced the durable metadata.
    Assertions.assertTrue(Long.parseLong(m.get("snapshots_committed")) >= 1);
    String state = tableProperty("openhouse.db.table", "optimize.cluster.state");
    Assertions.assertNotNull(state);
    Assertions.assertTrue(state.trim().startsWith("["));
    Assertions.assertNotNull(
        tableProperty("openhouse.db.table", "optimize.cluster.hwm-snapshot-id"));
  }

  @Test
  public void testOptimizeClusteringIncrementalSecondRunIsNoOp() {
    spark
        .sql(
            "ALTER TABLE openhouse.db.table SET TBLPROPERTIES ("
                + "'optimize.cluster.keys' = 'id', "
                + "'optimize.cluster.sort-mode' = 'sort', "
                + "'optimize.cluster.min-snapshot-age-minutes' = '0')")
        .show();

    optimize("OPTIMIZE openhouse.db.table");
    // No new data arrived, so the incremental run finds nothing above the last-clustered upper.
    Map<String, String> second = optimize("OPTIMIZE openhouse.db.table");
    Assertions.assertEquals("0", second.get("snapshots_committed"));
    Assertions.assertEquals(6, rowCount("openhouse.db.table"));
  }

  @Test
  public void testOptimizeRewriteManifestsPreservesRows() {
    Map<String, String> m = optimize("OPTIMIZE openhouse.db.table REWRITE MANIFESTS");
    Assertions.assertTrue(m.containsKey("files_after"));
    Assertions.assertEquals(6, rowCount("openhouse.db.table"));
  }

  @Test
  public void testOptimizeFullRewriteManifestsParsesAndRuns() {
    Map<String, String> m = optimize("OPTIMIZE openhouse.db.table FULL REWRITE MANIFESTS");
    Assertions.assertEquals(6, rowCount("openhouse.db.table"));
    Assertions.assertTrue(m.containsKey("files_removed"));
  }

  @Test
  public void testOptimizeNonOpenhouseTableThrows() {
    Assertions.assertThrows(
        Exception.class, () -> spark.sql("OPTIMIZE openhouse.db.not_openhouse").collect());
  }

  @Test
  public void testOptimizeCompactsMergeOnReadDeletesAndKeepsRowsCorrect() {
    // A merge-on-read table: the DELETE writes position delete files rather than rewriting data.
    spark
        .sql(
            "CREATE TABLE openhouse.db.mor (id bigint, data string, `openhouse.tableId` string) "
                + "USING iceberg TBLPROPERTIES ("
                + "'openhouse.tableId' = 'tableid', 'format-version' = '2', "
                + "'write.delete.mode' = 'merge-on-read')")
        .show();
    for (int i = 1; i <= 6; i++) {
      spark.sql("INSERT INTO openhouse.db.mor VALUES (" + i + ", 'd" + i + "', 'tableid')").show();
    }
    spark.sql("DELETE FROM openhouse.db.mor WHERE id = 3").show();
    Assertions.assertEquals(5, rowCount("openhouse.db.mor"));

    // OPTIMIZE rewrites data and then rewrite_position_delete_files cleans up the deletes made
    // dangling by that rewrite. The visible rows must be byte-for-byte correct across the rewrite.
    Map<String, String> m = optimize("OPTIMIZE openhouse.db.mor");
    Assertions.assertTrue(m.containsKey("files_after"));
    Assertions.assertEquals(5, rowCount("openhouse.db.mor"));
    Assertions.assertEquals(0, spark.sql("SELECT * FROM openhouse.db.mor WHERE id = 3").count());
  }

  @SneakyThrows
  @BeforeAll
  public void setupSpark() {
    Path unittest = new Path(Files.createTempDirectory("unittest").toString());
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                ("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    + "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions"))
            .config("spark.sql.catalog.openhouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.openhouse.type", "hadoop")
            .config("spark.sql.catalog.openhouse.warehouse", unittest.toString())
            .getOrCreate();
  }

  @BeforeEach
  public void setup() {
    spark
        .sql(
            "CREATE TABLE openhouse.db.table (id bigint, data string, `openhouse.tableId` string) USING iceberg")
        .show();
    spark
        .sql("ALTER TABLE openhouse.db.table SET TBLPROPERTIES ('openhouse.tableId' = 'tableid')")
        .show();
    for (int i = 1; i <= 6; i++) {
      spark
          .sql("INSERT INTO openhouse.db.table VALUES (" + i + ", 'd" + i + "', 'tableid')")
          .show();
    }
    spark
        .sql("CREATE TABLE openhouse.db.not_openhouse (id bigint, data string) USING iceberg")
        .show();
  }

  @AfterEach
  public void tearDown() {
    spark.sql("DROP TABLE IF EXISTS openhouse.db.table").show();
    spark.sql("DROP TABLE IF EXISTS openhouse.db.mor").show();
    spark.sql("DROP TABLE IF EXISTS openhouse.db.not_openhouse").show();
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }
}

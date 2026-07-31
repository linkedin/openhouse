package com.linkedin.openhouse.spark.statementtest;

import java.nio.file.Files;
import java.util.HashMap;
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
public class AnalyzeClusteringQualityStatementTest {

  private static SparkSession spark = null;

  /** Collapse the (metric, dimension, value) output into metric[/dimension] -> value. */
  private Map<String, String> analyze(String table) {
    Map<String, String> out = new HashMap<>();
    for (Row r :
        spark.sql("ANALYZE TABLE " + table + " COMPUTE CLUSTERING QUALITY").collectAsList()) {
      String metric = r.getString(0);
      String dimension = r.isNullAt(1) ? null : r.getString(1);
      out.put(dimension == null ? metric : metric + "/" + dimension, r.getString(2));
    }
    return out;
  }

  @Test
  public void testAnalyzeUnconfiguredReportsNotConfigured() {
    Map<String, String> m = analyze("openhouse.db.table");
    Assertions.assertEquals("false", m.get("clustering_configured"));
    // A not-configured table reports only the single flag row.
    Assertions.assertEquals(1, m.size());
  }

  @Test
  public void testAnalyzeAfterOptimizeReportsCoverageAndDepth() {
    spark
        .sql(
            "ALTER TABLE openhouse.db.table SET TBLPROPERTIES ("
                + "'optimize.cluster.keys' = 'id', "
                + "'optimize.cluster.sort-mode' = 'sort', "
                + "'optimize.cluster.min-snapshot-age-minutes' = '0')")
        .show();
    spark.sql("OPTIMIZE openhouse.db.table").collect();

    Map<String, String> m = analyze("openhouse.db.table");
    Assertions.assertEquals("true", m.get("clustering_configured"));
    Assertions.assertEquals("id", m.get("keys"));
    Assertions.assertEquals("sort", m.get("sort_mode"));
    Assertions.assertNotNull(m.get("config_id"));
    // After a full-scope OPTIMIZE, all bytes fall inside the clustered interval.
    Assertions.assertEquals("100.00", m.get("coverage_bytes_pct"));
    // Per-key depth rows are emitted for the leading key.
    Assertions.assertNotNull(m.get("depth_avg/id"));
    Assertions.assertNotNull(m.get("depth_max/id"));
    Assertions.assertNotNull(m.get("depth_avg_covered/id"));
    // The persisted interval state is echoed back.
    Assertions.assertTrue(m.get("state").trim().startsWith("["));
  }

  @Test
  public void testAnalyzeIsReadOnly() {
    spark
        .sql(
            "ALTER TABLE openhouse.db.table SET TBLPROPERTIES ("
                + "'optimize.cluster.keys' = 'id', "
                + "'optimize.cluster.min-snapshot-age-minutes' = '0')")
        .show();
    long snapshotsBefore = spark.sql("SELECT * FROM openhouse.db.table.snapshots").count();
    analyze("openhouse.db.table");
    long snapshotsAfter = spark.sql("SELECT * FROM openhouse.db.table.snapshots").count();
    // A read-only probe commits nothing.
    Assertions.assertEquals(snapshotsBefore, snapshotsAfter);
  }

  @Test
  public void testAnalyzeComputeStatisticsStillDelegatesToSpark() {
    // COMPUTE STATISTICS must NOT be intercepted by the OpenHouse grammar; it is handled by Spark.
    // For a v2 Iceberg table Spark rejects it with its own AnalysisException -- crucially NOT an
    // OpenhouseParseException -- which confirms the extension did not claim the statement.
    Exception e =
        Assertions.assertThrows(
            Exception.class,
            () -> spark.sql("ANALYZE TABLE openhouse.db.table COMPUTE STATISTICS").collect());
    Assertions.assertFalse(
        e
            instanceof
            com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException,
        "COMPUTE STATISTICS must delegate to Spark, not the OpenHouse parser");
  }

  @Test
  public void testAnalyzeNonOpenhouseTableThrows() {
    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql("ANALYZE TABLE openhouse.db.not_openhouse COMPUTE CLUSTERING QUALITY")
                .collect());
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
    spark.sql("DROP TABLE IF EXISTS openhouse.db.not_openhouse").show();
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }
}

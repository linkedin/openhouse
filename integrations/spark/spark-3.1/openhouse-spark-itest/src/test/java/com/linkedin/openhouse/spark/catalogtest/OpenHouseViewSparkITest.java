package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Cross-version parity suite for OpenHouse view support, run UNCHANGED in both the Spark 3.1
 * (Iceberg 1.2, table-only catalog) and Spark 3.5 (Iceberg 1.5, view-capable catalog) itest
 * modules. It physically lives in the spark-3.1 itest source tree; the spark-3.5 itest {@code
 * build.gradle} compiles the spark-3.1 test {@code srcDirs} into its own suite, so a single file
 * exercises both runtimes.
 *
 * <p>Views are left DISABLED (the default). The goal is to prove that turning the Iceberg 1.5
 * catalog into an Iceberg {@code ViewCatalog} does not change any observable behavior versus the
 * Iceberg 1.2 table-only catalog: table operations return identical rows/listings, and view
 * operations are rejected with Spark's public {@link AnalysisException} rather than leaking a raw
 * runtime exception. Assertions are made against {@link AnalysisException} (stable across Spark
 * 3.1/3.5) and never against Iceberg-internal exception types (which differ between 1.2 and 1.5).
 *
 * <p>The enabled view path (buildView -> loadView round-trip) cannot run on Spark 3.1 (its Iceberg
 * 1.2 catalog is not a {@code ViewCatalog}), so it is covered by a separate Spark-3.5-only test.
 */
public class OpenHouseViewSparkITest extends OpenHouseSparkITest {

  private static final String DB = "viewparity_db";

  /** Table create/read/list/drop must return identical rows and listings in both runtimes. */
  @Test
  public void testTableCreateReadListDropIdenticalOutcome() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse." + DB + ".t_parity (id INT, name STRING)");
      spark.sql("INSERT INTO openhouse." + DB + ".t_parity VALUES (1, 'a'), (2, 'b')");

      List<Row> rows =
          spark
              .sql("SELECT id, name FROM openhouse." + DB + ".t_parity ORDER BY id")
              .collectAsList();
      assertEquals(2, rows.size());
      assertEquals(1, rows.get(0).getInt(0));
      assertEquals("a", rows.get(0).getString(1));
      assertEquals(2, rows.get(1).getInt(0));

      List<String> tables =
          spark.sql("SHOW TABLES IN openhouse." + DB).collectAsList().stream()
              .map(r -> r.getString(1))
              .collect(Collectors.toList());
      assertTrue(tables.contains("t_parity"));

      spark.sql("DROP TABLE openhouse." + DB + ".t_parity");
      assertThrows(
          AnalysisException.class,
          () -> spark.sql("SELECT * FROM openhouse." + DB + ".t_parity").collectAsList());
    }
  }

  /**
   * Reading a non-existent relation must surface Spark's {@link AnalysisException} in both runtimes.
   * This is the case the loadView gate fix guards: the Iceberg 1.5 catalog must NOT leak {@code
   * UnsupportedOperationException} while Spark probes loadView during identifier resolution.
   */
  @Test
  public void testReadMissingRelationThrowsAnalysisException() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      assertThrows(
          AnalysisException.class,
          () -> spark.sql("SELECT * FROM openhouse." + DB + ".does_not_exist").collectAsList());
    }
  }

  /** CREATE VIEW must be rejected as an analysis error, not a raw runtime exception, in both. */
  @Test
  public void testCreateViewRejectedIdentically() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse." + DB + ".v_base (id INT)");
      assertThrows(
          AnalysisException.class,
          () ->
              spark.sql(
                  "CREATE VIEW openhouse."
                      + DB
                      + ".v_create AS SELECT * FROM openhouse."
                      + DB
                      + ".v_base"));
      spark.sql("DROP TABLE openhouse." + DB + ".v_base");
    }
  }

  /** ALTER VIEW must be rejected as an analysis error in both. */
  @Test
  public void testAlterViewRejectedIdentically() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      assertThrows(
          AnalysisException.class,
          () -> spark.sql("ALTER VIEW openhouse." + DB + ".v_missing AS SELECT 1 AS c"));
    }
  }

  /** DROP VIEW on a non-existent view must be rejected as an analysis error in both. */
  @Test
  public void testDropMissingViewRejectedIdentically() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      assertThrows(
          AnalysisException.class,
          () -> spark.sql("DROP VIEW openhouse." + DB + ".v_missing"));
    }
  }

  /**
   * SHOW VIEWS must present the same observable outcome: no OpenHouse-managed views and no raw
   * (non-analysis) leak. Spark 3.1 (no ViewCatalog) and Spark 3.5 (views disabled -> empty
   * listViews) may express this as either an {@link AnalysisException} or an empty result; both are
   * accepted, but a raw runtime leak is not.
   */
  @Test
  public void testShowViewsSameObservableOutcome() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Throwable thrown = null;
      List<Row> views = null;
      try {
        views = spark.sql("SHOW VIEWS IN openhouse." + DB).collectAsList();
      } catch (Exception e) {
        thrown = e;
      }
      if (thrown != null) {
        // Acceptable: Spark rejects SHOW VIEWS on this catalog as an analysis error, but it must
        // not leak a raw runtime exception from the catalog.
        assertTrue(
            thrown instanceof AnalysisException,
            "SHOW VIEWS must not leak a raw runtime exception; got " + thrown);
      } else {
        assertTrue(views.isEmpty(), "Expected no OpenHouse-managed views when views are disabled");
      }
    }
  }
}

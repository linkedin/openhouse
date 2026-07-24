package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The RTAS DML contract: once a table exists, ordinary DML must behave the same regardless of how
 * that table was produced. The "thing being tested" is a (preparation, DML operation) pair; the two
 * are independent, so the real matrix is every preparation crossed with every DML operation.
 *
 * <p>The DML operations are written as <em>composable</em> checks: each one asserts its own effect
 * relative to the current table state (a count delta, a row it just wrote), rather than an absolute
 * post-preparation snapshot. That makes them order-independent and repeatable — {@code
 * insertCheck(t); insertCheck(t)} both pass — so a single prepared table can be handed through the
 * whole sequence of checks, which is exactly what {@link #dmlComposesOnPreparedTable} does for each
 * of the preparations.
 *
 * <p>A preparation is itself composed from small steps ({@link #createSeeded}, {@link #rtas},
 * {@link #restore}); a {@link Lifecycle} is just a particular composition of them. Every
 * preparation converges to the same canonical seed {@code (1,'a'),(2,'b'),(3,'c')}.
 *
 * <p>Everything runs through Spark SQL against a real embedded OpenHouse server. Reading a
 * merge-on-read table back (which the checks do) is why this module excludes {@code iceberg-data}
 * from the test classpath; the dedicated merge-on-read tests further assert that the delete-file
 * path is actually taken.
 */
public class RtasDmlMatrixTest extends OpenHouseSparkITest {

  private static final String DB = "openhouse.dbRtasDml";
  private static final String SEED_VALUES = "(1, 'a'), (2, 'b'), (3, 'c')";
  private static final String JUNK_VALUES = "(97, 'x'), (98, 'y'), (99, 'z')";
  private static final String[] SEED_ROWS = {"1:a", "2:b", "3:c"};

  // Gives every prepared table a unique name so preparations never collide.
  private static final AtomicLong TABLE_SEQ = new AtomicLong();

  private SparkSession spark;

  @BeforeEach
  void setUp() throws Exception {
    // getOrCreate returns the shared active session; do NOT close it between cases.
    spark = getSparkSession();
  }

  /** Copy-on-write vs merge-on-read. Merge-on-read modifies via position delete files. */
  enum WriteMode {
    COW,
    MOR;

    boolean isMergeOnRead() {
      return this == MOR;
    }
  }

  /** Physical write format the table's data (and delete) files are produced in. */
  enum FileFormat {
    PARQUET,
    ORC;

    String property() {
      return name().toLowerCase();
    }
  }

  /** Whether the table is partitioned; PARTITIONED BY (data) puts each seed row in its own file. */
  enum Partitioning {
    UNPARTITIONED(""),
    PARTITIONED("PARTITIONED BY (data)");

    final String clause;

    Partitioning(String clause) {
      this.clause = clause;
    }
  }

  /**
   * How the table under test came to exist, expressed as a composition of preparation steps. Each
   * constant leaves the table holding exactly the canonical seed rows.
   */
  enum Lifecycle {
    /** A plain table, seeded directly. */
    BASE {
      @Override
      void prepare(SparkSession spark, String table, String using, String props) {
        createSeeded(spark, table, using, props);
      }
    },
    /** Seeded, then replaced via RTAS with the same seed. */
    RTAS {
      @Override
      void prepare(SparkSession spark, String table, String using, String props) {
        createSeeded(spark, table, using, props);
        rtas(spark, table, using, props, SEED_VALUES);
      }
    },
    /** Seeded, replaced via RTAS with junk, then restored to the pre-replace (seed) snapshot. */
    RTAS_RESTORE {
      @Override
      void prepare(SparkSession spark, String table, String using, String props) {
        createSeeded(spark, table, using, props);
        long seedSnapshot = latestSnapshot(spark, table);
        rtas(spark, table, using, props, JUNK_VALUES);
        restore(spark, table, seedSnapshot);
      }
    };

    abstract void prepare(SparkSession spark, String table, String using, String props);
  }

  /** Every (write mode, file format, partitioning, lifecycle) preparation. */
  static Stream<Arguments> preparations() {
    return Arrays.stream(WriteMode.values())
        .flatMap(
            writeMode ->
                Arrays.stream(FileFormat.values())
                    .flatMap(
                        format ->
                            Arrays.stream(Partitioning.values())
                                .flatMap(
                                    partitioning ->
                                        Arrays.stream(Lifecycle.values())
                                            .map(
                                                lifecycle ->
                                                    Arguments.of(
                                                        writeMode,
                                                        format,
                                                        partitioning,
                                                        lifecycle)))));
  }

  @ParameterizedTest(name = "{0}-{1}-{2}-{3}")
  @MethodSource("preparations")
  public void dmlComposesOnPreparedTable(
      WriteMode writeMode, FileFormat format, Partitioning partitioning, Lifecycle lifecycle) {
    String table = DB + ".t" + TABLE_SEQ.incrementAndGet();
    try {
      prepare(table, writeMode, format, partitioning, lifecycle);
      assertRows(table, SEED_ROWS);

      // The checks compose: each asserts its own effect, so they can run in sequence against the
      // one prepared table, and a check can even run twice in a row.
      readCheck(table);
      insertCheck(table);
      insertCheck(table);
      deleteCheck(table);
      updateCheck(table);
      mergeCheck(table);
      insertOverwriteCheck(table);
      readCheck(table);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  // ----- composable DML checks: each asserts its own effect on the current table state -----

  private void readCheck(String table) {
    long scanned = spark.sql("SELECT id, data FROM " + table).collectAsList().size();
    assertEquals(count(table), scanned, "a full scan should read back every row of " + table);
  }

  private void insertCheck(String table) {
    long before = count(table);
    spark.sql("INSERT INTO " + table + " VALUES (1001, 'ins')");
    assertEquals(before + 1, count(table), "insert should add exactly one row");
    assertTrue(hasRow(table, 1001, "ins"), "the inserted row should be present");
  }

  private void deleteCheck(String table) {
    spark.sql("INSERT INTO " + table + " VALUES (1002, 'del')");
    long before = count(table);
    spark.sql("DELETE FROM " + table + " WHERE id = 1002");
    assertEquals(before - 1, count(table), "delete should remove exactly the matched row");
    assertFalse(hasRow(table, 1002, "del"), "the deleted row should be gone");
  }

  private void updateCheck(String table) {
    spark.sql("INSERT INTO " + table + " VALUES (1003, 'orig')");
    spark.sql("UPDATE " + table + " SET data = 'upd' WHERE id = 1003");
    assertTrue(hasRow(table, 1003, "upd"), "the updated value should be present");
    assertFalse(hasRow(table, 1003, "orig"), "the old value should be gone");
  }

  private void mergeCheck(String table) {
    spark.sql(
        "MERGE INTO "
            + table
            + " t USING (SELECT 1004 AS id, 'mrg' AS data) s ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET t.data = s.data "
            + "WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)");
    assertTrue(hasRow(table, 1004, "mrg"), "the merged row should be present");
  }

  private void insertOverwriteCheck(String table) {
    // Force whole-table overwrite semantics so the assertion is partitioning-independent.
    spark.conf().set("spark.sql.sources.partitionOverwriteMode", "STATIC");
    try {
      spark.sql("INSERT OVERWRITE " + table + " VALUES (1005, 'ovr')");
    } finally {
      spark.conf().unset("spark.sql.sources.partitionOverwriteMode");
    }
    assertRows(table, "1005:ovr");
  }

  // ----- merge-on-read delete-file production (needs a partial-file delete of shared seed rows)
  // ---

  /**
   * The unpartitioned merge-on-read preparations. Here the seed rows share a data file, so deleting
   * one of them writes a position delete file. (A partitioned-by-data table puts each row in its
   * own file, so a single-row delete is a whole-file metadata delete with no position delete —
   * correct Iceberg behavior — which is why those are excluded.)
   */
  static Stream<Arguments> mergeOnReadUnpartitioned() {
    return Arrays.stream(FileFormat.values())
        .flatMap(
            format ->
                Arrays.stream(Lifecycle.values())
                    .map(lifecycle -> Arguments.of(format, lifecycle)));
  }

  @ParameterizedTest(name = "{0}-{1}")
  @MethodSource("mergeOnReadUnpartitioned")
  public void mergeOnReadDeleteWritesDeleteFiles(FileFormat format, Lifecycle lifecycle) {
    runOnPreparedTable(
        WriteMode.MOR,
        format,
        Partitioning.UNPARTITIONED,
        lifecycle,
        table -> {
          spark.sql("DELETE FROM " + table + " WHERE id = 2");
          assertRows(table, "1:a", "3:c");
          assertHasDeleteFiles(table);
        });
  }

  @ParameterizedTest(name = "{0}-{1}")
  @MethodSource("mergeOnReadUnpartitioned")
  public void mergeOnReadUpdateWritesDeleteFiles(FileFormat format, Lifecycle lifecycle) {
    runOnPreparedTable(
        WriteMode.MOR,
        format,
        Partitioning.UNPARTITIONED,
        lifecycle,
        table -> {
          spark.sql("UPDATE " + table + " SET data = 'updated' WHERE id = 2");
          assertRows(table, "1:a", "2:updated", "3:c");
          assertHasDeleteFiles(table);
        });
  }

  @ParameterizedTest(name = "{0}-{1}")
  @MethodSource("mergeOnReadUnpartitioned")
  public void mergeOnReadMergeWritesDeleteFiles(FileFormat format, Lifecycle lifecycle) {
    runOnPreparedTable(
        WriteMode.MOR,
        format,
        Partitioning.UNPARTITIONED,
        lifecycle,
        table -> {
          spark.sql(
              "MERGE INTO "
                  + table
                  + " t "
                  + "USING (SELECT * FROM VALUES (2, 'merged'), (9, 'new') AS s(id, data)) s "
                  + "ON t.id = s.id "
                  + "WHEN MATCHED THEN UPDATE SET t.data = s.data "
                  + "WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)");
          assertRows(table, "1:a", "2:merged", "3:c", "9:new");
          assertHasDeleteFiles(table);
        });
  }

  // ----- preparation steps and small query/assertion helpers -----

  private void prepare(
      String table,
      WriteMode writeMode,
      FileFormat format,
      Partitioning partitioning,
      Lifecycle lifecycle) {
    String using = "USING iceberg " + partitioning.clause;
    String props = "'write.format.default'='" + format.property() + "', 'replace.enabled'='true'";
    if (writeMode.isMergeOnRead()) {
      props +=
          ", 'format-version'='2', 'write.delete.mode'='merge-on-read', "
              + "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read'";
    }
    spark.sql("DROP TABLE IF EXISTS " + table);
    lifecycle.prepare(spark, table, using, props);
  }

  private void runOnPreparedTable(
      WriteMode writeMode,
      FileFormat format,
      Partitioning partitioning,
      Lifecycle lifecycle,
      java.util.function.Consumer<String> body) {
    String table = DB + ".t" + TABLE_SEQ.incrementAndGet();
    try {
      prepare(table, writeMode, format, partitioning, lifecycle);
      body.accept(table);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  private static void createSeeded(SparkSession spark, String table, String using, String props) {
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id int, data string) "
            + using
            + " TBLPROPERTIES ("
            + props
            + ")");
    spark.sql("INSERT INTO " + table + " VALUES " + SEED_VALUES);
  }

  private static void rtas(
      SparkSession spark, String table, String using, String props, String values) {
    spark.sql(
        "REPLACE TABLE "
            + table
            + " "
            + using
            + " TBLPROPERTIES ("
            + props
            + ") "
            + "AS SELECT * FROM VALUES "
            + values
            + " AS s(id, data)");
  }

  private static long latestSnapshot(SparkSession spark, String table) {
    return spark
        .sql("SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1")
        .collectAsList()
        .get(0)
        .getLong(0);
  }

  private static void restore(SparkSession spark, String table, long snapshotId) {
    spark.sql(
        "CALL openhouse.system.set_current_snapshot(table => '"
            + table.substring(table.indexOf('.') + 1)
            + "', snapshot_id => "
            + snapshotId
            + ")");
  }

  private long count(String table) {
    return spark.sql("SELECT count(*) FROM " + table).collectAsList().get(0).getLong(0);
  }

  private boolean hasRow(String table, int id, String data) {
    return !spark
        .sql("SELECT 1 FROM " + table + " WHERE id = " + id + " AND data = '" + data + "'")
        .collectAsList()
        .isEmpty();
  }

  /**
   * Asserts the table holds exactly the given rows, encoded as {@code "id:data"},
   * order-independent.
   */
  private void assertRows(String table, String... expected) {
    List<String> actual =
        spark.sql("SELECT id, data FROM " + table).collectAsList().stream()
            .map(row -> row.getInt(0) + ":" + row.getString(1))
            .sorted()
            .collect(Collectors.toList());
    List<String> want = Arrays.stream(expected).sorted().collect(Collectors.toList());
    assertEquals(want, actual, "row set mismatch for " + table);
  }

  private void assertHasDeleteFiles(String table) {
    long deleteFiles =
        spark
            .sql("SELECT count(*) FROM " + table + ".delete_files")
            .collectAsList()
            .get(0)
            .getLong(0);
    assertTrue(
        deleteFiles > 0,
        "merge-on-read DML on " + table + " should have produced position delete files");
  }
}

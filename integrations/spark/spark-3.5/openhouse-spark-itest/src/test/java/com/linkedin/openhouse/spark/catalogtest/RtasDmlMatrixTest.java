package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The RTAS DML contract: once a table exists, ordinary DML must behave the same regardless of how
 * that table was produced. We build a table for every combination of the properties that matter —
 * write mode (copy-on-write / merge-on-read), file format (Parquet / ORC), partitioning, and
 * lifecycle (a plain table, a table replaced via RTAS, and a table replaced then restored to its
 * pre-replace snapshot) — and each DML operation is its own test that runs against every one of
 * those preparations.
 *
 * <p>Every preparation deterministically converges to the SAME canonical seed rows {@code
 * (1,'a'),(2,'b'),(3,'c')}, so each operation has a single expected outcome that is valid for all
 * preparations: the whole point is that the preparation must not leak into DML behavior. Each
 * (preparation, operation) pair is an independent, real JUnit case driven entirely through Spark
 * SQL against a real embedded OpenHouse server. {@link #withTable} gives each table a scoped
 * lifetime.
 *
 * <p>The merge-on-read delete-file tests are separate and are constructed specifically for the case
 * that produces position delete files (unpartitioned merge-on-read); they also exercise the ORC
 * read-back that is why this module excludes {@code iceberg-data} from the test classpath.
 */
public class RtasDmlMatrixTest extends OpenHouseSparkITest {

  private static final String DB = "openhouse.dbRtasDml";
  private static final String SEED_VALUES = "(1, 'a'), (2, 'b'), (3, 'c')";
  private static final String JUNK_VALUES = "(97, 'x'), (98, 'y'), (99, 'z')";
  private static final String[] SEED_ROWS = {"1:a", "2:b", "3:c"};

  // Gives every prepared table a unique name so cases never collide.
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
   * How the table under test came to exist. Each constant builds a table and leaves it holding
   * exactly the canonical seed rows, so the DML assertions are identical across all three.
   */
  enum Lifecycle {
    /** A plain table, seeded directly. */
    BASE {
      @Override
      void prepare(SparkSession spark, String table, String using, String props) {
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
    },
    /** Created with a junk body, then replaced via RTAS with the canonical seed. */
    RTAS {
      @Override
      void prepare(SparkSession spark, String table, String using, String props) {
        spark.sql(
            "CREATE TABLE "
                + table
                + " (id int, data string) "
                + using
                + " TBLPROPERTIES ("
                + props
                + ")");
        spark.sql("INSERT INTO " + table + " VALUES " + JUNK_VALUES);
        spark.sql(
            "REPLACE TABLE "
                + table
                + " "
                + using
                + " TBLPROPERTIES ("
                + props
                + ") "
                + "AS SELECT * FROM VALUES "
                + SEED_VALUES
                + " AS s(id, data)");
      }
    },
    /**
     * Seeded, replaced via RTAS with junk, then restored to the pre-replace snapshot — which brings
     * back the seed and drops the junk body.
     */
    RTAS_RESTORE {
      @Override
      void prepare(SparkSession spark, String table, String using, String props) {
        spark.sql(
            "CREATE TABLE "
                + table
                + " (id int, data string) "
                + using
                + " TBLPROPERTIES ("
                + props
                + ")");
        spark.sql("INSERT INTO " + table + " VALUES " + SEED_VALUES);
        long seedSnapshot =
            spark
                .sql(
                    "SELECT snapshot_id FROM "
                        + table
                        + ".snapshots ORDER BY committed_at DESC LIMIT 1")
                .collectAsList()
                .get(0)
                .getLong(0);
        spark.sql(
            "REPLACE TABLE "
                + table
                + " "
                + using
                + " TBLPROPERTIES ("
                + props
                + ") "
                + "AS SELECT * FROM VALUES "
                + JUNK_VALUES
                + " AS s(id, data)");
        spark.sql(
            "CALL openhouse.system.set_current_snapshot(table => '"
                + table.substring(table.indexOf('.') + 1)
                + "', snapshot_id => "
                + seedSnapshot
                + ")");
      }
    };

    abstract void prepare(SparkSession spark, String table, String using, String props);
  }

  /** Every (write mode, file format, partitioning, lifecycle) preparation, one JUnit case each. */
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
  public void read(WriteMode writeMode, FileFormat format, Partitioning part, Lifecycle lifecycle) {
    withTable(writeMode, format, part, lifecycle, table -> assertRows(table, SEED_ROWS));
  }

  @ParameterizedTest(name = "{0}-{1}-{2}-{3}")
  @MethodSource("preparations")
  public void insert(
      WriteMode writeMode, FileFormat format, Partitioning part, Lifecycle lifecycle) {
    withTable(
        writeMode,
        format,
        part,
        lifecycle,
        table -> {
          spark.sql("INSERT INTO " + table + " VALUES (4, 'd')");
          assertRows(table, "1:a", "2:b", "3:c", "4:d");
        });
  }

  @ParameterizedTest(name = "{0}-{1}-{2}-{3}")
  @MethodSource("preparations")
  public void delete(
      WriteMode writeMode, FileFormat format, Partitioning part, Lifecycle lifecycle) {
    withTable(
        writeMode,
        format,
        part,
        lifecycle,
        table -> {
          spark.sql("DELETE FROM " + table + " WHERE id = 2");
          assertRows(table, "1:a", "3:c");
        });
  }

  @ParameterizedTest(name = "{0}-{1}-{2}-{3}")
  @MethodSource("preparations")
  public void update(
      WriteMode writeMode, FileFormat format, Partitioning part, Lifecycle lifecycle) {
    withTable(
        writeMode,
        format,
        part,
        lifecycle,
        table -> {
          spark.sql("UPDATE " + table + " SET data = 'updated' WHERE id = 2");
          assertRows(table, "1:a", "2:updated", "3:c");
        });
  }

  @ParameterizedTest(name = "{0}-{1}-{2}-{3}")
  @MethodSource("preparations")
  public void merge(
      WriteMode writeMode, FileFormat format, Partitioning part, Lifecycle lifecycle) {
    withTable(
        writeMode,
        format,
        part,
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
        });
  }

  @ParameterizedTest(name = "{0}-{1}-{2}-{3}")
  @MethodSource("preparations")
  public void insertOverwrite(
      WriteMode writeMode, FileFormat format, Partitioning part, Lifecycle lifecycle) {
    withTable(
        writeMode,
        format,
        part,
        lifecycle,
        table -> {
          // Force whole-table overwrite semantics so the assertion is partitioning-independent.
          spark.conf().set("spark.sql.sources.partitionOverwriteMode", "STATIC");
          try {
            spark.sql("INSERT OVERWRITE " + table + " VALUES (7, 'only')");
          } finally {
            spark.conf().unset("spark.sql.sources.partitionOverwriteMode");
          }
          assertRows(table, "7:only");
        });
  }

  /**
   * The unpartitioned merge-on-read preparations, where the seed rows share a data file so a
   * single-row modification writes a position delete file. (A partitioned-by-data table puts each
   * row in its own file, so a single-row delete is a whole-file metadata delete with no position
   * delete — correct Iceberg behavior — which is why those are excluded here.)
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
    withTable(
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
    withTable(
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
    withTable(
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

  /**
   * Builds a table for the given axes, hands it to {@code body} with a scoped lifetime, and always
   * drops it afterwards.
   */
  private void withTable(
      WriteMode writeMode,
      FileFormat format,
      Partitioning partitioning,
      Lifecycle lifecycle,
      Consumer<String> body) {
    String table = DB + ".t" + TABLE_SEQ.incrementAndGet();
    String using = "USING iceberg " + partitioning.clause;
    String props = "'write.format.default'='" + format.property() + "', 'replace.enabled'='true'";
    if (writeMode.isMergeOnRead()) {
      props +=
          ", 'format-version'='2', 'write.delete.mode'='merge-on-read', "
              + "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read'";
    }
    spark.sql("DROP TABLE IF EXISTS " + table);
    try {
      lifecycle.prepare(spark, table, using, props);
      body.accept(table);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + table);
    }
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

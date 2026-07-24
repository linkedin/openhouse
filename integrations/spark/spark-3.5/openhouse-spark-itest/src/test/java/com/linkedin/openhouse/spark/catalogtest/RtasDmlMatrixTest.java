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
 * The RTAS DML contract, as a matrix. The requirement is simple to state and broad to cover: once a
 * table exists, ordinary DML must behave the same regardless of how that table was produced. So we
 * multiplex table <em>preparations</em> over the properties that matter and run the same set of DML
 * operations against every one of them.
 *
 * <p>The design mirrors that intent directly:
 *
 * <ul>
 *   <li>Each axis is an enum ({@link WriteMode}, {@link FileFormat}, {@link Partitioning}, {@link
 *       Lifecycle}), and {@link #cases()} is their cartesian product built with streams.
 *   <li>A <em>preparation</em> is a thunk: {@link Lifecycle} knows how to build+seed a table, and
 *       {@link #withTable} gives that table a scoped lifetime (it is always dropped afterwards).
 *   <li>A <em>DML operation</em> is a function: each {@link DmlCheck} performs one operation and
 *       asserts its outcome, so the test body is just "prepare a table, apply the op".
 * </ul>
 *
 * <p>Every preparation deterministically converges to the SAME canonical seed rows {@code
 * (1,'a'),(2,'b'),(3,'c')}, so a single assertion is valid for all of them: the whole point is that
 * the preparation must not leak into DML behavior. Each (preparation, operation) pair is an
 * independent, real JUnit case, driven entirely through Spark SQL against a real embedded OpenHouse
 * server.
 *
 * <p>Merge-on-read cases additionally assert that the delete-producing operations actually took the
 * merge-on-read path (position delete files were written) and that the table still reads back
 * correctly afterwards — the ORC read-back this exercises is why the module excludes {@code
 * iceberg-data} from the test classpath.
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
   * How the table under test came to exist. Each constant is a preparation thunk that builds a
   * table and leaves it holding exactly the canonical seed rows, so the DML assertions are
   * identical across all three: a plain table, a table that was replaced via RTAS, and a table that
   * was replaced and then restored to its pre-replace snapshot.
   */
  enum Lifecycle {
    /** A plain table, seeded directly. */
    BASE {
      @Override
      void prepare(SparkSession spark, String table, String using, String props) {
        createTable(spark, table, using, props);
        insertValues(spark, table, SEED_VALUES);
      }
    },
    /** Created with junk, then replaced via RTAS with the canonical seed. */
    RTAS {
      @Override
      void prepare(SparkSession spark, String table, String using, String props) {
        createTable(spark, table, using, props);
        insertValues(spark, table, JUNK_VALUES);
        replaceAsSelect(spark, table, using, props, SEED_VALUES);
      }
    },
    /**
     * Seeded, replaced via RTAS with junk, then restored to the pre-replace snapshot — which brings
     * back the seed and drops the junk body.
     */
    RTAS_RESTORE {
      @Override
      void prepare(SparkSession spark, String table, String using, String props) {
        createTable(spark, table, using, props);
        insertValues(spark, table, SEED_VALUES);
        long seedSnapshot = latestSnapshotId(spark, table);
        replaceAsSelect(spark, table, using, props, JUNK_VALUES);
        restoreToSnapshot(spark, table, seedSnapshot);
      }
    };

    abstract void prepare(SparkSession spark, String table, String using, String props);
  }

  /**
   * A DML operation plus its expected outcome. {@code expectDeleteFiles} is true only when the
   * operation is expected to leave merge-on-read position delete files behind (see {@link
   * #expectDeleteFiles}).
   */
  enum DmlCheck {
    READ {
      @Override
      void run(SparkSession spark, String table, boolean expectDeleteFiles) {
        assertRows(spark, table, SEED_ROWS);
      }
    },
    INSERT {
      @Override
      void run(SparkSession spark, String table, boolean expectDeleteFiles) {
        spark.sql("INSERT INTO " + table + " VALUES (4, 'd')");
        assertRows(spark, table, "1:a", "2:b", "3:c", "4:d");
      }
    },
    DELETE {
      @Override
      void run(SparkSession spark, String table, boolean expectDeleteFiles) {
        spark.sql("DELETE FROM " + table + " WHERE id = 2");
        assertRows(spark, table, "1:a", "3:c");
        assertDeleteFilesPresent(spark, table, expectDeleteFiles);
      }
    },
    UPDATE {
      @Override
      void run(SparkSession spark, String table, boolean expectDeleteFiles) {
        spark.sql("UPDATE " + table + " SET data = 'updated' WHERE id = 2");
        assertRows(spark, table, "1:a", "2:updated", "3:c");
        assertDeleteFilesPresent(spark, table, expectDeleteFiles);
      }
    },
    MERGE {
      @Override
      void run(SparkSession spark, String table, boolean expectDeleteFiles) {
        spark.sql(
            "MERGE INTO "
                + table
                + " t "
                + "USING (SELECT * FROM VALUES (2, 'merged'), (9, 'new') AS s(id, data)) s "
                + "ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET t.data = s.data "
                + "WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)");
        assertRows(spark, table, "1:a", "2:merged", "3:c", "9:new");
        assertDeleteFilesPresent(spark, table, expectDeleteFiles);
      }
    },
    INSERT_OVERWRITE {
      @Override
      void run(SparkSession spark, String table, boolean expectDeleteFiles) {
        // Force whole-table overwrite semantics so the assertion is partitioning-independent.
        spark.conf().set("spark.sql.sources.partitionOverwriteMode", "STATIC");
        try {
          spark.sql("INSERT OVERWRITE " + table + " VALUES (7, 'only')");
        } finally {
          spark.conf().unset("spark.sql.sources.partitionOverwriteMode");
        }
        assertRows(spark, table, "7:only");
      }
    };

    abstract void run(SparkSession spark, String table, boolean expectDeleteFiles);
  }

  /** The cartesian product of every axis, one JUnit case per combination. */
  static Stream<Arguments> cases() {
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
                                            .flatMap(
                                                lifecycle ->
                                                    Arrays.stream(DmlCheck.values())
                                                        .map(
                                                            op ->
                                                                Arguments.of(
                                                                    writeMode,
                                                                    format,
                                                                    partitioning,
                                                                    lifecycle,
                                                                    op))))));
  }

  @ParameterizedTest(name = "{0}-{1}-{2}-{3} :: {4}")
  @MethodSource("cases")
  public void dml(
      WriteMode writeMode,
      FileFormat format,
      Partitioning partitioning,
      Lifecycle lifecycle,
      DmlCheck op) {
    withTable(
        writeMode,
        format,
        partitioning,
        lifecycle,
        table -> {
          // Sanity: every preparation must converge to the canonical seed before DML runs.
          assertRows(spark, table, SEED_ROWS);
          op.run(spark, table, expectDeleteFiles(writeMode, partitioning));
        });
  }

  /**
   * Builds a table for the given axes, hands it to {@code body} with a scoped lifetime, and always
   * drops it afterwards. The table is created and seeded by the {@link Lifecycle} preparation
   * thunk, so callers never see a half-built or leaked table.
   */
  private void withTable(
      WriteMode writeMode,
      FileFormat format,
      Partitioning partitioning,
      Lifecycle lifecycle,
      Consumer<String> body) {
    String table = DB + ".t" + TABLE_SEQ.incrementAndGet();
    String using = "USING iceberg " + partitioning.clause;
    String props = tableProps(writeMode, format);
    spark.sql("DROP TABLE IF EXISTS " + table);
    try {
      lifecycle.prepare(spark, table, using, props);
      body.accept(table);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  /**
   * A merge-on-read modification writes position delete files only when it deletes a strict subset
   * of a data file's rows. With PARTITIONED BY (data) every seed row lands in its own single-row
   * file, so a single-row delete removes the whole file as a metadata-only delete (no position
   * delete) — correct Iceberg behavior — hence delete files are only expected for the unpartitioned
   * merge-on-read cases, where the seed rows share a data file.
   */
  private static boolean expectDeleteFiles(WriteMode writeMode, Partitioning partitioning) {
    return writeMode.isMergeOnRead() && partitioning == Partitioning.UNPARTITIONED;
  }

  private static void createTable(SparkSession spark, String table, String using, String props) {
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id int, data string) "
            + using
            + " TBLPROPERTIES ("
            + props
            + ")");
  }

  private static void insertValues(SparkSession spark, String table, String values) {
    spark.sql("INSERT INTO " + table + " VALUES " + values);
  }

  private static void replaceAsSelect(
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

  private static void restoreToSnapshot(SparkSession spark, String table, long snapshotId) {
    spark.sql(
        "CALL openhouse.system.set_current_snapshot(table => '"
            + dbTable(table)
            + "', snapshot_id => "
            + snapshotId
            + ")");
  }

  private static long latestSnapshotId(SparkSession spark, String table) {
    return spark
        .sql("SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1")
        .collectAsList()
        .get(0)
        .getLong(0);
  }

  private static String tableProps(WriteMode writeMode, FileFormat format) {
    Stream<String> base =
        Stream.of("'write.format.default'='" + format.property() + "'", "'replace.enabled'='true'");
    Stream<String> mor =
        writeMode.isMergeOnRead()
            ? Stream.of(
                "'format-version'='2'",
                "'write.delete.mode'='merge-on-read'",
                "'write.update.mode'='merge-on-read'",
                "'write.merge.mode'='merge-on-read'")
            : Stream.empty();
    return Stream.concat(base, mor).collect(Collectors.joining(", "));
  }

  /**
   * Asserts the table holds exactly the given rows, encoded as {@code "id:data"},
   * order-independent.
   */
  private static void assertRows(SparkSession spark, String table, String... expected) {
    List<String> actual =
        spark.sql("SELECT id, data FROM " + table).collectAsList().stream()
            .map(row -> row.getInt(0) + ":" + row.getString(1))
            .sorted()
            .collect(Collectors.toList());
    List<String> want = Arrays.stream(expected).sorted().collect(Collectors.toList());
    assertEquals(want, actual, "row set mismatch for " + table);
  }

  private static void assertDeleteFilesPresent(
      SparkSession spark, String table, boolean expectDeleteFiles) {
    if (!expectDeleteFiles) {
      return;
    }
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

  /** The {@code db.table} identifier (the fully-qualified name without the catalog prefix). */
  private static String dbTable(String fullyQualifiedTable) {
    return fullyQualifiedTable.substring(fullyQualifiedTable.indexOf('.') + 1);
  }
}

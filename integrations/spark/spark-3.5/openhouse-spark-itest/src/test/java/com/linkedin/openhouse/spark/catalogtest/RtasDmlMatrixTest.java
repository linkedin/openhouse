package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
 * multiplex table <em>preparations</em> over the properties that matter — copy-on-write vs
 * merge-on-read, Parquet vs ORC, unpartitioned vs partitioned, and lifecycle (a plain table, a
 * table that has been replaced via RTAS, and a table that has been replaced and then restored to a
 * pre-replace snapshot) — and run the same set of DML operations against every one of them.
 *
 * <p>Every preparation deterministically converges to the SAME canonical seed rows {@code
 * (1,'a'),(2,'b'),(3,'c')}, so a single DML assertion is valid for all of them: the whole point is
 * that the preparation must not leak into DML behavior. Each (preparation, operation) pair is an
 * independent, real JUnit case (4 axes x DML operations), driven entirely through Spark SQL against
 * a real embedded OpenHouse server.
 *
 * <p>Merge-on-read cases additionally assert that the delete-producing operations actually took the
 * merge-on-read path (position delete files were written) and that the table still reads back
 * correctly afterwards — the ORC read-back that this exercises is why the module excludes {@code
 * iceberg-data} from the test classpath.
 */
public class RtasDmlMatrixTest extends OpenHouseSparkITest {

  private static final String DB = "openhouse.dbRtasDml";
  private static final String SEED_VALUES = "(1, 'a'), (2, 'b'), (3, 'c')";
  private static final String JUNK_VALUES = "(97, 'x'), (98, 'y'), (99, 'z')";

  private SparkSession spark;

  @BeforeEach
  void setUp() throws Exception {
    // getOrCreate returns the shared active session; do NOT close it between cases.
    spark = getSparkSession();
  }

  enum WriteMode {
    COW,
    MOR;

    boolean isMergeOnRead() {
      return this == MOR;
    }
  }

  enum FileFormat {
    PARQUET,
    ORC;

    String property() {
      return name().toLowerCase();
    }
  }

  enum Partitioning {
    UNPARTITIONED(""),
    PARTITIONED("PARTITIONED BY (data)");

    final String clause;

    Partitioning(String clause) {
      this.clause = clause;
    }
  }

  enum Lifecycle {
    BASE,
    RTAS,
    RTAS_RESTORE;
  }

  enum DmlOp {
    READ,
    INSERT,
    DELETE,
    UPDATE,
    MERGE,
    INSERT_OVERWRITE;
  }

  static Stream<Arguments> cases() {
    List<Arguments> out = new ArrayList<>();
    for (WriteMode writeMode : WriteMode.values()) {
      for (FileFormat format : FileFormat.values()) {
        for (Partitioning partitioning : Partitioning.values()) {
          for (Lifecycle lifecycle : Lifecycle.values()) {
            for (DmlOp op : DmlOp.values()) {
              out.add(Arguments.of(writeMode, format, partitioning, lifecycle, op));
            }
          }
        }
      }
    }
    return out.stream();
  }

  @ParameterizedTest(name = "{0}-{1}-{2}-{3} :: {4}")
  @MethodSource("cases")
  public void dml(
      WriteMode writeMode,
      FileFormat format,
      Partitioning partitioning,
      Lifecycle lifecycle,
      DmlOp op) {
    String table =
        String.format("%s.%s_%s_%s_%s_%s", DB, writeMode, format, partitioning, lifecycle, op)
            .toLowerCase();
    try {
      prepareSeededTable(table, writeMode, format, partitioning, lifecycle);
      // Sanity: every preparation must converge to the canonical seed before DML runs.
      assertRows(table, "1:a", "2:b", "3:c");

      runDml(table, op, writeMode, partitioning);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  /** Builds a table that ends holding exactly the canonical seed rows, per the requested axes. */
  private void prepareSeededTable(
      String table,
      WriteMode writeMode,
      FileFormat format,
      Partitioning partitioning,
      Lifecycle lifecycle) {
    String props = tableProps(writeMode, format);
    String using = "USING iceberg " + partitioning.clause;
    spark.sql("DROP TABLE IF EXISTS " + table);

    switch (lifecycle) {
      case BASE:
        spark.sql(
            "CREATE TABLE "
                + table
                + " (id int, data string) "
                + using
                + " TBLPROPERTIES ("
                + props
                + ")");
        spark.sql("INSERT INTO " + table + " VALUES " + SEED_VALUES);
        break;

      case RTAS:
        spark.sql(
            "CREATE TABLE "
                + table
                + " (id int, data string) "
                + using
                + " TBLPROPERTIES ("
                + props
                + ")");
        spark.sql("INSERT INTO " + table + " VALUES " + JUNK_VALUES);
        // Replace the junk body with the canonical seed.
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
        break;

      case RTAS_RESTORE:
        spark.sql(
            "CREATE TABLE "
                + table
                + " (id int, data string) "
                + using
                + " TBLPROPERTIES ("
                + props
                + ")");
        spark.sql("INSERT INTO " + table + " VALUES " + SEED_VALUES);
        long seedSnapshot = latestSnapshotId(table);
        // Replace with junk, then restore back to the seed snapshot (losing the junk body).
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
                + dbTable(table)
                + "', snapshot_id => "
                + seedSnapshot
                + ")");
        break;

      default:
        throw new IllegalArgumentException("unhandled lifecycle: " + lifecycle);
    }
  }

  private void runDml(String table, DmlOp op, WriteMode writeMode, Partitioning partitioning) {
    // A merge-on-read modification writes position delete files only when it deletes a strict
    // subset of a data file's rows. With PARTITIONED BY (data) every seed row lands in its own
    // single-row file, so a single-row DELETE removes the whole file as a metadata-only delete (no
    // position delete) — correct Iceberg behavior. We therefore only assert delete-file production
    // for the unpartitioned merge-on-read cases, where the seed rows share a data file.
    boolean expectDeleteFiles =
        writeMode.isMergeOnRead() && partitioning == Partitioning.UNPARTITIONED;
    switch (op) {
      case READ:
        assertRows(table, "1:a", "2:b", "3:c");
        break;

      case INSERT:
        spark.sql("INSERT INTO " + table + " VALUES (4, 'd')");
        assertRows(table, "1:a", "2:b", "3:c", "4:d");
        break;

      case DELETE:
        spark.sql("DELETE FROM " + table + " WHERE id = 2");
        assertRows(table, "1:a", "3:c");
        assertDeleteFilesPresentIf(table, expectDeleteFiles);
        break;

      case UPDATE:
        spark.sql("UPDATE " + table + " SET data = 'updated' WHERE id = 2");
        assertRows(table, "1:a", "2:updated", "3:c");
        assertDeleteFilesPresentIf(table, expectDeleteFiles);
        break;

      case MERGE:
        spark.sql(
            "MERGE INTO "
                + table
                + " t "
                + "USING (SELECT * FROM VALUES (2, 'merged'), (9, 'new') AS s(id, data)) s "
                + "ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET t.data = s.data "
                + "WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)");
        assertRows(table, "1:a", "2:merged", "3:c", "9:new");
        assertDeleteFilesPresentIf(table, expectDeleteFiles);
        break;

      case INSERT_OVERWRITE:
        // Force whole-table overwrite semantics so the assertion is partitioning-independent.
        spark.conf().set("spark.sql.sources.partitionOverwriteMode", "STATIC");
        try {
          spark.sql("INSERT OVERWRITE " + table + " VALUES (7, 'only')");
        } finally {
          spark.conf().unset("spark.sql.sources.partitionOverwriteMode");
        }
        assertRows(table, "7:only");
        break;

      default:
        throw new IllegalArgumentException("unhandled op: " + op);
    }
  }

  private static String tableProps(WriteMode writeMode, FileFormat format) {
    List<String> props = new ArrayList<>();
    props.add("'write.format.default'='" + format.property() + "'");
    props.add("'replace.enabled'='true'");
    if (writeMode.isMergeOnRead()) {
      props.add("'format-version'='2'");
      props.add("'write.delete.mode'='merge-on-read'");
      props.add("'write.update.mode'='merge-on-read'");
      props.add("'write.merge.mode'='merge-on-read'");
    }
    return String.join(", ", props);
  }

  /**
   * Asserts the table holds exactly the given rows, encoded as {@code "id:data"},
   * order-independent.
   */
  private void assertRows(String table, String... expected) {
    List<String> actual =
        spark.sql("SELECT id, data FROM " + table).collectAsList().stream()
            .map(r -> r.getInt(0) + ":" + r.getString(1))
            .sorted()
            .collect(Collectors.toList());
    List<String> want = Arrays.stream(expected).sorted().collect(Collectors.toList());
    assertEquals(want, actual, "row set mismatch for " + table);
  }

  private void assertDeleteFilesPresentIf(String table, boolean expectDeleteFiles) {
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

  private long latestSnapshotId(String table) {
    return spark
        .sql("SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1")
        .collectAsList()
        .get(0)
        .getLong(0);
  }

  /** The {@code db.table} identifier (the fully-qualified name without the catalog prefix). */
  private static String dbTable(String fullyQualifiedTable) {
    return fullyQualifiedTable.substring(fullyQualifiedTable.indexOf('.') + 1);
  }
}

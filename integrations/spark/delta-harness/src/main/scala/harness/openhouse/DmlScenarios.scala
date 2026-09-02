package harness

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit

/**
 * Data manipulation on the core table: the reads, deletes, updates, merges, inserts and overwrites the catalog
 * supports, and the row and snapshot change each one commits.
 *
 * Operations: 54 reusable DML operations over the six CoreTable columns (bigint, int, string, double, boolean, and a
 * string-encoded date), made up of 2 reads, 14 deletes, 13 updates, 16 merges, 6 inserts or overwrites, 1 null-string
 * delete, and 2 partition-scoped overwrites. Each operation covers a distinct SQL or DataFrame form, or a distinct
 * observable state change within its family. Every operation is defined once here, so a feature layer covers its own
 * table mode by crossing these same definitions with its own preparations.
 *
 * Preparation axes: ScenarioKit supplies the starting states. Six core layouts cross three file formats with
 * partitioned and unpartitioned tables. Three date-partitioned layouts receive the partition-scoped writes. Six
 * write-ordered layouts exercise the same catalog under a sort order. Six evolved layouts receive the 29 operations
 * that address columns by name. Null-string variants isolate the one operation that requires a null value.
 *
 * Case families: 804 cases in four families, `coreDmlCases` (312), `partitionedDmlCases` (6), `orderedDmlCases` (312)
 * and `evolvedDmlCases` (174).
 */
trait DmlScenarios extends ScenarioKit {
  import Rows._

  /** Every DML case, in preparation order: core, partition-scoped, write-ordered, then evolved. */
  lazy val dmlCases: List[Plan.Case] =
    coreDmlCases ++ partitionedDmlCases ++ orderedDmlCases ++ evolvedDmlCases

  /**
   * The reads. They select columns by name and write nothing, so they run on any preparation that starts from the three
   * seed rows, including one whose column list has grown past that shape.
   */
  lazy val readTestCases: List[DmlTestCase[CoreTable.type]] = List(
    readProjection,
    readFilter)

  /**
   * The DELETE that selects a null string. It applies to a preparation that already holds a row whose string column is
   * null, and it removes exactly that row.
   */
  lazy val nullStringRowTestCases: List[DmlTestCase[CoreTable.type]] = List(
    deleteByNullCondition)

  /**
   * The partition-scoped writes. They replace whole partitions, so they apply to a preparation that partitions the
   * table, and they cross with the partitioned preparations alone.
   */
  lazy val partitionedTableTestCases: List[DmlTestCase[CoreTable.type]] = List(
    insertDynamicOverwrite,
    overwritePartitions)

  // --- which cases a preparation is compatible with ---
  // Compatibility is a property of the starting state, so each list names the states it fits.

  /** Every DML case. Runs on any preparation that starts from three seed rows of the seed shape. */
  lazy val allDmlTestCases: List[DmlTestCase[CoreTable.type]] =
    readTestCases ++
      deleteTestCases ++
      updateTestCases ++
      mergeTestCases ++
      insertAndOverwriteTestCases

  /** The row-mutating cases: every DELETE, UPDATE and MERGE. */
  lazy val rowMutationTestCases: List[DmlTestCase[CoreTable.type]] =
    deleteTestCases ++ updateTestCases ++ mergeTestCases

  /**
   * The cases that address columns by name and never write a whole seed-shaped row, so they run on a preparation whose
   * column list has grown beyond the seed rows.
   */
  lazy val testCasesCompatibleWithAnAddedColumn: List[DmlTestCase[CoreTable.type]] =
    readTestCases ++ deleteTestCases ++ updateTestCases

  /**
   * Every DML case, with the partition-predicate DELETE marked as a known bug: the Spark and Iceberg rewrite crashes on
   * it when the table carries a write order.
   */
  lazy val orderedDmlTestCases: List[DmlTestCase[CoreTable.type]] =
    allDmlTestCases.map {
      case testCase if testCase == deleteByPartitionPredicate =>
        testCase.copy(knownBugReason = Some(
          "DELETE by partition predicate crashes in the Spark and Iceberg rewrite when the " +
            "table has a write order."))
      case testCase =>
        testCase
    }

  // --- standard preparations crossed with the cases they are compatible with ---

  /**
   * Every DML case on the core preparations, plus the null-string DELETE on the same preparations extended with a
   * null-string row.
   */
  lazy val coreDmlCases: List[Plan.Case] =
    preparedCoreTables.flatMap(preparation => allDmlTestCases.map(_.runOn(preparation))) ++
      preparedNullStringCoreTables.flatMap(preparation =>
        nullStringRowTestCases.map(_.runOn(preparation)))

  /** The partition-scoped writes on the partitioned preparations. */
  lazy val partitionedDmlCases: List[Plan.Case] =
    preparedPartitionedCoreTables.flatMap(preparation =>
      partitionedTableTestCases.map(_.runOn(preparation)))

  /** Every DML case on the write-ordered preparations, plus the null-string DELETE on their null-string form. */
  lazy val orderedDmlCases: List[Plan.Case] =
    preparedOrderedCoreTables.flatMap(preparation => orderedDmlTestCases.map(_.runOn(preparation))) ++
      preparedNullStringOrderedCoreTables.flatMap(preparation =>
        nullStringRowTestCases.map(_.runOn(preparation)))

  /** The cases that address columns by name, on the preparations that added a column. */
  lazy val evolvedDmlCases: List[Plan.Case] =
    preparedEvolvedCoreTables.flatMap(preparation =>
      testCasesCompatibleWithAnAddedColumn.map(_.runOn(preparation)))

  // --- the operations the surface above composes ---
  // Each case captures the table state, runs one operation, captures the state again, and asserts the row change and
  // the snapshot delta that operation caused. Deltas are relative, so a case holds on any preparation regardless of how
  // many snapshots the preparation itself committed.

  /**
   * SELECT of foo_col_string alone returns that column for every prepared row in key order and leaves the table state
   * unchanged.
   */
  private val readProjection: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "read.projection",
      table => {
        val before = table.state
        val projected = table.spark
          .sql(
            s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
              s"ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq
          .map(_.get(Core.string0))
        val after = table.state

        assert(
          projected == before.rows.sortBy(_.get(Core.long0)).map(_.get(Core.string0)),
          s"projection returned $projected")
        assert(after == before, "a read leaves the rows and the snapshot count unchanged")
      })

  /**
   * SELECT with a foo_col_long >= 2 predicate returns exactly the prepared rows whose key is 2 or greater and leaves
   * the table state unchanged.
   */
  private val readFilter: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "read.filter",
      table => {
        val before = table.state
        val selected = table.spark
          .sql(
            s"SELECT ${Core.long0.columnName} FROM ${table.name} " +
              s"WHERE ${Core.long0.columnName} >= 2 ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq
          .map(_.get(Core.long0))
        val after = table.state

        assert(
          selected == before.rows.map(_.get(Core.long0)).filter(_ >= 2).sorted,
          s"filtered read returned $selected")
        assert(after == before, "a read leaves the rows and the snapshot count unchanged")
      })

  /**
   * DELETE WHERE foo_col_string IS NULL removes exactly the prepared row whose string is null, leaves every other row
   * unchanged, and commits one snapshot.
   */
  private val deleteByNullCondition: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.byNullCondition",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.string0.columnName} IS NULL")
        val after = table.state

        assert(
          after.rows == before.rows.filter(row => Option(row.get(Core.string0)).nonEmpty),
          s"rows after the null-condition DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a null condition commits one snapshot")
      })

  /**
   * DELETE WHERE foo_col_date = '2024-01-01-00' removes the rows with that date, keeps the rest, and commits one
   * snapshot.
   */
  private val deleteByPartitionPredicate: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.byPartitionPredicate",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE " +
            s"${Core.date0.columnName} = '2024-01-01-00'")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.date0) == "2024-01-01-00"),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a partition predicate commits one snapshot")
      })

  /**
   * DELETE WHERE foo_col_long < 2 removes the rows below key 2, leaves every other row unchanged, and commits one
   * snapshot.
   */
  private val deleteByPredicate: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.byPredicate",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} < 2")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) < 2),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a predicate commits one snapshot")
      })

  /**
   * DELETE WHERE foo_col_long IN (1, 3) removes keys 1 and 3, leaves every other row exactly as prepared, and commits
   * one snapshot.
   */
  private val deleteByInList: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.byInList",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} IN (1, 3)")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(row => Set(1L, 3L)(row.get(Core.long0))),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by an IN list commits one snapshot")
      })

  /**
   * DELETE WHERE foo_col_long IN (subquery yielding 2) removes key 2, leaves every other row unchanged, and commits one
   * snapshot.
   */
  private val deleteByInSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.byInSubquery",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) == 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by an IN subquery commits one snapshot")
      })

  /**
   * DELETE WHERE foo_col_long NOT IN (subquery yielding 2) removes every key other than 2, leaves the row for key 2
   * unchanged, and commits one snapshot.
   */
  private val deleteByNotInSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.byNotInSubquery",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} NOT IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.filter(_.get(Core.long0) == 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a NOT IN subquery commits one snapshot")
      })

  /**
   * DELETE WHERE EXISTS (correlated subquery matching foo_col_long = 2) removes key 2, leaves every other row
   * unchanged, and commits one snapshot.
   */
  private val deleteByExistsSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.byExistsSubquery",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE EXISTS (" +
            "SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) == 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by an EXISTS subquery commits one snapshot")
      })

  /**
   * DELETE WHERE NOT EXISTS (correlated subquery matching foo_col_long = 2) removes every key other than 2, leaves the
   * row for key 2 unchanged, and commits one snapshot.
   */
  private val deleteByNotExistsSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.byNotExistsSubquery",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE NOT EXISTS (" +
            "SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")
        val after = table.state

        assert(
          after.rows == before.rows.filter(_.get(Core.long0) == 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a NOT EXISTS subquery commits one snapshot")
      })

  /**
   * DELETE WHERE foo_col_long = (scalar subquery yielding 2) removes key 2, leaves every other row unchanged, and
   * commits one snapshot.
   */
  private val deleteByScalarSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.byScalarSubquery",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = (" +
            "SELECT max(col1) FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) == 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a scalar subquery commits one snapshot")
      })

  /** DELETE FROM without a predicate empties the table and commits one snapshot. */
  private val deleteAll: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.all",
      table => {
        val before = table.state

        table.spark.sql(s"DELETE FROM ${table.name}")
        val after = table.state

        assert(after.rows.isEmpty, s"rows survived the unconditional DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an unconditional DELETE commits one snapshot")
      })

  /** DELETE WHERE foo_col_long = 999 matches no row, leaves every row unchanged, and still commits one snapshot. */
  private val deleteNone: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.none",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 999")
        val after = table.state

        assert(after.rows == before.rows, s"a no-match DELETE changed the rows: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a no-match DELETE with a real predicate still commits one snapshot")
      })

  /**
   * DELETE FROM <table> AS x WHERE x.foo_col_long < 2 resolves the alias, removes the rows below key 2, and commits one
   * snapshot.
   */
  private val deleteWithAlias: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.withAlias",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} AS x WHERE x.${Core.long0.columnName} < 2")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) < 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE through an alias commits one snapshot")
      })

  /** DELETE WHERE false is optimized away: the rows stay as they are and no snapshot is committed. */
  private val deleteWhereFalseNoSnapshot: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.whereFalse.noSnapshot",
      table => {
        val before = table.state

        table.spark.sql(s"DELETE FROM ${table.name} WHERE false")
        val after = table.state

        assert(after.rows == before.rows, s"DELETE WHERE false changed the rows: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount,
          "DELETE WHERE false must not commit a snapshot")
      })

  /** TRUNCATE TABLE empties the table and commits one snapshot. */
  private val deleteTruncate: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.truncate",
      table => {
        val before = table.state

        table.spark.sql(s"TRUNCATE TABLE ${table.name}")
        val after = table.state

        assert(after.rows.isEmpty, s"rows survived TRUNCATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "TRUNCATE commits one snapshot")
      })

  /**
   * DELETE against a snapshot-pinned identifier is rejected with an IllegalArgumentException naming that snapshot, and
   * the rows and the snapshot count stay unchanged.
   */
  private val deleteAtSnapshotRejected: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.atSnapshot.rejected",
      table => {
        val before = table.state
        val snapshotId = table.spark
          .sql(
            s"SELECT snapshot_id FROM ${table.name}.snapshots " +
              "ORDER BY committed_at DESC LIMIT 1")
          .collect()(0)
          .getLong(0)

        val exception = Check.intercept[IllegalArgumentException](
          table.spark.sql(
            s"DELETE FROM ${table.name}.snapshot_id_$snapshotId " +
              s"WHERE ${Core.long0.columnName} < 4"))
        val after = table.state

        assert(
          exception.getMessage ==
            s"Cannot delete from table at a specific snapshot: $snapshotId",
          s"unexpected rejection message: ${exception.getMessage}")
        assert(after == before, "a rejected DELETE leaves the rows and the snapshot count unchanged")
      })

  /**
   * The DELETE operations. They select rows by column name and write no new row, so they run on any preparation that
   * starts from the three seed rows, including one whose column list has grown past that shape.
   */
  private val deleteTestCases: List[DmlTestCase[CoreTable.type]] = List(
    deleteByPredicate,
    deleteByInList,
    deleteByInSubquery,
    deleteByNotInSubquery,
    deleteByExistsSubquery,
    deleteByNotExistsSubquery,
    deleteByScalarSubquery,
    deleteAll,
    deleteNone,
    deleteByPartitionPredicate,
    deleteWithAlias,
    deleteWhereFalseNoSnapshot,
    deleteTruncate,
    deleteAtSnapshotRejected)

  /**
   * UPDATE SET foo_col_string = 'X' WHERE foo_col_long = 2 rewrites that column for key 2 only, leaves every other row
   * unchanged, and commits one snapshot.
   */
  private val updateByPredicate: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byPredicate",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "X") else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by a predicate commits one snapshot")
      })

  /**
   * UPDATE SET foo_col_string = 'Z' without a WHERE clause rewrites that column for every row and commits one snapshot.
   */
  private val updateWithoutCondition: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.withoutCondition",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'Z'")
        val after = table.state

        assert(
          after.rows == before.rows.map(row => withColumnValue(row, Core.string0, "Z")),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an unconditional UPDATE commits one snapshot")
      })

  /** UPDATE ... WHERE foo_col_long = 99 matches no row, leaves every row unchanged, and still commits one snapshot. */
  private val updateNoMatch: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.noMatch",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'Y' " +
            s"WHERE ${Core.long0.columnName} = 99")
        val after = table.state

        assert(
          after.rows == before.rows,
          s"a no-match UPDATE changed the rows: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a no-match UPDATE still commits one snapshot")
      })

  /** UPDATE ... WHERE foo_col_long IN (subquery yielding 2) rewrites key 2 only and commits one snapshot. */
  private val updateByInSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byInSubquery",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "X") else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by an IN subquery commits one snapshot")
      })

  /**
   * UPDATE ... WHERE foo_col_long NOT IN (subquery yielding 2) rewrites every key other than 2 and commits one
   * snapshot.
   */
  private val updateByNotInSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byNotInSubquery",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} NOT IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) row else withColumnValue(row, Core.string0, "X")),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by a NOT IN subquery commits one snapshot")
      })

  /**
   * UPDATE ... WHERE EXISTS (correlated subquery matching foo_col_long = 2) rewrites key 2 only and commits one
   * snapshot.
   */
  private val updateByExistsSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byExistsSubquery",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            "WHERE EXISTS (SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "X") else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by an EXISTS subquery commits one snapshot")
      })

  /**
   * UPDATE ... WHERE NOT EXISTS (correlated subquery matching foo_col_long = 2) rewrites every key other than 2 and
   * commits one snapshot.
   */
  private val updateByNotExistsSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byNotExistsSubquery",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            "WHERE NOT EXISTS (SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) row else withColumnValue(row, Core.string0, "X")),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by a NOT EXISTS subquery commits one snapshot")
      })

  /** UPDATE ... WHERE foo_col_long = (scalar subquery yielding 2) rewrites key 2 only and commits one snapshot. */
  private val updateByScalarSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byScalarSubquery",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} = (" +
            "SELECT max(col1) FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "X") else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by a scalar subquery commits one snapshot")
      })

  /**
   * UPDATE <table> AS x SET x.foo_col_string ... WHERE x.foo_col_long = 2 resolves the alias on both sides, rewrites
   * key 2 only, and commits one snapshot.
   */
  private val updateWithAlias: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.withAlias",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} AS x SET x.${Core.string0.columnName} = 'X' " +
            s"WHERE x.${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "X") else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE through an alias commits one snapshot")
      })

  /**
   * UPDATE SET foo_col_string = 'X', foo_col_int = 99 WHERE foo_col_long = 2 rewrites both columns of key 2 in one
   * statement and commits one snapshot.
   */
  private val updateMultipleColumns: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.multipleColumns",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X', " +
            s"${Core.int0.columnName} = 99 WHERE ${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) {
              withColumnValue(withColumnValue(row, Core.string0, "X"), Core.int0, 99)
            } else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a multi-column UPDATE commits one snapshot")
      })

  /**
   * UPDATE SET foo_col_long = foo_col_long + 10 WHERE foo_col_long = 2 moves key 2 to key 12, leaves every other row
   * unchanged, and commits one snapshot.
   */
  private val updateByExpression: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byExpression",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET " +
            s"${Core.long0.columnName} = ${Core.long0.columnName} + 10 " +
            s"WHERE ${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.long0, 12L) else row)),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by an expression commits one snapshot")
      })

  /**
   * UPDATE SET foo_col_date = '2099-12-31-23' WHERE foo_col_long = 2 moves key 2 to another date partition value,
   * leaves every other row unchanged, and commits one snapshot.
   */
  private val updateMovePartition: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.movePartition",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET " +
            s"${Core.date0.columnName} = '2099-12-31-23' " +
            s"WHERE ${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) {
              withColumnValue(row, Core.date0, "2099-12-31-23")
            } else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a partition-moving UPDATE commits one snapshot")
      })

  /**
   * UPDATE SET foo_col_string = NULL WHERE foo_col_long = 2 stores a null in that column for key 2 only and commits one
   * snapshot.
   */
  private val updateNullAssignment: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.nullAssignment",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = NULL " +
            s"WHERE ${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, null) else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an UPDATE assigning null commits one snapshot")
      })

  /**
   * The UPDATE operations. They assign columns by name, so they run on any preparation that starts from the three seed
   * rows, including one whose column list has grown past that shape.
   */
  private val updateTestCases: List[DmlTestCase[CoreTable.type]] = List(
    updateByPredicate,
    updateWithoutCondition,
    updateNoMatch,
    updateByInSubquery,
    updateByNotInSubquery,
    updateByExistsSubquery,
    updateByNotExistsSubquery,
    updateByScalarSubquery,
    updateWithAlias,
    updateMultipleColumns,
    updateByExpression,
    updateMovePartition,
    updateNullAssignment)

  /**
   * MERGE with only a WHEN NOT MATCHED THEN INSERT * clause appends the two source rows (keys 4 and 5) with every
   * source column value, leaves the prepared rows unchanged, and commits one snapshot.
   */
  private val mergeInsertNotMatched: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.insertNotMatched",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($columnNameList)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows ++ Seq(
            Row(4L, 4, "row-4", 4.5, true, "2024-01-04-03"),
            Row(5L, 5, "row-5", 5.5, false, "2024-01-05-04"))),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE that inserts commits one snapshot")
      })

  /**
   * MERGE with only a WHEN MATCHED THEN UPDATE clause rewrites the matched key 2, leaves the unmatched rows unchanged,
   * and commits one snapshot.
   */
  private val mergeUpdateMatched: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.updateMatched",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT), 'M')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "M") else row),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE that updates commits one snapshot")
      })

  /**
   * MERGE with only a WHEN MATCHED THEN DELETE clause removes the matched keys 1 and 3, keeps the unmatched rows, and
   * commits one snapshot.
   */
  private val mergeDeleteMatched: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.deleteMatched",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(1 AS BIGINT)), (CAST(3 AS BIGINT))
                AS s(${Core.long0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN DELETE""")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(row => Set(1L, 3L)(row.get(Core.long0))),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE that deletes commits one snapshot")
      })

  /**
   * MERGE with both an UPDATE clause and an INSERT clause rewrites the matched key 2 and appends the unmatched key 7 in
   * a single statement, and commits one snapshot.
   */
  private val mergeUpsert: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.upsert",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 2, 'U', 2.5, true,  '2024-01-02-01'),
                  (CAST(7 AS BIGINT), 7, 'g', 7.5, false, '2024-01-07-06')
                AS s($columnNameList)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(
            before.rows.map(row =>
              if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "U") else row) :+
              Row(7L, 7, "g", 7.5, false, "2024-01-07-06")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an upsert MERGE commits one snapshot")
      })

  /**
   * MERGE with a WHEN NOT MATCHED BY SOURCE THEN DELETE clause removes every row the source does not carry, keeps the
   * matched key 2, and commits one snapshot.
   */
  private val mergeDeleteNotMatchedBySource: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.deleteNotMatchedBySource",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT))
                AS s(${Core.long0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED BY SOURCE THEN DELETE""")
        val after = table.state

        assert(
          after.rows == before.rows.filter(_.get(Core.long0) == 2L),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a not-matched-by-source MERGE commits one snapshot")
      })

  /**
   * MERGE with a WHEN MATCHED AND <condition> THEN UPDATE clause rewrites only the matched row that also satisfies the
   * condition (key 2), leaves matched key 3 unchanged, and commits one snapshot.
   */
  private val mergeConditionalUpdate: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.conditionalUpdate",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT), 'U2'),
                  (CAST(3 AS BIGINT), 'U3')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED AND s.${Core.long0.columnName} = 2 THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "U2") else row),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a conditional-update MERGE commits one snapshot")
      })

  /**
   * MERGE with two MATCHED clauses applies the first matching clause per row: key 2 is updated by the conditional
   * clause and key 3 falls through to the DELETE clause, in one snapshot.
   */
  private val mergeMultipleMatchedClauses: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.multipleMatchedClauses",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT), 'U'),
                  (CAST(3 AS BIGINT), 'x')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED AND s.${Core.long0.columnName} = 2 THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
              WHEN MATCHED THEN DELETE""")
        val after = table.state

        assert(
          after.rows == before.rows
            .filterNot(_.get(Core.long0) == 3L)
            .map(row =>
              if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "U") else row),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a multi-clause MERGE commits one snapshot")
      })

  /**
   * MERGE with a WHEN NOT MATCHED AND <condition> THEN INSERT clause appends only the source row that satisfies the
   * condition (key 4), skips key 5, and commits one snapshot.
   */
  private val mergeConditionalInsert: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.conditionalInsert",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($columnNameList)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED AND s.${Core.long0.columnName} = 4 THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(4L, 4, "row-4", 4.5, true, "2024-01-04-03")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a conditional-insert MERGE commits one snapshot")
      })

  /**
   * MERGE carrying UPDATE, INSERT, and NOT MATCHED BY SOURCE DELETE clauses updates key 2, inserts key 4, deletes the
   * rows the source omits, and commits one snapshot.
   */
  private val mergeAllClauses: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.allClauses",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 2, 'M2', 2.5, true,  '2024-01-02-01'),
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, false, '2024-01-04-03')
                AS s($columnNameList)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
              WHEN NOT MATCHED THEN INSERT *
              WHEN NOT MATCHED BY SOURCE THEN DELETE""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(
            before.rows
              .filter(_.get(Core.long0) == 2L)
              .map(row => withColumnValue(row, Core.string0, "M2")) :+
              Row(4L, 4, "row-4", 4.5, false, "2024-01-04-03")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE with every clause commits one snapshot")
      })

  /**
   * MERGE with WHEN MATCHED THEN UPDATE SET * copies every source column onto the matched key 2, leaves the unmatched
   * rows unchanged, and commits one snapshot.
   */
  private val mergeUpdateStar: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.updateStar",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 22, 'S2', 22.5, true, '2024-06-06-06')
                AS s($columnNameList)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE SET *""")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) {
              Row(2L, 22, "S2", 22.5, true, "2024-06-06-06")
            } else row),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an UPDATE SET * MERGE commits one snapshot")
      })

  /**
   * MERGE whose INSERT clause names a column subset appends key 7 with the named values, leaves the unnamed columns
   * null, and commits one snapshot.
   */
  private val mergeInsertExplicitColumns: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.insertExplicitColumns",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(7 AS BIGINT), 'g')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN
              INSERT (${Core.long0.columnName}, ${Core.string0.columnName})
              VALUES (s.${Core.long0.columnName}, s.${Core.string0.columnName})""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(7L, null, "g", null, null, null)),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an explicit-column MERGE insert commits one snapshot")
      })

  /**
   * MERGE whose source is a common table expression appends the key 8 that CTE yields, with null in every column the
   * CTE does not supply, and commits one snapshot.
   */
  private val mergeSourceCTE: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.sourceCTE",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                WITH src AS (
                  SELECT CAST(8 AS BIGINT) AS ${Core.long0.columnName}
                )
                SELECT * FROM src
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN
              INSERT (${Core.long0.columnName}) VALUES (s.${Core.long0.columnName})""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(8L, null, null, null, null, null)),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE from a CTE source commits one snapshot")
      })

  /**
   * MERGE whose source is a UNION ALL appends both keys the set operation yields (8 and 9), with null in every column
   * the source does not supply, and commits one snapshot.
   */
  private val mergeSourceSetOp: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.sourceSetOp",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT CAST(8 AS BIGINT) AS ${Core.long0.columnName}
                UNION ALL
                SELECT CAST(9 AS BIGINT)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN
              INSERT (${Core.long0.columnName}) VALUES (s.${Core.long0.columnName})""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows ++ Seq(
            Row(8L, null, null, null, null, null),
            Row(9L, null, null, null, null, null))),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE from a set-operation source commits one snapshot")
      })

  /**
   * After the table is emptied, MERGE with a NOT MATCHED INSERT clause inserts both source rows (keys 4 and 5) into the
   * empty target and commits one snapshot.
   */
  private val mergeIntoEmptyTarget: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.intoEmptyTarget",
      table => {
        table.spark.sql(s"DELETE FROM ${table.name}")
        val before = table.state

        assert(before.rows.isEmpty, s"precondition: the target is empty, got ${before.rows}")

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($columnNameList)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == Seq(
            Row(4L, 4, "row-4", 4.5, true, "2024-01-04-03"),
            Row(5L, 5, "row-5", 5.5, false, "2024-01-05-04")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE into an empty target commits one snapshot")
      })

  /**
   * MERGE whose source carries a null join key matches no target row on that key: only the non-null key 2 is updated,
   * no row is added or removed, and one snapshot is committed.
   */
  private val mergeNullJoinKey: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.nullJoinKey",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(NULL AS BIGINT), 'n'),
                  (CAST(2 AS BIGINT), 'M')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "M") else row),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE with a null join key commits one snapshot")
      })

  /**
   * MERGE with INSERT * resolves the source columns by name: key 7 lands with every source value in its named column
   * when the source lists its columns in another order, and one snapshot is committed.
   */
  private val mergeResolveByName: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.resolveByName",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  ('g', CAST(7 AS BIGINT), 7, 7.5, false, '2024-07-07-07')
                AS s(
                  ${Core.string0.columnName},
                  ${Core.long0.columnName},
                  ${Core.int0.columnName},
                  ${Core.double0.columnName},
                  ${Core.boolean0.columnName},
                  ${Core.date0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(7L, 7, "g", 7.5, false, "2024-07-07-07")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a name-resolved MERGE insert commits one snapshot")
      })

  /**
   * The MERGE operations. Their INSERT clauses write a whole seed-shaped row, so they run on a preparation whose column
   * list is still the seed shape.
   */
  private val mergeTestCases: List[DmlTestCase[CoreTable.type]] = List(
    mergeInsertNotMatched,
    mergeUpdateMatched,
    mergeDeleteMatched,
    mergeUpsert,
    mergeDeleteNotMatchedBySource,
    mergeConditionalUpdate,
    mergeMultipleMatchedClauses,
    mergeConditionalInsert,
    mergeAllClauses,
    mergeUpdateStar,
    mergeInsertExplicitColumns,
    mergeSourceCTE,
    mergeSourceSetOp,
    mergeIntoEmptyTarget,
    mergeNullJoinKey,
    mergeResolveByName)

  /**
   * INSERT INTO ... VALUES appends the two literal rows (keys 4 and 5), leaves the prepared rows unchanged, and commits
   * one snapshot.
   */
  private val insertInto: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "insert.into",
      table => {
        val before = table.state

        table.spark.sql(
          s"""INSERT INTO ${table.name} VALUES
                (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows ++ Seq(
            Row(4L, 4, "row-4", 4.5, true, "2024-01-04-03"),
            Row(5L, 5, "row-5", 5.5, false, "2024-01-05-04"))),
          s"rows after the INSERT: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "INSERT INTO commits one snapshot")
      })

  /**
   * INSERT INTO naming a subset of the columns is rejected by the engine with a message naming the omitted data, and
   * the rows and the snapshot count stay unchanged.
   */
  private val insertExplicitColumns: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "insert.explicitColumns",
      table => {
        val before = table.state

        val exception = Check.intercept[Exception](
          table.spark.sql(
            s"INSERT INTO ${table.name} " +
              s"(${Core.long0.columnName}, ${Core.string0.columnName}) " +
              "VALUES (CAST(4 AS BIGINT), 'd'), (CAST(5 AS BIGINT), 'e')"))
        val after = table.state
        val exceptionMessage = Option(exception.getMessage).getOrElse("")

        assert(
          exceptionMessage.toUpperCase.contains("CANNOT_FIND_DATA") ||
            exceptionMessage.toUpperCase.contains("CANNOT FIND DATA") ||
            exceptionMessage.toUpperCase.contains("INCOMPATIBLE_DATA"),
          "expected a partial-INSERT rejection naming the omitted column " +
            s"(engine limitation), got: ${exceptionMessage.take(200)}")
        assert(after == before, "a rejected INSERT leaves the rows and the snapshot count unchanged")
      })

  /**
   * INSERT INTO ... SELECT appends the row the SELECT produces (key 6), leaves the prepared rows unchanged, and commits
   * one snapshot.
   */
  private val insertIntoSelect: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "insert.intoSelect",
      table => {
        val before = table.state

        table.spark.sql(
          s"INSERT INTO ${table.name} SELECT * FROM VALUES " +
            s"(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05') " +
            s"AS s($columnNameList)")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(6L, 6, "row-6", 6.5, true, "2024-01-06-05")),
          s"rows after the INSERT: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "INSERT INTO ... SELECT commits one snapshot")
      })

  /**
   * The DataFrame writeTo(...).append() path appends the frame's row (key 6), keeps the prepared rows, and commits one
   * snapshot.
   */
  private val appendDataFrame: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "append.dataFrame",
      table => {
        val before = table.state

        table.spark
          .sql(
            s"SELECT * FROM VALUES " +
              s"(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05') " +
              s"AS s($columnNameList)")
          .writeTo(table.name)
          .append()
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(6L, 6, "row-6", 6.5, true, "2024-01-06-05")),
          s"rows after the append: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a DataFrame append commits one snapshot")
      })

  /**
   * INSERT OVERWRITE ... VALUES replaces the table contents with the two literal rows (keys 1 and 2) and commits one
   * snapshot.
   */
  private val insertOverwrite: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "insert.overwrite",
      table => {
        val before = table.state

        table.spark.sql(
          s"""INSERT OVERWRITE ${table.name} VALUES
                (CAST(1 AS BIGINT), 1, 'p', 1.5, false, '2024-01-01-00'),
                (CAST(2 AS BIGINT), 2, 'q', 2.5, true,  '2024-01-02-01')""")
        val after = table.state

        assert(
          after.rows == Seq(
            Row(1L, 1, "p", 1.5, false, "2024-01-01-00"),
            Row(2L, 2, "q", 2.5, true, "2024-01-02-01")),
          s"rows after the overwrite: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "INSERT OVERWRITE commits one snapshot")
      })

  /**
   * The DataFrame writeTo(...).overwrite(lit(true)) path replaces every row with the frame's row (key 8) and commits
   * one snapshot.
   */
  private val overwriteDataFrame: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "overwrite.dataFrame",
      table => {
        val before = table.state

        table.spark
          .sql(
            s"SELECT * FROM VALUES " +
              s"(CAST(8 AS BIGINT), 8, 'h', 8.5, false, '2024-01-08-07') " +
              s"AS s($columnNameList)")
          .writeTo(table.name)
          .overwrite(lit(true))
        val after = table.state

        assert(
          after.rows == Seq(Row(8L, 8, "h", 8.5, false, "2024-01-08-07")),
          s"rows after the overwrite: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a DataFrame overwrite commits one snapshot")
      })

  /**
   * The appends and the overwrites. They write whole seed-shaped rows, so they run on a preparation whose column list
   * is still the seed shape.
   */
  private val insertAndOverwriteTestCases: List[DmlTestCase[CoreTable.type]] = List(
    insertInto,
    insertExplicitColumns,
    insertIntoSelect,
    appendDataFrame,
    insertOverwrite,
    overwriteDataFrame)

  /**
   * Under partitionOverwriteMode=dynamic, INSERT OVERWRITE with one row replaces only that row's partition
   * (2024-01-01-00), leaves the rows of every other partition unchanged, and commits one snapshot.
   */
  private val insertDynamicOverwrite: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "insert.dynamicOverwrite",
      table => {
        val before = table.state

        table.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        try {
          table.spark.sql(
            s"INSERT OVERWRITE ${table.name} VALUES " +
              "(CAST(10 AS BIGINT), 10, 'p', 10.5, true, '2024-01-01-00')")
        } finally {
          table.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        }
        val after = table.state

        assert(
          after.rows == inKeyOrder(
            before.rows.filterNot(_.get(Core.date0) == "2024-01-01-00") :+
              Row(10L, 10, "p", 10.5, true, "2024-01-01-00")),
          s"rows after the dynamic overwrite: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a dynamic partition overwrite commits one snapshot")
      })

  /**
   * The DataFrame writeTo(...).overwritePartitions() path replaces only the partitions the frame carries
   * (2024-01-01-00), leaves the rows of every other partition unchanged, and commits one snapshot.
   */
  private val overwritePartitions: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "overwrite.partitions",
      table => {
        val before = table.state

        table.spark
          .sql(
            s"SELECT * FROM VALUES " +
              "(CAST(10 AS BIGINT), 10, 'p', 10.5, true, '2024-01-01-00') " +
              s"AS s($columnNameList)")
          .writeTo(table.name)
          .overwritePartitions()
        val after = table.state

        assert(
          after.rows == inKeyOrder(
            before.rows.filterNot(_.get(Core.date0) == "2024-01-01-00") :+
              Row(10L, 10, "p", 10.5, true, "2024-01-01-00")),
          s"rows after the partition overwrite: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a partition overwrite commits one snapshot")
      })

}

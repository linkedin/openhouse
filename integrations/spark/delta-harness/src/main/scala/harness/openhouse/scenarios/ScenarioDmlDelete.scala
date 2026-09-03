package harness

/**
 * Delete operations over the core table. Each case verifies the selected row removal and the snapshot behavior of its
 * SQL form.
 */
trait ScenarioDmlDelete extends ScenarioKit {
  import Rows._

  /**
   * The DELETE that selects a null string. It applies to a preparation that already holds a row whose string column is
   * null, and it removes exactly that row.
   */
  lazy val nullStringRowTestCases: List[DmlTestCase[CoreTable.type]] = List(
    deleteByNullCondition)

  /**
   * The DELETE operations. They select rows by column name and write no new row, so they run on any preparation that
   * starts from the three seed rows, including one whose column list has grown past that shape.
   */
  protected lazy val deleteTestCases: List[DmlTestCase[CoreTable.type]] = List(
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
}

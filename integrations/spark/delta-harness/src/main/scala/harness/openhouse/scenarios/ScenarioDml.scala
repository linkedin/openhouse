package harness

import org.apache.spark.sql.Row

/**
 * The core behavior slice: one exact read, append, overwrite, delete, update, and merge contract on each supported
 * columnar format.
 *
 * Each case starts from the same unpartitioned three-row table. The preparation proves table creation and seeding,
 * the body applies one operation, and the assertions prove the exact rows and snapshot delta it produced. Later
 * layers extend this operation catalog and reuse the same cases on additional table states.
 *
 * Case families: six operations over Parquet and ORC, contributing 12 cases.
 */
trait ScenarioDml extends ScenarioKit {
  import Rows._

  /** One representative from every principal read and write family, on each standard format. */
  lazy val dmlCases: List[TestCase] =
    preparedCoreFormats.flatMap(preparation =>
      coreDmlTestCases.map(_.runOn(preparation)))

  /** The six operations that prove the complete table lifecycle and each principal DML family. */
  lazy val coreDmlTestCases: List[DmlTestCase[CoreTable.type]] =
    List(
      readProjection,
      insertInto,
      insertOverwrite,
      deleteByPredicate,
      updateByPredicate,
      mergeUpsert)

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
   * INSERT INTO appends two literal rows, leaves every prepared row unchanged, and commits one snapshot.
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
   * INSERT OVERWRITE replaces the table contents with two literal rows and commits one snapshot.
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
   * MERGE with an UPDATE clause and an INSERT clause rewrites key 2, appends key 7, and commits one snapshot.
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
}

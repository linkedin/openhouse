package harness

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit

/**
 * Insert and overwrite operations over the core table. Each case verifies the appended or replaced rows and the
 * snapshot behavior of its SQL or DataFrame form.
 */
trait ScenarioDmlWrite extends TableTestFixtures {
  import Rows._

  /**
   * The appends and the overwrites. They write whole seed-shaped rows, so they run on a preparation whose column list
   * is still the seed shape.
   */
  protected lazy val insertAndOverwriteTestCases: List[DmlTestCase[CoreTable.type]] = List(
    insertInto,
    insertExplicitColumns,
    insertIntoSelect,
    appendDataFrame,
    insertOverwrite,
    overwriteDataFrame)

  /**
   * The partition-scoped writes. They replace whole partitions, so they apply to a preparation that partitions the
   * table, and they cross with the partitioned preparations alone.
   */
  lazy val partitionedTableTestCases: List[DmlTestCase[CoreTable.type]] = List(
    insertDynamicOverwrite,
    overwritePartitions)

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
                (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-01-03'),
                (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-01-04')""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows ++ Seq(
            Row(4L, 4, "row-4", 4.5, true, "2024-01-01-03"),
            Row(5L, 5, "row-5", 5.5, false, "2024-01-01-04"))),
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

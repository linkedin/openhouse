package harness

/**
 * Read operations over the core table. Each case captures the table state before and after the query and verifies that
 * reading changes neither the rows nor the snapshot count.
 */
trait ScenarioDmlRead extends TableTestFixtures {
  import Rows._

  /**
   * The reads. They select columns by name and write nothing, so they run on any preparation that starts from the three
   * seed rows, including one whose column list has grown past that shape.
   */
  protected lazy val readTestCases: List[DmlTestCase[CoreTable.type]] = List(
    readProjection,
    readFilter)

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
}

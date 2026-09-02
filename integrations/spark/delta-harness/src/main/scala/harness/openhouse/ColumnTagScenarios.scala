package harness

/**
 * Column tags: ALTER TABLE MODIFY COLUMN SET TAG records a classification on a column and leaves the values that
 * column returns exactly as they were written.
 *
 * Operations: SET TAG = (PII) on the string column, followed by a read of that column.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats.
 *
 * Case families: one family contributing 2 cases.
 */
trait ColumnTagScenarios extends ScenarioKit {

  /** The column-tag case, one file format at a time. */
  lazy val columnTagCases: List[Plan.Case] =
    standardFormats.map(format => setTagCase(preparedStandardTable(format)))

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * ALTER TABLE MODIFY COLUMN SET TAG = (PII) tags a column, and queries keep returning the values the seed wrote.
   */
  private def setTagCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("columnTag.setTag") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} MODIFY COLUMN " +
          s"${Core.string0.columnName} SET TAG = (PII)")

      val values = table.spark
        .sql(
          s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
            s"ORDER BY ${Core.long0.columnName}")
        .collect()
        .toSeq
        .map(_.getString(0))

      assert(
        values == Seq("row-1", "row-2", "row-3"),
        s"SET TAG changed the values the column returns: $values")
    }

}

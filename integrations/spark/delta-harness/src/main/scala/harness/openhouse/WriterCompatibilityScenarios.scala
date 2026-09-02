package harness

import org.apache.spark.sql.AnalysisException

/**
 * Writer compatibility: how a writer that names every column explicitly behaves after the table's column list has
 * grown.
 *
 * Operations: an explicit-column INSERT that lists the six core columns, run once before ADD COLUMN and once after.
 * The catalog accepts it before and rejects it after, naming the column the statement omits.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats.
 *
 * Case families: one family contributing 2 cases.
 */
trait WriterCompatibilityScenarios extends ScenarioKit {

  /** The explicit-column writer case, one file format at a time. */
  lazy val writerCompatibilityCases: List[Plan.Case] =
    standardFormats.map(format => afterAddColumnCase(preparedStandardTable(format)))

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * An explicit-column INSERT that worked before ADD COLUMN is rejected afterward, with an error naming the new
   * column.
   */
  private def afterAddColumnCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("writerCompatibility.afterAddColumn") { table =>
      val writerStatement =
        s"INSERT INTO ${table.name} ($columnNameList) VALUES " +
          "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')"
      table.spark.sql(writerStatement)
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "4",
        "explicit-column writer should work before schema evolution")

      table.spark.sql(
        s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(writerStatement))
      assert(
        exception.getMessage.contains("extra_col") &&
          (exception.getMessage.contains("CANNOT_FIND_DATA") ||
            exception.getMessage.toLowerCase.contains("cannot find data")),
        "a pre-evolution explicit-column writer is rejected after ADD COLUMN: " +
          exception.getMessage.take(160))
    }

}

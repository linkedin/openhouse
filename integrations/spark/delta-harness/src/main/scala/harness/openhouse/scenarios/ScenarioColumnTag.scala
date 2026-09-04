package harness

import scala.collection.JavaConverters._

/**
 * Column tags: ALTER TABLE MODIFY COLUMN SET TAG records a classification on a column and leaves that column's values
 * intact.
 *
 * Operations: SET TAG = (PII) on the string column, followed by a read of the persisted policies metadata and that
 * column's values.
 *
 * Preparation axes: the standard seeded core table in each columnar format.
 *
 * Case families: one family contributing 2 cases.
 */
trait ScenarioColumnTag extends ScenarioKit {

  /** The column-tag case, one file format at a time. */
  lazy val columnTagCases: List[TestCase] =
    fileFormats.map(format => setTagCase(preparedStandardTable(format)))

  /** ALTER TABLE MODIFY COLUMN SET TAG = (PII) records the exact tag on the column and preserves seed values. */
  private def setTagCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("columnTag.setTag") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} MODIFY COLUMN " +
          s"${Core.string0.columnName} SET TAG = (PII)")

      val columnTags =
        GovernancePolicies.objectField(
          GovernancePolicies.parse(tableProps(table.spark, table.name)),
          "columnTags")
      val tagsForColumn =
        columnTags
          .flatMap(GovernancePolicies.objectField(_, Core.string0.columnName))
          .flatMap(GovernancePolicies.stringArrayField(_, "tags"))
          .getOrElse(Seq.empty)

      assert(
        columnTags.exists(_.entrySet().asScala.map(_.getKey).toSet == Set(Core.string0.columnName)),
        s"column-tag metadata should name exactly ${Core.string0.columnName}: $columnTags")
      assert(
        tagsForColumn == Seq("PII"),
        s"column-tag metadata should store exactly PII on ${Core.string0.columnName}: $columnTags")

      val values = table.spark
        .sql(
          s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
            s"ORDER BY ${Core.long0.columnName}")
        .collect()
        .toSeq
        .map(_.getString(0))

      assert(
        values == Seq("row-1", "row-2", "row-3"),
        s"SET TAG should preserve the values the column returns: $values")
    }

}

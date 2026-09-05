package harness

/**
 * File format materialization: the write.format.default a table declares is the format its data files are actually
 * written in.
 *
 * Operations: read the declared write.format.default from the table properties, then list the data files the
 * preparation wrote and compare their extensions against it.
 *
 * Preparation axes: the eight standard preparations that leave data files behind, which are the four core layouts
 * (Parquet and ORC crossed with unpartitioned and date-partitioned) and the same four carrying a write sort order. A
 * feature layer covers its own table mode by passing its own preparations to `layoutFormatCasesFor`.
 *
 * Case families: one family, `format.materialization`, contributing 8 cases.
 */
trait ScenarioFileFormat extends CatalogConstraintTableFixtures {

  /** The format-materialization case on every standard preparation that writes data files. */
  lazy val fileFormatCases: List[TestCase] = layoutFormatCasesFor(layoutFormatPreparations)

  /**
   * The format-materialization case for each preparation given: every data file the preparation wrote carries the
   * extension of the table's declared write.format.default, and listing the files leaves the rows and the snapshot
   * count unchanged. It applies to any preparation that leaves data files behind, so each feature layer passes the
   * list its own preparations produce.
   */
  def layoutFormatCasesFor(
      preparations: List[TablePreparation[CoreTable.type]]
  ): List[TestCase] =
    preparations.map { preparation =>
      preparation.test("format.materialization") { table =>
        val before = table.state
        val declaredFormat = table.spark
          .sql(s"SHOW TBLPROPERTIES ${table.name} ('write.format.default')")
          .collect()(0)
          .getString(1)
        val filePaths = table.spark
          .sql(s"SELECT file_path FROM ${table.name}.files")
          .collect()
          .toSeq
          .map(_.getString(0))
        val after = table.state

        assert(
          filePaths.nonEmpty && filePaths.forall(_.toLowerCase.endsWith(s".$declaredFormat")),
          s"data files are not all .$declaredFormat: $filePaths")
        assert(after == before, "listing files leaves the rows and the snapshot count unchanged")
      }
    }

  /** The standard preparations that leave data files behind: the core and the write-ordered ones. */
  lazy val layoutFormatPreparations: List[TablePreparation[CoreTable.type]] =
    preparedCoreTables ++ preparedOrderedCoreTables

}

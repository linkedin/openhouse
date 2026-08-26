package harness

// The RTAS preparation kit. A replace-lineage table is created, seeded, and then re-specified by
// CREATE OR REPLACE TABLE AS SELECT, so every case that runs on one of these preparations exercises
// the replace path. The members are lazy so they initialize on first read, after every trait mixed
// into `object Scenarios` has been constructed.
trait RtasScenarioKit extends ScenarioKit {

  def createAndSeedRtas(partitioning: Partitioning, numberOfRows: Int, format: String): TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource ${partitioning.clause} " +
        s"TBLPROPERTIES ('write.format.default'='$format', 'replace.enabled'='true')")()
      .insert(numberOfRows)()
      .sql("prep.rtas")(t => s"CREATE OR REPLACE TABLE $t USING $dataSource ${partitioning.clause} " +
        s"TBLPROPERTIES ('write.format.default'='$format') AS SELECT * FROM $t")()
      // The OpenHouse user guide requires REFRESH TABLE after a replace: the Spark session caches
      // the table state it read before the replace, and REFRESH re-reads the committed metadata
      // pointer so later statements in the session see the replaced table.
      .sql("prep.rtas.refresh")(t => s"REFRESH TABLE $t")()

  // Create and seed, then CREATE OR REPLACE ... AS SELECT * re-specifying the same shape, so the
  // table holds the same three rows and was reached through the replace path. The cases run on all
  // six layouts, so a replaced table supports the same operations as a freshly created one.
  private def rtasPreparationDescription(partitioning: Partitioning, format: String): String =
    s"Three seed rows with keys 1, 2 and 3 in a $format table ${partitioning.description}, then " +
      "replaced by CREATE OR REPLACE TABLE AS SELECT re-specifying the same shape, so the table " +
      "holds the same three rows on replace lineage."

  lazy val preparedRtasCoreTables: List[TablePreparation[CoreTable.type]] =
    for {
      partitioning <- partitionings
      format       <- fileFormats
    } yield TablePreparation(
      s"${partitioning.label}/$format",
      createAndSeedRtas(partitioning, 3, format),
      "prep.rtas:",
      description = rtasPreparationDescription(partitioning, format))

  lazy val preparedRtasPartitionedCoreTables: List[TablePreparation[CoreTable.type]] =
    fileFormats.map { format =>
      TablePreparation(
        s"${partitionedByDate.label}/$format",
        createAndSeedRtas(partitionedByDate, 3, format),
        "prep.rtas:",
        description = rtasPreparationDescription(partitionedByDate, format))
    }

  lazy val preparedNullStringRtasCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedRtasCoreTables.map(withNullStringRow)

  lazy val rtasLayoutFormatPreparations: List[TablePreparation[CoreTable.type]] =
    preparedRtasCoreTables

  def rtasLayoutFormatCases: List[Plan.Case] =
    layoutFormatCasesFor(rtasLayoutFormatPreparations)
}

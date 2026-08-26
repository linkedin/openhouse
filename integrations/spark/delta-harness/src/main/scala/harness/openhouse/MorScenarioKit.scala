package harness

// The merge-on-read preparation kit. A merge-on-read table is format version 2 with the delete,
// update and merge modes set to merge-on-read, so a mutation records position-delete files while
// preserving the untouched data files. This layer sits above RTAS, so it also owns the replace-lineage
// merge-on-read preparations. The members are lazy so they initialize on first read, after every
// trait mixed into `object Scenarios` has been constructed.
trait MorScenarioKit extends RtasScenarioKit {

  // Merge-on-read layouts use the standard shapes and record DELETE, UPDATE and MERGE changes in
  // position-delete files. Only mutation operations run against these format-version 2 layouts.
  private def morLayout(partitioning: Partitioning, format: String): Layout =
    Layout(
      s"mor-${partitioning.label}/$format",
      s"a merge-on-read format-version 2 $format table ${partitioning.description}",
      table =>
        s"CREATE TABLE $table ($columnDefinitions) USING $dataSource ${partitioning.clause} " +
          s"TBLPROPERTIES ('write.format.default'='$format', 'format-version'='2', " +
          s"'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')")

  lazy val morLayouts: List[Layout] =
    for {
      format       <- fileFormats
      partitioning <- partitionings
    } yield morLayout(partitioning, format)

  lazy val unpartitionedMorLayouts: List[Layout] =
    fileFormats.map(format => morLayout(unpartitioned, format))

  // Layouts that pin how a DELETE is written physically. Both set `write.distribution-mode=none`
  // and stay unpartitioned so a single seed INSERT lands every row in ONE data file. Deleting a
  // strict subset is then a partial-file match, which Iceberg satisfies by writing a position
  // delete under merge-on-read and by rewriting the data file under copy-on-write. The general
  // `morLayouts` seed spreads rows over several files, where a delete aligned with a file boundary
  // is satisfied by dropping that whole file, so these layouts are what make the physical outcome
  // deterministic across formats.
  lazy val morVerifyLayouts: List[Layout] =
    fileFormats.map(format => Layout(
      s"mor-verify/$format",
      s"a merge-on-read format-version 2 $format table with no partitioning that writes one data file per insert",
      table =>
        s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
          s"'write.format.default'='$format', 'format-version'='2', 'write.distribution-mode'='none', " +
          s"'write.delete.mode'='merge-on-read')"))

  lazy val cowVerifyLayouts: List[Layout] =
    fileFormats.map(format => Layout(
      s"cow-verify/$format",
      s"a copy-on-write format-version 2 $format table with no partitioning that writes one data file per insert",
      table =>
        s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
          s"'write.format.default'='$format', 'format-version'='2', 'write.distribution-mode'='none', " +
          s"'write.delete.mode'='copy-on-write')"))

  lazy val preparedMorCoreTables: List[TablePreparation[CoreTable.type]] =
    morLayouts.map(layout =>
      TablePreparation(
        layout.label,
        createAndSeed(layout, 3),
        description = s"Three seed rows with keys 1, 2 and 3 in ${layout.description}, " +
          "so a mutation writes position-delete files."))

  // Seed every row into ONE data file. A plain seed INSERT spreads the rows over a couple of files,
  // where a delete aligned with a file boundary is satisfied by dropping that whole file. The
  // `COALESCE(1)` hint forces a single write task and so a single data file, which makes a
  // strict-subset delete a partial-file match: merge-on-read writes a position delete for it, and
  // copy-on-write rewrites the data file.
  def createAndSeedSingleFile(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(layout.create)()
      .sql(s"seed($numberOfRows, one-file)")(table =>
        s"INSERT INTO $table SELECT /*+ COALESCE(1) */ * FROM (${RowGenerator.valuesClause(Core, numberOfRows)}) AS seed")(
        view => assert(view.after.size == numberOfRows,
          s"single-file seed expected $numberOfRows rows, got ${view.after.size}"))

  // Seed one data file on a merge-on-read layout, then delete a strict subset, which leaves a live
  // position-delete file. A table in this state exercises the scan path where the reader applies a
  // position delete, so the read cases run against rows that survive that filtering.
  def createAndSeedMorDeleted(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    createAndSeedSingleFile(layout, numberOfRows)
      .step("prep.morDelete") { (spark, table) =>
        spark.sql(s"DELETE FROM $table WHERE ${Core.long0.columnName} = 1")   // a strict subset, so Iceberg writes a position delete
      } { view =>
        assert(view.after.size == numberOfRows - 1, s"MoR prep delete failed: ${view.after.size}")
        val deleteFiles = view.spark.sql(s"SELECT count(*) FROM ${view.table}.all_delete_files").collect()(0).getLong(0)
        assert(deleteFiles == 1, s"MoR prep must leave a live position-delete file, got $deleteFiles")
      }

  lazy val preparedMorReadCoreTables: List[TablePreparation[CoreTable.type]] =
    morVerifyLayouts.map { layout =>
      TablePreparation(
        layout.label,
        createAndSeedMorDeleted(layout, 3),
        "prep.morRead:",
        description = s"Three seed rows written as one data file in ${layout.description}, then the " +
          "row with key 1 deleted merge-on-read, so keys 2 and 3 remain behind a live position-delete " +
          "file that the reader applies at scan time.")
    }

  // RTAS preparation on a MERGE-ON-READ table: the replace re-specifies the MoR delete, update, and
  // merge modes, so the mutation cases exercise the MoR write path on a replace-lineage table.
  protected def morPropsFmt(format: String) = s"'write.format.default'='$format', 'format-version'='2', " +
    "'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read'"

  def createAndSeedRtasMor(partitioning: Partitioning, numberOfRows: Int, format: String): TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource ${partitioning.clause} " +
        s"TBLPROPERTIES (${morPropsFmt(format)}, 'replace.enabled'='true')")()
      .insert(numberOfRows)()
      .sql("prep.rtasMor")(t => s"CREATE OR REPLACE TABLE $t USING $dataSource ${partitioning.clause} " +
        s"TBLPROPERTIES (${morPropsFmt(format)}) AS SELECT * FROM $t")()
      // The OpenHouse user guide requires REFRESH TABLE after a replace, so the Spark session
      // reads the committed metadata pointer before the preparation returns.
      .sql("prep.rtasMor.refresh")(t => s"REFRESH TABLE $t")()

  lazy val preparedRtasMorCoreTables: List[TablePreparation[CoreTable.type]] =
    fileFormats.map { format =>
      TablePreparation(
        s"mor-${unpartitioned.label}/$format",
        createAndSeedRtasMor(unpartitioned, 3, format),
        "prep.rtasMor:",
        description = s"Three seed rows with keys 1, 2 and 3 in a merge-on-read format-version 2 " +
          s"$format table ${unpartitioned.description}, then replaced by CREATE OR REPLACE TABLE AS " +
          "SELECT re-specifying the merge-on-read modes, so mutations run on replace lineage.")
    }

  lazy val preparedNullStringMorCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedMorCoreTables.map(withNullStringRow)

  lazy val preparedNullStringRtasMorCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedRtasMorCoreTables.map(withNullStringRow)

  lazy val morReadLayoutFormatPreparations: List[TablePreparation[CoreTable.type]] =
    preparedMorReadCoreTables

  def morReadLayoutFormatCases: List[Plan.Case] =
    layoutFormatCasesFor(morReadLayoutFormatPreparations)
}

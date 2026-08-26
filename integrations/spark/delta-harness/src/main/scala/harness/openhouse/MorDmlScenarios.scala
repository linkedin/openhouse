package harness

// The merge-on-read DML buckets. The mutation buckets are merge-on-read preparation lists crossed
// with the shared DML test-case lists that DmlScenarios names, so a merge-on-read table runs the
// same row-delta assertions as a copy-on-write one. The delete-file-mode bucket is the exception:
// it asserts the physical difference between the two write modes directly.
trait MorDmlScenarios extends MorScenarioKit { this: DmlScenarios =>
  import Rows._

  lazy val morDmlCases: List[Plan.Case] =
    preparedMorCoreTables.flatMap(preparation => rowMutationTestCases.map(_.runOn(preparation))) ++
      preparedNullStringMorCoreTables.flatMap(preparation =>
        nullStringRowTestCases.map(_.runOn(preparation)))

  lazy val rtasMorDmlCases: List[Plan.Case] =
    preparedRtasMorCoreTables.flatMap(preparation =>
      rowMutationTestCases.map(_.runOn(preparation))) ++
      preparedNullStringRtasMorCoreTables.flatMap(preparation =>
        nullStringRowTestCases.map(_.runOn(preparation)))

  lazy val morReadDmlCases: List[Plan.Case] =
    preparedMorReadCoreTables.flatMap(preparation => readTestCases.map(_.runOn(preparation)))

  // --- merge-on-read versus copy-on-write: prove the physical difference ---
  // The rest of the merge-on-read preparations reuse the row-delta assertions, which hold
  // identically whether the write was copy-on-write or merge-on-read. These two pin the physical
  // difference: a merge-on-read delete adds a position-delete file, a copy-on-write delete rewrites
  // the data file and adds none. Both are prepared with a single seed data file and delete a strict
  // subset (one of three rows), so the write is a partial-file match and the outcome is
  // deterministic across formats.

  private lazy val preparedSingleFileMorTables: List[TablePreparation[CoreTable.type]] =
    morVerifyLayouts.map(layout =>
      TablePreparation(
        layout.label,
        createAndSeedSingleFile(layout, 3),
        description = s"Three seed rows with keys 1, 2 and 3 written as one data file in " +
          s"${layout.description}."))

  private lazy val preparedSingleFileCowTables: List[TablePreparation[CoreTable.type]] =
    cowVerifyLayouts.map(layout =>
      TablePreparation(
        layout.label,
        createAndSeedSingleFile(layout, 3),
        description = s"Three seed rows with keys 1, 2 and 3 written as one data file in " +
          s"${layout.description}."))

  private lazy val morWritesDeleteFiles: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "mor.writesDeleteFiles",
      s"A merge-on-read DELETE WHERE ${Core.long0.columnName} < 2 against a single data file removes " +
        "the matching row, records the removal in at least one position-delete file, and commits one " +
        "snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} < 2")
        val after = table.state
        val deleteFileCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}.delete_files")
          .collect()(0)
          .getLong(0)

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) < 2),
          s"strict-subset DELETE returned an unexpected row set: ${after.rows}")
        assert(
          deleteFileCount >= 1,
          "merge-on-read DELETE should write a position-delete file")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a merge-on-read DELETE commits one snapshot")
      })

  private lazy val cowWritesNoDeleteFiles: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "cow.writesNoDeleteFiles",
      s"A copy-on-write DELETE WHERE ${Core.long0.columnName} < 2 against a single data file removes " +
        "the matching row by rewriting that file, leaves the table with no delete files, and commits " +
        "one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} < 2")
        val after = table.state
        val deleteFileCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}.delete_files")
          .collect()(0)
          .getLong(0)

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) < 2),
          s"strict-subset DELETE returned an unexpected row set: ${after.rows}")
        assert(
          deleteFileCount == 0,
          "copy-on-write DELETE should not write delete files")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a copy-on-write DELETE commits one snapshot")
      })

  lazy val deleteFileModeCases: List[Plan.Case] =
    preparedSingleFileMorTables.map(morWritesDeleteFiles.runOn) ++
      preparedSingleFileCowTables.map(cowWritesNoDeleteFiles.runOn)
}

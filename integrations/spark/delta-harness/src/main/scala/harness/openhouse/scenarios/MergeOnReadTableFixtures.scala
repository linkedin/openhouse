package harness

/**
 * The merge-on-read starting states.
 *
 * A merge-on-read table is format version 2 whose delete, update and merge modes are merge-on-read, so a mutation
 * records position-delete files beside the data files it matched and leaves those data files in place. Copy-on-write
 * rewrites the matched data file instead. That physical difference is the whole subject of this layer, so these
 * fixtures supply the starting states that put a table on one write path or the other and leave the operations to the
 * foundation.
 *
 * Several families need a table whose delete is a partial-file match, because a delete aligned with a whole data file
 * is satisfied by dropping that file on either write path and the two modes become indistinguishable. The verify
 * layouts seed through a single write task so all three rows land in one data file, which makes a strict-subset
 * delete a partial-file match and the physical outcome deterministic in both formats.
 *
 * The members are lazy so they initialize on first read, after every trait mixed into `object Scenarios` has been
 * constructed.
 */
trait MergeOnReadTableFixtures extends RtasTableFixtures {

  /** Every merge-on-read layout: each file format crossed with each partitioning. */
  lazy val mergeOnReadLayouts: List[Layout] =
    for {
      format       <- fileFormats
      partitioning <- partitionings
    } yield mergeOnReadLayout(partitioning, format)

  /**
   * One merge-on-read layout per file format that pins how a mutation is written physically. It carries all three
   * merge-on-read modes, so an UPDATE and a MERGE take the same write path a DELETE does, and it sets
   * write.distribution-mode to none while staying unpartitioned, so a single seed INSERT lands every row in one data
   * file and a strict-subset mutation is a partial-file match that Iceberg satisfies with a position delete.
   */
  lazy val mergeOnReadVerifyLayouts: List[Layout] =
    fileFormats.map(format =>
      Layout(
        s"mor-verify/$format",
        table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
            s"${mergeOnReadProperties(format)}, 'write.distribution-mode'='none')"))

  /**
   * The copy-on-write counterpart of `mergeOnReadVerifyLayouts`, identical except that all three modes are
   * copy-on-write, so the pair isolates the write mode as the only difference between the two physical outcomes.
   */
  lazy val copyOnWriteVerifyLayouts: List[Layout] =
    fileFormats.map(format =>
      Layout(
        s"cow-verify/$format",
        table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
            s"${copyOnWriteProperties(format)}, 'write.distribution-mode'='none')"))

  /** One preparation per merge-on-read layout: created, then seeded with the standard rows. */
  lazy val preparedMergeOnReadCoreTables: List[TablePreparation[CoreTable.type]] =
    mergeOnReadLayouts.map(layout =>
      TablePreparation(
        layout.label,
        createCoreTable(layout).insert(standardSeedRowCount)(),
        mergeOnReadCasePrefix))

  /** The merge-on-read core preparations, each carrying one row whose string column is null. */
  lazy val preparedNullStringMergeOnReadCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedMergeOnReadCoreTables.map(withNullStringRow)

  /**
   * One replace-lineage merge-on-read preparation per file format: the standard seed in an unpartitioned table,
   * re-specified in place by CREATE OR REPLACE TABLE AS SELECT that restates the merge-on-read modes, then refreshed.
   * A mutation on the result runs on replace lineage and the merge-on-read write path at once, which is the one
   * direct dependency this layer has on its parent.
   */
  lazy val preparedReplacedMergeOnReadCoreTables: List[TablePreparation[CoreTable.type]] =
    fileFormats.map(format =>
      TablePreparation(
        s"mor-${unpartitioned.label}/$format",
        replaceLineageMergeOnRead(unpartitioned, format),
        replacedMergeOnReadCasePrefix))

  /** The replace-lineage merge-on-read preparations, each carrying one row whose string column is null. */
  lazy val preparedNullStringReplacedMergeOnReadCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedReplacedMergeOnReadCoreTables.map(withNullStringRow)

  /**
   * One preparation per merge-on-read verify layout: three seed rows in one data file, then the row with key 1
   * deleted merge-on-read, so keys 2 and 3 remain behind a live position-delete file the reader applies at scan time.
   */
  lazy val preparedDeletedMergeOnReadTables: List[TablePreparation[CoreTable.type]] =
    preparedDeletedTables(mergeOnReadVerifyLayouts, deletedMergeOnReadCasePrefix)

  /** The prefix that marks a case ID as running on a merge-on-read table. */
  val mergeOnReadCasePrefix: String = "prep.mor:"

  /** The prefix that marks a case ID as running on a merge-on-read table reached through a replace. */
  val replacedMergeOnReadCasePrefix: String = "prep.rtasMor:"

  /** The prefix that marks a case ID as running on a table that already carries a live position-delete file. */
  val deletedMergeOnReadCasePrefix: String = "prep.morRead:"

  // --- the layouts, seeds and starting states the merge-on-read families build on ---

  /**
   * One merge-on-read layout: a format-version 2 table whose delete, update and merge modes are merge-on-read, so a
   * mutation records its change in position-delete files and leaves the untouched data files in place.
   */
  private def mergeOnReadLayout(partitioning: Partitioning, format: String): Layout =
    Layout(
      s"mor-${partitioning.label}/$format",
      table =>
        s"CREATE TABLE $table ($columnDefinitions) USING $dataSource ${partitioning.clause} " +
          s"TBLPROPERTIES (${mergeOnReadProperties(format)})")

  /**
   * The merge-on-read table property fragment for `format`: format-version 2 with the delete, update and merge modes
   * all set to merge-on-read.
   */
  protected def mergeOnReadProperties(format: String): String =
    s"'write.format.default'='$format', 'format-version'='2', " +
      "'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', " +
      "'write.merge.mode'='merge-on-read'"

  /**
   * The copy-on-write table property fragment for `format`: format-version 2 with the delete, update and merge modes
   * all set to copy-on-write, so a mutation rewrites the data file it matched.
   */
  protected def copyOnWriteProperties(format: String): String =
    s"'write.format.default'='$format', 'format-version'='2', " +
      "'write.delete.mode'='copy-on-write', 'write.update.mode'='copy-on-write', " +
      "'write.merge.mode'='copy-on-write'"

  /** The three properties that decide which write path a mutation takes. */
  val writeModePropertyNames: List[String] =
    List("write.delete.mode", "write.update.mode", "write.merge.mode")

  /**
   * Creates the table under `layout`, then seeds the standard rows through a single write task so they land in one
   * data file. The COALESCE(1) hint is what forces the single file, which keeps a strict-subset delete a partial-file
   * match: merge-on-read writes a position delete for it, and copy-on-write rewrites the data file.
   */
  protected def singleFileSeed(layout: Layout): TableTest[CoreTable.type] =
    createCoreTable(layout)
      .sql(s"seed($standardSeedRowCount, one-file)")(table =>
        s"INSERT INTO $table SELECT /*+ COALESCE(1) */ * FROM " +
          s"(${RowGenerator.valuesClause(Core, standardSeedRowCount)}) AS seed")(view =>
        assert(
          view.after.size == standardSeedRowCount,
          s"the single-file seed lands $standardSeedRowCount rows, found ${view.after.size}"))

  /**
   * Seeds the standard rows into one data file, then deletes the row with key 1. The table holds keys 2 and 3 behind
   * a live position-delete file, which is the state every coexistence family starts from and the one a delete file
   * makes reachable.
   */
  protected def deletedMergeOnReadLineage(layout: Layout): TableTest[CoreTable.type] =
    singleFileSeed(layout)
      .step("prep.morDelete")((spark, table) =>
        spark.sql(s"DELETE FROM $table WHERE ${Core.long0.columnName} = 1"))(view => {
        assert(
          view.after.size == standardSeedRowCount - 1,
          s"the preparation delete leaves ${standardSeedRowCount - 1} rows, found ${view.after.size}")
        assert(
          currentDeleteFileCount(view.spark, view.table) == 1,
          s"the preparation leaves one live position-delete file, found " +
            s"${currentDeleteFileCount(view.spark, view.table)}")
      })

  /** One preparation per layout given: three seed rows in one data file, with key 1 deleted merge-on-read. */
  protected def preparedDeletedTables(
      layouts: List[Layout],
      casePrefix: String): List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(layout.label, deletedMergeOnReadLineage(layout), casePrefix))

  /**
   * The number of delete files the table's current snapshot references, which is what a reader applies at scan time.
   * Every assertion about the table as it stands now reads this.
   */
  protected def currentDeleteFileCount(
      spark: org.apache.spark.sql.SparkSession,
      table: String): Long =
    spark.sql(s"SELECT count(*) FROM $table.delete_files").collect()(0).getLong(0)

  /**
   * The snapshot the table's main branch currently reads from, read from the refs metadata table, which names exactly
   * one snapshot per branch.
   */
  protected def currentSnapshotId(
      spark: org.apache.spark.sql.SparkSession,
      table: String): Long =
    spark
      .sql(s"SELECT snapshot_id FROM $table.refs WHERE name = 'main'")
      .collect()
      .toSeq
      .map(_.getLong(0)) match {
      case Seq(snapshotId) => snapshotId
      case mainSnapshotIds =>
        throw new AssertionError(s"main names one snapshot, found $mainSnapshotIds")
    }

  /** The snapshot IDs the table still retains. */
  protected def retainedSnapshotIds(
      spark: org.apache.spark.sql.SparkSession,
      table: String): Seq[Long] =
    spark
      .sql(s"SELECT snapshot_id FROM $table.snapshots")
      .collect()
      .toSeq
      .map(_.getLong(0))

  /** The manifest paths the table's current snapshot references, for the given manifest content code. */
  protected def currentManifestPaths(
      spark: org.apache.spark.sql.SparkSession,
      table: String,
      manifestContent: Int): Set[String] =
    spark
      .sql(s"SELECT path FROM $table.manifests WHERE content = $manifestContent")
      .collect()
      .toSeq
      .map(_.getString(0))
      .toSet

  /** The manifest content code for the manifests that list data files. */
  protected val dataManifestContent: Int = 0

  /** The manifest content code for the manifests that list delete files. */
  protected val deleteManifestContent: Int = 1

  /**
   * The data-file paths the table's current snapshot references. The `files` metadata table lists delete files
   * alongside data files, so the content code selects the data files on their own.
   */
  protected def currentDataFilePaths(
      spark: org.apache.spark.sql.SparkSession,
      table: String): Set[String] =
    spark
      .sql(s"SELECT file_path FROM $table.files WHERE content = $dataFileContent")
      .collect()
      .toSeq
      .map(_.getString(0))
      .toSet

  /** The number of data files the table's current snapshot references. */
  protected def currentDataFileCount(
      spark: org.apache.spark.sql.SparkSession,
      table: String): Long =
    currentDataFilePaths(spark, table).size.toLong

  /** The file content code for a data file, as the `files` metadata table reports it. */
  protected val dataFileContent: Int = 0

  /** The persisted value of `propertyName`, which is what the table is actually configured with. */
  protected def persistedProperty(
      spark: org.apache.spark.sql.SparkSession,
      table: String,
      propertyName: String): Option[String] =
    tableProperties(spark, table).get(propertyName)

  /** The live keys the table reads back, in key order, with every position delete applied. */
  protected def liveKeys(spark: org.apache.spark.sql.SparkSession, table: String): Seq[Long] =
    spark
      .sql(s"SELECT ${Core.long0.columnName} FROM $table ORDER BY ${Core.long0.columnName}")
      .collect()
      .toSeq
      .map(_.getLong(0))

  /** The standard seed written as one data file in a merge-on-read table in `format`. */
  protected def preparedSingleFileMergeOnReadTable(
      format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      singleFileSeed(
        Layout(
          format,
          table =>
            s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
              s"${mergeOnReadProperties(format)})")))

  /** The standard seed written as one data file in a copy-on-write table in `format`. */
  protected def preparedSingleFileCopyOnWriteTable(
      format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      singleFileSeed(
        Layout(
          format,
          table =>
            s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
              s"${copyOnWriteProperties(format)})")))

  /** The standard seed in a merge-on-read table in `format`, labelled so its IDs name the write mode they ran on. */
  protected def preparedMergeOnReadTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      s"mor/$format",
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES (${mergeOnReadProperties(format)})")()
        .insert(standardSeedRowCount)())

  /**
   * Creates a replace-lineage merge-on-read table: the standard seed, re-specified in place by CREATE OR REPLACE
   * TABLE AS SELECT restating the merge-on-read modes, then refreshed so the Spark session reads the committed
   * metadata pointer. Each step validates the state it leaves, so a mutation case that runs on the result starts from
   * a known baseline.
   */
  private def replaceLineageMergeOnRead(
      partitioning: Partitioning,
      format: String): TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(table =>
        s"CREATE TABLE $table ($columnDefinitions) USING $dataSource ${partitioning.clause} " +
          s"TBLPROPERTIES (${mergeOnReadProperties(format)}, 'replace.enabled'='true')")()
      .insert(standardSeedRowCount)()
      .sql("prep.rtasMor")(table =>
        s"CREATE OR REPLACE TABLE $table USING $dataSource ${partitioning.clause} " +
          s"TBLPROPERTIES (${mergeOnReadProperties(format)}) AS SELECT * FROM $table")(view => {
        assertSeededMergeOnReadShape(view, "prep.rtasMor")
        assert(
          view.snapshotsAfter == view.snapshotsBefore + 1,
          s"prep.rtasMor commits one snapshot, went from ${view.snapshotsBefore} to " +
            s"${view.snapshotsAfter}")
      })
      .sql("prep.rtasMor.refresh")(table => s"REFRESH TABLE $table")(view => {
        assertSeededMergeOnReadShape(view, "prep.rtasMor.refresh")
        assert(
          view.snapshotsAfter == view.snapshotsBefore,
          s"prep.rtasMor.refresh reads committed metadata and commits nothing, went from " +
            s"${view.snapshotsBefore} to ${view.snapshotsAfter} snapshots")
      })

  /**
   * The state both replace-lineage steps leave behind: the standard seed rows in key order, unchanged by the step,
   * under exactly the core columns, on a table still configured merge-on-read. Asserting it here means a mutation
   * case always compares against a known baseline.
   */
  private def assertSeededMergeOnReadShape(
      view: StepView[CoreTable.type],
      stepLabel: String): Unit = {
    val schemaColumnNames = view.spark.table(view.table).schema.fieldNames.toSeq
    val configuredWriteModes = writeModePropertyNames.map(propertyName =>
      propertyName -> persistedProperty(view.spark, view.table, propertyName))

    assert(
      schemaColumnNames == Core.columnNames,
      s"$stepLabel presents the core schema, found $schemaColumnNames")
    assert(
      view.after == view.before,
      s"$stepLabel keeps every row it started from, went from ${view.before} to ${view.after}")
    assert(
      view.after.size == standardSeedRowCount,
      s"$stepLabel holds the $standardSeedRowCount standard seed rows, found ${view.after.size}")
    assert(
      view.after.map(row => Rows.TypedRow(row).get(Core.long0)) ==
        (1L to standardSeedRowCount.toLong).toList,
      s"$stepLabel holds the standard seed keys, found " +
        s"${view.after.map(row => Rows.TypedRow(row).get(Core.long0))}")
    assert(
      configuredWriteModes == writeModePropertyNames.map(_ -> Some("merge-on-read")),
      s"$stepLabel keeps every write mode on the merge-on-read path, found $configuredWriteModes")
  }

}

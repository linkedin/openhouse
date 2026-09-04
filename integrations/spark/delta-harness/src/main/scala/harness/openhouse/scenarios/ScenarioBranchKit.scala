package harness

import org.apache.spark.sql.{Row, SparkSession}

/**
 * The branch and write-audit-publish starting states, and the small set of primitives the families above share.
 *
 * A named reference is the subject of this layer. A table carries one reference per branch and one per tag; `main` is
 * the reference every ordinary read and write resolves to. A branch names its own snapshot, so a write aimed at a
 * branch advances that branch's snapshot and leaves `main` on the snapshot it already named. Write-audit-publish is
 * the second subject: a table with `write.wap.enabled` commits a write made under `spark.wap.id` as a snapshot that
 * no reference names, so the write is durable and stays out of the table every reader of `main` sees until a publish
 * moves `main` onto it.
 *
 * Every case here owns a freshly generated table, so a reference name written inside a case is unique to that case
 * and a case creates each reference it names. The preparations create their references through the same statements a
 * case would, and each one proves the reference set, the snapshot each reference names, the rows each reference
 * reads, the column order and its own configuration before any case runs on it.
 *
 * The members are lazy so they initialize on first read, after every trait mixed into `object Scenarios` has been
 * constructed.
 */
trait ScenarioBranchKit extends ScenarioMergeOnReadKit {

  /** The branch every branch preparation creates and the branch families write through. */
  val auditBranchName: String = "audit"

  /** The reference type the `refs` metadata table reports for a branch. */
  val branchReferenceType: String = "BRANCH"

  /** The reference type the `refs` metadata table reports for a tag. */
  val tagReferenceType: String = "TAG"

  /** The reference every ordinary read and write resolves to. */
  val mainBranchName: String = "main"

  /** The table property that lets a write made under `spark.wap.id` commit a snapshot no reference names. */
  val writeAuditPublishProperty: String = "write.wap.enabled"

  /** The session setting that stages a write under one identifier. */
  val writeAuditPublishIdSetting: String = "spark.wap.id"

  /** The session setting that routes a session's reads and writes at one branch. */
  val writeAuditPublishBranchSetting: String = "spark.wap.branch"

  // --- the starting states the branch and write-audit-publish families build on ---

  /**
   * One branched preparation per columnar format: the standard seed with branch `audit` created at the snapshot
   * `main` names, so a case starts from two references over one snapshot and any divergence between them is the
   * case's own doing.
   */
  lazy val preparedBranchedTables: List[TablePreparation[CoreTable.type]] =
    fileFormats.map(branchedTable)

  /**
   * One routed branched preparation per columnar format: the standard seed with `write.wap.enabled` set and branch
   * `audit` created, so a case can route a session at the branch through `spark.wap.branch`, which the catalog
   * accepts on a write-audit-publish table.
   */
  lazy val preparedRoutedBranchTables: List[TablePreparation[CoreTable.type]] =
    fileFormats.map(routedBranchTable)

  /**
   * One branched two-snapshot preparation per columnar format: the parent's two-snapshot table with
   * `write.wap.enabled` set and branch `audit` created at the second snapshot, so a case has one snapshot from before
   * the branch point to travel to and a branch to diverge from it.
   */
  lazy val preparedBranchedTwoSnapshotTables: List[TablePreparation[CoreTable.type]] =
    fileFormats.map(branchedTwoSnapshotTable)

  /**
   * One branched merge-on-read preparation per columnar format: the parent's verify layout seeded into a single data
   * file with branch `audit` created, so a strict-subset mutation on the branch is a partial-file match and the
   * catalog answers it with a position-delete file.
   */
  lazy val preparedBranchedMergeOnReadTables: List[TablePreparation[CoreTable.type]] =
    mergeOnReadVerifyLayouts.map(layout =>
      TablePreparation(
        layout.label,
        singleFileSeed(layout)
          .sql("prep.createAuditBranch")(table =>
            s"ALTER TABLE $table CREATE BRANCH $auditBranchName")(view =>
            assertBranchedShape(view, "prep.createAuditBranch"))))

  /**
   * One write-audit-publish preparation per columnar format: the standard seed with `write.wap.enabled` set
   * afterwards, so a write made under `spark.wap.id` commits a snapshot no reference names.
   */
  lazy val preparedWriteAuditPublishTables: List[TablePreparation[CoreTable.type]] =
    fileFormats.map(writeAuditPublishTable)

  /**
   * One declared-at-CREATE preparation per columnar format: the standard seed in a table that named
   * `write.wap.enabled` in its CREATE statement, so a case reads the flag back from a table that never altered it.
   */
  lazy val preparedWriteAuditPublishAtCreateTables: List[TablePreparation[CoreTable.type]] =
    fileFormats.map(writeAuditPublishAtCreateTable)

  /** One two-snapshot preparation per columnar format, which the parent owns and this layer branches from. */
  lazy val preparedTwoSnapshotTables: List[TablePreparation[CoreTable.type]] =
    fileFormats.map(preparedTwoSnapshotTable)

  // --- the statements each starting state is built from ---

  private def branchedTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      seededCoreTable(format)
        .sql("prep.createAuditBranch")(table =>
          s"ALTER TABLE $table CREATE BRANCH $auditBranchName")(view =>
          assertBranchedShape(view, "prep.createAuditBranch")))

  private def routedBranchTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      seededCoreTable(format)
        .sql("prep.enableWriteAuditPublish")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('$writeAuditPublishProperty'='true')")(view =>
          assertWriteAuditPublishEnabled(view, "prep.enableWriteAuditPublish"))
        .sql("prep.createAuditBranch")(table =>
          s"ALTER TABLE $table CREATE BRANCH $auditBranchName")(view =>
          assertBranchedShape(view, "prep.createAuditBranch")))

  private def branchedTwoSnapshotTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      preparedTwoSnapshotTable(format).preparation
        .sql("prep.enableWriteAuditPublish")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('$writeAuditPublishProperty'='true')")(view =>
          assertWriteAuditPublishEnabled(view, "prep.enableWriteAuditPublish"))
        .sql("prep.createAuditBranch")(table =>
          s"ALTER TABLE $table CREATE BRANCH $auditBranchName")(view =>
          assertBranchedShape(view, "prep.createAuditBranch")))

  private def writeAuditPublishTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      seededCoreTable(format)
        .sql("prep.enableWriteAuditPublish")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('$writeAuditPublishProperty'='true')")(view =>
          assertWriteAuditPublishEnabled(view, "prep.enableWriteAuditPublish")))

  private def writeAuditPublishAtCreateTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
            s"'write.format.default'='$format', '$writeAuditPublishProperty'='true')")()
        .insert(standardSeedRowCount)(view => {
          assert(
            persistedProperty(view.spark, view.table, writeAuditPublishProperty).contains("true"),
            s"a table that declared $writeAuditPublishProperty at CREATE reads it back as true, found " +
              s"${persistedProperty(view.spark, view.table, writeAuditPublishProperty)}")
          assert(
            keysOf(view.after) == (1L to standardSeedRowCount.toLong).toList,
            s"the seed holds the standard keys, found ${keysOf(view.after)}")
          assert(
            schemaColumnNames(view.spark, view.table) == Core.columnNames,
            s"the seed presents the core schema, found ${schemaColumnNames(view.spark, view.table)}")
          assert(
            referenceNames(view.spark, view.table) == Seq(mainBranchName),
            s"the seed leaves $mainBranchName as the only reference, found " +
              s"${referenceNames(view.spark, view.table)}")
        }))

  // --- the session scopes a case writes through ---

  /**
   * Runs `write` with `spark.wap.id` set to `writeAuditPublishId`, then clears the setting on every outcome, so the
   * commit `write` makes is staged under that identifier and every later statement in the case commits normally.
   */
  protected def stagingUnder[T](spark: SparkSession, writeAuditPublishId: String)(write: => T): T = {
    spark.conf.set(writeAuditPublishIdSetting, writeAuditPublishId)
    try write
    finally spark.conf.unset(writeAuditPublishIdSetting)
  }

  /**
   * Runs `body` with `spark.wap.branch` set to `branch`, then clears the setting on every outcome, so the reads and
   * writes `body` performs resolve at that branch and every later statement in the case resolves at `main`.
   */
  protected def routedAt[T](spark: SparkSession, branch: String)(body: => T): T = {
    spark.conf.set(writeAuditPublishBranchSetting, branch)
    try body
    finally spark.conf.unset(writeAuditPublishBranchSetting)
  }

  /**
   * Runs `use` while the case owns reference `reference` on `table`. `create` issues the statement that adds the
   * reference and ownership starts the moment it returns, so a name that is already taken leaves the existing
   * reference intact and the DROP afterwards removes only the reference this call created. `table` is read again at
   * cleanup time, so a case that renames the table underneath drops the reference through the name the table
   * currently answers to. A failure in `create` or in `use` stays the primary failure and a cleanup failure rides
   * along as a suppressed exception.
   */
  private[harness] def withOwnedReference(
      runStatement: String => Unit,
      table: => String,
      referenceType: String,
      reference: String)(create: => Unit)(use: => Unit): Unit =
    OwnedTableLifecycle.withOwnership(
      runStatement(s"ALTER TABLE $table DROP $referenceType $reference")) { markReferenceCreated =>
      create
      markReferenceCreated()
      use
    }

  // --- the reference, row and snapshot lookups every branch family reads ---

  /** Every reference the table carries, as name, type and the snapshot it names, in name order. */
  protected def referenceEntries(spark: SparkSession, table: String): Seq[(String, String, Long)] =
    spark
      .sql(s"SELECT name, type, snapshot_id FROM $table.refs ORDER BY name")
      .collect()
      .toSeq
      .map(row => (row.getString(0), row.getString(1), row.getLong(2)))

  /** The names of the references the table carries, in name order. */
  protected def referenceNames(spark: SparkSession, table: String): Seq[String] =
    referenceEntries(spark, table).map { case (name, _, _) => name }

  /** The snapshot `reference` names. The `refs` metadata table names exactly one snapshot per reference. */
  protected def referenceSnapshotId(spark: SparkSession, table: String, reference: String): Long =
    referenceEntries(spark, table).collect {
      case (name, _, snapshotId) if name == reference => snapshotId
    } match {
      case Seq(snapshotId) => snapshotId
      case named           => throw new AssertionError(s"$reference names one snapshot, found $named")
    }

  /** The rows `reference` reads, in key order, under exactly the core columns. */
  protected def rowsOn(spark: SparkSession, table: String, reference: String): Seq[Row] =
    spark
      .sql(
        s"SELECT $columnNameList FROM $table VERSION AS OF '$reference' " +
          s"ORDER BY ${Core.long0.columnName}")
      .collect()
      .toSeq

  /** The keys `rows` hold, in the order the rows are given. */
  protected def keysOf(rows: Seq[Row]): Seq[Long] =
    rows.map(row => Rows.TypedRow(row).get(Core.long0))

  /** The column names the table presents, in schema order. */
  protected def schemaColumnNames(spark: SparkSession, table: String): Seq[String] =
    spark.table(table).schema.fieldNames.toSeq

  /** The snapshot a snapshot descends from, which is what a publish and a fast-forward move `main` along. */
  protected def parentSnapshotId(spark: SparkSession, table: String, snapshotId: Long): Option[Long] =
    spark
      .sql(s"SELECT parent_id FROM $table.snapshots WHERE snapshot_id = $snapshotId")
      .collect()
      .toSeq
      .map(row => if (row.isNullAt(0)) None else Some(row.getLong(0))) match {
      case Seq(parent) => parent
      case parents     => throw new AssertionError(s"snapshot $snapshotId has one row, found $parents")
    }

  /** One core row in the seed shape, keyed by `key` and tagged in the string column, as the reader returns it. */
  protected def expectedCoreRow(key: Long, tag: String): Row =
    Row(key, key.toInt, tag, key.toDouble + 0.5, key % 2 == 0, Core.dateLiteral(key.toInt))

  /**
   * The data-file paths a read through `reference` scans, read from the `_file` metadata column of that read. The
   * paths come from the reference's own scan, so they are the data files the snapshot that reference names owns.
   */
  protected def scannedDataFilePathsOn(spark: SparkSession, table: String, reference: String): Set[String] =
    spark
      .sql(s"SELECT DISTINCT _file FROM $table.branch_$reference")
      .collect()
      .toSeq
      .map(_.getString(0))
      .toSet

  /** Every delete-file path the table has ever referenced, which spans the snapshots each reference names. */
  protected def allDeleteFilePaths(spark: SparkSession, table: String): Set[String] =
    spark
      .sql(s"SELECT file_path FROM $table.all_delete_files")
      .collect()
      .toSeq
      .map(_.getString(0))
      .toSet

  /** The snapshots a commit staged under `writeAuditPublishId` produced, oldest first. */
  protected def stagedSnapshotIds(spark: SparkSession, table: String, writeAuditPublishId: String): Seq[Long] =
    spark
      .sql(
        s"SELECT snapshot_id FROM $table.snapshots WHERE summary['wap.id'] = " +
          s"'$writeAuditPublishId' ORDER BY committed_at")
      .collect()
      .toSeq
      .map(_.getLong(0))

  /** The one snapshot a commit staged under `writeAuditPublishId` produced. */
  protected def stagedSnapshotId(spark: SparkSession, table: String, writeAuditPublishId: String): Long =
    stagedSnapshotIds(spark, table, writeAuditPublishId) match {
      case Seq(snapshotId) => snapshotId
      case staged =>
        throw new AssertionError(s"$writeAuditPublishId stages one snapshot, found $staged")
    }

  /** Moves `main` onto `snapshotId` through cherrypick_snapshot, which is the publish a snapshot identifier reaches. */
  protected def cherryPick(spark: SparkSession, table: String, snapshotId: Long): Seq[Row] =
    spark
      .sql(s"CALL openhouse.system.cherrypick_snapshot('${catalogRelative(table)}', ${snapshotId}L)")
      .collect()
      .toSeq

  /** Moves `main` onto the snapshot staged under `writeAuditPublishId` through publish_changes. */
  protected def publishChanges(spark: SparkSession, table: String, writeAuditPublishId: String): Seq[Row] =
    spark
      .sql(
        s"CALL openhouse.system.publish_changes(table => '${catalogRelative(table)}', " +
          s"wap_id => '$writeAuditPublishId')")
      .collect()
      .toSeq

  /** Moves `target` onto the snapshot `source` names through fast_forward. */
  protected def fastForward(spark: SparkSession, table: String, target: String, source: String): Seq[Row] =
    spark
      .sql(s"CALL openhouse.system.fast_forward('${catalogRelative(table)}', '$target', '$source')")
      .collect()
      .toSeq

  /** Expires every snapshot no reference protects, keeping the newest one. */
  protected def expireUnreferencedSnapshots(spark: SparkSession, table: String): Seq[Row] =
    spark
      .sql(
        s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', " +
          "older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
      .collect()
      .toSeq

  // --- the shape each preparation proves before a case runs on it ---

  /** The standard seed in `format`: an unpartitioned core table, created and seeded with the standard rows. */
  private def seededCoreTable(format: String): TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(table => coreCreate(table, format))()
      .insert(standardSeedRowCount)()

  /**
   * The state a branch-creating preparation step leaves: `main` and `audit` both name the snapshot `main` already
   * named, the step commits no snapshot, both references read the rows the table already held in key order, and the
   * table presents the core columns in their declared order.
   */
  private def assertBranchedShape(view: StepView[CoreTable.type], stepLabel: String): Unit = {
    val mainSnapshotId = referenceSnapshotId(view.spark, view.table, mainBranchName)

    assert(
      view.snapshotsAfter == view.snapshotsBefore,
      s"$stepLabel names an existing snapshot and commits none, went from ${view.snapshotsBefore} " +
        s"to ${view.snapshotsAfter} snapshots")
    assert(
      referenceEntries(view.spark, view.table) ==
        Seq(
          (auditBranchName, branchReferenceType, mainSnapshotId),
          (mainBranchName, branchReferenceType, mainSnapshotId)),
      s"$stepLabel leaves $auditBranchName and $mainBranchName on one snapshot, found " +
        s"${referenceEntries(view.spark, view.table)}")
    assert(
      view.after == view.before,
      s"$stepLabel keeps every row the table held, went from ${view.before} to ${view.after}")
    assert(
      rowsOn(view.spark, view.table, auditBranchName) == view.after,
      s"$stepLabel leaves $auditBranchName reading the rows $mainBranchName reads, found " +
        s"${rowsOn(view.spark, view.table, auditBranchName)}")
    assert(
      schemaColumnNames(view.spark, view.table) == Core.columnNames,
      s"$stepLabel presents the core schema, found ${schemaColumnNames(view.spark, view.table)}")
  }

  /**
   * The state a write-audit-publish preparation step leaves: the flag reads `true`, the step commits no snapshot and
   * keeps every row the table held, the table presents the core columns in their declared order, and `main` is the
   * only reference the table carries.
   */
  private def assertWriteAuditPublishEnabled(view: StepView[CoreTable.type], stepLabel: String): Unit = {
    assert(
      persistedProperty(view.spark, view.table, writeAuditPublishProperty).contains("true"),
      s"$stepLabel persists $writeAuditPublishProperty as true, found " +
        s"${persistedProperty(view.spark, view.table, writeAuditPublishProperty)}")
    assert(
      view.snapshotsAfter == view.snapshotsBefore,
      s"$stepLabel changes table metadata and commits no snapshot, went from " +
        s"${view.snapshotsBefore} to ${view.snapshotsAfter} snapshots")
    assert(
      view.after == view.before,
      s"$stepLabel keeps every row the table held, went from ${view.before} to ${view.after}")
    assert(
      schemaColumnNames(view.spark, view.table) == Core.columnNames,
      s"$stepLabel presents the core schema, found ${schemaColumnNames(view.spark, view.table)}")
    assert(
      referenceNames(view.spark, view.table) == Seq(mainBranchName),
      s"$stepLabel leaves $mainBranchName as the only reference, found " +
        s"${referenceNames(view.spark, view.table)}")
  }

}

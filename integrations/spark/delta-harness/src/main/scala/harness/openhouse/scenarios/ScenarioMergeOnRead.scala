package harness

/**
 * Merge-on-read: what changes when a mutation records a position-delete file beside the data file it matched and
 * leaves that data file in place.
 *
 * The reusable DML contract has to hold identically on both write paths, because a caller writes the same SQL either
 * way. Everything else in this file is behavior only a merge-on-read table has: the physical delete file itself, the
 * surface a table reaches once data files and a live delete file sit side by side, the metadata that exposes the
 * delete, the changelog's ability to decode it, the history that spans it, and the maintenance procedures that fold
 * or carry it. Maintenance is large enough to review on its own, so it lives in ScenarioMergeOnReadMaintenance and
 * joins the one contribution this layer names.
 *
 * Operations, DML: the row-mutating operations `ScenarioDml` defines, its null-string DELETE and its reads, reused as
 * data. A merge-on-read table runs the same statements and the same row and snapshot delta assertions as a
 * copy-on-write one, so this file holds one definition of each preparation and none of each operation.
 *
 * Operations, merge-on-read contract: 26 focused families. Nineteen live here and cover the physical delete file
 * against its copy-on-write counterpart, a mode change applied partway through a table's life, the six operations
 * that run once a delete file is live, the position_deletes metadata table, the three changelog operations a scan
 * decodes and the two it reports as unsupported, format materialization with a delete file present, the delete-file
 * replication property, and time travel and rollback across the delete. Seven more live in
 * ScenarioMergeOnReadMaintenance.
 *
 * Preparation axes: the write mode is the axis this layer adds. Four merge-on-read layouts cross the two columnar
 * formats with unpartitioned and date-partitioned tables; two replace-lineage merge-on-read layouts put the same
 * mutations on a table that also went through a replace, which is this layer's one dependency on its parent; and two
 * verify layouts per write mode seed into a single data file so a strict-subset delete is a partial-file match and
 * the physical outcome is deterministic.
 *
 * Case families: 320 cases. The DML axis contributes 268 in three families, and the merge-on-read contract
 * contributes 52 in 26 families: 38 in the 19 families here and 14 in the 7 maintenance families.
 */
trait ScenarioMergeOnRead extends ScenarioMergeOnReadMaintenance {
  this: ScenarioDml with ScenarioStandardDml with ChangelogSupport =>

  /** Every merge-on-read case: the reusable DML operations on merge-on-read tables, then the write-mode contract. */
  lazy val mergeOnReadCases: List[TestCase] =
    mergeOnReadDmlCases ++ mergeOnReadContractCases ++ mergeOnReadMaintenanceCases

  /**
   * The reusable DML operations on merge-on-read tables: every row-mutating operation on the four merge-on-read
   * preparations and the two replace-lineage ones, the null-string DELETE on their null-string forms, and the reads
   * on the preparations that already carry a live position-delete file.
   */
  lazy val mergeOnReadDmlCases: List[TestCase] =
    mergeOnReadCoreDmlCases ++ replacedMergeOnReadDmlCases ++ deletedMergeOnReadDmlCases

  /** Every row-mutating operation on the merge-on-read preparations, plus the null-string DELETE on their null form. */
  lazy val mergeOnReadCoreDmlCases: List[TestCase] =
    preparedMergeOnReadCoreTables.flatMap(preparation =>
      rowMutationTestCases.map(_.runOn(preparation))) ++
      preparedNullStringMergeOnReadCoreTables.flatMap(preparation =>
        nullStringRowTestCases.map(_.runOn(preparation)))

  /** The same operations on the replace-lineage merge-on-read preparations, so both paths apply at once. */
  lazy val replacedMergeOnReadDmlCases: List[TestCase] =
    preparedReplacedMergeOnReadCoreTables.flatMap(preparation =>
      rowMutationTestCases.map(_.runOn(preparation))) ++
      preparedNullStringReplacedMergeOnReadCoreTables.flatMap(preparation =>
        nullStringRowTestCases.map(_.runOn(preparation)))

  /** The reads on preparations carrying a live position-delete file, so each read applies one at scan time. */
  lazy val deletedMergeOnReadDmlCases: List[TestCase] =
    preparedDeletedMergeOnReadTables.flatMap(preparation =>
      readTestCases.map(_.runOn(preparation)))

  /** Every merge-on-read contract case outside maintenance, in the order this file introduces them. */
  lazy val mergeOnReadContractCases: List[TestCase] =
    deleteFileCases ++
      deleteModeCases ++
      deleteFileCoexistenceCases ++
      mergeOnReadMetadataCases ++
      mergeOnReadChangelogCases ++
      mergeOnReadFileFormatCases ++
      mergeOnReadFileReplicationCases ++
      mergeOnReadHistoryCases

  // --- 1. the physical delete file, and the copy-on-write outcome it is defined against ---

  /**
   * A strict-subset DELETE against a single data file, run once on each write mode. Merge-on-read records the removal
   * in a position-delete file and keeps the data file; copy-on-write rewrites the data file and leaves no delete
   * file. Both remove the same row and commit one snapshot, so the write mode is the only difference.
   */
  lazy val deleteFileCases: List[TestCase] =
    mergeOnReadVerifyLayouts.map(layout =>
      TablePreparation(layout.label, singleFileSeed(layout))
        .test("mergeOnRead.deleteFile.writesDeleteFile")(table =>
          assertSubsetDeleteOutcome(table, expectedDeleteFileCount = 1))) ++
      copyOnWriteVerifyLayouts.map(layout =>
        TablePreparation(layout.label, singleFileSeed(layout))
          .test("mergeOnRead.deleteFile.copyOnWriteRewritesDataFile")(table =>
            assertSubsetDeleteOutcome(table, expectedDeleteFileCount = 0)))

  /**
   * Runs the strict-subset DELETE and asserts the outcome both write modes share, namely that the matching row is
   * gone and exactly one snapshot was committed, together with the delete-file count the mode under test produces.
   */
  private def assertSubsetDeleteOutcome(
      table: PreparedTable[CoreTable.type],
      expectedDeleteFileCount: Long): Unit = {
    val before = table.state

    table.spark.sql(s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} < 2")
    val after = table.state

    assert(
      liveKeys(table.spark, table.name) == Seq(2L, 3L),
      s"the strict-subset delete leaves keys 2 and 3, found ${liveKeys(table.spark, table.name)}")
    assert(
      currentDeleteFileCount(table.spark, table.name) == expectedDeleteFileCount,
      s"the delete leaves $expectedDeleteFileCount delete files, found " +
        s"${currentDeleteFileCount(table.spark, table.name)}")
    assert(
      after.snapshotCount == before.snapshotCount + 1,
      s"the delete commits one snapshot, went from ${before.snapshotCount} to ${after.snapshotCount}")
  }

  // --- 2. choosing the write mode partway through a table's life ---

  /**
   * Switching a copy-on-write table's delete mode to merge-on-read makes the next partial-file DELETE write a
   * position-delete file and keep the untouched rows in the data file, so the mode a table carries at commit time is
   * the one that decides how the delete is written.
   */
  lazy val deleteModeCases: List[TestCase] =
    fileFormats.map(format =>
      preparedSingleFileCopyOnWriteTable(format)
        .test("mergeOnRead.deleteMode.alterToMergeOnRead") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')")
          table.spark.sql(s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")

          assert(
            currentDeleteFileCount(table.spark, table.name) == 1,
            s"the mode change makes the delete write one delete file, found " +
              s"${currentDeleteFileCount(table.spark, table.name)}")
          assert(
            liveKeys(table.spark, table.name) == Seq(2L, 3L),
            s"the delete after the mode change leaves keys 2 and 3, found " +
              s"${liveKeys(table.spark, table.name)}")
        })

  // --- 3. the surface a table reaches once a delete file is live beside its data ---

  /**
   * The six operations that behave differently once data files and a live position-delete file sit side by side. A
   * read or an insert on a delete-free merge-on-read table is identical to copy-on-write, so every family here starts
   * from the state where a delete file is already live.
   */
  lazy val deleteFileCoexistenceCases: List[TestCase] =
    preparedDeletedMergeOnReadTables.flatMap { preparation =>
      List(
        appendOverDeleteFileCase(preparation),
        secondDeleteOverDeleteFileCase(preparation),
        updateOverDeleteFileCase(preparation),
        filteredReadOverDeleteFileCase(preparation),
        compactDeletesOverDeleteFileCase(preparation),
        mergeOverDeleteFileCase(preparation))
    }

  /**
   * Asserts the table persists `propertyName` as merge-on-read before the mutation under test runs. The row
   * assertions hold on either write path, so this guard is what ties the case to the merge-on-read path it claims to
   * cover.
   */
  private def assertConfiguredMergeOnRead(
      table: PreparedTable[CoreTable.type],
      propertyName: String): Unit = {
    val configuredMode = persistedProperty(table.spark, table.name, propertyName)

    assert(
      configuredMode.contains("merge-on-read"),
      s"the table persists $propertyName as merge-on-read, found $configuredMode")
  }

  /** An INSERT over a live position-delete file adds its row and keeps the deleted key out of the live rows. */
  private def appendOverDeleteFileCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.coexistence.append") { table =>
      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6L, "row-6")}")

      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L, 6L),
        s"the append lands beside the live delete, found ${liveKeys(table.spark, table.name)}")
    }

  /** A second DELETE over a live position-delete file removes its row and the table still carries delete files. */
  private def secondDeleteOverDeleteFileCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.coexistence.secondDelete") { table =>
      table.spark.sql(s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 2")

      assert(
        liveKeys(table.spark, table.name) == Seq(3L),
        s"the second delete leaves key 3, found ${liveKeys(table.spark, table.name)}")
      assert(
        currentDeleteFileCount(table.spark, table.name) >= 1,
        s"the second delete keeps delete files live, found " +
          s"${currentDeleteFileCount(table.spark, table.name)}")
    }

  /** An UPDATE over a live position-delete file changes its row's value and keeps the live key set. */
  private def updateOverDeleteFileCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.coexistence.update") { table =>
      assertConfiguredMergeOnRead(table, "write.update.mode")
      table.spark.sql(
        s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'cx' " +
          s"WHERE ${Core.long0.columnName} = 3")
      val updatedValue = table.spark
        .sql(
          s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} = 3")
        .collect()(0)
        .getString(0)

      assert(updatedValue == "cx", s"the update over a live delete sets the value, found $updatedValue")
      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L),
        s"the update keeps the live key set, found ${liveKeys(table.spark, table.name)}")
    }

  /** A filtered read over a live position-delete file returns the live rows the filter selects. */
  private def filteredReadOverDeleteFileCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.coexistence.filteredRead") { table =>
      val selectedKeys = table.spark
        .sql(
          s"SELECT ${Core.long0.columnName} FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2 ORDER BY ${Core.long0.columnName}")
        .collect()
        .toSeq
        .map(_.getLong(0))

      assert(
        selectedKeys == Seq(2L),
        s"the filter applies the position delete, found $selectedKeys")
    }

  /** Compacting the position deletes over a live delete file keeps the live rows. */
  private def compactDeletesOverDeleteFileCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.coexistence.compactDeletes") { table =>
      table.spark.sql(
        "CALL openhouse.system.rewrite_position_delete_files(" +
          s"table => '${catalogRelative(table.name)}', " +
          "options => map('rewrite-all', 'true'))")

      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L),
        s"compacting the deletes keeps the live rows, found ${liveKeys(table.spark, table.name)}")
    }

  /** A MERGE over a live position-delete file updates its matched row and keeps the live key set. */
  private def mergeOverDeleteFileCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.coexistence.merge") { table =>
      assertConfiguredMergeOnRead(table, "write.merge.mode")
      table.spark.sql(
        s"MERGE INTO ${table.name} target " +
          "USING (SELECT CAST(3 AS BIGINT) key) source " +
          s"ON target.${Core.long0.columnName} = source.key " +
          s"WHEN MATCHED THEN UPDATE SET ${Core.string0.columnName} = 'mg'")
      val mergedValue = table.spark
        .sql(
          s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} = 3")
        .collect()(0)
        .getString(0)

      assert(mergedValue == "mg", s"the merge over a live delete sets the value, found $mergedValue")
      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L),
        s"the merge keeps the live key set, found ${liveKeys(table.spark, table.name)}")
    }

  // --- 4. the metadata that exposes what the reader will apply ---

  /**
   * After a merge-on-read DELETE, the position_deletes metadata table reports exactly the one delete entry the
   * mutation created, so what the reader applies at scan time is visible to a caller reading metadata.
   */
  lazy val mergeOnReadMetadataCases: List[TestCase] =
    fileFormats.map(format =>
      preparedSingleFileMergeOnReadTable(format)
        .test("mergeOnRead.metadata.positionDeletes") { table =>
          table.spark.sql(s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
          val positionDeleteCount = table.spark
            .sql(s"SELECT count(*) FROM ${table.name}.position_deletes")
            .collect()(0)
            .getLong(0)

          assert(
            positionDeleteCount == 1,
            s"position_deletes exposes the one position delete, found $positionDeleteCount")
        })

  // --- 5. what a changelog scan makes of a merge-on-read history ---

  /**
   * The changelog on a merge-on-read table. The append, the INSERT OVERWRITE and the row-level DELETE leave the
   * change feed decodable and report exactly the rows they changed, so those three are pinned row by row. The
   * UPDATE and the MERGE leave position-delete files that a changelog scan reports as unsupported, so each of those
   * is pinned as a rejection.
   */
  lazy val mergeOnReadChangelogCases: List[TestCase] =
    fileFormats.flatMap { format =>
      changelogOperations
        .filter(operation => decodableChangelogOperationNames.contains(operation.name))
        .map(operation =>
          decodableChangelogCase(preparedMergeOnReadTable(format), operation)) ++
        changelogOperations
          .filterNot(operation => decodableChangelogOperationNames.contains(operation.name))
          .map(operation =>
            rejectedChangelogCase(preparedMergeOnReadTable(format), operation))
    }

  /** The operations whose merge-on-read change feed a changelog scan decodes, because they leave no delete file. */
  private val decodableChangelogOperationNames =
    Set("changelog.append", "changelog.overwrite", "changelog.delete")

  /** The message a changelog scan reports when the range it was asked for spans position-delete files. */
  private val changelogDeleteFileRejectionMessage = "Delete files are currently not supported"

  /**
   * The exact change rows each decodable operation reports on a merge-on-read table, as change type followed by the
   * core columns in their declared order. Asserting the whole row pins which row the feed attributes each change
   * to, so an operation that reported the right number of changes against the wrong row fails here.
   */
  private val expectedChangeRowsByOperation: Map[String, List[List[Any]]] = Map(
    "changelog.append" ->
      List(List("INSERT", 6L, 6, "row-6", 6.5d, true, "2024-01-06-05")),
    "changelog.overwrite" ->
      List(List("DELETE", 3L, 3, "row-3", 3.5d, false, "2024-01-01-02")),
    "changelog.delete" ->
      List(List("DELETE", 1L, 1, "row-1", 1.5d, false, "2024-01-01-00")))

  /**
   * On a merge-on-read table, the operation's change feed reports exactly the rows it changed, so the write mode
   * leaves the decodable part of the changelog contract as it is.
   */
  private def decodableChangelogCase(
      preparation: TablePreparation[CoreTable.type],
      operation: ChangelogOperation): TestCase =
    preparation.test(s"mergeOnRead.${operation.name}") { table =>
      val expectedChangeRows = expectedChangeRowsByOperation
        .getOrElse(
          operation.name,
          throw new AssertionError(s"${operation.name} declares the change rows it reports"))
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(operation.statement(table.name))
      val changelogView = changelogViewFrom(table, seedSnapshotId)
      val actualChangeRows = table.spark
        .sql(
          s"SELECT _change_type, $columnNameList FROM $changelogView " +
            s"ORDER BY _change_type, ${Core.long0.columnName}")
        .collect()
        .toSeq
        .map(_.toSeq.toList)

      assert(
        actualChangeRows == expectedChangeRows,
        s"${operation.name} reports $expectedChangeRows on a merge-on-read table, " +
          s"found $actualChangeRows")
      assert(
        changeCounts(table, changelogView) == operation.expectedChangeCounts,
        s"${operation.name} agrees with the shared histogram " +
          s"${operation.expectedChangeCounts}, found ${changeCounts(table, changelogView)}")
    }

  /**
   * On a merge-on-read table, reading the operation's change feed reports that delete files are unsupported, so a
   * caller learns the range is undecodable and can fall back to a range the scan does decode.
   */
  private def rejectedChangelogCase(
      preparation: TablePreparation[CoreTable.type],
      operation: ChangelogOperation): TestCase =
    preparation.test(s"mergeOnRead.${operation.name}.rejected") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(operation.statement(table.name))
      val rejection = Check.intercept[UnsupportedOperationException] {
        val view = changelogViewFrom(table, seedSnapshotId)
        table.spark.sql(s"SELECT * FROM $view").collect()
      }

      assert(
        Exceptions
          .causeChain(rejection)
          .exists(cause =>
            Option(cause.getMessage).exists(_.contains(changelogDeleteFileRejectionMessage))),
        s"the rejection names delete files as unsupported, found: ${rejection.getMessage.take(200)}")
    }

  // --- 6. the properties a merge-on-read write path owns ---

  /**
   * Format materialization on a table that already carries a live position-delete file: the data files still carry
   * the extension of the declared write.format.default, so a delete file present alongside them leaves the format
   * contract as it is. The case body is the foundation's, reused as data.
   */
  lazy val mergeOnReadFileFormatCases: List[TestCase] =
    preparedDeletedMergeOnReadTables.map { preparation =>
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

  /**
   * write.delete-file-replication is the property the delete-file writer resolves into a block replication factor, so
   * it applies exactly where a mutation writes a position-delete file. The property round-trips through the catalog,
   * survives the DELETE that uses it, and the DELETE physically writes the delete file the property describes. The
   * local catalog asserts the property and the delete file; HDFS verifies block replication in its own environment.
   */
  lazy val mergeOnReadFileReplicationCases: List[TestCase] =
    fileFormats.map(format =>
      preparedSingleFileMergeOnReadTable(format)
        .test("mergeOnRead.fileReplication.deleteFileProperty") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('write.delete-file-replication'='2')")

          assert(
            tableProps(table.spark, table.name).get("write.delete-file-replication").contains("2"),
            s"the delete-file replication property round-trips, found " +
              s"${tableProps(table.spark, table.name).get("write.delete-file-replication")}")

          table.spark.sql(s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")

          assert(
            currentDeleteFileCount(table.spark, table.name) == 1,
            s"the delete writes the position-delete file the property describes, found " +
              s"${currentDeleteFileCount(table.spark, table.name)}")
          assert(
            liveKeys(table.spark, table.name) == Seq(2L, 3L),
            s"the delete leaves keys 2 and 3, found ${liveKeys(table.spark, table.name)}")
          assert(
            tableProps(table.spark, table.name).get("write.delete-file-replication").contains("2"),
            "the delete-file replication property survives the delete that used it")
        })

  // --- 7. reading the history a position delete sits in ---

  /**
   * Snapshot history over a live position-delete file. The delete is a commit like any other, so the snapshot before
   * it still reads the removed row, and a rollback to that snapshot brings the row back into the live set.
   */
  lazy val mergeOnReadHistoryCases: List[TestCase] =
    preparedDeletedMergeOnReadTables.flatMap { preparation =>
      List(
        timeTravelBeforeDeleteCase(preparation),
        rollbackUndoesDeleteCase(preparation))
    }

  /** The current read applies the delete, while the snapshot before it still reads the removed row. */
  private def timeTravelBeforeDeleteCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.history.timeTravelBeforeDelete") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      val preDeleteKeys = table.spark
        .sql(
          s"SELECT ${Core.long0.columnName} FROM ${table.name} VERSION AS OF $seedSnapshotId " +
            s"ORDER BY ${Core.long0.columnName}")
        .collect()
        .toSeq
        .map(_.getLong(0))

      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L),
        s"the current read applies the delete, found ${liveKeys(table.spark, table.name)}")
      assert(
        preDeleteKeys == Seq(1L, 2L, 3L),
        s"the snapshot before the delete reads the removed row, found $preDeleteKeys")
    }

  /** A rollback to the snapshot before the delete brings the removed row back into the live set. */
  private def rollbackUndoesDeleteCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.history.rollbackUndoesDelete") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(
        "CALL openhouse.system.rollback_to_snapshot(" +
          s"table => '${catalogRelative(table.name)}', " +
          s"snapshot_id => ${seedSnapshotId}L)")

      assert(
        liveKeys(table.spark, table.name) == Seq(1L, 2L, 3L),
        s"the rollback restores the position-deleted row, found ${liveKeys(table.spark, table.name)}")
    }

}

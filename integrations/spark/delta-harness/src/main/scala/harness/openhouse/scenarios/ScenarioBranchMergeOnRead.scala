package harness

/**
 * Branch writes on a merge-on-read table: a mutation aimed at a branch records a position-delete file, and the
 * operations that bring a branch onto `main` carry that file with the rest of the snapshot.
 *
 * A merge-on-read mutation leaves the data file it matched in place and records the removal in a delete file the
 * reader applies at scan time. That file belongs to the snapshot the mutation committed, so a branch mutation puts it
 * where only the branch reads it, and `main` reads its data files with no delete applied until a fast-forward or a
 * cherry-pick moves it onto the snapshot the mutation produced.
 *
 * Operations: a DELETE and an UPDATE aimed at the branch, fast_forward of `main` onto the branch after each of them,
 * cherrypick_snapshot of the branch's delete snapshot, and REPLACE BRANCH back to the snapshot from before the
 * delete.
 *
 * Preparation axes: file format. Each family runs on the merge-on-read verify layout in both columnar formats, whose
 * seed lands all three rows in one data file, so a strict-subset delete is a partial-file match and the catalog
 * answers it with a position-delete file rather than a rewritten data file. Branch `audit` is created on that seed,
 * so both references start on one snapshot with no delete file present.
 *
 * Case families: 5 families over 2 formats, contributing 10 cases.
 */
trait ScenarioBranchMergeOnRead extends BranchTableFixtures {
  import Rows._

  /** Every branch merge-on-read case, one merge-on-read verify preparation at a time. */
  lazy val branchMergeOnReadCases: List[TestCase] =
    preparedBranchedMergeOnReadTables.map(branchDeleteWritesADeleteFile) ++
      preparedBranchedMergeOnReadTables.map(fastForwardCarriesTheDeleteFile) ++
      preparedBranchedMergeOnReadTables.map(fastForwardCarriesTheBranchUpdate) ++
      preparedBranchedMergeOnReadTables.map(cherryPickCarriesTheDeleteFile) ++
      preparedBranchedMergeOnReadTables.map(replaceBranchUndoesTheDelete)

  // --- the case bodies the surface above composes ---

  /**
   * A DELETE aimed at the branch records one position-delete file. The branch reads the two rows that survive it,
   * `main` keeps its binding and reads all three, and the data file the seed wrote stays the one file `main` reads.
   */
  private def branchDeleteWritesADeleteFile(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.mergeOnRead.branchDeleteWritesADeleteFile") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)
      val dataFilePathsBefore = currentDataFilePaths(table.spark, table.name)

      assert(
        allDeleteFilePaths(table.spark, table.name).isEmpty,
        s"the seeded table carries no delete file, found " +
          s"${allDeleteFilePaths(table.spark, table.name)}")
      assert(
        dataFilePathsBefore.size == 1,
        s"the seed lands its rows in one data file, found $dataFilePathsBefore")

      table.spark.sql(
        s"DELETE FROM ${table.name}.branch_$auditBranchName WHERE ${Core.long0.columnName} = 1")

      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows.filter(row => row.get(Core.long0) != 1L),
        s"$auditBranchName reads the rows the delete left, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        allDeleteFilePaths(table.spark, table.name).size == 1,
        s"the branch delete records one position-delete file, found " +
          s"${allDeleteFilePaths(table.spark, table.name)}")
      assert(
        currentDeleteFileCount(table.spark, table.name) == 0,
        s"$mainBranchName applies no delete file, found " +
          s"${currentDeleteFileCount(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the branch delete leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        currentDataFilePaths(table.spark, table.name) == dataFilePathsBefore,
        s"the branch delete leaves the data file $mainBranchName reads, found " +
          s"${currentDataFilePaths(table.spark, table.name)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == branchPointSnapshotId,
        s"$mainBranchName still names $branchPointSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
    }

  /**
   * fast_forward moves `main` onto the branch snapshot the delete produced, so `main` applies the position-delete
   * file the branch wrote and keeps reading the data file the seed wrote. Both references name that snapshot and read
   * the same two rows.
   */
  private def fastForwardCarriesTheDeleteFile(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.mergeOnRead.fastForwardCarriesTheDeleteFile") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)
      val dataFilePathsBefore = currentDataFilePaths(table.spark, table.name)

      table.spark.sql(
        s"DELETE FROM ${table.name}.branch_$auditBranchName WHERE ${Core.long0.columnName} = 1")

      val branchHeadSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)

      fastForward(table.spark, table.name, mainBranchName, auditBranchName)

      assert(
        referenceEntries(table.spark, table.name) ==
          Seq(
            (auditBranchName, branchReferenceType, branchHeadSnapshotId),
            (mainBranchName, branchReferenceType, branchHeadSnapshotId)),
        s"both references name the branch head afterwards, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows.filter(row => row.get(Core.long0) != 1L),
        s"$mainBranchName reads the rows the branch delete left, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        currentDeleteFileCount(table.spark, table.name) == 1,
        s"$mainBranchName applies the position-delete file the branch wrote, found " +
          s"${currentDeleteFileCount(table.spark, table.name)}")
      assert(
        currentDataFilePaths(table.spark, table.name) == dataFilePathsBefore,
        s"$mainBranchName reads the data file the seed wrote, found " +
          s"${currentDataFilePaths(table.spark, table.name)}")
      assert(
        branchHeadSnapshotId != branchPointSnapshotId,
        s"the branch delete commits a snapshot past $branchPointSnapshotId")
    }

  /**
   * fast_forward carries a branch UPDATE the same way: `main` reads the value the branch wrote, keeps the row count
   * the table had, and applies the position-delete file that retired the row's earlier version alongside the data
   * file the update appended.
   */
  private def fastForwardCarriesTheBranchUpdate(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.mergeOnRead.fastForwardCarriesTheBranchUpdate") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)
      val dataFilePathsBefore = currentDataFilePaths(table.spark, table.name)

      table.spark.sql(
        s"UPDATE ${table.name}.branch_$auditBranchName " +
          s"SET ${Core.string0.columnName} = 'branch-update' WHERE ${Core.long0.columnName} = 2")

      val branchHeadSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)

      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the branch update leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")

      fastForward(table.spark, table.name, mainBranchName, auditBranchName)

      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "branch-update")
            else row),
        s"$mainBranchName reads the value the branch update wrote, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == branchHeadSnapshotId,
        s"$mainBranchName names the branch head afterwards, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
      assert(
        currentDeleteFileCount(table.spark, table.name) == 1,
        s"$mainBranchName applies the position-delete file the update wrote, found " +
          s"${currentDeleteFileCount(table.spark, table.name)}")
      assert(
        currentDataFilePaths(table.spark, table.name).size == dataFilePathsBefore.size + 1,
        s"$mainBranchName reads the seed data file and the one the update appended, found " +
          s"${currentDataFilePaths(table.spark, table.name)}")
    }

  /**
   * cherrypick_snapshot replays the branch's delete snapshot onto `main`, so `main` applies the position-delete file
   * and reads the two rows that survive it. The branch keeps the binding it had.
   */
  private def cherryPickCarriesTheDeleteFile(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.mergeOnRead.cherryPickCarriesTheDeleteFile") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)
      val dataFilePathsBefore = currentDataFilePaths(table.spark, table.name)

      table.spark.sql(
        s"DELETE FROM ${table.name}.branch_$auditBranchName WHERE ${Core.long0.columnName} = 1")

      val branchHeadSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)

      cherryPick(table.spark, table.name, branchHeadSnapshotId)

      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows.filter(row => row.get(Core.long0) != 1L),
        s"$mainBranchName reads the rows the branch delete left, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        currentDeleteFileCount(table.spark, table.name) == 1,
        s"$mainBranchName applies the position-delete file the branch wrote, found " +
          s"${currentDeleteFileCount(table.spark, table.name)}")
      assert(
        currentDataFilePaths(table.spark, table.name) == dataFilePathsBefore,
        s"$mainBranchName reads the data file the seed wrote, found " +
          s"${currentDataFilePaths(table.spark, table.name)}")
      assert(
        referenceSnapshotId(table.spark, table.name, auditBranchName) == branchHeadSnapshotId,
        s"the cherry-pick leaves $auditBranchName on $branchHeadSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, auditBranchName)}")
    }

  /**
   * REPLACE BRANCH back to the snapshot from before the delete rebinds the branch to a snapshot with no delete file,
   * so the branch reads all three rows again. `main` keeps its binding and its rows, and the branch accepts a write
   * from the position it was rebound to.
   */
  private def replaceBranchUndoesTheDelete(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.mergeOnRead.replaceBranchUndoesTheDelete") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      table.spark.sql(
        s"DELETE FROM ${table.name}.branch_$auditBranchName WHERE ${Core.long0.columnName} = 1")

      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows.filter(row => row.get(Core.long0) != 1L),
        s"$auditBranchName reads the rows the delete left, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")

      table.spark.sql(
        s"ALTER TABLE ${table.name} REPLACE BRANCH $auditBranchName " +
          s"AS OF VERSION $branchPointSnapshotId")

      assert(
        referenceEntries(table.spark, table.name) ==
          Seq(
            (auditBranchName, branchReferenceType, branchPointSnapshotId),
            (mainBranchName, branchReferenceType, branchPointSnapshotId)),
        s"REPLACE BRANCH rebinds $auditBranchName to $branchPointSnapshotId, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) == seededRows,
        s"$auditBranchName reads the rows the snapshot before the delete held, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the rebinding leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")

      table.spark.sql(
        s"DELETE FROM ${table.name}.branch_$auditBranchName WHERE ${Core.long0.columnName} = 3")

      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows.filter(row => row.get(Core.long0) != 3L),
        s"the rebound branch accepts a delete, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the delete on the rebound branch leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

}

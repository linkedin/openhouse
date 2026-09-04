package harness

import org.apache.iceberg.exceptions.ValidationException
import org.apache.spark.sql.Row

/**
 * Bringing one reference onto another: fast-forward, which rebinds a reference to a snapshot that already descends
 * from it, and cherry-pick, which replays one snapshot's change onto `main`.
 *
 * Both operations are named by the references they act on, so every case here states the source and the target it
 * asked for and then proves the binding each reference holds afterwards. A fast-forward is defined only while the
 * target's snapshot is an ancestor of the source's, so the diverged case is part of the contract rather than an edge
 * of it.
 *
 * Operations: fast_forward of `main` onto a branch that is ahead of it, fast_forward of a branch onto `main`,
 * fast_forward across a divergence, cherrypick_snapshot of a branch-only snapshot onto `main`, and
 * cherrypick_snapshot of a snapshot `main` already descends from.
 *
 * Preparation axes: file format. Each family runs in both columnar formats on the branched table, whose preparation
 * already proved `main` and `audit` name one snapshot, so every divergence a case reads is the one it created.
 *
 * Case families: 5 families over 2 formats, contributing 10 cases.
 */
trait ScenarioBranchMerge extends ScenarioBranchKit {

  /** Every branch-merge case, in the order this file introduces the families. */
  lazy val branchMergeCases: List[TestCase] =
    preparedBranchedTables.map(fastForwardMainOntoTheBranch) ++
      preparedBranchedTables.map(fastForwardTheBranchOntoMain) ++
      preparedBranchedTables.map(fastForwardDivergentRejected) ++
      preparedBranchedTables.map(cherryPickBranchSnapshot) ++
      preparedBranchedTables.map(cherryPickMergedSnapshotRejected)

  // --- the case bodies the surface above composes ---

  /**
   * fast_forward names `main` as the target and `audit` as the source, and reports the binding it moved `main` from
   * and the one it moved `main` to. Afterwards both references name the branch head and read the same rows, and
   * `main` accepts a write that advances it past the branch.
   */
  private def fastForwardMainOntoTheBranch(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.merge.fastForwardMainOntoTheBranch") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "ahead-one")}")
      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(7, "ahead-two")}")

      val branchHeadSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val branchRows = rowsOn(table.spark, table.name, auditBranchName)

      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == branchPointSnapshotId,
        s"the branch writes leave $mainBranchName on $branchPointSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")

      val fastForwardReport = fastForward(table.spark, table.name, mainBranchName, auditBranchName)

      assert(
        fastForwardReport == Seq(Row(mainBranchName, branchPointSnapshotId, branchHeadSnapshotId)),
        s"fast_forward reports moving $mainBranchName from $branchPointSnapshotId to " +
          s"$branchHeadSnapshotId, found $fastForwardReport")
      assert(
        referenceEntries(table.spark, table.name) ==
          Seq(
            (auditBranchName, branchReferenceType, branchHeadSnapshotId),
            (mainBranchName, branchReferenceType, branchHeadSnapshotId)),
        s"both references name the branch head afterwards, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == branchRows,
        s"$mainBranchName reads the rows the branch built, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        branchRows == seededRows ++
          Seq(expectedCoreRow(6L, "ahead-one"), expectedCoreRow(7L, "ahead-two")),
        s"the branch built its rows from the seed, found $branchRows")

      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(8, "after-merge")}")

      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          branchRows :+ expectedCoreRow(8L, "after-merge"),
        s"$mainBranchName accepts a write after the fast-forward, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, auditBranchName) == branchHeadSnapshotId,
        s"the write after the fast-forward leaves $auditBranchName on $branchHeadSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, auditBranchName)}")
    }

  /**
   * fast_forward names `audit` as the target and `main` as the source when the branch is the reference that fell
   * behind. It reports the same pair of bindings in that direction, both references name the head `main` reached,
   * and the branch accepts a write from its new position.
   */
  private def fastForwardTheBranchOntoMain(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.merge.fastForwardTheBranchOntoMain") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "main-ahead")}")

      val mainHeadSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      assert(
        rowsOn(table.spark, table.name, auditBranchName) == seededRows,
        s"the write on $mainBranchName leaves the rows $auditBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")

      val fastForwardReport = fastForward(table.spark, table.name, auditBranchName, mainBranchName)

      assert(
        fastForwardReport == Seq(Row(auditBranchName, branchPointSnapshotId, mainHeadSnapshotId)),
        s"fast_forward reports moving $auditBranchName from $branchPointSnapshotId to " +
          s"$mainHeadSnapshotId, found $fastForwardReport")
      assert(
        referenceEntries(table.spark, table.name) ==
          Seq(
            (auditBranchName, branchReferenceType, mainHeadSnapshotId),
            (mainBranchName, branchReferenceType, mainHeadSnapshotId)),
        s"both references name the head $mainBranchName reached, found " +
          s"${referenceEntries(table.spark, table.name)}")

      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(7, "branch-again")}")

      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows ++ Seq(expectedCoreRow(6L, "main-ahead"), expectedCoreRow(7L, "branch-again")),
        s"$auditBranchName accepts a write from the head it was moved to, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == mainHeadSnapshotId,
        s"the write on $auditBranchName leaves $mainBranchName on $mainHeadSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
    }

  /**
   * fast_forward is defined while the target's snapshot is an ancestor of the source's. Once both references have
   * committed since they parted, the catalog rejects the call as an argument failure naming the target, the source
   * and the ancestry it needed, and both references keep the bindings and the rows they had.
   */
  private def fastForwardDivergentRejected(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.merge.fastForwardDivergent.rejected") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "branch-side")}")
      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(7, "main-side")}")

      val referencesBefore = referenceEntries(table.spark, table.name)
      val branchRows = rowsOn(table.spark, table.name, auditBranchName)
      val mainRows = rowsOn(table.spark, table.name, mainBranchName)

      val rejection = Check.intercept[IllegalArgumentException](
        fastForward(table.spark, table.name, mainBranchName, auditBranchName))

      assert(
        rejection.getMessage.contains(
          s"Cannot fast-forward: $mainBranchName is not an ancestor of $auditBranchName"),
        s"the rejection names the ancestry fast_forward requires, found " +
          s"${rejection.getMessage.take(160)}")
      assert(
        referenceEntries(table.spark, table.name) == referencesBefore,
        s"the rejected fast_forward leaves every reference where it was, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) == branchRows &&
          rowsOn(table.spark, table.name, mainBranchName) == mainRows,
        "the rejected fast_forward leaves both references reading their rows")
    }

  /**
   * cherrypick_snapshot replays a branch-only snapshot onto `main`. It reports the snapshot it took and the snapshot
   * `main` now names, `main` reads the branch's row, and the branch keeps the binding it had.
   */
  private def cherryPickBranchSnapshot(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.merge.cherryPickBranchSnapshot") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "picked")}")

      val branchHeadSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val publishReport = cherryPick(table.spark, table.name, branchHeadSnapshotId)
      val mainHeadSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      assert(
        publishReport == Seq(Row(branchHeadSnapshotId, mainHeadSnapshotId)),
        s"cherrypick_snapshot reports taking $branchHeadSnapshotId onto $mainHeadSnapshotId, found " +
          s"$publishReport")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows :+ expectedCoreRow(6L, "picked"),
        s"$mainBranchName reads the row the branch committed, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, auditBranchName) == branchHeadSnapshotId,
        s"the cherry-pick leaves $auditBranchName on $branchHeadSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, auditBranchName)}")
      assert(
        mainHeadSnapshotId != branchPointSnapshotId,
        s"the cherry-pick moves $mainBranchName off $branchPointSnapshotId")
    }

  /**
   * A snapshot `main` already descends from is rejected as a cherry-pick, because the change it carries is part of
   * what `main` reads. The reference set and the rows both references read stay as they were.
   */
  private def cherryPickMergedSnapshotRejected(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.merge.cherryPickMergedSnapshot.rejected") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "merged-once")}")

      val branchHeadSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)

      fastForward(table.spark, table.name, mainBranchName, auditBranchName)

      val referencesBefore = referenceEntries(table.spark, table.name)
      val mainRows = rowsOn(table.spark, table.name, mainBranchName)

      val rejection = Check.intercept[ValidationException](
        cherryPick(table.spark, table.name, branchHeadSnapshotId))

      assert(
        rejection.getMessage.contains("ancestor"),
        s"the rejection names the ancestry that already carries the change, found " +
          s"${rejection.getMessage.take(160)}")
      assert(
        referenceEntries(table.spark, table.name) == referencesBefore,
        s"the rejected cherry-pick leaves every reference where it was, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == mainRows,
        s"the rejected cherry-pick leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

}

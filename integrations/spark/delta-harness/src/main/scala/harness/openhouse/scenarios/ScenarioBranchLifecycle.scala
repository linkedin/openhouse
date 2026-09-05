package harness

import org.apache.iceberg.exceptions.ValidationException

/**
 * Branch and tag lifecycle: the named references a table carries, the snapshot each one names, and what the catalog
 * does with a statement that names a reference the table does not carry.
 *
 * A reference is a name bound to one snapshot. Creating one binds the name without committing anything, replacing one
 * rebinds it to a snapshot the table already retains, and dropping one releases the name and the protection it gave
 * the snapshot it held. Every case here reads the `refs` metadata table, so it proves the reference set, the type of
 * each reference and the snapshot each one names rather than a row count that several bindings could produce.
 *
 * Operations: CREATE BRANCH at the head `main` names, CREATE BRANCH AS OF VERSION an earlier snapshot, DROP BRANCH,
 * REPLACE BRANCH AS OF VERSION, CREATE TAG followed by an expiration run, an INSERT aimed at a branch the table never
 * carried, a CREATE BRANCH over a name the table already carries, and a DROP BRANCH of a name the table never
 * carried.
 *
 * Preparation axes: file format, and how much history the table carries. Each family runs in both columnar formats.
 * The families that bind a name to the current head start from the standard seeded table; the families that bind a
 * name to an earlier snapshot start from the two-snapshot table; the families that act on an existing branch start
 * from the branched table, whose preparation already proved `main` and `audit` name one snapshot.
 *
 * Case families: 8 families over 2 formats, contributing 16 cases.
 */
trait ScenarioBranchLifecycle extends BranchTableFixtures {

  /** Every branch and tag lifecycle case, in the order this file introduces the families. */
  lazy val branchLifecycleCases: List[TestCase] =
    preparedCoreFormats.map(createBranchBindsTheCurrentHead) ++
      preparedTwoSnapshotTables.map(createBranchBindsAnEarlierSnapshot) ++
      preparedBranchedTables.map(dropBranchReleasesTheName) ++
      preparedBranchedTwoSnapshotTables.map(replaceBranchRebindsTheName) ++
      preparedTwoSnapshotTables.map(createTagPinsItsSnapshot) ++
      preparedCoreFormats.map(writeToMissingBranchRejected) ++
      preparedBranchedTables.map(createExistingBranchRejected) ++
      preparedCoreFormats.map(dropMissingBranchRejected)

  // --- the case bodies the surface above composes ---

  /**
   * CREATE BRANCH binds the new name to the snapshot `main` already names. The table gains one reference and no
   * snapshot, both references read the same rows, and the retained snapshots are the ones the table already held.
   */
  private def createBranchBindsTheCurrentHead(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.lifecycle.createBranchBindsTheCurrentHead") { table =>
      val headSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)
      val retainedBefore = retainedSnapshotIds(table.spark, table.name).sorted
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      assert(
        referenceEntries(table.spark, table.name) ==
          Seq((mainBranchName, branchReferenceType, headSnapshotId)),
        s"the seeded table carries $mainBranchName alone, found " +
          s"${referenceEntries(table.spark, table.name)}")

      withOwnedReference(table.spark.sql(_), table.name, branchReferenceType, auditBranchName)(
        table.spark.sql(s"ALTER TABLE ${table.name} CREATE BRANCH $auditBranchName")) {
        assert(
          referenceEntries(table.spark, table.name) ==
            Seq(
              (auditBranchName, branchReferenceType, headSnapshotId),
              (mainBranchName, branchReferenceType, headSnapshotId)),
          s"CREATE BRANCH binds $auditBranchName to the head $mainBranchName names, found " +
            s"${referenceEntries(table.spark, table.name)}")
        assert(
          retainedSnapshotIds(table.spark, table.name).sorted == retainedBefore,
          s"CREATE BRANCH names an existing snapshot and commits none, found " +
            s"${retainedSnapshotIds(table.spark, table.name).sorted}")
        assert(
          rowsOn(table.spark, table.name, auditBranchName) == seededRows,
          s"$auditBranchName reads the rows $mainBranchName reads, found " +
            s"${rowsOn(table.spark, table.name, auditBranchName)}")
        assert(
          rowsOn(table.spark, table.name, mainBranchName) == seededRows,
          s"CREATE BRANCH keeps the rows $mainBranchName reads, found " +
            s"${rowsOn(table.spark, table.name, mainBranchName)}")
      }
    }

  /**
   * CREATE BRANCH AS OF VERSION binds the new name to the snapshot named, which is the one before the head. The
   * branch reads the three rows that snapshot held while `main` stays on the head and its five rows.
   */
  private def createBranchBindsAnEarlierSnapshot(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.lifecycle.createBranchBindsAnEarlierSnapshot") { table =>
      val ancestry = snapshotIds(table.spark, table.name)
      val headSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      assert(ancestry.size == 2, s"the two-snapshot table retains two snapshots, found $ancestry")
      assert(
        headSnapshotId == ancestry.last,
        s"$mainBranchName names the newer snapshot ${ancestry.last}, found $headSnapshotId")

      withOwnedReference(table.spark.sql(_), table.name, branchReferenceType, auditBranchName)(
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH $auditBranchName AS OF VERSION ${ancestry.head}")) {
        assert(
          referenceEntries(table.spark, table.name) ==
            Seq(
              (auditBranchName, branchReferenceType, ancestry.head),
              (mainBranchName, branchReferenceType, headSnapshotId)),
          s"CREATE BRANCH AS OF VERSION binds $auditBranchName to ${ancestry.head}, found " +
            s"${referenceEntries(table.spark, table.name)}")
        assert(
          keysOf(rowsOn(table.spark, table.name, auditBranchName)) == List(1L, 2L, 3L),
          s"$auditBranchName reads the rows its snapshot held, found " +
            s"${keysOf(rowsOn(table.spark, table.name, auditBranchName))}")
        assert(
          keysOf(rowsOn(table.spark, table.name, mainBranchName)) == List(1L, 2L, 3L, 4L, 5L),
          s"$mainBranchName keeps the rows the head holds, found " +
            s"${keysOf(rowsOn(table.spark, table.name, mainBranchName))}")
      }
    }

  /**
   * DROP BRANCH releases the name. The table carries `main` alone afterwards, `main` still names the snapshot it
   * named and reads the rows it read, and the snapshot the branch held stays retained because `main` names it too.
   */
  private def dropBranchReleasesTheName(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.lifecycle.dropBranchReleasesTheName") { table =>
      val headSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)
      val retainedBefore = retainedSnapshotIds(table.spark, table.name).sorted

      table.spark.sql(s"ALTER TABLE ${table.name} DROP BRANCH $auditBranchName")

      assert(
        referenceEntries(table.spark, table.name) ==
          Seq((mainBranchName, branchReferenceType, headSnapshotId)),
        s"DROP BRANCH leaves $mainBranchName alone on $headSnapshotId, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        retainedSnapshotIds(table.spark, table.name).sorted == retainedBefore,
        s"DROP BRANCH releases a name and expires no snapshot, found " +
          s"${retainedSnapshotIds(table.spark, table.name).sorted}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"DROP BRANCH keeps the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

  /**
   * REPLACE BRANCH AS OF VERSION rebinds an existing branch to an earlier snapshot. The branch reads the rows that
   * snapshot held, `main` keeps its own binding and rows, and the branch accepts a write from its new position.
   */
  private def replaceBranchRebindsTheName(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.lifecycle.replaceBranchRebindsTheName") { table =>
      val ancestry = snapshotIds(table.spark, table.name)
      val headSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)
      val mainRows = rowsOn(table.spark, table.name, mainBranchName)

      table.spark.sql(
        s"ALTER TABLE ${table.name} REPLACE BRANCH $auditBranchName AS OF VERSION ${ancestry.head}")

      assert(
        referenceEntries(table.spark, table.name) ==
          Seq(
            (auditBranchName, branchReferenceType, ancestry.head),
            (mainBranchName, branchReferenceType, headSnapshotId)),
        s"REPLACE BRANCH rebinds $auditBranchName to ${ancestry.head}, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        keysOf(rowsOn(table.spark, table.name, auditBranchName)) == List(1L, 2L, 3L),
        s"$auditBranchName reads the rows ${ancestry.head} held, found " +
          s"${keysOf(rowsOn(table.spark, table.name, auditBranchName))}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == mainRows,
        s"REPLACE BRANCH keeps the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")

      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "after-replace")}")

      assert(
        keysOf(rowsOn(table.spark, table.name, auditBranchName)) == List(1L, 2L, 3L, 6L),
        s"the rebound branch accepts a write, found " +
          s"${keysOf(rowsOn(table.spark, table.name, auditBranchName))}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == mainRows,
        s"the write on the rebound branch keeps $mainBranchName as it was, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

  /**
   * CREATE TAG binds a name of type TAG to the head. A later write to `main` and an expiration run that keeps one
   * snapshot leave the tagged snapshot retained, so the tag still reads the rows the table held when it was taken.
   */
  private def createTagPinsItsSnapshot(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.lifecycle.createTagPinsItsSnapshot") { table =>
      val taggedSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)
      val taggedRows = rowsOn(table.spark, table.name, mainBranchName)
      val releaseTagName = "release"

      withOwnedReference(table.spark.sql(_), table.name, tagReferenceType, releaseTagName)(
        table.spark.sql(s"ALTER TABLE ${table.name} CREATE TAG $releaseTagName")) {
        assert(
          referenceEntries(table.spark, table.name) ==
            Seq(
              (mainBranchName, branchReferenceType, taggedSnapshotId),
              (releaseTagName, tagReferenceType, taggedSnapshotId)),
          s"CREATE TAG binds $releaseTagName to $taggedSnapshotId as a $tagReferenceType, found " +
            s"${referenceEntries(table.spark, table.name)}")

        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "after-tag")}")
        expireUnreferencedSnapshots(table.spark, table.name)

        val headSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

        assert(
          referenceEntries(table.spark, table.name) ==
            Seq(
              (mainBranchName, branchReferenceType, headSnapshotId),
              (releaseTagName, tagReferenceType, taggedSnapshotId)),
          s"expiration keeps $releaseTagName on $taggedSnapshotId, found " +
            s"${referenceEntries(table.spark, table.name)}")
        assert(
          retainedSnapshotIds(table.spark, table.name).sorted ==
            Seq(taggedSnapshotId, headSnapshotId).sorted,
          s"expiration retains the tagged snapshot and the head, found " +
            s"${retainedSnapshotIds(table.spark, table.name).sorted}")
        assert(
          rowsOn(table.spark, table.name, releaseTagName) == taggedRows,
          s"$releaseTagName reads the rows the table held when it was taken, found " +
            s"${rowsOn(table.spark, table.name, releaseTagName)}")
        assert(
          keysOf(rowsOn(table.spark, table.name, mainBranchName)) == List(1L, 2L, 3L, 4L, 5L, 6L),
          s"$mainBranchName reads the row written after the tag, found " +
            s"${keysOf(rowsOn(table.spark, table.name, mainBranchName))}")
      }
    }

  /**
   * An INSERT aimed at a branch the table never carried is rejected as a validation failure naming the missing
   * reference. The table keeps its reference set, its rows and its snapshots.
   */
  private def writeToMissingBranchRejected(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.lifecycle.writeToMissingBranch.rejected") { table =>
      val headSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)
      val missingBranchName = "missing"

      withCleanupStatement(
        table.spark.sql(_),
        s"ALTER TABLE ${table.name} DROP BRANCH IF EXISTS $missingBranchName") {
        val rejection = Check.intercept[ValidationException](
          table.spark.sql(
            s"INSERT INTO ${table.name}.branch_$missingBranchName VALUES ${coreRow(6, "missing")}"))

        assert(
          rejection.getMessage.contains(missingBranchName) &&
            rejection.getMessage.contains("does not exist"),
          s"the rejection names the missing branch, found ${rejection.getMessage.take(160)}")
        assert(
          referenceEntries(table.spark, table.name) ==
            Seq((mainBranchName, branchReferenceType, headSnapshotId)),
          s"the rejected write leaves the reference set as it was, found " +
            s"${referenceEntries(table.spark, table.name)}")
        assert(
          rowsOn(table.spark, table.name, mainBranchName) == seededRows,
          s"the rejected write leaves the rows as they were, found " +
            s"${rowsOn(table.spark, table.name, mainBranchName)}")
      }
    }

  /**
   * CREATE BRANCH over a name the table already carries is rejected as an argument failure, and the existing branch
   * keeps the snapshot it named and the rows it read.
   */
  private def createExistingBranchRejected(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.lifecycle.createExistingBranch.rejected") { table =>
      val referencesBefore = referenceEntries(table.spark, table.name)
      val branchRows = rowsOn(table.spark, table.name, auditBranchName)

      val rejection = Check.intercept[IllegalArgumentException](
        table.spark.sql(s"ALTER TABLE ${table.name} CREATE BRANCH $auditBranchName"))

      assert(
        rejection.getMessage.contains(auditBranchName) &&
          rejection.getMessage.contains("already exists"),
        s"the rejection names the branch that already exists, found ${rejection.getMessage.take(160)}")
      assert(
        referenceEntries(table.spark, table.name) == referencesBefore,
        s"the rejected create leaves the reference set as it was, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) == branchRows,
        s"the rejected create leaves $auditBranchName reading the rows it read, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
    }

  /**
   * DROP BRANCH of a name the table never carried is rejected as an argument failure naming the branch, and the
   * table keeps the one reference it carries.
   */
  private def dropMissingBranchRejected(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.lifecycle.dropMissingBranch.rejected") { table =>
      val referencesBefore = referenceEntries(table.spark, table.name)
      val missingBranchName = "missing"

      val rejection = Check.intercept[IllegalArgumentException](
        table.spark.sql(s"ALTER TABLE ${table.name} DROP BRANCH $missingBranchName"))

      assert(
        rejection.getMessage.contains(missingBranchName),
        s"the rejection names the missing branch, found ${rejection.getMessage.take(160)}")
      assert(
        referenceEntries(table.spark, table.name) == referencesBefore,
        s"the rejected drop leaves the reference set as it was, found " +
          s"${referenceEntries(table.spark, table.name)}")
    }

}

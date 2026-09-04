package harness

import org.apache.iceberg.exceptions.ValidationException
import org.apache.spark.sql.Row

/**
 * Write-audit-publish staging and publish: a write made under `spark.wap.id` commits its data and its snapshot while
 * `main` goes on reading the rows it already read, and a publish is what moves `main` onto that snapshot.
 *
 * A staged snapshot is durable and unreferenced. The table's reference set is exactly `main` while a write is staged
 * and exactly `main` once it is published, so the staged snapshot is reachable through its identifier rather than
 * through a name the table carries. That is what makes the audit step possible: the data is committed and readable by
 * snapshot while every reader of `main` sees the table as it was.
 *
 * Operations: an INSERT, an INSERT OVERWRITE, a MERGE and an UPDATE made under an identifier, a DELETE made under one,
 * two identifiers staged at once and published one at a time by identifier, cherrypick_snapshot and publish_changes as
 * the two publishes, a second publish of a write already published, and a publish of a staged snapshot an expiration
 * removed.
 *
 * Preparation axes: file format. Each family runs in both columnar formats on the write-audit-publish table, whose
 * preparation already proved the property is set, the seed rows are in place and `main` is the only reference.
 *
 * Case families: 10 families over 2 formats, contributing 20 cases.
 */
trait ScenarioWriteAuditPublishStaging extends ScenarioBranchKit {
  import Rows._

  /** Every write-audit-publish staging and publish case, in the order this file introduces the families. */
  lazy val writeAuditPublishStagingCases: List[TestCase] =
    preparedWriteAuditPublishTables.map(stagedInsertStaysOffMain) ++
      preparedWriteAuditPublishTables.map(stagedOverwriteStaysOffMain) ++
      preparedWriteAuditPublishTables.map(stagedMergeStaysOffMain) ++
      preparedWriteAuditPublishTables.map(stagedUpdateStaysOffMain) ++
      preparedWriteAuditPublishTables.map(stagedDeleteCommitsToMain) ++
      preparedWriteAuditPublishTables.map(twoIdentifiersStageIndependently) ++
      preparedWriteAuditPublishTables.map(cherryPickPublishesTheStagedWrite) ++
      preparedWriteAuditPublishTables.map(publishChangesPublishesTheStagedWrite) ++
      preparedWriteAuditPublishTables.map(republishingTheSameWriteRejected) ++
      preparedWriteAuditPublishTables.map(publishingAnExpiredStagedWriteRejected)

  // --- the case bodies the surface above composes ---

  /**
   * A staged INSERT commits one snapshot carrying its identifier. `main` keeps its binding and its rows, the table
   * carries `main` alone, and the row the insert wrote is readable through the staged snapshot.
   */
  private def stagedInsertStaysOffMain(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.staging.stagedInsertStaysOffMain") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)
      val seededSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      stagingUnder(table.spark, "staged-insert") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "staged-insert")}")
      }

      val stagedCommitId = stagedSnapshotId(table.spark, table.name, "staged-insert")

      assert(
        referenceEntries(table.spark, table.name) ==
          Seq((mainBranchName, branchReferenceType, seededSnapshotId)),
        s"the staged write leaves $mainBranchName as the only reference, on $seededSnapshotId, " +
          s"found ${referenceEntries(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the staged write leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        parentSnapshotId(table.spark, table.name, stagedCommitId).contains(seededSnapshotId),
        s"the staged snapshot descends from $seededSnapshotId, found " +
          s"${parentSnapshotId(table.spark, table.name, stagedCommitId)}")
      assert(
        keysOf(
          table.spark
            .sql(
              s"SELECT $columnNameList FROM ${table.name} VERSION AS OF $stagedCommitId " +
                s"ORDER BY ${Core.long0.columnName}")
            .collect()
            .toSeq) == List(1L, 2L, 3L, 6L),
        "the staged snapshot reads the row the staged write added")
    }

  /**
   * A staged INSERT OVERWRITE commits the replacement it produced without changing what `main` reads, so the widest
   * row-replacing write is still an audit step until it is published.
   */
  private def stagedOverwriteStaysOffMain(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.staging.stagedOverwriteStaysOffMain") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)
      val seededSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      stagingUnder(table.spark, "staged-overwrite") {
        table.spark.sql(s"INSERT OVERWRITE ${table.name} VALUES ${coreRow(6, "staged-overwrite")}")
      }

      val stagedCommitId = stagedSnapshotId(table.spark, table.name, "staged-overwrite")

      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the staged overwrite leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceEntries(table.spark, table.name) ==
          Seq((mainBranchName, branchReferenceType, seededSnapshotId)),
        s"the staged overwrite leaves $mainBranchName as the only reference, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        table.spark
          .sql(
            s"SELECT $columnNameList FROM ${table.name} VERSION AS OF $stagedCommitId " +
              s"ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq == Seq(expectedCoreRow(6L, "staged-overwrite")),
        "the staged snapshot reads the row the overwrite left behind")
    }

  /**
   * A staged MERGE commits its matched update and its unmatched insert together, and `main` reads neither of them
   * until the snapshot is published.
   */
  private def stagedMergeStaysOffMain(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.staging.stagedMergeStaysOffMain") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      stagingUnder(table.spark, "staged-merge") {
        table.spark.sql(
          s"MERGE INTO ${table.name} AS target USING " +
            "(SELECT CAST(1 AS BIGINT) AS key UNION ALL SELECT CAST(6 AS BIGINT) AS key) AS source " +
            s"ON target.${Core.long0.columnName} = source.key " +
            s"WHEN MATCHED THEN UPDATE SET target.${Core.string0.columnName} = 'staged-merge' " +
            s"WHEN NOT MATCHED THEN INSERT ($columnNameList) VALUES " +
            "(source.key, 6, 'staged-merge', 6.5, true, '2024-01-01-05')")
      }

      val stagedCommitId = stagedSnapshotId(table.spark, table.name, "staged-merge")

      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the staged merge leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        table.spark
          .sql(
            s"SELECT $columnNameList FROM ${table.name} VERSION AS OF $stagedCommitId " +
              s"ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq ==
          seededRows.map(row =>
            if (row.get(Core.long0) == 1L) withColumnValue(row, Core.string0, "staged-merge")
            else row) :+ expectedCoreRow(6L, "staged-merge"),
        "the staged snapshot reads both halves of the merge")
    }

  /**
   * A staged UPDATE commits the new value while `main` goes on reading the old one, so a correction can be reviewed
   * against the table it will replace.
   */
  private def stagedUpdateStaysOffMain(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.staging.stagedUpdateStaysOffMain") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      stagingUnder(table.spark, "staged-update") {
        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'staged-update' " +
            s"WHERE ${Core.long0.columnName} = 1")
      }

      val stagedCommitId = stagedSnapshotId(table.spark, table.name, "staged-update")

      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the staged update leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        table.spark
          .sql(
            s"SELECT $columnNameList FROM ${table.name} VERSION AS OF $stagedCommitId " +
              s"ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq ==
          seededRows.map(row =>
            if (row.get(Core.long0) == 1L) withColumnValue(row, Core.string0, "staged-update")
            else row),
        "the staged snapshot reads the value the update wrote")
    }

  /**
   * A DELETE the catalog answers from metadata commits to `main` under an identifier, because the identifier applies
   * to the write path a row-producing statement takes. `main` reads the remaining rows at once and the table stages
   * nothing.
   */
  private def stagedDeleteCommitsToMain(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.staging.stagedDeleteCommitsToMain") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      stagingUnder(table.spark, "staged-delete") {
        table.spark.sql(s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
      }

      assert(
        stagedSnapshotIds(table.spark, table.name, "staged-delete").isEmpty,
        s"the delete stages nothing, found " +
          s"${stagedSnapshotIds(table.spark, table.name, "staged-delete")}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows.filter(row => row.get(Core.long0) != 1L),
        s"$mainBranchName reads the rows the delete left, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceNames(table.spark, table.name) == Seq(mainBranchName),
        s"the delete leaves $mainBranchName as the only reference, found " +
          s"${referenceNames(table.spark, table.name)}")
    }

  /**
   * Two writes staged under different identifiers are two independent snapshots that both descend from the snapshot
   * `main` named when they were staged. publish_changes selects one of them by identifier, so publishing the first
   * puts its row on `main` and leaves the second's row off it, and publishing the second afterwards adds its row on
   * top of the first. Staging names no reference, so `main` is the only reference throughout and both staged
   * snapshots stay retained. `main` accepts the next write once both are published.
   */
  private def twoIdentifiersStageIndependently(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.staging.twoIdentifiersStageIndependently") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)
      val seededSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      stagingUnder(table.spark, "first") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "first")}")
      }
      stagingUnder(table.spark, "second") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(7, "second")}")
      }

      val firstStagedSnapshotId = stagedSnapshotId(table.spark, table.name, "first")
      val secondStagedSnapshotId = stagedSnapshotId(table.spark, table.name, "second")

      assert(
        firstStagedSnapshotId != secondStagedSnapshotId,
        s"the two identifiers stage two snapshots, found $firstStagedSnapshotId twice")
      assert(
        parentSnapshotId(table.spark, table.name, firstStagedSnapshotId)
          .contains(seededSnapshotId) &&
          parentSnapshotId(table.spark, table.name, secondStagedSnapshotId)
            .contains(seededSnapshotId),
        s"both staged snapshots descend from $seededSnapshotId, found " +
          s"${parentSnapshotId(table.spark, table.name, firstStagedSnapshotId)} and " +
          s"${parentSnapshotId(table.spark, table.name, secondStagedSnapshotId)}")
      assert(
        referenceEntries(table.spark, table.name) ==
          Seq((mainBranchName, branchReferenceType, seededSnapshotId)),
        s"staging names no reference, so $mainBranchName is the only one, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"both staged writes leave the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")

      val firstPublishReport = publishChanges(table.spark, table.name, "first")
      val afterFirstSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      assert(
        firstPublishReport == Seq(Row(firstStagedSnapshotId, afterFirstSnapshotId)),
        s"publish_changes selects the snapshot the first identifier staged, found " +
          s"$firstPublishReport")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows :+ expectedCoreRow(6L, "first"),
        s"$mainBranchName reads the first identifier's row alone, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        stagedSnapshotIds(table.spark, table.name, "second") == Seq(secondStagedSnapshotId),
        s"the second identifier still names the snapshot it staged, found " +
          s"${stagedSnapshotIds(table.spark, table.name, "second")}")
      assert(
        referenceNames(table.spark, table.name) == Seq(mainBranchName),
        s"publishing by identifier names no new reference, found " +
          s"${referenceNames(table.spark, table.name)}")

      val secondPublishReport = publishChanges(table.spark, table.name, "second")
      val afterSecondSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      assert(
        secondPublishReport == Seq(Row(secondStagedSnapshotId, afterSecondSnapshotId)),
        s"publish_changes selects the snapshot the second identifier staged, found " +
          s"$secondPublishReport")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows ++ Seq(expectedCoreRow(6L, "first"), expectedCoreRow(7L, "second")),
        s"$mainBranchName reads both identifiers' rows, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        parentSnapshotId(table.spark, table.name, afterSecondSnapshotId)
          .contains(afterFirstSnapshotId),
        s"the second publish builds on the snapshot the first left $mainBranchName on, found " +
          s"${parentSnapshotId(table.spark, table.name, afterSecondSnapshotId)}")
      assert(
        referenceNames(table.spark, table.name) == Seq(mainBranchName),
        s"both publishes leave $mainBranchName as the only reference, found " +
          s"${referenceNames(table.spark, table.name)}")
      assert(
        Seq(firstStagedSnapshotId, secondStagedSnapshotId).forall(
          retainedSnapshotIds(table.spark, table.name).contains),
        s"both staged snapshots stay retained, found " +
          s"${retainedSnapshotIds(table.spark, table.name)}")

      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(8, "after-publish")}")

      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows ++ Seq(
            expectedCoreRow(6L, "first"),
            expectedCoreRow(7L, "second"),
            expectedCoreRow(8L, "after-publish")),
        s"$mainBranchName accepts a write once both identifiers are published, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

  /**
   * cherrypick_snapshot names the staged snapshot and reports the snapshot `main` now holds. `main` reads the staged
   * rows under the schema it already presented, the table still carries `main` alone, the staged snapshot stays
   * retained, and `main` accepts the next write.
   */
  private def cherryPickPublishesTheStagedWrite(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.publish.cherryPickPublishesTheStagedWrite") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      stagingUnder(table.spark, "published") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "published")}")
      }

      val stagedCommitId = stagedSnapshotId(table.spark, table.name, "published")
      val publishReport = cherryPick(table.spark, table.name, stagedCommitId)
      val publishedSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      assert(
        publishReport == Seq(Row(stagedCommitId, publishedSnapshotId)),
        s"cherrypick_snapshot reports publishing $stagedCommitId onto $publishedSnapshotId, found " +
          s"$publishReport")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows :+ expectedCoreRow(6L, "published"),
        s"$mainBranchName reads the staged rows, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        schemaColumnNames(table.spark, table.name) == Core.columnNames,
        s"the publish leaves the schema the table presents, found " +
          s"${schemaColumnNames(table.spark, table.name)}")
      assert(
        referenceNames(table.spark, table.name) == Seq(mainBranchName),
        s"the publish leaves $mainBranchName as the only reference, found " +
          s"${referenceNames(table.spark, table.name)}")
      assert(
        retainedSnapshotIds(table.spark, table.name).contains(stagedCommitId),
        s"the publish keeps the staged snapshot retained, found " +
          s"${retainedSnapshotIds(table.spark, table.name)}")

      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(7, "after-publish")}")

      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows ++ Seq(expectedCoreRow(6L, "published"), expectedCoreRow(7L, "after-publish")),
        s"$mainBranchName accepts a write after the publish, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

  /**
   * publish_changes names the identifier rather than the snapshot and reaches the same result: it reports the staged
   * snapshot it found and the snapshot `main` now holds, and `main` reads the staged rows.
   */
  private def publishChangesPublishesTheStagedWrite(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.publish.publishChangesPublishesTheStagedWrite") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      stagingUnder(table.spark, "by-identifier") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "by-identifier")}")
      }

      val stagedCommitId = stagedSnapshotId(table.spark, table.name, "by-identifier")
      val publishReport = publishChanges(table.spark, table.name, "by-identifier")
      val publishedSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      assert(
        publishReport == Seq(Row(stagedCommitId, publishedSnapshotId)),
        s"publish_changes reports publishing $stagedCommitId onto $publishedSnapshotId, found " +
          s"$publishReport")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows :+ expectedCoreRow(6L, "by-identifier"),
        s"$mainBranchName reads the staged rows, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceNames(table.spark, table.name) == Seq(mainBranchName),
        s"the publish leaves $mainBranchName as the only reference, found " +
          s"${referenceNames(table.spark, table.name)}")
    }

  /**
   * A write that has already been published is rejected as a validation failure when it is published again, so a
   * retried publish cannot apply the same rows twice. `main` keeps the binding and the rows the first publish gave
   * it.
   */
  private def republishingTheSameWriteRejected(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.publish.republishingTheSameWrite.rejected") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      stagingUnder(table.spark, "republished") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "republished")}")
      }

      val stagedCommitId = stagedSnapshotId(table.spark, table.name, "republished")

      cherryPick(table.spark, table.name, stagedCommitId)

      val publishedSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)
      val rejection = Check.intercept[ValidationException](
        cherryPick(table.spark, table.name, stagedCommitId))

      assert(
        rejection.getMessage.contains("already"),
        s"the rejection names the publish that already applied, found " +
          s"${rejection.getMessage.take(160)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == publishedSnapshotId,
        s"the rejected publish leaves $mainBranchName on $publishedSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows :+ expectedCoreRow(6L, "republished"),
        s"the rejected publish leaves the rows the first publish gave $mainBranchName, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

  /**
   * A staged snapshot is unreferenced, so an expiration that keeps the referenced ones removes it and the identifier
   * stops naming anything the table holds. Publishing it afterwards is rejected as an argument failure naming the
   * identifier, and `main` keeps its rows.
   */
  private def publishingAnExpiredStagedWriteRejected(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.publish.publishingAnExpiredStagedWrite.rejected") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      stagingUnder(table.spark, "expired") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "expired")}")
      }

      val stagedCommitId = stagedSnapshotId(table.spark, table.name, "expired")

      expireUnreferencedSnapshots(table.spark, table.name)

      assert(
        !retainedSnapshotIds(table.spark, table.name).contains(stagedCommitId),
        s"expiration removes the unreferenced staged snapshot, found " +
          s"${retainedSnapshotIds(table.spark, table.name)}")
      assert(
        stagedSnapshotIds(table.spark, table.name, "expired").isEmpty,
        s"the identifier names nothing the table holds, found " +
          s"${stagedSnapshotIds(table.spark, table.name, "expired")}")

      val rejection = Check.intercept[ValidationException](
        publishChanges(table.spark, table.name, "expired"))

      assert(
        rejection.getMessage.contains("unknown WAP ID") && rejection.getMessage.contains("expired"),
        s"the rejection names the identifier it could not find, found " +
          s"${rejection.getMessage.take(160)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the rejected publish leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

}

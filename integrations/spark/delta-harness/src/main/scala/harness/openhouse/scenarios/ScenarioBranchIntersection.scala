package harness

import org.apache.spark.sql.AnalysisException

/**
 * Where a branch meets the rest of the table: time travel to a snapshot from before the branch point, a table
 * rename, the maintenance procedures that decide which snapshots a table keeps, a table-wide evolution, and the file
 * format a branch-routed write materializes.
 *
 * Each family is the branch's side of one parent capability. Time travel names a snapshot rather than a reference, so
 * a read that names one resolves against that snapshot and a session already routed at a branch is rejected for
 * naming a second starting point. A rename moves the table's metadata, so every reference the table carries answers
 * under the new name. Maintenance is defined against the references a table carries, so expiration keeps the snapshot
 * each reference names and compaction rewrites the files one reference reads. Evolution and the declared file format
 * belong to the table, so a write aimed at a branch obeys both.
 *
 * Operations: VERSION AS OF and TIMESTAMP AS OF a pre-branch snapshot, and the rejection a routed session gets for
 * naming one; ALTER TABLE RENAME TO; expire_snapshots over a table carrying two diverged references;
 * rewrite_data_files on `main`; expire_snapshots aimed at the snapshot a reference names; ALTER TABLE WRITE ORDERED
 * BY followed by a branch write; and a branch write whose data files are read back from the table's file metadata.
 *
 * Preparation axes: file format, and how much history the table carries. Each family runs in both columnar formats.
 * The families that need a snapshot from before the branch point start from the branched two-snapshot table; the
 * families that act on the branch itself start from the branched table.
 *
 * Case families: 7 families over 2 formats, contributing 14 cases.
 */
trait ScenarioBranchIntersection extends BranchTableFixtures {

  /** Every branch-intersection case, in the order this file introduces the families. */
  lazy val branchIntersectionCases: List[TestCase] =
    preparedBranchedTwoSnapshotTables.map(timeTravelBeforeTheBranchPoint) ++
      preparedBranchedTwoSnapshotTables.map(renameCarriesEveryReference) ++
      preparedBranchedTwoSnapshotTables.map(expireKeepsEveryReferenceHead) ++
      preparedBranchedTwoSnapshotTables.map(rewriteDataFilesKeepsTheBranch) ++
      preparedBranchedTables.map(expireReferenceTargetRejected) ++
      preparedBranchedTables.map(branchWriteAfterWriteOrder) ++
      preparedBranchedTables.map(branchWriteMaterializesTheDeclaredFormat)

  // --- the case bodies the surface above composes ---

  /**
   * A snapshot from before the branch point still reads the rows it held once the branch has committed past it, both
   * by identifier and by the commit timestamp that identifies the same snapshot. A session routed at the branch reads
   * the branch rows, and naming a snapshot inside that session is rejected, because the read has been given two
   * starting points at once.
   */
  private def timeTravelBeforeTheBranchPoint(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.timeTravel.beforeTheBranchPoint") { table =>
      val ancestry = snapshotIds(table.spark, table.name)
      val branchPointCommitTimestamp = table.spark
        .sql(
          s"SELECT CAST(committed_at AS STRING) FROM ${table.name}.snapshots " +
            "ORDER BY committed_at LIMIT 1")
        .collect()(0)
        .getString(0)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "after-branch")}")

      val preBranchRows = table.spark
        .sql(
          s"SELECT $columnNameList FROM ${table.name} VERSION AS OF ${ancestry.head} " +
            s"ORDER BY ${Core.long0.columnName}")
        .collect()
        .toSeq

      assert(
        keysOf(preBranchRows) == List(1L, 2L, 3L),
        s"the snapshot before the branch point reads the rows it held, found ${keysOf(preBranchRows)}")
      assert(
        table.spark
          .sql(
            s"SELECT $columnNameList FROM ${table.name} " +
              s"TIMESTAMP AS OF '$branchPointCommitTimestamp' ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq == preBranchRows,
        "the commit timestamp of that snapshot resolves to the same rows its identifier does")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the branch write leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")

      routedAt(table.spark, auditBranchName) {
        val rejection = Check.intercept[IllegalArgumentException](
          table.spark
            .sql(
              s"SELECT $columnNameList FROM ${table.name} VERSION AS OF ${ancestry.head} " +
                s"ORDER BY ${Core.long0.columnName}")
            .collect())

        assert(
          rejection.getMessage.contains("Cannot override ref") &&
            rejection.getMessage.contains(ancestry.head.toString),
          s"the rejection names the snapshot the routed session was asked to read, found " +
            s"${rejection.getMessage.take(160)}")
        assert(
          PreparedTable.currentRows(table.spark, table.name, Core) ==
            seededRows :+ expectedCoreRow(6L, "after-branch"),
          s"the routed read without a named snapshot returns the branch rows, found " +
            s"${PreparedTable.currentRows(table.spark, table.name, Core)}")
      }
    }

  /**
   * ALTER TABLE RENAME TO moves the table's metadata, so the renamed table carries the same references bound to the
   * same snapshots, reads the same rows through each of them, resolves the same pre-branch snapshot, and accepts
   * writes on both references. The old name stops resolving.
   */
  private def renameCarriesEveryReference(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.rename.referencesFollowTheTable") { table =>
      val ancestry = snapshotIds(table.spark, table.name)

      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "before-rename")}")

      val referencesBefore = referenceEntries(table.spark, table.name)
      val branchRows = rowsOn(table.spark, table.name, auditBranchName)
      val mainRows = rowsOn(table.spark, table.name, mainBranchName)
      val renamedTable = s"${table.name}_renamed"

      withTrackedRename(table.spark.sql(_), table.name) { renameTo =>
        renameTo(renamedTable)

        assert(
          referenceEntries(table.spark, renamedTable) == referencesBefore,
          s"the renamed table carries the references the table carried, found " +
            s"${referenceEntries(table.spark, renamedTable)}")
        assert(
          rowsOn(table.spark, renamedTable, auditBranchName) == branchRows &&
            rowsOn(table.spark, renamedTable, mainBranchName) == mainRows,
          "the renamed table reads the same rows through each reference")
        assert(
          keysOf(
            table.spark
              .sql(
                s"SELECT $columnNameList FROM $renamedTable VERSION AS OF ${ancestry.head} " +
                  s"ORDER BY ${Core.long0.columnName}")
              .collect()
              .toSeq) == List(1L, 2L, 3L),
          "the renamed table resolves the snapshot from before the branch point")

        val rejection = Check.intercept[AnalysisException](
          table.spark.sql(s"SELECT count(*) FROM ${table.name}").collect())

        assert(
          rejection.getMessage.contains(catalogRelativeTableName(table.name).split('.').last),
          s"the rejection names the table the old name no longer resolves to, found " +
            s"${rejection.getMessage.take(160)}")

        table.spark.sql(s"INSERT INTO $renamedTable VALUES ${coreRow(7, "after-rename")}")
        table.spark.sql(
          s"INSERT INTO $renamedTable.branch_$auditBranchName VALUES ${coreRow(8, "branch-after-rename")}")

        assert(
          rowsOn(table.spark, renamedTable, mainBranchName) ==
            mainRows :+ expectedCoreRow(7L, "after-rename"),
          s"the renamed table accepts a write on $mainBranchName, found " +
            s"${rowsOn(table.spark, renamedTable, mainBranchName)}")
        assert(
          rowsOn(table.spark, renamedTable, auditBranchName) ==
            branchRows :+ expectedCoreRow(8L, "branch-after-rename"),
          s"the renamed table accepts a write on $auditBranchName, found " +
            s"${rowsOn(table.spark, renamedTable, auditBranchName)}")

        renameTo(table.name)
      }
    }

  /**
   * Snapshot expiration keeps the snapshot each reference names. Once `main` and the branch have both committed, an
   * expiration that retains one snapshot leaves exactly the two heads, keeps both references bound where they were,
   * and both references still read their rows and accept a write.
   */
  private def expireKeepsEveryReferenceHead(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.maintenance.expireKeepsEveryReferenceHead") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "branch-side")}")
      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(7, "main-side")}")

      val referencesBefore = referenceEntries(table.spark, table.name)
      val branchRows = rowsOn(table.spark, table.name, auditBranchName)
      val mainRows = rowsOn(table.spark, table.name, mainBranchName)

      assert(
        retainedSnapshotIds(table.spark, table.name).size == 4,
        s"the table retains the seed, the second seed commit and the two divergent commits, found " +
          s"${retainedSnapshotIds(table.spark, table.name)}")

      expireUnreferencedSnapshots(table.spark, table.name)

      assert(
        retainedSnapshotIds(table.spark, table.name).sorted ==
          referencesBefore.map { case (_, _, snapshotId) => snapshotId }.sorted,
        s"expiration retains exactly the snapshot each reference names, found " +
          s"${retainedSnapshotIds(table.spark, table.name).sorted}")
      assert(
        referenceEntries(table.spark, table.name) == referencesBefore,
        s"expiration leaves every reference where it was, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) == branchRows &&
          rowsOn(table.spark, table.name, mainBranchName) == mainRows,
        "expiration leaves both references reading their rows")

      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(8, "after-expire")}")
      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(9, "branch-after-expire")}")

      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          mainRows :+ expectedCoreRow(8L, "after-expire"),
        s"$mainBranchName accepts a write after expiration, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          branchRows :+ expectedCoreRow(9L, "branch-after-expire"),
        s"$auditBranchName accepts a write after expiration, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
    }

  /**
   * rewrite_data_files rewrites the files `main` reads and reports how many it rewrote and added. The branch keeps
   * the snapshot it names and the rows it reads, and `main` reads exactly the rows it read before the compaction.
   */
  private def rewriteDataFilesKeepsTheBranch(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.maintenance.rewriteDataFilesKeepsTheBranch") { table =>
      val branchSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val branchRows = rowsOn(table.spark, table.name, auditBranchName)
      val mainRows = rowsOn(table.spark, table.name, mainBranchName)
      val dataFilePathsBefore = currentDataFilePaths(table.spark, table.name)
      val rewriteReport = table.spark
        .sql(
          "CALL openhouse.system.rewrite_data_files(" +
            s"table => '${catalogRelativeTableName(table.name)}', " +
            "options => map('rewrite-all', 'true'))")
        .collect()(0)

      assert(
        dataFilePathsBefore.size >= 2,
        s"the two seed commits leave the files this compaction folds, found " +
          s"${dataFilePathsBefore.size}")
      assert(
        rewriteReport.getInt(0) == dataFilePathsBefore.size,
        s"the compaction rewrites the ${dataFilePathsBefore.size} data files it started from, " +
          s"rewrote ${rewriteReport.getInt(0)}")
      assert(
        rewriteReport.getInt(1) == currentDataFilePaths(table.spark, table.name).size,
        s"the compaction adds the data files it left behind, added ${rewriteReport.getInt(1)}")
      assert(
        currentDataFilePaths(table.spark, table.name).intersect(dataFilePathsBefore).isEmpty,
        s"the compaction leaves $mainBranchName reading files it wrote, found " +
          s"${currentDataFilePaths(table.spark, table.name).intersect(dataFilePathsBefore)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == mainRows,
        s"the compaction keeps the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, auditBranchName) == branchSnapshotId,
        s"the compaction leaves $auditBranchName on $branchSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, auditBranchName)}")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) == branchRows,
        s"the compaction keeps the rows $auditBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
    }

  /**
   * Expiration aimed at the snapshot a reference names is rejected as an argument failure naming that reference. The
   * reference set, the retained snapshots and the rows each reference reads stay as they were.
   */
  private def expireReferenceTargetRejected(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.maintenance.expireReferenceTarget.rejected") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "protected")}")

      val branchSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val referencesBefore = referenceEntries(table.spark, table.name)
      val retainedBefore = retainedSnapshotIds(table.spark, table.name).sorted
      val branchRows = rowsOn(table.spark, table.name, auditBranchName)

      val rejection = Check.intercept[IllegalArgumentException](
        table.spark.sql(
          "CALL openhouse.system.expire_snapshots(" +
            s"table => '${catalogRelativeTableName(table.name)}', " +
            s"snapshot_ids => ARRAY(${branchSnapshotId}L))"))

      assert(
        rejection.getMessage.contains(auditBranchName),
        s"the rejection names the reference that protects the snapshot, found " +
          s"${rejection.getMessage.take(160)}")
      assert(
        referenceEntries(table.spark, table.name) == referencesBefore,
        s"the rejected expiration leaves every reference where it was, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        retainedSnapshotIds(table.spark, table.name).sorted == retainedBefore,
        s"the rejected expiration retains every snapshot, found " +
          s"${retainedSnapshotIds(table.spark, table.name).sorted}")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) == branchRows,
        s"the rejected expiration keeps the rows $auditBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
    }

  /**
   * A write sort order applies to the table, so a branch write made after ALTER TABLE WRITE ORDERED BY lands under
   * the range distribution the sort order sets. The branch reads its seeded rows and the written one, and `main`
   * reads the rows it read.
   */
  private def branchWriteAfterWriteOrder(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.tableEvolution.branchWriteAfterWriteOrder") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      table.spark.sql(s"ALTER TABLE ${table.name} WRITE ORDERED BY ${Core.long0.columnName}")

      assert(
        persistedProperty(table.spark, table.name, "write.distribution-mode").contains("range"),
        s"the write order sets the range distribution the table writes under, found " +
          s"${persistedProperty(table.spark, table.name, "write.distribution-mode")}")
      assert(
        referenceEntries(table.spark, table.name) ==
          Seq(
            (auditBranchName, branchReferenceType, branchPointSnapshotId),
            (mainBranchName, branchReferenceType, branchPointSnapshotId)),
        s"the write order commits no snapshot, found ${referenceEntries(table.spark, table.name)}")

      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "ordered-write")}")

      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows :+ expectedCoreRow(6L, "ordered-write"),
        s"$auditBranchName reads the row written under the sort order, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the ordered branch write leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

  /**
   * A branch write materializes its data in the format the table declares, so every data file the write adds carries
   * that format's extension. The files `main` reads stay exactly the ones it already read.
   */
  /**
   * A branch write materializes its data in the format the table declares. The branch advances from the branch point
   * to a snapshot that descends from it, reads its seeded rows and the inserted one, and scans the files it already
   * scanned plus the ones the write added, each of them carrying the declared format's extension. `main` keeps its
   * binding, the rows it read and the files its snapshot references, so the added files are the branch's alone.
   */
  private def branchWriteMaterializesTheDeclaredFormat(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.fileFormat.branchWriteMaterializesTheDeclaredFormat") { table =>
      val declaredFormat = table.spark
        .sql(s"SHOW TBLPROPERTIES ${table.name} ('write.format.default')")
        .collect()(0)
        .getString(1)
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val mainSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)
      val mainDataFilePaths = currentDataFilePaths(table.spark, table.name)
      val mainRows = rowsOn(table.spark, table.name, mainBranchName)
      val branchFilePathsBefore =
        scannedDataFilePathsOn(table.spark, table.name, auditBranchName)

      assert(
        branchFilePathsBefore == mainDataFilePaths,
        s"$auditBranchName starts on the files $mainBranchName reads, found $branchFilePathsBefore")

      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "materialized")}")

      val branchHeadSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val branchFilePathsAfter = scannedDataFilePathsOn(table.spark, table.name, auditBranchName)
      val addedFilePaths = branchFilePathsAfter.diff(branchFilePathsBefore)

      assert(
        parentSnapshotId(table.spark, table.name, branchHeadSnapshotId)
          .contains(branchPointSnapshotId),
        s"$auditBranchName advances to a snapshot descending from $branchPointSnapshotId, found " +
          s"${parentSnapshotId(table.spark, table.name, branchHeadSnapshotId)}")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          mainRows :+ expectedCoreRow(6L, "materialized"),
        s"$auditBranchName reads its seeded rows and the inserted row, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        addedFilePaths.nonEmpty && branchFilePathsAfter == branchFilePathsBefore ++ addedFilePaths,
        s"$auditBranchName scans the files it already scanned and the ones the write added, found " +
          s"$branchFilePathsAfter")
      assert(
        addedFilePaths.forall(_.toLowerCase.endsWith(s".$declaredFormat")),
        s"every data file the branch write adds is a .$declaredFormat file, found $addedFilePaths")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == mainSnapshotId,
        s"$mainBranchName still names $mainSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
      assert(
        currentDataFilePaths(table.spark, table.name) == mainDataFilePaths,
        s"the snapshot $mainBranchName names references the files it already referenced, found " +
          s"${currentDataFilePaths(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == mainRows,
        s"the branch write leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

}

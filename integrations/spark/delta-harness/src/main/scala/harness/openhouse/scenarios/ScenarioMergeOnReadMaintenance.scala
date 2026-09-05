package harness

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Maintenance over a live position-delete file.
 *
 * Each maintenance procedure has to decide whether to fold the delete into the data it rewrites, carry it forward, or
 * leave it in place. All of them keep the row the delete removed out of the live row set, so a table that accumulates
 * delete files stays maintainable and a maintenance run stays safe to schedule.
 *
 * Every family reads the procedure's own report of what it rewrote, expired or removed, so each case proves the
 * effect it is named for and not only that the rows survived it.
 *
 * Operations: rewrite_data_files, rewrite_position_delete_files, expire_snapshots, rewrite_manifests,
 * remove_orphan_files against an orphan the case plants and backdates itself, and a compaction followed by an
 * expiration whose two effects are proven separately.
 *
 * Preparation axes: the two merge-on-read verify layouts, each seeded into one data file with key 1 deleted, for the
 * six families that start from a live delete; and the single-file merge-on-read table in each format for the family
 * that writes its own delete first.
 *
 * Case families: seven families contributing 14 cases, 12 on the two deleted preparations and 2 on the self-deleting
 * one.
 */
trait ScenarioMergeOnReadMaintenance extends MergeOnReadTableFixtures {

  /** Every merge-on-read maintenance case, one deleted preparation at a time, then the self-deleting family. */
  lazy val mergeOnReadMaintenanceCases: List[TestCase] =
    preparedDeletedMergeOnReadTables.flatMap { preparation =>
      List(
        rewriteDataFilesLeavesDanglingDeleteCase(preparation),
        rewritePositionDeleteFilesFoldsDanglingDeleteCase(preparation),
        expireSnapshotsKeepsDeleteCase(preparation),
        rewriteManifestsKeepsDeleteCase(preparation),
        removeOrphanFilesKeepsDeleteCase(preparation),
        compactThenExpireKeepsDeleteCase(preparation))
    } ++ fileFormats.map(format =>
      rewritePositionDeleteFilesCompactsCase(preparedSingleFileMergeOnReadTable(format)))

  // --- the case bodies the surface above composes ---

  /**
   * rewrite_data_files folds the live delete into the data it compacts, so the deleted key stays gone and the two live
   * rows read back, while the position-delete file it superseded stays referenced until a later procedure clears it.
   */
  private def rewriteDataFilesLeavesDanglingDeleteCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.maintenance.rewriteDataFilesFoldsDelete") { table =>
      assert(
        currentDeleteFileCount(table.spark, table.name) == 1,
        s"the preparation leaves the delete this compaction folds, found " +
          s"${currentDeleteFileCount(table.spark, table.name)}")

      val dataFilePathsBefore = currentDataFilePaths(table.spark, table.name)
      val rewriteReport = table.spark
        .sql(
          "CALL openhouse.system.rewrite_data_files(" +
            s"table => '${catalogRelativeTableName(table.name)}', " +
            "options => map('rewrite-all', 'true'))")
        .collect()(0)
      val dataFilePathsAfter = currentDataFilePaths(table.spark, table.name)

      assert(
        rewriteReport.getInt(0) == dataFilePathsBefore.size,
        s"the compaction rewrites the ${dataFilePathsBefore.size} data files it started from, " +
          s"rewrote ${rewriteReport.getInt(0)}")
      assert(
        rewriteReport.getInt(1) == dataFilePathsAfter.size,
        s"the compaction adds the ${dataFilePathsAfter.size} data files it left behind, added " +
          s"${rewriteReport.getInt(1)}")
      assert(
        dataFilePathsAfter.intersect(dataFilePathsBefore).isEmpty,
        s"every data file the compaction rewrote leaves the current set, " +
          s"${dataFilePathsAfter.intersect(dataFilePathsBefore)} stayed")
      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L),
        s"compaction folds the delete and keeps the live rows, found " +
          s"${liveKeys(table.spark, table.name)}")
    }

  /**
   * rewrite_position_delete_files after a compaction clears the position-delete file the compaction superseded, and
   * the live row set stays as it was, so the two procedures together return the table to a delete-free state.
   */
  private def rewritePositionDeleteFilesFoldsDanglingDeleteCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.maintenance.rewritePositionDeletesClearsDangling") { table =>
      table.spark.sql(
        "CALL openhouse.system.rewrite_data_files(" +
          s"table => '${catalogRelativeTableName(table.name)}', " +
          "options => map('rewrite-all', 'true'))")
      val danglingDeleteFileCount = currentDeleteFileCount(table.spark, table.name)
      val rewriteReport = table.spark
        .sql(
          "CALL openhouse.system.rewrite_position_delete_files(" +
            s"table => '${catalogRelativeTableName(table.name)}', " +
            "options => map('rewrite-all', 'true'))")
        .collect()(0)

      assert(
        danglingDeleteFileCount >= 1,
        s"the compaction leaves the delete file this call clears, found $danglingDeleteFileCount")
      assert(
        rewriteReport.getInt(0) == danglingDeleteFileCount,
        s"the call rewrites the $danglingDeleteFileCount delete files it found, rewrote " +
          s"${rewriteReport.getInt(0)}")
      assert(
        rewriteReport.getInt(1) == currentDeleteFileCount(table.spark, table.name),
        s"the call adds the ${currentDeleteFileCount(table.spark, table.name)} delete files it " +
          s"left behind, added ${rewriteReport.getInt(1)}")
      assert(
        currentDeleteFileCount(table.spark, table.name) == 0,
        s"the folded delete is cleared, found " +
          s"${currentDeleteFileCount(table.spark, table.name)} delete files")
      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L),
        s"clearing the folded delete keeps the live rows, found " +
          s"${liveKeys(table.spark, table.name)}")
    }

  /**
   * expire_snapshots drops every snapshot the table no longer needs to retain and keeps the one it currently reads
   * from, so the history shrinks to the retained snapshot while the live rows and the delete file the reader applies
   * stay exactly as they were.
   */
  private def expireSnapshotsKeepsDeleteCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.maintenance.expireSnapshotsKeepsDelete") { table =>
      val snapshotIdsBefore = retainedSnapshotIds(table.spark, table.name)
      val currentSnapshotIdBefore = currentSnapshotId(table.spark, table.name)
      val deleteFileCountBefore = currentDeleteFileCount(table.spark, table.name)

      assert(
        snapshotIdsBefore.size >= 2,
        s"the preparation leaves history for the expiration to drop, found $snapshotIdsBefore")

      table.spark.sql(
        "CALL openhouse.system.expire_snapshots(" +
          s"table => '${catalogRelativeTableName(table.name)}', " +
          s"older_than => TIMESTAMP '$expirationCutoff', " +
          "retain_last => 1)")
      val snapshotIdsAfter = retainedSnapshotIds(table.spark, table.name)

      assert(
        snapshotIdsAfter == Seq(currentSnapshotIdBefore),
        s"the expiration retains the snapshot the table reads from and drops the rest, " +
          s"went from $snapshotIdsBefore to $snapshotIdsAfter")
      assert(
        snapshotIdsBefore.filterNot(_ == currentSnapshotIdBefore).forall(expiredSnapshotId =>
          !snapshotIdsAfter.contains(expiredSnapshotId)),
        s"every superseded snapshot is gone, found $snapshotIdsAfter")
      assert(
        currentSnapshotId(table.spark, table.name) == currentSnapshotIdBefore,
        "the expiration leaves the table reading from the snapshot it was already on")
      assert(
        currentDeleteFileCount(table.spark, table.name) == deleteFileCountBefore,
        s"the expiration keeps the $deleteFileCountBefore delete files the reader applies, found " +
          s"${currentDeleteFileCount(table.spark, table.name)}")
      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L),
        s"expiration keeps the delete applied, found ${liveKeys(table.spark, table.name)}")
    }

  /**
   * rewrite_manifests replaces the manifests the table references with the ones it wrote, and the data files, delete
   * files and live rows the manifests point at stay exactly as they were.
   */
  private def rewriteManifestsKeepsDeleteCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.maintenance.rewriteManifestsKeepsDelete") { table =>
      // Each append commits its own data manifest, so the rewrite has several to merge into one.
      val appendedKeys = List(4L, 5L, 6L)
      appendedKeys.foreach(key =>
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(key, s"row-$key")}"))

      // rewrite_manifests rewrites the data manifests, so the data manifests present beforehand are the eligible set
      // and the delete manifests are the part it is expected to leave alone.
      val dataManifestPathsBefore =
        currentManifestPaths(table.spark, table.name, dataManifestContent)
      val deleteManifestPathsBefore =
        currentManifestPaths(table.spark, table.name, deleteManifestContent)
      val dataFileCountBefore = currentDataFileCount(table.spark, table.name)
      val deleteFileCountBefore = currentDeleteFileCount(table.spark, table.name)
      val rewriteReport = table.spark
        .sql(
          "CALL openhouse.system.rewrite_manifests(" +
            s"table => '${catalogRelativeTableName(table.name)}')")
        .collect()(0)
      val dataManifestPathsAfter =
        currentManifestPaths(table.spark, table.name, dataManifestContent)

      assert(
        dataManifestPathsBefore.size >= 2,
        s"the appends leave several data manifests for the rewrite to merge, found " +
          s"${dataManifestPathsBefore.size}")
      assert(
        rewriteReport.getInt(0) == dataManifestPathsBefore.size,
        s"the call rewrites the ${dataManifestPathsBefore.size} data manifests it started from, " +
          s"rewrote ${rewriteReport.getInt(0)}")
      assert(
        rewriteReport.getInt(1) == dataManifestPathsAfter.size,
        s"the call adds the ${dataManifestPathsAfter.size} data manifests it left behind, added " +
          s"${rewriteReport.getInt(1)}")
      assert(
        dataManifestPathsAfter.intersect(dataManifestPathsBefore).isEmpty,
        s"every data manifest the rewrite merged leaves the current set, " +
          s"${dataManifestPathsAfter.intersect(dataManifestPathsBefore)} stayed")
      assert(
        currentManifestPaths(table.spark, table.name, deleteManifestContent) ==
          deleteManifestPathsBefore,
        s"the rewrite leaves the delete manifests as they were, found " +
          s"${currentManifestPaths(table.spark, table.name, deleteManifestContent)}")
      assert(
        currentDataFileCount(table.spark, table.name) == dataFileCountBefore,
        s"manifest rewriting keeps the $dataFileCountBefore data files, found " +
          s"${currentDataFileCount(table.spark, table.name)}")
      assert(
        currentDeleteFileCount(table.spark, table.name) == deleteFileCountBefore,
        s"manifest rewriting keeps the $deleteFileCountBefore delete files, found " +
          s"${currentDeleteFileCount(table.spark, table.name)}")
      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L) ++ appendedKeys,
        s"manifest rewriting keeps the delete applied, found ${liveKeys(table.spark, table.name)}")
    }

  /**
   * remove_orphan_files removes exactly the unreferenced file the case plants under the table's data directory and
   * leaves every referenced data file, delete file and row in place.
   *
   * The case owns the orphan end to end: it writes the file itself, backdates its modification time behind the
   * cutoff through the Hadoop FileSystem the table's own path resolves to, asserts the procedure reports that one
   * location, and deletes the orphan on the way out if the procedure left it. The table's referenced files were
   * written moments ago, so they sit ahead of the cutoff and are outside the removal window, which is what makes
   * "exactly the orphan" a real assertion.
   *
   * Locations are compared as fully qualified paths resolved through the same filesystem, so the scheme and
   * authority the procedure reports line up with the planted path on local storage and on HDFS alike.
   */
  private def removeOrphanFilesKeepsDeleteCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.maintenance.removeOrphanFilesRemovesTheOrphan") { table =>
      val dataFileCountBefore = currentDataFileCount(table.spark, table.name)
      val deleteFileCountBefore = currentDeleteFileCount(table.spark, table.name)
      val fileSystem = tableFileSystem(table)
      val orphanPath = plantBackdatedOrphanFile(table, fileSystem)

      OwnedTableLifecycle.withCleanup(
        if (fileSystem.exists(orphanPath)) {
          assert(
            fileSystem.delete(orphanPath, false),
            s"the case removes the orphan it planted at $orphanPath")
        }) {
        val removedPaths = table.spark
          .sql(
            "CALL openhouse.system.remove_orphan_files(" +
              s"table => '${catalogRelativeTableName(table.name)}', " +
              s"older_than => TIMESTAMP '${orphanRemovalCutoffTimestamp()}')")
          .collect()
          .toSeq
          .map(row => qualified(fileSystem, new Path(row.getString(0))))

        assert(
          removedPaths == Seq(orphanPath),
          s"the call removes exactly the planted orphan $orphanPath, removed $removedPaths")
        assert(
          !fileSystem.exists(orphanPath),
          s"the removed orphan is gone from storage, $orphanPath is still there")
        assert(
          currentDataFileCount(table.spark, table.name) == dataFileCountBefore,
          s"orphan removal keeps the $dataFileCountBefore referenced data files, found " +
            s"${currentDataFileCount(table.spark, table.name)}")
        assert(
          currentDeleteFileCount(table.spark, table.name) == deleteFileCountBefore,
          s"orphan removal keeps the $deleteFileCountBefore referenced delete files, found " +
            s"${currentDeleteFileCount(table.spark, table.name)}")
        assert(
          liveKeys(table.spark, table.name) == Seq(2L, 3L),
          s"orphan removal keeps the delete applied, found ${liveKeys(table.spark, table.name)}")
      }
    }

  /** The filesystem the table's own data files resolve through, which is the one the procedure reports against. */
  private def tableFileSystem(table: PreparedTable[CoreTable.type]): FileSystem =
    referencedDataFilePath(table).getFileSystem(table.spark.sessionState.newHadoopConf())

  /** One data-file path the table's current snapshot references. */
  private def referencedDataFilePath(table: PreparedTable[CoreTable.type]): Path =
    new Path(
      table.spark
        .sql(s"SELECT file_path FROM ${table.name}.files LIMIT 1")
        .collect()(0)
        .getString(0))

  /**
   * The fully qualified form of `path` on `fileSystem`, carrying its scheme and authority. Comparing qualified paths
   * keeps the assertion correct whether the procedure reports a bare path, a `file:` URI or an `hdfs:` URI.
   */
  private def qualified(fileSystem: FileSystem, path: Path): Path =
    path.makeQualified(fileSystem.getUri, fileSystem.getWorkingDirectory)

  /**
   * Writes an unreferenced file beside the table's data files and backdates it well behind the removal cutoff, then
   * returns its qualified path. Placing it beside a referenced data file puts it inside the directory tree the
   * procedure scans, and it is unreferenced because no manifest names it.
   */
  private def plantBackdatedOrphanFile(
      table: PreparedTable[CoreTable.type],
      fileSystem: FileSystem): Path = {
    val orphanPath = qualified(
      fileSystem,
      new Path(referencedDataFilePath(table).getParent, "harness-planted-orphan.parquet"))
    val backdatedModificationTime =
      System.currentTimeMillis() - TimeUnit.DAYS.toMillis(orphanAgeDays)

    fileSystem.create(orphanPath, true).close()
    fileSystem.setTimes(orphanPath, backdatedModificationTime, -1L)

    assert(
      fileSystem.getFileStatus(orphanPath).getModificationTime == backdatedModificationTime,
      s"the planted orphan carries the backdated modification time the cutoff is measured " +
        s"against, found ${fileSystem.getFileStatus(orphanPath).getModificationTime} for " +
        s"$backdatedModificationTime")
    orphanPath
  }

  /** How far behind the present the planted orphan's modification time sits. */
  private val orphanAgeDays = 30L

  /** How far behind the present the removal cutoff sits, which the procedure requires to exceed 24 hours. */
  private val orphanRemovalCutoffDays = 7L

  /**
   * The older_than cutoff for orphan removal: far enough back that the procedure accepts it and the table's
   * just-written files sit ahead of it, and recent enough that the planted orphan sits behind it.
   */
  private def orphanRemovalCutoffTimestamp(): String =
    LocalDateTime
      .now()
      .minusDays(orphanRemovalCutoffDays)
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

  /**
   * The pair a scheduled maintenance run issues together, with each half proven on its own: the compaction rewrites
   * the delete files it found, then the expiration drops the history that compaction superseded and leaves the table
   * reading from one snapshot. The live rows are the same at the end as at the start.
   */
  private def compactThenExpireKeepsDeleteCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.maintenance.compactThenExpireKeepsDelete") { table =>
      val deleteFileCountBefore = currentDeleteFileCount(table.spark, table.name)
      val compactionReport = table.spark
        .sql(
          "CALL openhouse.system.rewrite_position_delete_files(" +
            s"table => '${catalogRelativeTableName(table.name)}', " +
            "options => map('rewrite-all', 'true'))")
        .collect()(0)

      assert(
        deleteFileCountBefore >= 1,
        s"the preparation leaves the delete files this compaction rewrites, found $deleteFileCountBefore")
      assert(
        compactionReport.getInt(0) == deleteFileCountBefore,
        s"the compaction rewrites the $deleteFileCountBefore delete files it found, rewrote " +
          s"${compactionReport.getInt(0)}")
      assert(
        compactionReport.getInt(1) == currentDeleteFileCount(table.spark, table.name),
        s"the compaction adds the ${currentDeleteFileCount(table.spark, table.name)} delete " +
          s"files it left behind, added ${compactionReport.getInt(1)}")
      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L),
        s"the compaction keeps the delete applied, found ${liveKeys(table.spark, table.name)}")

      val snapshotIdsBeforeExpiration = retainedSnapshotIds(table.spark, table.name)
      val currentSnapshotIdBeforeExpiration = currentSnapshotId(table.spark, table.name)
      table.spark.sql(
        "CALL openhouse.system.expire_snapshots(" +
          s"table => '${catalogRelativeTableName(table.name)}', " +
          s"older_than => TIMESTAMP '$expirationCutoff', " +
          "retain_last => 1)")
      val snapshotIdsAfterExpiration = retainedSnapshotIds(table.spark, table.name)

      assert(
        snapshotIdsBeforeExpiration.size >= 2,
        s"the compaction leaves history for the expiration to drop, found $snapshotIdsBeforeExpiration")
      assert(
        snapshotIdsAfterExpiration == Seq(currentSnapshotIdBeforeExpiration),
        s"the expiration retains the compacted snapshot and drops the rest, went from " +
          s"$snapshotIdsBeforeExpiration to $snapshotIdsAfterExpiration")
      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L),
        s"compaction followed by expiration keeps the delete applied, found " +
          s"${liveKeys(table.spark, table.name)}")
    }

  /**
   * The older_than cutoff for snapshot expiration. It is far ahead of any snapshot the harness commits, so every
   * snapshot outside the retained one is inside the expiration window and the call has real work to do.
   */
  private val expirationCutoff = "2999-01-01 00:00:00"

  /**
   * A merge-on-read DELETE writes one position-delete file, and rewrite_position_delete_files compacts it while the
   * two surviving rows stay readable, so the procedure is available to a table that accumulates delete files.
   */
  private def rewritePositionDeleteFilesCompactsCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("mergeOnRead.maintenance.rewritePositionDeleteFiles") { table =>
      assert(
        persistedProperty(table.spark, table.name, "write.delete.mode").contains("merge-on-read"),
        "the table persists write.delete.mode as merge-on-read before the delete under test")

      table.spark.sql(s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")

      assert(
        currentDeleteFileCount(table.spark, table.name) == 1,
        s"the merge-on-read delete writes one position-delete file, found " +
          s"${currentDeleteFileCount(table.spark, table.name)}")

      val rewriteReport = table.spark
        .sql(
          "CALL openhouse.system.rewrite_position_delete_files(" +
            s"table => '${catalogRelativeTableName(table.name)}', " +
            "options => map('rewrite-all', 'true'))")
        .collect()(0)

      assert(
        rewriteReport.getInt(0) == 1,
        s"the call rewrites the one delete file it found, rewrote ${rewriteReport.getInt(0)}")
      assert(
        rewriteReport.getInt(1) == currentDeleteFileCount(table.spark, table.name),
        s"the call adds the ${currentDeleteFileCount(table.spark, table.name)} delete files it " +
          s"left behind, added ${rewriteReport.getInt(1)}")
      assert(
        liveKeys(table.spark, table.name) == Seq(2L, 3L),
        s"compacting the position deletes keeps the live rows, found " +
          s"${liveKeys(table.spark, table.name)}")
    }

}

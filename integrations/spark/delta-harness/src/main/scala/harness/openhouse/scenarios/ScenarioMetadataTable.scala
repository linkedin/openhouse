package harness

import org.apache.spark.sql.SparkSession
import scala.annotation.tailrec

/**
 * Metadata tables: the hidden metadata columns a scan exposes and the Iceberg metadata tables the catalog serves
 * alongside every table.
 *
 * Operations: select the hidden _file, _pos, _spec_id and _partition columns; query every metadata table the catalog
 * serves (entries, files, manifests, snapshots, history, refs, partitions, metadata_log_entries, data_files and the
 * all_* variants); and read the snapshot, history, files and manifests counts of a two-snapshot table.
 *
 * Preparation axes: in each of the two columnar formats, the standard seeded core table for the hidden-column family
 * and the two-snapshot core table for the two families that count metadata rows.
 *
 * Case families: three families contributing 6 cases.
 */
trait ScenarioMetadataTable extends ScenarioKit {

  /** Every metadata-table case, one file format at a time. */
  lazy val metadataTableCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        hiddenColumnsCase(preparedStandardTable(format)),
        tableSweepCase(preparedTwoSnapshotTable(format)),
        snapshotAndHistoryCase(preparedTwoSnapshotTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * Selecting the hidden metadata columns _file, _pos, _spec_id and _partition returns one row per seed row, each
   * carrying a populated file path and a position of zero or greater.
   */
  private def hiddenColumnsCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("metadata.hiddenColumns") { table =>
      val rows = table.spark
        .sql(s"SELECT _file, _pos, _spec_id, _partition FROM ${table.name}")
        .collect()
        .toSeq
      val lineage = mainSnapshotLineage(table.spark, table.name)
      val activeFiles = activeDataFileEntries(table.spark, table.name)
      val hiddenFilePaths = rows.map(row => filePath(row.getString(0))).distinct
      val hiddenSpecIds = rows.map(_.getInt(2)).distinct
      val positionsByFile = rows.groupBy(row => filePath(row.getString(0))).map { case (path, fileRows) =>
        path -> fileRows.map(_.getLong(1)).sorted.toList
      }
      val expectedPositionsByFile = activeFiles.map { activeFile =>
        activeFile.path -> (0L until activeFile.recordCount).toList
      }.toMap

      assert(
        rows.size == standardSeedRowCount,
        s"hidden metadata columns should return 3 rows, got ${rows.size}")
      assert(lineage.size == 1, s"the seeded table has one active snapshot lineage, found $lineage")
      assert(
        activeFiles.map(_.snapshotId).distinct == lineage,
        s"the active data-file entries belong to the current snapshot: $activeFiles lineage=$lineage")
      assert(
        hiddenFilePaths.toSet == activeFiles.map(_.path).toSet,
        s"_file must name the active data file paths: hidden=$hiddenFilePaths active=$activeFiles")
      assert(
        hiddenFilePaths.forall(_.nonEmpty),
        "_file should be populated for every row")
      assert(
        positionsByFile == expectedPositionsByFile,
        s"_pos should enumerate rows within each active file: hidden=$positionsByFile active=$activeFiles")
      assert(
        hiddenSpecIds == activeFiles.map(_.specId).distinct,
        s"_spec_id must match the active data-file specs: hidden=$hiddenSpecIds active=$activeFiles")
    }

  /**
   * Every Iceberg metadata table is queryable, and the snapshots metadata table reports the table's 2 snapshots.
   */
  private def tableSweepCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("metadata.tableSweep") { table =>
      val metadataTables = Seq(
        "entries",
        "files",
        "manifests",
        "snapshots",
        "history",
        "refs",
        "partitions",
        "metadata_log_entries",
        "data_files",
        "all_data_files",
        "all_manifests",
        "all_entries",
        "all_files")
      metadataTables.foreach { metadataTable =>
        table.spark.sql(s"SELECT * FROM ${table.name}.`$metadataTable` LIMIT 1").collect()
      }

      assertTwoSnapshotMetadataIdentity(table, "metadata.tableSweep")
    }

  /**
   * The snapshots and history metadata tables report the same two-snapshot lineage as the main ref, and the file and
   * manifest metadata belongs to those active snapshots.
   */
  private def snapshotAndHistoryCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("metadata.snapshotAndHistory") { table =>
      assertTwoSnapshotMetadataIdentity(table, "metadata.snapshotAndHistory")
    }

  private final case class SnapshotNode(snapshotId: Long, parentSnapshotId: Option[Long])

  private final case class ActiveDataFileEntry(snapshotId: Long, specId: Int, path: String, recordCount: Long)

  private final case class ManifestEntry(addedSnapshotId: Long, path: String)

  private def assertTwoSnapshotMetadataIdentity(table: PreparedTable[CoreTable.type], family: String): Unit = {
    val lineage = mainSnapshotLineage(table.spark, table.name)
    val historySnapshotIds = table.spark
      .sql(s"SELECT snapshot_id FROM ${table.name}.history ORDER BY made_current_at, snapshot_id")
      .collect()
      .toSeq
      .map(_.getLong(0))
    val activeFiles = activeDataFileEntries(table.spark, table.name)
    val manifests = activeManifests(table.spark, table.name)

    assert(lineage.size == 2, s"$family expects a two-snapshot main lineage, found $lineage")
    assert(
      snapshotIdsByCommitTime(table.spark, table.name) == lineage,
      s"$family snapshots must follow the main lineage")
    assert(historySnapshotIds == lineage, s"$family history must follow the main lineage: $historySnapshotIds")
    assert(
      activeFiles.map(_.snapshotId).toSet == lineage.toSet,
      s"$family active data files must belong to the lineage snapshots: active=$activeFiles lineage=$lineage")
    assert(
      activeFiles.groupBy(_.snapshotId).map { case (snapshotId, files) =>
        snapshotId -> files.map(_.path).distinct.size
      } ==
        lineage.map(snapshotId => snapshotId -> 2).toMap,
      s"$family expects two active data files from each lineage snapshot, found $activeFiles")
    assert(
      manifests.map(_.addedSnapshotId).toSet == lineage.toSet,
      s"$family active manifests must belong to the lineage snapshots: manifests=$manifests lineage=$lineage")
    assert(
      manifests.groupBy(_.addedSnapshotId).map { case (snapshotId, entries) =>
        snapshotId -> entries.map(_.path).distinct.size
      } == lineage.map(snapshotId => snapshotId -> 1).toMap,
      s"$family expects one active manifest from each lineage snapshot, found $manifests")
    assert(
      manifests.forall(_.path.nonEmpty),
      s"$family active manifest paths must be populated: $manifests")
  }

  private def mainSnapshotLineage(spark: SparkSession, table: String): List[Long] = {
    val parentBySnapshot = spark
      .sql(s"SELECT snapshot_id, parent_id FROM $table.snapshots")
      .collect()
      .toSeq
      .map(row => SnapshotNode(row.getLong(0), if (row.isNullAt(1)) None else Some(row.getLong(1))))
      .map(node => node.snapshotId -> node.parentSnapshotId)
      .toMap
    val mainSnapshot = mainSnapshotId(spark, table)

    @tailrec
    def walk(snapshotId: Long, childLineage: List[Long]): List[Long] =
      parentBySnapshot.get(snapshotId) match {
        case Some(Some(parentSnapshotId)) =>
          walk(parentSnapshotId, snapshotId :: childLineage)
        case Some(None) =>
          snapshotId :: childLineage
        case None =>
          throw new AssertionError(s"snapshot $snapshotId is missing from $table.snapshots")
      }

    walk(mainSnapshot, Nil)
  }

  private def mainSnapshotId(spark: SparkSession, table: String): Long =
    spark
      .sql(s"SELECT snapshot_id FROM $table.refs WHERE name = 'main'")
      .collect()
      .toSeq
      .map(_.getLong(0)) match {
      case Seq(snapshotId) => snapshotId
      case snapshotIds     => throw new AssertionError(s"main names one active snapshot, found $snapshotIds")
    }

  private def snapshotIdsByCommitTime(spark: SparkSession, table: String): List[Long] =
    spark
      .sql(s"SELECT snapshot_id FROM $table.snapshots ORDER BY committed_at, snapshot_id")
      .collect()
      .toSeq
      .map(_.getLong(0))
      .toList

  private def activeDataFileEntries(spark: SparkSession, table: String): Seq[ActiveDataFileEntry] =
    spark
      .sql(
        s"SELECT snapshot_id, data_file.spec_id, data_file.file_path, data_file.record_count FROM $table.entries " +
          "WHERE status != 2 AND data_file.content = 0 ORDER BY snapshot_id, data_file.file_path")
      .collect()
      .toSeq
      .map(row => ActiveDataFileEntry(row.getLong(0), row.getInt(1), filePath(row.getString(2)), row.getLong(3)))

  private def activeManifests(spark: SparkSession, table: String): Seq[ManifestEntry] =
    spark
      .sql(s"SELECT added_snapshot_id, path FROM $table.manifests ORDER BY added_snapshot_id, path")
      .collect()
      .toSeq
      .map(row => ManifestEntry(row.getLong(0), filePath(row.getString(1))))

  private def filePath(value: String): String = value.stripPrefix("file:")
}

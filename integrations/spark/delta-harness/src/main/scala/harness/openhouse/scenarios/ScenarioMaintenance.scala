package harness

import java.nio.file.{Files, Paths}
import java.nio.file.attribute.FileTime

import org.apache.spark.sql.{Row, SparkSession}

/**
 * Maintenance: the procedures that rewrite a table's files and metadata while preserving the rows a reader sees.
 *
 * Operations: expire_snapshots down to the newest snapshot, rewrite_data_files over the table's data files,
 * remove_orphan_files over the table's directory, rewrite_manifests over a fragmented manifest list, remove_orphan_
 * files against a planted backdated stray file, and rewrite_data_files across an ADD COLUMN.
 *
 * Preparation axes: in each of the two columnar formats, the two-snapshot core table for the three procedures that run
 * over an existing history, an unseeded core table for the manifest family, which fragments the manifest list itself,
 * and the standard seeded core table for the planted-orphan and schema-evolution families.
 *
 * Case families: six families contributing 12 cases.
 */
trait ScenarioMaintenance extends MaintenanceTableFixtures {

  /** Every maintenance case, one file format at a time. */
  lazy val maintenanceCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        expireSnapshotsCase(preparedTwoSnapshotTable(format)),
        rewriteDataFilesCase(preparedTwoSnapshotTable(format)),
        removeOrphanFilesCase(preparedTwoSnapshotTable(format)),
        rewriteManifestsCase(preparedEmptyStandardTable(format)),
        removeOrphanFilesPlantedCase(preparedStandardTable(format)),
        rewriteDataFilesAfterAddColumnCase(preparedStandardTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** expire_snapshots with retain_last=1 removes the seed snapshot and leaves all 5 current rows intact. */
  private def expireSnapshotsCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("maintenance.expireSnapshots") { table =>
      table.spark.sql(
        "CALL openhouse.system.expire_snapshots(" +
          s"table => '${catalogRelativeTableName(table.name)}', " +
          "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
          "retain_last => 1)")

      assert(
        table.rows.size == 5,
        "expire_snapshots must preserve the current data")
      assert(
        table.snapshotCount < table.preparedSnapshotCount,
        "expire_snapshots must reduce the snapshot count: " +
          s"${table.preparedSnapshotCount} -> ${table.snapshotCount}")
    }

  /** rewrite_data_files compacts the data files and leaves all 5 rows intact. */
  private def rewriteDataFilesCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("maintenance.rewriteDataFiles") { table =>
      val schemaBefore = coreSchema(table.spark, table.name)
      val rowsBefore = table.rows
      val snapshotsBefore = table.snapshotCount
      val currentSnapshotBefore = currentSnapshotId(table.spark, table.name)
      val filesBefore = activeDataFileStats(table.spark, table.name)

      assert(filesBefore.count == 4, s"rewrite_data_files starts from four active files, found $filesBefore")
      val result = rewriteDataFiles(
        table.spark,
        table.name,
        "'rewrite-all', 'true', 'min-input-files', '2'")
      val filesAfter = activeDataFileStats(table.spark, table.name)

      assertRewriteResult(result, filesBefore.count, 1L, filesBefore.bytes)
      assert(filesAfter.count == 1, s"rewrite_data_files compacts four active files into one, found $filesAfter")
      assert(
        filesAfter.count < filesBefore.count,
        s"rewrite_data_files reduces active files: $filesBefore -> $filesAfter")
      assert(filesAfter.bytes > 0L, s"rewrite_data_files leaves one non-empty active file, found $filesAfter")
      assert(
        table.snapshotCount == snapshotsBefore + 1,
        s"rewrite_data_files commits one snapshot: $snapshotsBefore -> ${table.snapshotCount}")
      assert(
        currentSnapshotId(table.spark, table.name) != currentSnapshotBefore,
        s"rewrite_data_files advances main from snapshot $currentSnapshotBefore")
      assert(coreSchema(table.spark, table.name) == schemaBefore, "rewrite_data_files preserves the core schema")
      assert(table.rows == rowsBefore, s"rewrite_data_files preserves exact rows: ${table.rows}")
    }

  /** remove_orphan_files over a table with only catalog-owned files leaves all 5 rows intact. */
  private def removeOrphanFilesCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("maintenance.removeOrphanFiles") { table =>
      table.spark.sql(
        "CALL openhouse.system.remove_orphan_files(" +
          s"table => '${catalogRelativeTableName(table.name)}', " +
          "older_than => TIMESTAMP '2020-01-01 00:00:00')")

      assert(table.rows.size == 5, "orphan removal must preserve rows")
    }

  /**
   * After 5 single-row inserts fragment the manifest list, rewrite_manifests compacts it to fewer manifests while
   * preserving all 5 rows.
   */
  private def rewriteManifestsCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("maintenance.rewriteManifests") { table =>
      (1 to 5).foreach(index =>
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            coreRow(index, s"r$index")))
      val manifestCountBefore = table.spark
        .sql(s"SELECT count(*) FROM ${table.name}.manifests")
        .collect()(0)
        .getLong(0)
      table.spark.sql(
        "CALL openhouse.system.rewrite_manifests(" +
          s"table => '${catalogRelativeTableName(table.name)}', " +
          "use_caching => false)")
      val manifestCountAfter = table.spark
        .sql(s"SELECT count(*) FROM ${table.name}.manifests")
        .collect()(0)
        .getLong(0)

      assert(
        queryCount(table.spark, s"SELECT count(*) FROM ${table.name}") == "5",
        "rewrite_manifests should preserve the five rows")
      assert(
        manifestCountBefore >= 2 &&
          manifestCountAfter < manifestCountBefore,
        "rewrite_manifests should compact the manifest set: " +
          s"before=$manifestCountBefore after=$manifestCountAfter")
    }

  /**
   * remove_orphan_files deletes a planted, backdated stray file next to a real data file while the table's 3 live rows
   * remain intact.
   */
  private def removeOrphanFilesPlantedCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("maintenance.removeOrphanFiles.planted") { table =>
      val dataFile = table.spark
        .sql(s"SELECT file_path FROM ${table.name}.files LIMIT 1")
        .collect()(0)
        .getString(0)
        .stripPrefix("file:")
      val orphanFile = Paths
        .get(dataFile)
        .getParent
        .resolve(s"${table.name.split('.').last}_orphan.parquet")
      Files.write(orphanFile, "orphan-payload".getBytes)
      Files.setLastModifiedTime(orphanFile, FileTime.fromMillis(1546300800000L))

      table.spark.sql(
        "CALL openhouse.system.remove_orphan_files(" +
          s"table => '${catalogRelativeTableName(table.name)}', " +
          "older_than => TIMESTAMP '2020-01-01 00:00:00')")
      assert(
        Files.notExists(orphanFile),
        "remove_orphan_files should delete the planted orphan")
      assert(
        queryCount(table.spark, s"SELECT count(*) FROM ${table.name}") == "3",
        "remove_orphan_files should preserve live data")
    }

  /**
   * Compacting a table after an ADD COLUMN and inserts into the new column preserves all rows, the new column's
   * non-null values, and null for rows written before the column was added.
   */
  private def rewriteDataFilesAfterAddColumnCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("maintenance.rewriteDataFiles.afterAddColumn") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColumnRowNine")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColumnRowTen")
      val schemaBefore = table.spark.table(table.name).schema.fieldNames.toSeq
      val rowsBefore = coreRowsWithExtraColumn(table.spark, table.name)
      val snapshotsBefore = table.snapshotCount
      val currentSnapshotBefore = currentSnapshotId(table.spark, table.name)
      val filesBefore = activeDataFileStats(table.spark, table.name)

      assert(filesBefore.count == 4, s"rewrite_data_files starts from four active files, found $filesBefore")
      val result = rewriteDataFiles(
        table.spark,
        table.name,
        "'rewrite-all', 'true', 'min-input-files', '2'")
      val filesAfter = activeDataFileStats(table.spark, table.name)
      val rowsAfter = coreRowsWithExtraColumn(table.spark, table.name)

      assertRewriteResult(result, filesBefore.count, 1L, filesBefore.bytes)
      assert(
        filesAfter.count == 1,
        s"rewrite_data_files compacts four evolved-schema active files into one, found $filesAfter")
      assert(
        filesAfter.count < filesBefore.count,
        s"rewrite_data_files reduces active files: $filesBefore -> $filesAfter")
      assert(filesAfter.bytes > 0L, s"rewrite_data_files leaves one non-empty active file, found $filesAfter")
      assert(
        table.snapshotCount == snapshotsBefore + 1,
        s"rewrite_data_files commits one snapshot: $snapshotsBefore -> ${table.snapshotCount}")
      assert(
        currentSnapshotId(table.spark, table.name) != currentSnapshotBefore,
        s"rewrite_data_files advances main from snapshot $currentSnapshotBefore")
      assert(
        table.spark.table(table.name).schema.fieldNames.toSeq == schemaBefore,
        "rewrite_data_files preserves the evolved schema")
      assert(rowsAfter == rowsBefore, s"rewrite_data_files preserves exact evolved rows: $rowsAfter")
      assert(
        queryCount(
          table.spark,
          s"SELECT count(*) FROM ${table.name} WHERE extra_col IN (42, 43)") == "2",
        "compaction should preserve both evolved values")
      assert(
        queryCount(
          table.spark,
          s"SELECT count(*) FROM ${table.name} WHERE extra_col IS NULL") == "3",
        "pre-evolution rows should remain null")
    }

  private final case class ActiveDataFileStats(count: Long, bytes: Long)

  private final case class RewriteDataFilesResult(
      rewrittenDataFilesCount: Long,
      addedDataFilesCount: Long,
      rewrittenBytesCount: Long,
      failedDataFilesCount: Long)

  private def activeDataFileStats(spark: SparkSession, table: String): ActiveDataFileStats = {
    val row = spark
      .sql(s"SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM $table.data_files")
      .collect()(0)

    ActiveDataFileStats(row.getLong(0), row.getLong(1))
  }

  private def rewriteDataFiles(
      spark: SparkSession,
      table: String,
      options: String): RewriteDataFilesResult = {
    val result = spark.sql(
      "CALL openhouse.system.rewrite_data_files(" +
        s"table => '${catalogRelativeTableName(table)}', " +
        s"options => map($options))")
    val rows = result.collect().toSeq

    assert(rows.size == 1, s"rewrite_data_files returns one result row, found ${rows.size}")
    RewriteDataFilesResult(
      longField(rows.head, "rewritten_data_files_count"),
      longField(rows.head, "added_data_files_count"),
      longField(rows.head, "rewritten_bytes_count"),
      longField(rows.head, "failed_data_files_count"))
  }

  private def longField(row: Row, field: String): Long =
    Option(row.getAs[Any](field)) match {
      case Some(value: java.lang.Number) => value.longValue()
      case Some(value)                   => throw new AssertionError(s"$field must be numeric, found $value")
      case None                          => throw new AssertionError(s"$field must be populated in $row")
    }

  private def assertRewriteResult(
      result: RewriteDataFilesResult,
      expectedRewrittenFiles: Long,
      expectedAddedFiles: Long,
      expectedRewrittenBytes: Long): Unit = {
    assert(
      result == RewriteDataFilesResult(expectedRewrittenFiles, expectedAddedFiles, expectedRewrittenBytes, 0L),
      s"rewrite_data_files result must report the completed rewrite, found $result")
  }

  private def currentSnapshotId(spark: SparkSession, table: String): Long =
    spark
      .sql(s"SELECT snapshot_id FROM $table.refs WHERE name = 'main'")
      .collect()
      .toSeq
      .map(_.getLong(0)) match {
      case Seq(snapshotId) => snapshotId
      case snapshotIds     => throw new AssertionError(s"main names one active snapshot, found $snapshotIds")
    }

  private def coreSchema(spark: SparkSession, table: String): Seq[String] =
    spark.table(table).schema.fieldNames.toSeq

  private def coreRowsWithExtraColumn(spark: SparkSession, table: String): Seq[Row] =
    spark
      .sql(s"SELECT $columnNameList, extra_col FROM $table ORDER BY ${Core.long0.columnName}")
      .collect()
      .toSeq

}

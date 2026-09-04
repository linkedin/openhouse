package harness

import org.apache.spark.sql.{Row, SparkSession}

/**
 * Compaction planning: rewrite_data_files packs multiple live data files into one replacement file, and Iceberg
 * exposes the file-sequence metadata a planner can use while preserving every row.
 *
 * Operations: rewrite_data_files with rewrite-all over a table whose data files are unevenly sized, and
 * rewrite_data_files with rewrite-all over append commits whose captured snapshot order carries strictly increasing
 * file_sequence_numbers in current and all entry metadata.
 *
 * Preparation axes: one table per family, built inside the case with write.distribution-mode=none so each insert
 * commits its own data file. The bin-packing family runs in each of the two columnar formats. File-sequence metadata
 * follows append commit order the same way in every file format, so that family runs on Parquet alone.
 *
 * Case families: two families contributing 3 cases.
 */
trait ScenarioCompactionPlanning extends ScenarioKit {

  /** The bin-packing case in each columnar format, then the file-sequence metadata case on Parquet. */
  lazy val compactionPlanningCases: List[TestCase] =
    fileFormats.map(format =>
      TestCase(
        s"compactionPlanning.binPackByFileLength @ $format",
        binPackByFileLengthCase(format))) ++
      List(
        TestCase("compactionPlanning.fileSequenceMetadata @ parquet", fileSequenceMetadataCase))

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** The count and the total byte size of the table's current data files. */
  private def activeDataFileStats(spark: SparkSession, table: String): ActiveDataFileStats = {
    val stats = spark
      .sql(s"SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM $table.data_files")
      .collect()(0)

    ActiveDataFileStats(stats.getLong(0), stats.getLong(1))
  }

  private def rewriteAll(spark: SparkSession, table: String): RewriteDataFilesResult = {
    val result = spark.sql(
      s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}', " +
        "options => map('rewrite-all', 'true'))")
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

  /**
   * Compacting a table whose data files are unevenly sized preserves the row count and every row's value, which is the
   * observable result of packing rewrite groups by file length. The case observes the resulting row and file state.
   */
  private def binPackByFileLengthCase(format: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = TableTest.nextQualifiedTableName(ctx.namespace)

    withOwnedTable(spark.sql(_), table)(
      spark.sql(
        s"CREATE TABLE $table (id bigint, s string) USING $dataSource TBLPROPERTIES (" +
          s"'write.format.default'='$format', 'write.distribution-mode'='none')")) {
      // Unevenly sized data files: a tiny one, a small one, and a big one.
      spark.sql(s"INSERT INTO $table VALUES (1,'a')")
      spark.sql(s"INSERT INTO $table VALUES (2,'b'),(3,'c')")
      spark.sql(s"INSERT INTO $table SELECT id, repeat('x', 200) FROM range(100, 400, 1, 1)")

      val filesBefore = activeDataFileStats(spark, table)
      val rowsBefore = spark.sql(s"SELECT id, s FROM $table ORDER BY id").collect().toSeq
      val snapshotCountBefore = PreparedTable.snapshotCount(spark, table)

      assert(filesBefore.count == 4, s"[$format] bin-pack starts from exactly 4 active files, found $filesBefore")
      assert(filesBefore.bytes > 0L, s"[$format] bin-pack starts from non-empty active files, found $filesBefore")
      val result = rewriteAll(spark, table)
      val filesAfter = activeDataFileStats(spark, table)

      assert(
        result == RewriteDataFilesResult(4L, 1L, filesBefore.bytes, 0L),
        s"[$format] bin-pack result must report four rewritten files into one new file, found $result")
      assert(filesAfter.count == 1, s"[$format] bin-pack compacts four active files into one, found $filesAfter")
      assert(
        filesAfter.count < filesBefore.count,
        s"[$format] bin-pack reduces active files: $filesBefore -> $filesAfter")
      assert(filesAfter.bytes > 0L, s"[$format] bin-pack leaves one non-empty active file, found $filesAfter")
      assert(
        PreparedTable.snapshotCount(spark, table) == snapshotCountBefore + 1,
        s"[$format] bin-pack commits one snapshot: $snapshotCountBefore -> " +
          s"${PreparedTable.snapshotCount(spark, table)}")
      assert(
        spark.sql(s"SELECT id, s FROM $table ORDER BY id").collect().toSeq == rowsBefore,
        s"[$format] bin-pack preserves exact rows")
    }
  }

  /**
   * Each append commit becomes the table's main snapshot. The entries and all_entries metadata tables expose exactly
   * one current data-file entry for each captured append snapshot, and those entries carry file_sequence_number values
   * that strictly increase in captured commit order. The rewrite then consumes those files and preserves the row set.
   */
  private def fileSequenceMetadataCase(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = TableTest.nextQualifiedTableName(ctx.namespace)

    withOwnedTable(spark.sql(_), table)(
      spark.sql(
        s"CREATE TABLE $table (id bigint, s string) USING $dataSource TBLPROPERTIES (" +
          "'write.format.default'='parquet', 'write.distribution-mode'='none')")) {
      // Several commits produce several data files with distinct, increasing file-sequence-numbers.
      val numberOfCommits = 4
      val appendCommits = (0 until numberOfCommits).map { commitIndex =>
        spark.sql(s"INSERT INTO $table VALUES (${commitIndex}L, 'c$commitIndex')")
        AppendCommit(commitIndex, currentSnapshotId(spark, table))
      }.toList

      val filesBefore = activeDataFileStats(spark, table)
      val currentEntries = appendCommits.map(commit => entryForSnapshot(spark, table, "entries", commit))
      val allEntries = appendCommits.map(commit => entryForSnapshot(spark, table, "all_entries", commit))
      val sequenceNumbers = currentEntries.map(_.fileSequenceNumber)

      assert(
        appendCommits.map(_.snapshotId).distinct.size == numberOfCommits,
        s"each append commit advances main to a distinct snapshot, found $appendCommits")
      assert(currentEntries == allEntries, s"entries and all_entries must expose the same append files: $allEntries")
      assert(
        currentEntries.map(_.commitIndex) == appendCommits.map(_.commitIndex),
        s"entry metadata must be read in captured commit order: $currentEntries")
      assert(
        currentEntries.forall(entry => entry.recordCount == 1L && entry.path.nonEmpty),
        s"each captured append snapshot must own one populated one-row data file: $currentEntries")
      assert(
        currentEntries.forall(_.status == 1),
        s"each captured append snapshot must expose an added data-file entry: $currentEntries")
      assert(
        currentEntries.map(_.snapshotId) == appendCommits.map(_.snapshotId),
        s"entry snapshot IDs must match the captured append snapshots: $currentEntries")
      assert(
        sequenceNumbers.sliding(2).forall(pair => pair.head < pair.last),
        s"file sequence numbers must strictly increase in captured commit order: $sequenceNumbers")
      val rowsBefore = spark.sql(s"SELECT id, s FROM $table ORDER BY id").collect().toSeq
      val snapshotCountBefore = PreparedTable.snapshotCount(spark, table)

      assert(filesBefore.count == 4, s"file-sequence case starts from exactly 4 active files, found $filesBefore")
      assert(filesBefore.bytes > 0L, s"file-sequence case starts from non-empty active files, found $filesBefore")
      val result = rewriteAll(spark, table)
      val filesAfter = activeDataFileStats(spark, table)

      assert(
        result == RewriteDataFilesResult(4L, 1L, filesBefore.bytes, 0L),
        s"file-sequence rewrite must report four sequenced files into one new file, found $result")
      assert(filesAfter.count == 1, s"file-sequence rewrite compacts four active files into one, found $filesAfter")
      assert(
        PreparedTable.snapshotCount(spark, table) == snapshotCountBefore + 1,
        s"file-sequence rewrite commits one snapshot: $snapshotCountBefore -> " +
          s"${PreparedTable.snapshotCount(spark, table)}")
      assert(
        spark.sql(s"SELECT id, s FROM $table ORDER BY id").collect().toSeq == rowsBefore,
        s"file-sequence rewrite preserves exact rows")
    }
  }

  private final case class ActiveDataFileStats(count: Long, bytes: Long)

  private final case class AppendCommit(commitIndex: Int, snapshotId: Long)

  private final case class FileSequenceEntry(
      commitIndex: Int,
      snapshotId: Long,
      status: Int,
      fileSequenceNumber: Long,
      path: String,
      recordCount: Long)

  private final case class RewriteDataFilesResult(
      rewrittenDataFilesCount: Long,
      addedDataFilesCount: Long,
      rewrittenBytesCount: Long,
      failedDataFilesCount: Long)

  private def currentSnapshotId(spark: SparkSession, table: String): Long =
    spark
      .sql(s"SELECT snapshot_id FROM $table.refs WHERE name = 'main'")
      .collect()
      .toSeq
      .map(_.getLong(0)) match {
      case Seq(snapshotId) => snapshotId
      case snapshotIds     => throw new AssertionError(s"main names one active snapshot, found $snapshotIds")
    }

  private def entryForSnapshot(
      spark: SparkSession,
      table: String,
      metadataTable: String,
      appendCommit: AppendCommit): FileSequenceEntry = {
    val rows = spark
      .sql(
        s"SELECT snapshot_id, status, file_sequence_number, data_file.file_path, data_file.record_count " +
          s"FROM $table.$metadataTable " +
          s"WHERE snapshot_id = ${appendCommit.snapshotId} AND data_file.content = 0")
      .collect()
      .toSeq

    assert(
      rows.size == 1,
      s"$metadataTable must expose exactly one current entry for $appendCommit, found ${rows.size}: $rows")
    FileSequenceEntry(
      appendCommit.commitIndex,
      rows.head.getLong(0),
      rows.head.getInt(1),
      rows.head.getLong(2),
      rows.head.getString(3).stripPrefix("file:"),
      rows.head.getLong(4))
  }

}

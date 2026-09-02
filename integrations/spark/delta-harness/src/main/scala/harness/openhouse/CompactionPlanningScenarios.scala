package harness

import org.apache.spark.sql.SparkSession

/**
 * Compaction planning: rewrite_data_files packs data files into rewrite groups weighted by file length and spends a
 * budget in file-sequence-number order, and the rewrite it commits preserves every row.
 *
 * Operations: rewrite_data_files with rewrite-all over a table whose data files are unevenly sized, and
 * rewrite_data_files with rewrite-all over a table whose live data-file entries carry distinct, increasing
 * file_sequence_numbers.
 *
 * Preparation axes: one table per family, built inside the case with write.distribution-mode=none so each insert
 * commits its own data file. The bin-packing family runs in each of the two columnar formats. Sequence numbers order
 * commits the same way in every file format, so the ordering family runs on Parquet alone.
 *
 * Case families: two families contributing 3 cases.
 */
trait CompactionPlanningScenarios extends ScenarioKit {

  /** The bin-packing case in each columnar format, then the file-sequence ordering case on Parquet. */
  lazy val compactionPlanningCases: List[Plan.Case] =
    standardFormats.map(format =>
      Plan.Case(
        s"compactionPlanning.binPackByFileLength @ $format",
        binPackByFileLengthCase(format))) ++
      List(
        Plan.Case("compactionPlanning.fileSequenceOrder @ parquet", fileSequenceOrderCase))

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** The count and the total byte size of the table's current data files. */
  private def dataFileStats(spark: SparkSession, table: String): (Long, Long) = {
    val stats = spark
      .sql(s"SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM $table.data_files")
      .collect()(0)
    (stats.getLong(0), stats.getLong(1))
  }

  private def rewriteAll(spark: SparkSession, table: String): Unit =
    spark.sql(
      s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}', " +
        "options => map('rewrite-all', 'true'))")

  /**
   * Compacting a table whose data files are unevenly sized preserves the row count and every row's value, which is the
   * observable result of packing rewrite groups by file length; the weighting itself is a planner decision that no SQL
   * surface exposes.
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
      spark.sql(s"INSERT INTO $table SELECT id, repeat('x', 200) FROM range(100, 400)")

      val (filesBefore, bytesBefore) = dataFileStats(spark, table)
      assert(filesBefore >= 3, s"[$format] expected at least 3 uneven data files, got $filesBefore")
      val rowsBefore = countOf(spark, s"SELECT count(*) FROM $table")

      rewriteAll(spark, table)

      val (filesAfter, bytesAfter) = dataFileStats(spark, table)
      assert(
        countOf(spark, s"SELECT count(*) FROM $table") == rowsBefore,
        s"[$format] rewrite_data_files changed the row count from $rowsBefore")
      val smallestRowValue =
        spark.sql(s"SELECT s FROM $table WHERE id = 1").collect()(0).getString(0)
      assert(smallestRowValue == "a", s"[$format] rewrite altered a row: id=1 s=$smallestRowValue")

      println(
        s"DIAG compactionPlanning.binPackByFileLength[$format]: filesBefore=$filesBefore " +
          s"bytesBefore=$bytesBefore filesAfter=$filesAfter bytesAfter=$bytesAfter rows=$rowsBefore")
    }
  }

  /**
   * file_sequence_number is exposed on the live data-file entries of the entries metadata table and increases
   * monotonically across commits, and rewrite_data_files with rewrite-all preserves the row count and the row set. A
   * budgeted rewrite spends its budget in file-sequence-number order, so that column is the observable half of the
   * ordering decision.
   */
  private def fileSequenceOrderCase(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = TableTest.nextQualifiedTableName(ctx.namespace)

    withOwnedTable(spark.sql(_), table)(
      spark.sql(
        s"CREATE TABLE $table (id bigint, s string) USING $dataSource TBLPROPERTIES (" +
          "'write.format.default'='parquet', 'write.distribution-mode'='none')")) {
      // Several commits produce several data files with distinct, increasing file-sequence-numbers.
      val numberOfCommits = 4
      (0 until numberOfCommits).foreach { commitIndex =>
        spark.sql(s"INSERT INTO $table VALUES (${commitIndex}L, 'c$commitIndex')")
      }

      val sequenceNumbers = spark
        .sql(
          s"SELECT file_sequence_number FROM $table.entries " +
            "WHERE status != 2 AND data_file.content = 0 ORDER BY file_sequence_number")
        .collect()
        .toSeq
        .map(_.getLong(0))
      assert(
        sequenceNumbers.size >= numberOfCommits,
        s"expected at least $numberOfCommits live data-file entries with sequence numbers, " +
          s"got ${sequenceNumbers.size}: $sequenceNumbers")
      assert(
        sequenceNumbers == sequenceNumbers.sorted,
        s"file sequence numbers not monotonic: $sequenceNumbers")
      assert(
        sequenceNumbers.distinct.size >= 2,
        s"expected multiple distinct file sequence numbers, got ${sequenceNumbers.distinct}")
      val rowsBefore = countOf(spark, s"SELECT count(*) FROM $table")

      rewriteAll(spark, table)

      assert(
        countOf(spark, s"SELECT count(*) FROM $table") == rowsBefore,
        s"rewrite changed the row count from $rowsBefore")
      val keys = spark.sql(s"SELECT id FROM $table ORDER BY id").collect().toSeq.map(_.getLong(0))
      assert(keys == (0 until numberOfCommits).map(_.toLong), s"rewrite altered the row set: $keys")

      println(
        s"DIAG compactionPlanning.fileSequenceOrder: fileSequenceNumbers=" +
          s"${sequenceNumbers.mkString(",")} filesAfter=" +
          s"${dataFileStats(spark, table)._1} rows=$rowsBefore")
    }
  }

}

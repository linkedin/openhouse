package harness

import org.apache.iceberg.TableProperties
import org.apache.iceberg.spark.{Spark3Util, SparkSQLProperties}
import scala.collection.JavaConverters._

/**
 * Scan planning: the split size decides how the read path combines data files into read tasks, and every split size
 * returns the same rows.
 *
 * Operations: read a six-file table under a large and a tiny spark.sql.iceberg.split-size, comparing the row set and
 * the read RDD partition count; then plan the same table directly through the Iceberg scan API under a split size
 * above the whole table and one below a single file, comparing the task-group counts.
 *
 * Preparation axes: one table per file format, built inside the case with write.distribution-mode=none and
 * read.split.open-file-cost=1 and filled by six separate inserts, so it holds six separately weighted data files.
 *
 * Case families: one family contributing 2 cases.
 */
trait ScenarioScanPlanning extends ScenarioKit {

  /** The split-size case, one file format at a time. */
  lazy val scanPlanningCases: List[Plan.Case] =
    standardFormats.map(format =>
      Plan.Case(s"scanPlanning.splitSize @ $format", splitSizeCase(format)))

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * Over several small files, a large split size combines them into fewer read tasks and a tiny split size splits them
   * into more, visible through rdd.getNumPartitions, and both reads return the same rows. The planner shows the same
   * effect directly: a split size above the whole table plans one task group, and a split size below one file plans
   * one group per file.
   */
  private def splitSizeCase(format: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = TableTest.nextQualifiedTableName(ctx.namespace)

    // distribution=none plus several separate inserts produces several distinct data files. An open-file-cost of 1
    // sets each file's planning weight to its byte length, making split-size the knob that governs task-group count.
    withOwnedTable(spark.sql(_), table)(
      spark.sql(
        s"CREATE TABLE $table (id bigint, s string) USING $dataSource TBLPROPERTIES (" +
          s"'write.format.default'='$format', 'write.distribution-mode'='none', " +
          "'read.split.open-file-cost'='1')")) {
      val numberOfFiles = 6
      (0 until numberOfFiles).foreach { fileIndex =>
        spark.sql(s"INSERT INTO $table SELECT ${fileIndex}L, repeat('r$fileIndex', 4000)")
      }
      val fileCount = spark.sql(s"SELECT count(*) FROM $table.data_files").collect()(0).getLong(0)
      assert(
        fileCount >= 2,
        s"[$format] expected multiple data files for a split test, got $fileCount")

      val splitSizeKey = SparkSQLProperties.SPLIT_SIZE // "spark.sql.iceberg.split-size"
      val savedSplitSize = spark.conf.getOption(splitSizeKey)
      def keys(): Seq[Long] =
        spark.sql(s"SELECT id FROM $table ORDER BY id").collect().toSeq.map(_.getLong(0))
      def readPartitionCount(): Int = spark.sql(s"SELECT * FROM $table").rdd.getNumPartitions
      val expectedKeys = (0 until numberOfFiles).map(_.toLong)
      try {
        // The row set is invariant under the split size, while the read RDD partition count follows it.
        spark.conf.set(splitSizeKey, (512L * 1024 * 1024).toString)
        val keysUnderLargeSplit = keys()
        val partitionsUnderLargeSplit = readPartitionCount()
        spark.conf.set(splitSizeKey, "1")
        val keysUnderTinySplit = keys()
        val partitionsUnderTinySplit = readPartitionCount()
        assert(
          keysUnderLargeSplit == expectedKeys && keysUnderTinySplit == expectedKeys,
          s"[$format] split-size must leave the row set alone: large=$keysUnderLargeSplit " +
            s"tiny=$keysUnderTinySplit expected=$expectedKeys")
        assert(
          partitionsUnderTinySplit >= partitionsUnderLargeSplit,
          s"[$format] a smaller split-size must keep or raise the read RDD partition count: " +
            s"tiny=$partitionsUnderTinySplit large=$partitionsUnderLargeSplit")

        // The same knob checked directly at the planner: with open-file-cost=1 each file's planning weight is its
        // byte length, so a split-size below one file combines nothing (one task group per file) while a split-size
        // above the whole table combines everything into one group.
        val icebergTable = Spark3Util.loadIcebergTable(spark, table)
        val targetSizeKey = TableProperties.SPLIT_SIZE // "read.split.target-size"
        def plannedTaskGroups(splitBytes: Long): Int =
          icebergTable
            .newScan()
            .option(targetSizeKey, splitBytes.toString)
            .planTasks()
            .asScala
            .size
        val groupsUnderLargeSplit = plannedTaskGroups(512L * 1024 * 1024)
        val groupsUnderTinySplit = plannedTaskGroups(1L)
        assert(
          groupsUnderLargeSplit == 1,
          s"[$format] a split-size above the whole table should plan 1 task group, " +
            s"got $groupsUnderLargeSplit")
        assert(
          groupsUnderTinySplit == fileCount,
          s"[$format] a split-size below one file should plan one task group per file ($fileCount), " +
            s"got $groupsUnderTinySplit")

        println(
          s"DIAG scanPlanning.splitSize[$format]: key='$splitSizeKey' files=$fileCount " +
            s"readPartitions(large=$partitionsUnderLargeSplit,tiny=$partitionsUnderTinySplit) " +
            s"taskGroups(large=$groupsUnderLargeSplit,tiny=$groupsUnderTinySplit)")
      } finally {
        // The split size is session state, not a table, so the case restores whatever the session held before it.
        savedSplitSize match {
          case Some(value) => spark.conf.set(splitSizeKey, value)
          case None        => spark.conf.unset(splitSizeKey)
        }
      }
    }
  }

}

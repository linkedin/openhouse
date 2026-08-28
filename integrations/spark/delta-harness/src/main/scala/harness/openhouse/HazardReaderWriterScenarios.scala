package harness

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.iceberg.exceptions.BadRequestException
import org.apache.iceberg.exceptions.ValidationException
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

// The copy-on-write reader, writer and hazard families. The reader and writer cases pin the
// changelog view, the incremental read and the structured-streaming reader and writer against a
// plain copy-on-write table. The hazard cases pin what happens when two operations that can
// interfere are run against the same table. Plan crosses every family here with the parquet and
// orc file formats. `cowCreate` states the standard copy-on-write table shape, so a feature layer
// reaches it through a self-type on this trait.
trait HazardReaderWriterScenarios extends ScenarioKit {
  import Rows._

  /** The CREATE statement for a copy-on-write table in the given file format. */
  protected def cowCreate(t: String, fmt: String): String =
    s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$fmt')"

  /**
   * Three seed rows in a copy-on-write table in the given file format. Each family builds its own
   * table from this recipe, so a family reads on its own.
   */
  private def cowPreparation(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table => cowCreate(table, format))()
        .insert(3)())

  /** A changelog view over an appended row reports exactly one INSERT and no DELETE. */
  private def readerWriterChangelogAppendCase(format: String): Plan.Case =
    cowPreparation(format).test("readerWriter.changelog.append") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      val view = table.spark
        .sql(
          "CALL openhouse.system.create_changelog_view(" +
            s"table => '${catalogRelative(table.name)}', " +
            s"options => map('start-snapshot-id', '$seedSnapshotId'))")
        .collect()(0)
        .getString(0)
      val changeTypes = table.spark
        .sql(s"SELECT _change_type, count(*) AS c FROM $view GROUP BY _change_type")
        .collect()
        .map(row => row.getString(0) -> row.getLong(1))
        .toMap

      println(s"DIAG changelog.append: $changeTypes")
      assert(
        changeTypes.getOrElse("INSERT", 0L) == 1 &&
          !changeTypes.contains("DELETE"),
        s"append changelog must contain one INSERT and no DELETE: $changeTypes")
    }

  /** The changelog case for an append, on three seed rows in the given file format. */
  def readerWriterChangelogAppendCases(format: String): List[Plan.Case] =
    List(
      readerWriterChangelogAppendCase(format))

  /**
   * A changelog view over an INSERT OVERWRITE that drops one row reports exactly that row as a
   * DELETE.
   */
  private def readerWriterChangelogOverwriteCase(format: String): Plan.Case =
    cowPreparation(format).test("readerWriter.changelog.overwrite") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(
        s"INSERT OVERWRITE ${table.name} " +
          s"SELECT * FROM ${table.name} " +
          s"WHERE ${Core.long0.columnName} <= 2")
      val view = table.spark
        .sql(
          "CALL openhouse.system.create_changelog_view(" +
            s"table => '${catalogRelative(table.name)}', " +
            s"options => map('start-snapshot-id', '$seedSnapshotId'))")
        .collect()(0)
        .getString(0)
      val changeTypes = table.spark
        .sql(s"SELECT _change_type, count(*) AS c FROM $view GROUP BY _change_type")
        .collect()
        .map(row => row.getString(0) -> row.getLong(1))
        .toMap

      println(s"DIAG changelog.overwrite: $changeTypes")
      assert(
        changeTypes == Map("DELETE" -> 1L),
        s"overwrite changelog must contain the one removed row: $changeTypes")
    }

  /** The changelog case for an INSERT OVERWRITE, on three seed rows in the given file format. */
  def readerWriterChangelogOverwriteCases(format: String): List[Plan.Case] =
    List(
      readerWriterChangelogOverwriteCase(format))

  /** A changelog view over a DELETE reports exactly one DELETE and no INSERT. */
  private def readerWriterChangelogDeleteCase(format: String): Plan.Case =
    cowPreparation(format).test("readerWriter.changelog.delete") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(
        s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
      val view = table.spark
        .sql(
          "CALL openhouse.system.create_changelog_view(" +
            s"table => '${catalogRelative(table.name)}', " +
            s"options => map('start-snapshot-id', '$seedSnapshotId'))")
        .collect()(0)
        .getString(0)
      val changeTypes = table.spark
        .sql(s"SELECT _change_type, count(*) AS c FROM $view GROUP BY _change_type")
        .collect()
        .map(row => row.getString(0) -> row.getLong(1))
        .toMap

      println(s"DIAG changelog.delete: $changeTypes")
      assert(
        changeTypes.getOrElse("DELETE", 0L) == 1 &&
          !changeTypes.contains("INSERT"),
        s"delete changelog must contain one DELETE and no INSERT: $changeTypes")
    }

  /** The changelog case for a DELETE, on three seed rows in the given file format. */
  def readerWriterChangelogDeleteCases(format: String): List[Plan.Case] =
    List(
      readerWriterChangelogDeleteCase(format))

  /**
   * A changelog view over an UPDATE reports the old row as a DELETE and the new value as an
   * INSERT.
   */
  private def readerWriterChangelogUpdateCase(format: String): Plan.Case =
    cowPreparation(format).test("readerWriter.changelog.update") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(
        s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'upd' " +
          s"WHERE ${Core.long0.columnName} = 2")
      val view = table.spark
        .sql(
          "CALL openhouse.system.create_changelog_view(" +
            s"table => '${catalogRelative(table.name)}', " +
            s"options => map('start-snapshot-id', '$seedSnapshotId'))")
        .collect()(0)
        .getString(0)
      val changeTypes = table.spark
        .sql(s"SELECT _change_type, count(*) AS c FROM $view GROUP BY _change_type")
        .collect()
        .map(row => row.getString(0) -> row.getLong(1))
        .toMap

      println(s"DIAG changelog.update: $changeTypes")
      assert(
        changeTypes == Map("DELETE" -> 1L, "INSERT" -> 1L),
        s"update changelog must contain the old and new row versions: $changeTypes")
    }

  /** The changelog case for an UPDATE, on three seed rows in the given file format. */
  def readerWriterChangelogUpdateCases(format: String): List[Plan.Case] =
    List(
      readerWriterChangelogUpdateCase(format))

  /**
   * A changelog view over a MERGE that updates one row and inserts another reports one DELETE and
   * two INSERTs.
   */
  private def readerWriterChangelogMergeCase(format: String): Plan.Case =
    cowPreparation(format).test("readerWriter.changelog.merge") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(
        s"MERGE INTO ${table.name} target " +
          "USING (SELECT CAST(2 AS BIGINT) key " +
          "UNION ALL SELECT CAST(9 AS BIGINT)) source " +
          s"ON target.${Core.long0.columnName} = source.key " +
          s"WHEN MATCHED THEN UPDATE SET ${Core.string0.columnName} = 'm' " +
          "WHEN NOT MATCHED THEN INSERT " +
          s"(${Core.long0.columnName}, ${Core.int0.columnName}, " +
          s"${Core.string0.columnName}, ${Core.double0.columnName}, " +
          s"${Core.boolean0.columnName}, ${Core.datePartition.columnName}) " +
          "VALUES (source.key, 9, 'row-9', 9.5, true, '2024-01-09-01')")
      val view = table.spark
        .sql(
          "CALL openhouse.system.create_changelog_view(" +
            s"table => '${catalogRelative(table.name)}', " +
            s"options => map('start-snapshot-id', '$seedSnapshotId'))")
        .collect()(0)
        .getString(0)
      val changeTypes = table.spark
        .sql(s"SELECT _change_type, count(*) AS c FROM $view GROUP BY _change_type")
        .collect()
        .map(row => row.getString(0) -> row.getLong(1))
        .toMap

      println(s"DIAG changelog.merge: $changeTypes")
      assert(
        changeTypes == Map("DELETE" -> 1L, "INSERT" -> 2L),
        s"merge changelog must contain one update and one insert: $changeTypes")
    }

  /** The changelog case for a MERGE, on three seed rows in the given file format. */
  def readerWriterChangelogMergeCases(format: String): List[Plan.Case] =
    List(
      readerWriterChangelogMergeCase(format))

  /** An incremental scan spanning an appended row returns exactly that one row. */
  private def readerWriterIncrementalAppendCase(format: String): Plan.Case =
    cowPreparation(format).test("readerWriter.incremental.append") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      val currentSnapshotId = snapshotIds(table.spark, table.name).last
      val addedRowCount = table.spark.read
        .format("iceberg")
        .option("start-snapshot-id", seedSnapshotId)
        .option("end-snapshot-id", currentSnapshotId)
        .load(table.name)
        .count()

      println(s"DIAG incremental.append: added=$addedRowCount")
      assert(
        addedRowCount == 1,
        s"append incremental scan should contain one row, got $addedRowCount")
    }

  /** An incremental scan spanning a DELETE-only snapshot returns no rows. */
  private def readerWriterIncrementalDeleteCase(format: String): Plan.Case =
    cowPreparation(format).test("readerWriter.incremental.delete") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(
        s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
      val currentSnapshotId = snapshotIds(table.spark, table.name).last
      val addedRowCount = table.spark.read
        .format("iceberg")
        .option("start-snapshot-id", seedSnapshotId)
        .option("end-snapshot-id", currentSnapshotId)
        .load(table.name)
        .count()

      println(s"DIAG incremental.delete: added=$addedRowCount")
      assert(
        addedRowCount == 0,
        s"delete-only incremental scan must not return appended rows: $addedRowCount")
    }

  /** An incremental scan spanning an INSERT OVERWRITE that only removes rows returns no rows. */
  private def readerWriterIncrementalOverwriteCase(format: String): Plan.Case =
    cowPreparation(format).test("readerWriter.incremental.overwrite") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(
        s"INSERT OVERWRITE ${table.name} " +
          s"SELECT * FROM ${table.name} " +
          s"WHERE ${Core.long0.columnName} <= 2")
      val currentSnapshotId = snapshotIds(table.spark, table.name).last
      val addedRowCount = table.spark.read
        .format("iceberg")
        .option("start-snapshot-id", seedSnapshotId)
        .option("end-snapshot-id", currentSnapshotId)
        .load(table.name)
        .count()

      println(s"DIAG incremental.overwrite: added=$addedRowCount")
      assert(
        addedRowCount == 0,
        s"overwrite-only incremental scan must not return appended rows: $addedRowCount")
    }

  /** An incremental scan spanning an UPDATE-only snapshot returns no rows. */
  private def readerWriterIncrementalUpdateCase(format: String): Plan.Case =
    cowPreparation(format).test("readerWriter.incremental.update") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(
        s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'upd' " +
          s"WHERE ${Core.long0.columnName} = 2")
      val currentSnapshotId = snapshotIds(table.spark, table.name).last
      val addedRowCount = table.spark.read
        .format("iceberg")
        .option("start-snapshot-id", seedSnapshotId)
        .option("end-snapshot-id", currentSnapshotId)
        .load(table.name)
        .count()

      println(s"DIAG incremental.update: added=$addedRowCount")
      assert(
        addedRowCount == 0,
        s"update-only incremental scan must not return appended rows: $addedRowCount")
    }

  /**
   * A streaming read of the table delivers the seed rows on first run and the newly inserted row
   * after restart, into a destination table.
   */
  private def readerWriterStreamAppendCase(format: String): Plan.Case =
    cowPreparation(format).test("readerWriter.stream.append") { table =>
      val destination = s"${table.name}_s"
      table.spark.sql(s"DROP TABLE IF EXISTS $destination")
      table.spark.sql(cowCreate(destination, format))
      val checkpoint =
        java.nio.file.Files.createTempDirectory("ck-rw").toString
      def runStream(): Unit = {
        val query = table.spark.readStream
          .table(table.name)
          .writeStream
          .format("iceberg")
          .outputMode("append")
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .option("checkpointLocation", checkpoint)
          .toTable(destination)
        assert(query.awaitTermination(120000), "stream did not finish")
        query.stop()
      }

      try {
        runStream()
        assert(
          countOf(table.spark, s"SELECT count(*) FROM $destination") == "3",
          "initial stream did not deliver the seed")
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        runStream()
        assert(
          countOf(table.spark, s"SELECT count(*) FROM $destination") == "4",
          "stream restart did not deliver the appended row")
      } finally {
        table.spark.sql(s"DROP TABLE IF EXISTS $destination")
      }
    }

  /**
   * An append-only stream restarted after a DELETE snapshot was written fails, with an error
   * mentioning delete or overwrite.
   */
  private def readerWriterStreamDeleteRejectedCase(format: String): Plan.Case =
    cowPreparation(format).test("readerWriter.stream.deleteRejected") { table =>
      val destination = s"${table.name}_sd"
      table.spark.sql(s"DROP TABLE IF EXISTS $destination")
      table.spark.sql(cowCreate(destination, format))
      val checkpoint =
        java.nio.file.Files.createTempDirectory("ck-rwd").toString
      def runStream(): Unit = {
        val query = table.spark.readStream
          .table(table.name)
          .writeStream
          .format("iceberg")
          .outputMode("append")
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .option("checkpointLocation", checkpoint)
          .toTable(destination)
        assert(query.awaitTermination(120000), "stream did not finish")
        query.stop()
      }

      try {
        runStream()
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
        val exception = Check.intercept[Exception](runStream())

        println(
          "DIAG stream.afterDelete: " +
            s"${exception.getClass.getSimpleName} :: " +
            Option(exception.getMessage).getOrElse("").take(140))
        assert(
          Exceptions.causeChain(exception).exists(error =>
            Option(error.getMessage).exists(message =>
              message.toLowerCase.contains("delete") ||
                message.toLowerCase.contains("overwrite"))),
          "append-only stream should reject a delete snapshot")
      } finally {
        table.spark.sql(s"DROP TABLE IF EXISTS $destination")
      }
    }

  /**
   * The incremental reads between two snapshots and the structured-streaming reader and writer, on
   * three seed rows in the given file format.
   */
  def readerWriterIncrementalAndStreamCases(format: String): List[Plan.Case] =
    List(
      readerWriterIncrementalAppendCase(format),
      readerWriterIncrementalDeleteCase(format),
      readerWriterIncrementalOverwriteCase(format),
      readerWriterIncrementalUpdateCase(format),
      readerWriterStreamAppendCase(format),
      readerWriterStreamDeleteRejectedCase(format))

  /**
   * A streaming read that resumes after its earliest offset snapshot has been expired fails, with
   * an error naming the expired or missing snapshot.
   */
  private def hazardStreamExpiredCheckpointCase(
      format: String,
      basePreparation: TablePreparation[CoreTable.type]): Plan.Case =
    basePreparation.test("hazard.stream.expiredCheckpoint") { table =>
      val destination = s"${table.name}_sink"
      table.spark.sql(s"DROP TABLE IF EXISTS $destination")
      table.spark.sql(cowCreate(destination, format))
      val checkpoint =
        java.nio.file.Files.createTempDirectory("ck-hazard").toString
      def runStream(): Unit = {
        val query = table.spark.readStream
          .table(table.name)
          .writeStream
          .format("iceberg")
          .outputMode("append")
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .option("checkpointLocation", checkpoint)
          .toTable(destination)
        assert(query.awaitTermination(120000), "stream did not finish")
        query.stop()
      }

      try {
        runStream()
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM $destination") == "3",
          "initial stream should deliver the seed")

        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        runStream()
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM $destination") == "4",
          "control restart should deliver one incremental row")

        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
        table.spark.sql(
          "CALL openhouse.system.expire_snapshots(" +
            s"table => '${catalogRelative(table.name)}', " +
            "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
            "retain_last => 1)")
        val exception = Check.intercept[Exception](runStream())

        assert(
          Exceptions.causeChain(exception).exists(error =>
            Option(error.getMessage).exists(message =>
              message.contains("expired or removed") ||
                message.contains("Cannot load current offset") ||
                message.contains("Cannot find snapshot"))),
          "stream restart should report the expired checkpoint offset")
      } finally {
        table.spark.sql(s"DROP TABLE IF EXISTS $destination")
      }
    }

  /**
   * After expire_snapshots removes a changelog start point, create_changelog_view over that start
   * point either throws or reports fewer changes than the table's history holds, and any message it
   * throws leaves expiration unnamed. The case covers three start points: an expired snapshot ID, a
   * timestamp older than the whole history, and a timestamp inside the expired range.
   */
  private def hazardCdcExpiredRangeCase(
      basePreparation: TablePreparation[CoreTable.type]): Plan.Case =
    basePreparation.test("hazard.cdc.expiredRange") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
      val snapshots = snapshotIds(table.spark, table.name)
      val firstTimestamp = table.spark
        .sql(
          s"SELECT committed_at FROM ${table.name}.snapshots " +
            "ORDER BY committed_at LIMIT 1")
        .collect()(0)
        .getTimestamp(0)
      val middleTimestamp = table.spark
        .sql(
          s"SELECT committed_at FROM ${table.name}.snapshots " +
            s"WHERE snapshot_id = ${snapshots(1)}")
        .collect()(0)
        .getTimestamp(0)
      table.spark.sql(
        "CALL openhouse.system.expire_snapshots(" +
          s"table => '${catalogRelative(table.name)}', " +
          "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
          "retain_last => 1)")
      def changelog(
          optionKey: String,
          optionValue: String,
          trueChangeCount: Long): String =
        try {
          val view = table.spark
            .sql(
              "CALL openhouse.system.create_changelog_view(" +
                s"table => '${catalogRelative(table.name)}', " +
                s"options => map('$optionKey', '$optionValue'))")
            .collect()(0)
            .getString(0)
          val actualChangeCount = table.spark
            .sql(s"SELECT count(*) FROM $view")
            .collect()(0)
            .getLong(0)
          if (actualChangeCount < trueChangeCount) {
            s"SILENT under-report: $actualChangeCount of " +
              s"$trueChangeCount true changes"
          } else {
            s"FULL: $actualChangeCount of $trueChangeCount"
          }
        } catch {
          case exception: Throwable =>
            s"TYPED: ${exception.getClass.getSimpleName} :: " +
              Option(exception.getMessage).getOrElse("").take(140)
        }
      val explicitSnapshotOutcome =
        changelog("start-snapshot-id", snapshots.head.toString, 5)
      val beforeHistoryOutcome =
        changelog(
          "start-timestamp",
          (firstTimestamp.getTime - 1000).toString,
          5)
      val middleHistoryOutcome =
        changelog(
          "start-timestamp",
          (middleTimestamp.getTime - 1).toString,
          2)

      println(s"DIAG cdc.explicitExpiredId: $explicitSnapshotOutcome")
      println(s"DIAG cdc.tsBeforeHistory:  $beforeHistoryOutcome")
      println(s"DIAG cdc.tsMidExpired:     $middleHistoryOutcome")
      Seq(
        "explicitId" -> explicitSnapshotOutcome,
        "tsBeforeHistory" -> beforeHistoryOutcome,
        "tsMidExpired" -> middleHistoryOutcome).foreach {
        case (label, outcome) =>
          assert(
            !outcome.startsWith("FULL"),
            s"expired-lineage changelog returned full truth for $label")
          assert(
            !outcome.toLowerCase.contains("expir"),
            s"expired-lineage message now names expiration for $label")
      }
    }

  /**
   * The hazards a reader or a consumer meets when maintenance lands underneath it. Every case
   * starts from three seed rows in a copy-on-write table in the given file format.
   */
  def hazardReaderCases(format: String): List[Plan.Case] = {
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table => cowCreate(table, format))()
        .insert(3)())

    List(
      hazardStreamExpiredCheckpointCase(format, basePreparation),
      hazardCdcExpiredRangeCase(basePreparation))
  }

  /**
   * An explicit-column INSERT that worked before ADD COLUMN is rejected afterward, with an error
   * naming the new column.
   */
  private def hazardAddColumnBreaksWritersCase(
      basePreparation: TablePreparation[CoreTable.type]): Plan.Case =
    basePreparation.test("hazard.addColumn.breaksWriters") { table =>
      val allColumns =
        Core.tableColumns.map(_.columnName).mkString(", ")
      val writerStatement =
        s"INSERT INTO ${table.name} ($allColumns) VALUES " +
          "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')"
      table.spark.sql(writerStatement)
      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name}") == "4",
        "explicit-column writer should work before schema evolution")

      table.spark.sql(
        s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(writerStatement))
      assert(
        exception.getMessage.contains("extra_col") &&
          (exception.getMessage.contains("CANNOT_FIND_DATA") ||
            exception.getMessage.toLowerCase.contains("cannot find data")),
        "pre-evolution explicit-column writer should fail after ADD COLUMN")
    }

  /**
   * The hazard an explicit-column writer meets after a column is added. The case starts from three
   * seed rows in a copy-on-write table in the given file format.
   */
  def hazardWriterCases(format: String): List[Plan.Case] = {
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table => cowCreate(table, format))()
        .insert(3)())

    List(
      hazardAddColumnBreaksWritersCase(basePreparation))
  }

  /**
   * While a table is REST-locked, an expire_snapshots call is rejected and snapshots keep
   * accumulating. After the lock is deleted, expire_snapshots succeeds and the snapshot count
   * drops, so the lock blocks every maintenance commit while it is held.
   */
  def hazardLockStarvesMaintenance(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_lockmaint"
    val Array(db, tbl) = table.stripPrefix("openhouse.").split("\\.", 2)
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(coreCreateParquet(table))
    spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, 3)}")
    spark.sql(s"INSERT INTO $table VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
    try {
      val (lockStatus, lockBody) = Rest.post(ctx, s"/v1/databases/$db/tables/$tbl/lock", """{"locked":true}""")
      assert(lockStatus >= 200 && lockStatus < 300, s"lock POST failed: $lockStatus $lockBody")
      val snapsBefore = spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0)
      val e = Check.intercept[Exception](spark.sql(
        s"CALL openhouse.system.expire_snapshots(table => '${table.stripPrefix("openhouse.")}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)"))
      assert(Exceptions.causeChain(e).exists(t => Option(t.getMessage).exists(_.toLowerCase.contains("locked"))),
        s"expected LOCKED rejection for the maintenance commit: ${e.getClass.getName} ${Option(e.getMessage).getOrElse("").take(180)}")
      spark.sql(s"REFRESH TABLE $table")
      val snapsAfter = spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0)
      assert(snapsAfter == snapsBefore, "locked table must accumulate snapshots (maintenance starved)")
      val (unlockStatus, _) = Rest.delete(ctx, s"/v1/databases/$db/tables/$tbl/lock")
      assert(unlockStatus >= 200 && unlockStatus < 300, "unlock failed")
      spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${table.stripPrefix("openhouse.")}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
      spark.sql(s"REFRESH TABLE $table")
      assert(spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0) < snapsBefore,
        "maintenance must proceed after unlock")
    } finally {
      Rest.delete(ctx, s"/v1/databases/$db/tables/$tbl/lock")
      spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  /** The hazard cases the embedded REST server drives. */
  val hazardContextCases: List[Plan.Case] =
    List(
      Plan.Case(
        "hazard.lock.starvesMaintenance @ embedded",
        hazardLockStarvesMaintenance))

}

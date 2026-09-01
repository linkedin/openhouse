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

trait MaintControlScenarios extends ScenarioKit {
  import Rows._

  /**
   * A five-row table across two snapshots in the given file format: a 3-row seed commit, then a 2-row insert committed
   * at a later timestamp. Time travel, restore and maintenance all start from this state.
   */
  private def twoSnapshotPreparation(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(format, coreTwoSnapshots(format))

  /**
   * VERSION AS OF the first snapshot ID reads the 3 rows the seed commit wrote, and VERSION AS OF the second reads all
   * 5 rows.
   */
  private def timeTravelVersionAsOfCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("timeTravel.versionAsOf") { table =>
      val snapshots = snapshotIds(table.spark, table.name)

      assert(
        table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} " +
              s"VERSION AS OF ${snapshots(0)}")
          .collect()(0)
          .getLong(0) == 3)
      assert(
        table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} " +
              s"VERSION AS OF ${snapshots(1)}")
          .collect()(0)
          .getLong(0) == 5)
    }

  /** TIMESTAMP AS OF the first commit's time reads the 3 rows that commit wrote. */
  private def timeTravelTimestampAsOfCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("timeTravel.timestampAsOf") { table =>
      val firstCommitTimestamp = table.spark
        .sql(
          s"SELECT CAST(committed_at AS STRING) FROM ${table.name}.snapshots " +
            "ORDER BY committed_at LIMIT 1")
        .collect()(0)
        .getString(0)

      assert(
        table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} " +
              s"TIMESTAMP AS OF '$firstCommitTimestamp'")
          .collect()(0)
          .getLong(0) == 3)
    }

  /**
   * The snapshots and history metadata tables each report the table's 2 snapshots, and the files and manifests metadata
   * tables report at least 1 row.
   */
  private def timeTravelMetadataTablesCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("timeTravel.metadataTables") { table =>
      def metadataRowCount(metadataTable: String): Long =
        table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name}.$metadataTable")
          .collect()(0)
          .getLong(0)

      assert(metadataRowCount("snapshots") == 2)
      assert(metadataRowCount("history") == 2)
      assert(
        metadataRowCount("files") >= 1 &&
          metadataRowCount("manifests") >= 1)
    }

  /** An incremental read spanning both snapshots returns the 2 rows the second commit added. */
  private def timeTravelIncrementalReadCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("timeTravel.incrementalRead") { table =>
      val snapshots = snapshotIds(table.spark, table.name)
      val addedRowCount = table.spark.read
        .format("iceberg")
        .option("start-snapshot-id", snapshots(0))
        .option("end-snapshot-id", snapshots(1))
        .load(table.name)
        .count()

      assert(addedRowCount == 2)
    }

  /** Time travel across both snapshots of the two-snapshot table, in parquet and in orc. */
  val timeTravelCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val preparation = twoSnapshotPreparation(format)

      List(
        timeTravelVersionAsOfCase(preparation),
        timeTravelTimestampAsOfCase(preparation),
        timeTravelMetadataTablesCase(preparation),
        timeTravelIncrementalReadCase(preparation))
    }

  /** rollback_to_snapshot to the first snapshot restores the 3 rows the seed commit wrote. */
  private def restoreRollbackToSnapshotCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("restore.rollbackToSnapshot") { table =>
      val firstSnapshotId =
        snapshotIds(table.spark, table.name).head

      table.spark.sql(
        "CALL openhouse.system.rollback_to_snapshot(" +
          s"'${catalogRelative(table.name)}', $firstSnapshotId)")

      assert(table.rows.size == 3)
    }

  /** set_current_snapshot to the first snapshot restores the 3 rows the seed commit wrote. */
  private def restoreSetCurrentSnapshotCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("restore.setCurrentSnapshot") { table =>
      val firstSnapshotId =
        snapshotIds(table.spark, table.name).head

      table.spark.sql(
        "CALL openhouse.system.set_current_snapshot(" +
          s"'${catalogRelative(table.name)}', $firstSnapshotId)")

      assert(table.rows.size == 3)
    }

  /** Restore back to the seed snapshot of the two-snapshot table, in parquet and in orc. */
  val restoreRollbackCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val preparation = twoSnapshotPreparation(format)

      List(
        restoreRollbackToSnapshotCase(preparation),
        restoreSetCurrentSnapshotCase(preparation))
    }

  /** expire_snapshots with retain_last=1 removes the seed snapshot and leaves all 5 current rows unchanged. */
  private def maintenanceExpireSnapshotsCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("maintenance.expireSnapshots") { table =>
      table.spark.sql(
        "CALL openhouse.system.expire_snapshots(" +
          s"table => '${catalogRelative(table.name)}', " +
          "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
          "retain_last => 1)")

      assert(
        table.rows.size == 5,
        "expire_snapshots changed the current data")
      assert(
        table.snapshotCount < table.preparedSnapshotCount,
        "expire_snapshots did not remove a snapshot: " +
          s"${table.preparedSnapshotCount} -> ${table.snapshotCount}")
    }

  /** rewrite_data_files compacts the data files and leaves all 5 rows unchanged. */
  private def maintenanceRewriteDataFilesCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("maintenance.rewriteDataFiles") { table =>
      table.spark.sql(
        "CALL openhouse.system.rewrite_data_files(" +
          s"table => '${catalogRelative(table.name)}')")

      assert(table.rows.size == 5, "compaction changed rows")
    }

  /** remove_orphan_files leaves all 5 rows unchanged. */
  private def maintenanceRemoveOrphanFilesCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("maintenance.removeOrphanFiles") { table =>
      table.spark.sql(
        "CALL openhouse.system.remove_orphan_files(" +
          s"table => '${catalogRelative(table.name)}', " +
          "older_than => TIMESTAMP '2020-01-01 00:00:00')")

      assert(table.rows.size == 5, "orphan removal changed rows")
    }

  /** The maintenance procedures run over the two-snapshot table, in parquet and in orc. */
  val maintenanceCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val preparation = twoSnapshotPreparation(format)

      List(
        maintenanceExpireSnapshotsCase(preparation),
        maintenanceRewriteDataFilesCase(preparation),
        maintenanceRemoveOrphanFilesCase(preparation))
    }

  /**
   * POSTing a table lock causes a following Spark UPDATE to be rejected server-side with LOCKED_TABLE_OPERATION, and
   * DELETEing the lock lets a later UPDATE apply. The lock endpoint has no SQL surface, so the case drives it over HTTP
   * against the embedded server, which runs the same TablesController and TablesServiceImpl as production.
   */
  def controlLockEnforcement(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_lock"
    val Array(db, tbl) = table.stripPrefix("openhouse.").split("\\.", 2)
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(coreCreateParquet(table))
    spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, 3)}")
    try {
      val (lockStatus, lockBody) = Rest.post(ctx, s"/v1/databases/$db/tables/$tbl/lock", """{"locked":true}""")
      assert(lockStatus >= 200 && lockStatus < 300, s"lock POST failed: $lockStatus $lockBody")
      val e = Check.intercept[Exception](spark.sql(
        s"UPDATE $table SET ${Core.string0.columnName} = 'locked-write' WHERE ${Core.long0.columnName} = 1"))
      assert(Exceptions.causeChain(e).exists(t => Option(t.getMessage).exists(_.toLowerCase.contains("locked"))),
        s"expected a locked-table rejection, got: ${e.getMessage.take(200)}")
      val (unlockStatus, unlockBody) = Rest.delete(ctx, s"/v1/databases/$db/tables/$tbl/lock")
      assert(unlockStatus >= 200 && unlockStatus < 300, s"unlock DELETE failed: $unlockStatus $unlockBody")
      spark.sql(s"UPDATE $table SET ${Core.string0.columnName} = 'unlocked-write' WHERE ${Core.long0.columnName} = 1")
      assert(spark.sql(s"SELECT count(*) FROM $table WHERE ${Core.string0.columnName} = 'unlocked-write'").collect()(0).getLong(0) == 1,
        "post-unlock update did not apply")
    } finally spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  /** The control-plane cases, each driven over HTTP against the embedded server. */
  val controlPlaneCases: List[Plan.Case] =
    List(
      Plan.Case(
        "control.lock.enforcement @ embedded",
        controlLockEnforcement))

}

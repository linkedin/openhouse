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

  // Time travel and restore/rollback.
  // A two-snapshot base: seed 3 rows (snapshot A), then insert 2 more (snapshot B). Format is a
  // parameter so each case below runs against every supported file format.

  val timeTravelCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val preparation = TablePreparation(
        format,
        coreTwoSnapshots(format),
        description = s"Five seed rows across two snapshots in a $format table.")

      List(
        preparation.test(
          "timeTravel.versionAsOf",
          "VERSION AS OF the first snapshot ID reads 3 rows and VERSION AS OF the second reads " +
            "5 rows.") { table =>
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
        },
        preparation.test(
          "timeTravel.timestampAsOf",
          "TIMESTAMP AS OF the first commit's time reads that snapshot's 3 rows.") { table =>
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
        },
        preparation.test(
          "timeTravel.metadataTables",
          "The snapshots and history metadata tables each report 2 rows, and the files and " +
            "manifests metadata tables report at least 1 row.") { table =>
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
        },
        preparation.test(
          "timeTravel.incrementalRead",
          "An incremental read spanning the two seed snapshots returns exactly the 2 rows added " +
            "by the second snapshot.") { table =>
          val snapshots = snapshotIds(table.spark, table.name)
          val addedRowCount = table.spark.read
            .format("iceberg")
            .option("start-snapshot-id", snapshots(0))
            .option("end-snapshot-id", snapshots(1))
            .load(table.name)
            .count()

          assert(addedRowCount == 2)
        })
    }

  val restoreRollbackCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val preparation = TablePreparation(
        format,
        coreTwoSnapshots(format),
        description = s"Five seed rows across two snapshots in a $format table.")

      List(
        preparation.test(
          "restore.rollbackToSnapshot",
          "rollback_to_snapshot to the first snapshot restores the table to its 3-row state.") { table =>
          val firstSnapshotId =
            snapshotIds(table.spark, table.name).head

          table.spark.sql(
            "CALL openhouse.system.rollback_to_snapshot(" +
              s"'${catalogRelative(table.name)}', $firstSnapshotId)")

          assert(table.rows.size == 3)
        },
        preparation.test(
          "restore.setCurrentSnapshot",
          "set_current_snapshot to the first snapshot restores the table to its 3-row state.") { table =>
          val firstSnapshotId =
            snapshotIds(table.spark, table.name).head

          table.spark.sql(
            "CALL openhouse.system.set_current_snapshot(" +
              s"'${catalogRelative(table.name)}', $firstSnapshotId)")

          assert(table.rows.size == 3)
        })
    }

  val maintenanceCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val preparation = TablePreparation(
        format,
        coreTwoSnapshots(format),
        description = s"Five seed rows across two snapshots in a $format table.")

      List(
        preparation.test(
          "maintenance.expireSnapshots",
          "expire_snapshots with retain_last=1 removes an old snapshot and leaves the current 5 " +
            "rows unchanged.") { table =>
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
        },
        preparation.test(
          "maintenance.rewriteDataFiles",
          "rewrite_data_files compacts the table's data files while preserving all 5 rows.") { table =>
          table.spark.sql(
            "CALL openhouse.system.rewrite_data_files(" +
              s"table => '${catalogRelative(table.name)}')")

          assert(table.rows.size == 5, "compaction changed rows")
        },
        preparation.test(
          "maintenance.removeOrphanFiles",
          "remove_orphan_files leaves all 5 rows unchanged.") { table =>
          table.spark.sql(
            "CALL openhouse.system.remove_orphan_files(" +
              s"table => '${catalogRelative(table.name)}', " +
              "older_than => TIMESTAMP '2020-01-01 00:00:00')")

          assert(table.rows.size == 5, "orphan removal changed rows")
        })
    }

  // Control-plane (REST) operations with no SQL surface, driven through the embedded server's
  // HTTP API. Lock enforcement: POST /lock is a real public endpoint; a subsequent Spark mutation
  // is rejected server-side with LOCKED_TABLE_OPERATION, and DELETE /lock restores mutability. The
  // embedded server runs the real TablesController and TablesServiceImpl, so this exercises the
  // production REST path.
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

  val controlPlaneCases: List[Plan.Case] =
    List(
      Plan.Case(
        "control.lock.enforcement @ embedded",
        controlLockEnforcement,
        description = "POSTing a table lock causes a subsequent UPDATE to be rejected, and " +
          "DELETEing the lock allows a following UPDATE to apply."))


}

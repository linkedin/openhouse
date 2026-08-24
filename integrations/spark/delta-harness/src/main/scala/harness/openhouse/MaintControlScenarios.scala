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

  // ── time travel + restore/rollback ──────────────────────────────────────────────────────
  // A two-snapshot base: seed 3 rows (snapshot A), then insert 2 more (snapshot B).
  // Format is a PARAMETER, not baked in — so any block built on this base can multiplex across formats.

  val timeTravelCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val preparation = TablePreparation(
        format,
        coreTwoSnapshots(format))

      List(
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
        },
        preparation.test("timeTravel.timestampAsOf") { table =>
          val firstCommitTimestamp = table.spark
            .sql(
              s"SELECT committed_at FROM ${table.name}.snapshots " +
                "ORDER BY committed_at LIMIT 1")
            .collect()(0)
            .getTimestamp(0)

          assert(
            table.spark
              .sql(
                s"SELECT count(*) FROM ${table.name} " +
                  s"TIMESTAMP AS OF '$firstCommitTimestamp'")
              .collect()(0)
              .getLong(0) == 3)
        },
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
        },
        preparation.test("timeTravel.incrementalRead") { table =>
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
        coreTwoSnapshots(format))

      List(
        preparation.test("restore.rollbackToSnapshot") { table =>
          val firstSnapshotId =
            snapshotIds(table.spark, table.name).head

          table.spark.sql(
            "CALL openhouse.system.rollback_to_snapshot(" +
              s"'${catalogRelative(table.name)}', $firstSnapshotId)")

          assert(table.rows.size == 3)
        },
        preparation.test("restore.setCurrentSnapshot") { table =>
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
        coreTwoSnapshots(format))

      List(
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
        },
        preparation.test("maintenance.rewriteDataFiles") { table =>
          table.spark.sql(
            "CALL openhouse.system.rewrite_data_files(" +
              s"table => '${catalogRelative(table.name)}')")

          assert(table.rows.size == 5, "compaction changed rows")
        },
        preparation.test("maintenance.removeOrphanFiles") { table =>
          table.spark.sql(
            "CALL openhouse.system.remove_orphan_files(" +
              s"table => '${catalogRelative(table.name)}', " +
              "older_than => TIMESTAMP '2020-01-01 00:00:00')")

          assert(table.rows.size == 5, "orphan removal changed rows")
        })
    }

  // ── Control-plane (REST) ops with no SQL surface — driven via the embedded server's HTTP API ──
  // Lock enforcement: POST /lock (a real public entry), then a Spark mutation is rejected server-side
  // (LOCKED_TABLE_OPERATION); DELETE /lock restores mutability. High-fidelity — the embedded server
  // runs the real TablesController/TablesServiceImpl (see REST-FIDELITY-EVAL.md).
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

  // Undrop lifecycle — TAGGED SKIP (Plan.knownBugs). Not runnable at fidelity in the embedded harness:
  // (1) the embedded HouseTableRepository is a @Primary in-memory STUB (HouseTablesH2Repository) — a
  //     test here would exercise the shim's own reimplementation, not the real HTS soft-delete logic;
  // (2) the public Tables DELETE hard-codes purge=true, so drop→soft-delete is unreachable via the
  //     customer API in ANY environment (undrop is HTS-admin-only — a product finding).
  // Real fidelity needs an embedded HTS (SpringH2HtsApplication) + de-@Primary-ing the stub. The body
  // documents the intended list→restore flow for that future harness.
  def controlUndropLifecycle(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_undrop"
    val Array(db, tbl) = table.stripPrefix("openhouse.").split("\\.", 2)
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(coreCreateParquet(table))
    spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, 3)}")
    // (intended, once a real HTS soft-deletes the table:)
    val (listStatus, listBody) = Rest.get(ctx, s"/v1/databases/$db/softDeletedTables")
    assert(listStatus == 200 && listBody.contains(tbl), "soft-deleted table should be listed")
    val (restoreStatus, _) = Rest.put(ctx, s"/v1/databases/$db/tables/$tbl/restore?deletedAtMs=0", "")
    assert(restoreStatus >= 200 && restoreStatus < 300, "restore should succeed")
    assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "restored table keeps its rows")
    spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  val controlPlaneCases: List[Plan.Case] =
    List(
      Plan.Case(
        "control.lock.enforcement @ embedded",
        controlLockEnforcement),
      Plan.Case(
        "control.undrop.lifecycle @ embedded",
        controlUndropLifecycle))

  // ── Undrop admin-lifecycle block (Phase 5 — REAL HTS only, HtsAdmin.enabled) ─────────────────
  // With an embedded real HTS the full soft-delete → list → restore / purge lifecycle is exercisable
  // (the customer DROP still hard-deletes — soft-delete is driven directly on HTS). These are the
  // HTS-admin lifecycle cases that sit ALONGSIDE the surface-doubling undrop battery.

  // Soft-delete → the customer softDeletedTables listing shows it → restore → rows intact.
  def undropAdminRestoreRoundTrip(ctx: Ctx): Unit = {
    val (table, db, tbl) = undropSeed(ctx, "t_undrop_rt")
    val (sd, sdb) = HtsAdmin.softDelete(db, tbl); assert(sd >= 200 && sd < 300, s"soft-delete failed ($sd): $sdb")
    val (ls, lb) = Rest.get(ctx, s"/v1/databases/$db/softDeletedTables")
    assert(ls == 200 && lb.contains(tbl), s"soft-deleted table not listed via Tables API ($ls): $lb")
    val ms = HtsAdmin.softDeletedAtMs(db, tbl).getOrElse(throw new AssertionError(s"no deletedAtMs for $db.$tbl"))
    val (rs, rb) = HtsAdmin.restore(db, tbl, ms); assert(rs >= 200 && rs < 300, s"restore failed ($rs): $rb")
    assert(ctx.spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "restored table lost rows")
    ctx.spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // Two soft-deleted tables both appear in the listing (paging/enumeration works).
  def undropAdminListSoftDeleted(ctx: Ctx): Unit = {
    val (_, db, t1) = undropSeed(ctx, "t_undrop_l1")
    val (_, _,  t2) = undropSeed(ctx, "t_undrop_l2")
    assert(HtsAdmin.softDelete(db, t1)._1 / 100 == 2, "soft-delete t1 failed")
    assert(HtsAdmin.softDelete(db, t2)._1 / 100 == 2, "soft-delete t2 failed")
    val (ls, lb) = Rest.get(ctx, s"/v1/databases/$db/softDeletedTables")
    assert(ls == 200 && lb.contains(t1) && lb.contains(t2), s"both soft-deleted tables should list ($ls): $lb")
  }

  // Restore AFTER purge must be rejected — purge is permanent. Pin whatever the real HTS returns
  // (a 4xx; the point is that restore no longer succeeds once the row is purged).
  def undropAdminRestoreAfterPurgeRejected(ctx: Ctx): Unit = {
    val (_, db, tbl) = undropSeed(ctx, "t_undrop_purge")
    assert(HtsAdmin.softDelete(db, tbl)._1 / 100 == 2, "soft-delete failed")
    val ms = HtsAdmin.softDeletedAtMs(db, tbl).getOrElse(throw new AssertionError("no deletedAtMs"))
    // purge everything deleted before a far-future instant → removes this row permanently
    val (ps, _) = Rest.delete(ctx, s"/v1/databases/$db/tables/$tbl/purge?purgeAfterMs=${Long.MaxValue}")
    assert(ps / 100 == 2, s"purge should succeed ($ps)")
    val (rs, _) = HtsAdmin.restore(db, tbl, ms)
    assert(rs >= 400, s"restore after purge must be rejected, got $rs")
  }

  def undropAdminCases: List[Plan.Case] =
    if (HtsAdmin.enabled) {
      List(
        Plan.Case(
          "undropAdmin.restoreRoundTrip",
          undropAdminRestoreRoundTrip),
        Plan.Case(
          "undropAdmin.listSoftDeleted",
          undropAdminListSoftDeleted),
        Plan.Case(
          "undropAdmin.restoreAfterPurgeRejected",
          undropAdminRestoreAfterPurgeRejected))
    } else {
      Nil
    }


}

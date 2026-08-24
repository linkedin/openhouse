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

trait BranchWapScenarios extends ScenarioKit {
  import Rows._

  // ── Undrop 3-way compositions (Block 9, real HTS only) — restore's state-preservation, per feature ──
  // The undrop:* battery proves the whole op catalog works post-restore. These are pointed 3-way
  // chains that set up a SPECIFIC feature's state (branch / snapshot history / evolved schema),
  // destroy via soft-delete→restore, then consume that exact feature — the direct modality check that
  // restore's destruction set does not intersect refs / lineage / schema.

  // A pre-existing branch must survive the drop→undrop round-trip.
  def interactUndropBranchSurvives(ctx: Ctx): Unit = {
    val (table, db, tbl) = undropSeed(ctx, "t_ud_branch")
    ctx.spark.sql(s"ALTER TABLE $table CREATE BRANCH b")
    ctx.spark.sql(s"INSERT INTO $table.branch_b ${RowGenerator.valuesClause(Core, 2)}")   // branch diverges: 3+2=5
    softDeleteRestore(ctx, db, tbl)
    assert(ctx.spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "main row set changed across undrop")
    assert(ctx.spark.sql(s"SELECT count(*) FROM $table VERSION AS OF 'b'").collect()(0).getLong(0) == 5, "branch 'b' did not survive undrop")
    ctx.spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // Snapshot history (time travel) must survive restore.
  def interactUndropTimeTravelSurvives(ctx: Ctx): Unit = {
    val (table, db, tbl) = undropSeed(ctx, "t_ud_tt")
    val firstSnap = ctx.spark.sql(s"SELECT snapshot_id FROM $table.snapshots ORDER BY committed_at LIMIT 1").collect()(0).getLong(0)
    ctx.spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, 2)}")            // 2nd snapshot: 5 rows
    softDeleteRestore(ctx, db, tbl)
    assert(ctx.spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 5, "current state changed across undrop")
    assert(ctx.spark.sql(s"SELECT count(*) FROM $table VERSION AS OF $firstSnap").collect()(0).getLong(0) == 3,
      "pre-restore snapshot not time-travellable after undrop (lineage lost)")
    ctx.spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // Evolved schema must survive restore, and the restored table must still accept the evolved shape.
  def interactUndropSchemaSurvives(ctx: Ctx): Unit = {
    val (table, db, tbl) = undropSeed(ctx, "t_ud_schema")
    ctx.spark.sql(s"ALTER TABLE $table ADD COLUMN extra int")
    ctx.spark.sql(s"INSERT INTO $table VALUES (CAST(9 AS BIGINT), 9, 'row-9', 9.5, false, '2024-01-09-08', 99)")
    softDeleteRestore(ctx, db, tbl)
    assert(ctx.spark.sql(s"SELECT extra FROM $table WHERE ${Core.long0.columnName} = 9").collect()(0).getInt(0) == 99,
      "evolved column value lost across undrop")
    ctx.spark.sql(s"INSERT INTO $table VALUES (CAST(10 AS BIGINT), 10, 'row-10', 10.5, true, '2024-01-10-09', 100)")
    assert(ctx.spark.sql(s"SELECT count(*) FROM $table WHERE extra IS NOT NULL").collect()(0).getLong(0) == 2,
      "restored table did not accept the evolved schema for new writes")
    ctx.spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  def undropInteractionCases: List[Plan.Case] =
    if (HtsAdmin.enabled) {
      List(
        Plan.Case(
          "interact.undrop.branchSurvives",
          interactUndropBranchSurvives),
        Plan.Case(
          "interact.undrop.timeTravelSurvives",
          interactUndropTimeTravelSurvives),
        Plan.Case(
          "interact.undrop.schemaSurvives",
          interactUndropSchemaSurvives))
    } else {
      Nil
    }

  val wapStagedCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val preparation = TablePreparation(
        format,
        TableTest(Core)
          .sql("create")(table =>
            s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
              s"TBLPROPERTIES ('write.format.default'='$format')")()
          .insert(3)()
          .sql("enableWap")(table =>
            s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")())

      List(
        preparation.test("wapStaged.insert") { table =>
          table.spark.conf.set("spark.wap.id", "wS")
          try {
            table.spark.sql(
              s"INSERT INTO ${table.name} VALUES ${coreRow(99, "staged")}")
          } finally {
            table.spark.conf.unset("spark.wap.id")
          }
          val mainRowCount = table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0)
          val stagedSnapshotCount = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name}.snapshots " +
                "WHERE summary['wap.id'] = 'wS'")
            .collect()(0)
            .getLong(0)

          println(
            "DIAG wapStaged.insert: " +
              s"mainPreCount=$mainRowCount stagedSnapshots=$stagedSnapshotCount")
          assert(mainRowCount == 3, "staged insert changed main before publish")

          val stagedSnapshotId = table.spark
            .sql(
              s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                "WHERE summary['wap.id'] = 'wS'")
            .collect()(0)
            .getLong(0)
          table.spark.sql(
            "CALL openhouse.system.cherrypick_snapshot(" +
              s"'${catalogRelative(table.name)}', $stagedSnapshotId)")

          assert(
            table.spark
              .sql(s"SELECT count(*) FROM ${table.name}")
              .collect()(0)
              .getLong(0) == 4,
            "publishing the staged insert did not advance main")
        },
        preparation.test("wapStaged.overwrite") { table =>
          table.spark.conf.set("spark.wap.id", "wS")
          try {
            table.spark.sql(
              s"INSERT OVERWRITE ${table.name} VALUES ${coreRow(7, "ow")}")
          } finally {
            table.spark.conf.unset("spark.wap.id")
          }
          val mainRowCount = table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0)
          val stagedSnapshotCount = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name}.snapshots " +
                "WHERE summary['wap.id'] = 'wS'")
            .collect()(0)
            .getLong(0)

          println(
            "DIAG wapStaged.overwrite: " +
              s"mainPreCount=$mainRowCount stagedSnapshots=$stagedSnapshotCount")
          assert(mainRowCount == 3, "staged overwrite changed main before publish")

          val stagedSnapshotId = table.spark
            .sql(
              s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                "WHERE summary['wap.id'] = 'wS'")
            .collect()(0)
            .getLong(0)
          table.spark.sql(
            "CALL openhouse.system.cherrypick_snapshot(" +
              s"'${catalogRelative(table.name)}', $stagedSnapshotId)")

          assert(
            table.spark
              .sql(s"SELECT count(*) FROM ${table.name}")
              .collect()(0)
              .getLong(0) == 1,
            "publishing the staged overwrite did not replace main")
        },
        preparation.test("wapStaged.delete.bypassesWap") { table =>
          table.spark.conf.set("spark.wap.id", "wD")
          try {
            table.spark.sql(
              s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
          } finally {
            table.spark.conf.unset("spark.wap.id")
          }
          val mainRowCount = table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0)
          val stagedSnapshotCount = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name}.snapshots " +
                "WHERE summary['wap.id'] = 'wD'")
            .collect()(0)
            .getLong(0)

          println(
            "DIAG wapStaged.delete.bypassesWap: " +
              s"mainAfterStagedDelete=$mainRowCount " +
              s"stagedSnapshots=$stagedSnapshotCount")
          assert(
            mainRowCount == 2 && stagedSnapshotCount == 0,
            "staged DELETE should commit directly to main without a WAP snapshot")
        },
        preparation.test("wapStaged.merge") { table =>
          table.spark.conf.set("spark.wap.id", "wS")
          try {
            table.spark.sql(
              s"MERGE INTO ${table.name} " +
                "USING (SELECT CAST(99 AS BIGINT) AS key) source " +
                s"ON ${table.name}.${Core.long0.columnName} = source.key " +
                "WHEN NOT MATCHED THEN INSERT " +
                s"(${Core.columnNames.mkString(", ")}) " +
                "VALUES (source.key, 9, 'm', 9.5, true, '2024-01-09-01')")
          } finally {
            table.spark.conf.unset("spark.wap.id")
          }
          val mainRowCount = table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0)
          val stagedSnapshotCount = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name}.snapshots " +
                "WHERE summary['wap.id'] = 'wS'")
            .collect()(0)
            .getLong(0)

          println(
            "DIAG wapStaged.merge: " +
              s"mainPreCount=$mainRowCount stagedSnapshots=$stagedSnapshotCount")
          assert(mainRowCount == 3, "staged merge changed main before publish")

          val stagedSnapshotId = table.spark
            .sql(
              s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                "WHERE summary['wap.id'] = 'wS'")
            .collect()(0)
            .getLong(0)
          table.spark.sql(
            "CALL openhouse.system.cherrypick_snapshot(" +
              s"'${catalogRelative(table.name)}', $stagedSnapshotId)")

          assert(
            table.spark
              .sql(s"SELECT count(*) FROM ${table.name}")
              .collect()(0)
              .getLong(0) == 4,
            "publishing the staged merge did not advance main")
        },
        preparation.test("wapStaged.update.valueVisibleOnlyAfterPublish") { table =>
          table.spark.conf.set("spark.wap.id", "wU")
          try {
            table.spark.sql(
              s"UPDATE ${table.name} " +
                s"SET ${Core.string0.columnName} = 'staged-upd' " +
                s"WHERE ${Core.long0.columnName} = 1")
          } finally {
            table.spark.conf.unset("spark.wap.id")
          }
          val valueBeforePublish = table.spark
            .sql(
              s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
                s"WHERE ${Core.long0.columnName} = 1")
            .collect()(0)
            .getString(0)

          assert(
            valueBeforePublish != "staged-upd",
            s"staged update changed main before publish: $valueBeforePublish")

          val stagedSnapshotId = table.spark
            .sql(
              s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                "WHERE summary['wap.id'] = 'wU'")
            .collect()(0)
            .getLong(0)
          table.spark.sql(
            "CALL openhouse.system.cherrypick_snapshot(" +
              s"'${catalogRelative(table.name)}', $stagedSnapshotId)")
          val valueAfterPublish = table.spark
            .sql(
              s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
                s"WHERE ${Core.long0.columnName} = 1")
            .collect()(0)
            .getString(0)

          assert(
            valueAfterPublish == "staged-upd",
            s"published update returned $valueAfterPublish")
        },
        preparation.test("wapStaged.twoIdsIndependent") { table =>
          def stageInsert(wapId: String, key: Int): Unit = {
            table.spark.conf.set("spark.wap.id", wapId)
            try {
              table.spark.sql(
                s"INSERT INTO ${table.name} VALUES " +
                  coreRow(key, s"s-$wapId"))
            } finally {
              table.spark.conf.unset("spark.wap.id")
            }
          }
          def snapshotId(wapId: String): Long =
            table.spark
              .sql(
                s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                  s"WHERE summary['wap.id'] = '$wapId'")
              .collect()(0)
              .getLong(0)

          stageInsert("wa", 101)
          stageInsert("wb", 102)
          assert(
            table.spark
              .sql(s"SELECT count(*) FROM ${table.name}")
              .collect()(0)
              .getLong(0) == 3,
            "a staged ID changed main before publish")

          table.spark.sql(
            "CALL openhouse.system.cherrypick_snapshot(" +
              s"'${catalogRelative(table.name)}', ${snapshotId("wa")})")
          assert(
            table.spark
              .sql(s"SELECT count(*) FROM ${table.name}")
              .collect()(0)
              .getLong(0) == 4,
            "publishing wa did not advance main")
          assert(
            table.spark
              .sql(
                s"SELECT count(*) FROM ${table.name} " +
                  s"WHERE ${Core.long0.columnName} = 102")
              .collect()(0)
              .getLong(0) == 0,
            "wb published before its cherry-pick")

          table.spark.sql(
            "CALL openhouse.system.cherrypick_snapshot(" +
              s"'${catalogRelative(table.name)}', ${snapshotId("wb")})")
          assert(
            table.spark
              .sql(s"SELECT count(*) FROM ${table.name}")
              .collect()(0)
              .getLong(0) == 5,
            "publishing wb did not advance main")
        },
        preparation.test("wapStaged.expireVsStaged") { table =>
          table.spark.conf.set("spark.wap.id", "wE")
          try {
            table.spark.sql(
              s"INSERT INTO ${table.name} VALUES ${coreRow(200, "stg")}")
          } finally {
            table.spark.conf.unset("spark.wap.id")
          }
          val stagedSnapshotId = table.spark
            .sql(
              s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                "WHERE summary['wap.id'] = 'wE'")
            .collect()(0)
            .getLong(0)

          table.spark.sql(
            "CALL openhouse.system.expire_snapshots(" +
              s"table => '${catalogRelative(table.name)}', " +
              "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
              "retain_last => 1)")
          val survivedExpiration = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name}.snapshots " +
                s"WHERE snapshot_id = $stagedSnapshotId")
            .collect()(0)
            .getLong(0)
          val publishOutcome =
            try {
              table.spark.sql(
                "CALL openhouse.system.cherrypick_snapshot(" +
                  s"'${catalogRelative(table.name)}', $stagedSnapshotId)")
              "published"
            } catch {
              case NonFatal(exception) =>
                s"stranded:${Exceptions.root(exception).getClass.getSimpleName}"
            }

          println(
            "DIAG wapStaged.expireVsStaged: " +
              s"stagedSurvivedExpire=$survivedExpiration " +
              s"cherrypickAfterExpire=$publishOutcome")
          assert(
            survivedExpiration == 0 && publishOutcome.startsWith("stranded"),
            "expiration should remove and strand the unreferenced staged snapshot")
        })
    }

  val branchDdlCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val preparation = TablePreparation(
        format,
        TableTest(Core)
          .sql("create")(table =>
            s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
              s"TBLPROPERTIES ('write.format.default'='$format')")()
          .insert(3)()
          .sql("enableWap")(table =>
            s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")()
          .sql("createBranch")(table =>
            s"ALTER TABLE $table CREATE BRANCH bddl")())

      List(
        preparation.test("branchDdl.addColumn.leaksToMain") { table =>
          table.spark.conf.set("spark.wap.branch", "bddl")
          val outcome =
            try {
              table.spark.sql(
                s"ALTER TABLE ${table.name} ADD COLUMN br_added int")
              "accepted"
            } catch {
              case NonFatal(exception) =>
                s"rejected:${Exceptions.root(exception).getClass.getSimpleName}"
            } finally {
              table.spark.conf.unset("spark.wap.branch")
            }
          val columnNames = table.spark
            .sql(s"DESCRIBE TABLE ${table.name}")
            .collect()
            .map(_.getString(0).trim)
            .toSet

          println(
            "DIAG branchDdl.addColumn.leaksToMain: " +
              s"branch-routed DDL $outcome")
          assert(
            columnNames.contains("br_added"),
            "ADD COLUMN on a branch should change the table-global schema")
        },
        preparation.test("branchDdl.setTblProp.leaksToMain") { table =>
          table.spark.conf.set("spark.wap.branch", "bddl")
          val outcome =
            try {
              table.spark.sql(
                s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
                  "('user.branchkey'='v1')")
              "accepted"
            } catch {
              case NonFatal(exception) =>
                s"rejected:${Exceptions.root(exception).getClass.getSimpleName}"
            } finally {
              table.spark.conf.unset("spark.wap.branch")
            }
          val properties = table.spark
            .sql(s"SHOW TBLPROPERTIES ${table.name}")
            .collect()
            .map(row => row.getString(0) -> row.getString(1))
            .toMap

          println(
            "DIAG branchDdl.setTblProp.leaksToMain: " +
              s"branch-routed DDL $outcome")
          assert(
            properties.get("user.branchkey").contains("v1"),
            "SET TBLPROPERTIES on a branch should change table-global properties")
        },
        preparation.test("branchDdl.alterColumnComment.leaksToMain") { table =>
          table.spark.conf.set("spark.wap.branch", "bddl")
          val outcome =
            try {
              table.spark.sql(
                s"ALTER TABLE ${table.name} " +
                  s"ALTER COLUMN ${Core.string0.columnName} COMMENT 'br-comment'")
              "accepted"
            } catch {
              case NonFatal(exception) =>
                s"rejected:${Exceptions.root(exception).getClass.getSimpleName}"
            } finally {
              table.spark.conf.unset("spark.wap.branch")
            }
          val comment = table.spark
            .sql(s"DESCRIBE TABLE ${table.name}")
            .collect()
            .find(_.getString(0).trim == Core.string0.columnName)
            .map(_.getString(2))
            .getOrElse("")

          println(
            "DIAG branchDdl.alterColumnComment.leaksToMain: " +
              s"branch-routed DDL $outcome")
          assert(
            Option(comment).getOrElse("").contains("br-comment"),
            "ALTER COLUMN COMMENT on a branch should change table-global metadata")
        },
        preparation.test("branchDdl.dropColumn.rejected") { table =>
          table.spark.conf.set("spark.wap.branch", "bddl")
          val outcome =
            try {
              table.spark.sql(
                s"ALTER TABLE ${table.name} " +
                  s"DROP COLUMN ${Core.string0.columnName}")
              "accepted"
            } catch {
              case NonFatal(exception) =>
                s"rejected:${Exceptions.root(exception).getClass.getSimpleName}"
            } finally {
              table.spark.conf.unset("spark.wap.branch")
            }
          val columnNames = table.spark
            .sql(s"DESCRIBE TABLE ${table.name}")
            .collect()
            .map(_.getString(0).trim)
            .toSet

          println(
            "DIAG branchDdl.dropColumn.rejected: " +
              s"branch-routed DDL $outcome")
          assert(
            columnNames.contains(Core.string0.columnName),
            "DROP COLUMN should remain rejected while a branch is selected")
        })
    }

  val branchingCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val preparation = TablePreparation(
        format,
        TableTest(Core)
          .sql("create")(table =>
            s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
              s"TBLPROPERTIES ('write.format.default'='$format')")()
          .insert(3)())

      List(
        preparation.test("branch.direct.isolation") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} CREATE BRANCH b")
          table.spark.sql(
            s"INSERT INTO ${table.name}.branch_b VALUES " +
              coreRow(99, "branch"))
          val branchRowCount = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name} VERSION AS OF 'b'")
            .collect()(0)
            .getLong(0)
          val mainRowCount = table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0)

          assert(
            branchRowCount == 4,
            s"branch b should have 4 rows, got $branchRowCount")
          assert(
            mainRowCount == 3,
            s"main should be unchanged at 3 rows, got $mainRowCount")
        },
        preparation.test("branch.wapConf.routing") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
              "('write.wap.enabled'='true')")
          table.spark.sql(
            s"ALTER TABLE ${table.name} CREATE BRANCH wapbr")
          table.spark.conf.set("spark.wap.branch", "wapbr")
          val branchRowCount =
            try {
              table.spark.sql(
                s"INSERT INTO ${table.name} VALUES ${coreRow(99, "wap")}")
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0)
            } finally {
              table.spark.conf.unset("spark.wap.branch")
            }
          val mainRowCount = table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0)

          assert(
            branchRowCount == 4,
            s"branch-routed read should see 4 rows, got $branchRowCount")
          assert(
            mainRowCount == 3,
            s"branch-routed write changed main to $mainRowCount rows")
        },
        preparation.test("wap.stagePublish") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
              "('write.wap.enabled'='true')")
          table.spark.conf.set("spark.wap.id", "w1")
          try {
            table.spark.sql(
              s"INSERT INTO ${table.name} VALUES ${coreRow(99, "staged")}")
          } finally {
            table.spark.conf.unset("spark.wap.id")
          }
          val mainBeforePublish = table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0)
          assert(
            mainBeforePublish == 3,
            s"staged write changed main to $mainBeforePublish rows")

          val stagedSnapshotId = table.spark
            .sql(
              s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                "WHERE summary['wap.id'] = 'w1'")
            .collect()(0)
            .getLong(0)
          table.spark.sql(
            "CALL openhouse.system.cherrypick_snapshot(" +
              s"'${catalogRelative(table.name)}', $stagedSnapshotId)")
          val mainAfterPublish = table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0)

          assert(
            mainAfterPublish == 4,
            s"publishing the staged write left main at $mainAfterPublish rows")
        },
        preparation.test("branch.ddlLeak.addColumn") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
              "('write.wap.enabled'='true')")
          table.spark.sql(
            s"ALTER TABLE ${table.name} CREATE BRANCH leakbr")
          table.spark.conf.set("spark.wap.branch", "leakbr")
          try {
            table.spark.sql(
              s"ALTER TABLE ${table.name} ADD COLUMN leaked_col int")
          } finally {
            table.spark.conf.unset("spark.wap.branch")
          }
          val mainColumnNames =
            table.spark.table(table.name).schema.fields.map(_.name).toSeq

          assert(
            mainColumnNames.contains("leaked_col"),
            "ADD COLUMN on a branch should change the table-global schema")
        },
        preparation.test("branch.dml.updateDelete") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
              "('write.wap.enabled'='true')")
          table.spark.sql(
            s"ALTER TABLE ${table.name} CREATE BRANCH dmlbr")
          table.spark.conf.set("spark.wap.branch", "dmlbr")
          try {
            table.spark.sql(
              s"UPDATE ${table.name} " +
                s"SET ${Core.string0.columnName} = 'br-upd' " +
                s"WHERE ${Core.long0.columnName} = 1")
            table.spark.sql(
              s"DELETE FROM ${table.name} " +
                s"WHERE ${Core.long0.columnName} = 2")
          } finally {
            table.spark.conf.unset("spark.wap.branch")
          }
          val branchRowCount = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name} VERSION AS OF 'dmlbr'")
            .collect()(0)
            .getLong(0)
          val mainRowCount = table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0)
          val branchValue = table.spark
            .sql(
              s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
                "VERSION AS OF 'dmlbr' " +
                s"WHERE ${Core.long0.columnName} = 1")
            .collect()(0)
            .getString(0)

          assert(
            branchRowCount == 2,
            s"branch should have 2 rows after delete, got $branchRowCount")
          assert(
            mainRowCount == 3,
            s"branch DML changed main to $mainRowCount rows")
          assert(
            branchValue == "br-upd",
            s"branch update returned $branchValue")
        },
        preparation.test("branch.lifecycle.tag") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} CREATE TAG mytag")
          val tagCount = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name}.refs " +
                "WHERE name = 'mytag' AND type = 'TAG'")
            .collect()(0)
            .getLong(0)

          assert(tagCount == 1, "CREATE TAG did not create the tag ref")
        },
        preparation.test("branch.lifecycle.dropBranch") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} CREATE BRANCH tmpbr")
          val branchCountBeforeDrop = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name}.refs " +
                "WHERE name = 'tmpbr'")
            .collect()(0)
            .getLong(0)
          assert(
            branchCountBeforeDrop == 1,
            "CREATE BRANCH did not create the branch ref")

          table.spark.sql(
            s"ALTER TABLE ${table.name} DROP BRANCH tmpbr")
          val branchCountAfterDrop = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name}.refs " +
                "WHERE name = 'tmpbr'")
            .collect()(0)
            .getLong(0)

          assert(
            branchCountAfterDrop == 0,
            "DROP BRANCH did not remove the branch ref")
        },
        preparation.test("branch.neg.wapIdAndBranch") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
              "('write.wap.enabled'='true')")
          table.spark.sql(
            s"ALTER TABLE ${table.name} CREATE BRANCH nb")
          table.spark.conf.set("spark.wap.id", "w1")
          table.spark.conf.set("spark.wap.branch", "nb")
          try {
            val exception = Check.intercept[ValidationException](
              table.spark.sql(
                s"INSERT INTO ${table.name} VALUES ${coreRow(99, "x")}"))
            assert(
              exception.getMessage.contains("Cannot set both WAP ID and branch"),
              s"unexpected validation message: ${exception.getMessage.take(140)}")
          } finally {
            table.spark.conf.unset("spark.wap.id")
            table.spark.conf.unset("spark.wap.branch")
          }
        },
        preparation.test("branch.neg.insertNonexistentBranch") { table =>
          val exception = Check.intercept[ValidationException](
            table.spark.sql(
              s"INSERT INTO ${table.name}.branch_nope VALUES " +
                coreRow(99, "x")))

          assert(
            exception.getMessage.contains("does not exist"),
            s"unexpected validation message: ${exception.getMessage.take(140)}")
        })
    }




}

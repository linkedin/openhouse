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

trait BranchWapScenarios extends BranchScenarioKit {
  import Rows._

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
            s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")(),
        description = s"Three seed rows in a $format table with write.wap.enabled set to true.")

      List(
        preparation.test(
          "wapStaged.insert",
          "A staged INSERT under spark.wap.id does not change main until its snapshot is " +
            "cherry-picked, after which main includes the inserted row.") { table =>
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
        preparation.test(
          "wapStaged.overwrite",
          "A staged INSERT OVERWRITE under spark.wap.id does not change main until its snapshot " +
            "is cherry-picked, after which main is replaced by the overwritten rows.") { table =>
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
        preparation.test(
          "wapStaged.delete.bypassesWap",
          "A DELETE issued under spark.wap.id commits directly to main with no staged snapshot, " +
            "unlike INSERT, OVERWRITE, and MERGE.") { table =>
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
        preparation.test(
          "wapStaged.merge",
          "A staged MERGE INSERT under spark.wap.id does not change main until its snapshot is " +
            "cherry-picked, after which main includes the merged row.") { table =>
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
        preparation.test(
          "wapStaged.update.valueVisibleOnlyAfterPublish",
          "A staged UPDATE under spark.wap.id leaves the old value visible on main until its " +
            "snapshot is cherry-picked, after which main reads the updated value.") { table =>
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
        preparation.test(
          "wapStaged.twoIdsIndependent",
          "Two inserts staged under different spark.wap.id values publish independently: " +
            "cherry-picking one advances main without exposing the other's row until it too is " +
            "cherry-picked.") { table =>
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
        preparation.test(
          "wapStaged.expireVsStaged",
          "Expiring snapshots with retain_last=1 removes an unreferenced staged WAP snapshot, and " +
            "cherry-picking it afterward fails because the snapshot is gone.") { table =>
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
            s"ALTER TABLE $table CREATE BRANCH bddl")(),
        description = s"Three seed rows in a $format table with write.wap.enabled set to true and " +
          "branch bddl created.")

      List(
        preparation.test(
          "branchDdl.addColumn.leaksToMain",
          "ALTER TABLE ADD COLUMN issued while spark.wap.branch selects a branch is accepted and " +
            "adds the column to the table's global schema, visible on main.") { table =>
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
        preparation.test(
          "branchDdl.setTblProp.leaksToMain",
          "ALTER TABLE SET TBLPROPERTIES issued while spark.wap.branch selects a branch is accepted " +
            "and changes the table's global properties, visible on main.") { table =>
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
        preparation.test(
          "branchDdl.alterColumnComment.leaksToMain",
          "ALTER TABLE ALTER COLUMN COMMENT issued while spark.wap.branch selects a branch is " +
            "accepted and changes the table's global column comment, visible on main.") { table =>
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
        preparation.test(
          "branchDdl.dropColumn.rejected",
          "ALTER TABLE DROP COLUMN issued while spark.wap.branch selects a branch is rejected, and " +
            "the column remains present.") { table =>
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
            outcome.startsWith("rejected:"),
            s"DROP COLUMN should be rejected while a branch is selected: $outcome")
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
          .insert(3)(),
        description = s"Three seed rows in a $format table with no branches or WAP configuration.")

      List(
        preparation.test(
          "branch.direct.isolation",
          "Inserting directly into a created branch adds a row visible only when reading that " +
            "branch, and main keeps its original 3 rows.") { table =>
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
        preparation.test(
          "branch.wapConf.routing",
          "With write.wap.enabled set and spark.wap.branch selecting a branch, an INSERT and the " +
            "following read both route to that branch, leaving main at its original 3 rows.") { table =>
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
        preparation.test(
          "wap.stagePublish",
          "A staged INSERT under spark.wap.id leaves main at its original 3 rows until its " +
            "snapshot is cherry-picked, after which main includes the inserted row.") { table =>
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
        preparation.test(
          "branch.ddlLeak.addColumn",
          "ALTER TABLE ADD COLUMN issued while spark.wap.branch selects a branch changes the " +
            "table's global schema, visible on main.") { table =>
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
        preparation.test(
          "branch.dml.updateDelete",
          "UPDATE and DELETE issued while spark.wap.branch selects a branch change only that " +
            "branch's rows and leave main at its original 3 rows.") { table =>
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
        preparation.test(
          "branch.lifecycle.tag",
          "A tag pins its snapshot through a later insert and snapshot expiration: the tag still " +
            "reads 3 rows, main reads 4 rows including the new one, and the tagged snapshot is not " +
            "expired.") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} CREATE TAG mytag")
          val taggedSnapshotId = table.spark
            .sql(
              s"SELECT snapshot_id FROM ${table.name}.refs " +
                "WHERE name = 'mytag' AND type = 'TAG'")
            .collect()(0)
            .getLong(0)

          table.spark.sql(
            s"INSERT INTO ${table.name} VALUES " +
              "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
          table.spark.sql(
            "CALL openhouse.system.expire_snapshots(" +
              s"table => '${catalogRelative(table.name)}', " +
              "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
              "retain_last => 1)")

          assert(
            table.spark
              .sql(
                s"SELECT count(*) FROM ${table.name} VERSION AS OF 'mytag'")
              .collect()(0)
              .getLong(0) == 3,
            "the tag should read the snapshot captured before the insert")
          assert(
            table.spark
              .sql(
                s"SELECT count(*) FROM ${table.name}")
              .collect()(0)
              .getLong(0) == 4,
            "the main branch should include the inserted row")
          assert(
            table.spark
              .sql(
                s"SELECT count(*) FROM ${table.name}.snapshots " +
                  s"WHERE snapshot_id = $taggedSnapshotId")
              .collect()(0)
              .getLong(0) == 1,
            "snapshot expiration should retain the snapshot referenced by the tag")
        },
        preparation.test(
          "branch.lifecycle.dropBranch",
          "CREATE BRANCH adds a ref that DROP BRANCH then removes.") { table =>
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
        preparation.test(
          "branch.neg.wapIdAndBranch",
          "Setting both spark.wap.id and spark.wap.branch on a write is rejected with a validation " +
            "error naming the conflict.") { table =>
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
        preparation.test(
          "branch.neg.insertNonexistentBranch",
          "Inserting into a branch name that was never created is rejected with a validation error " +
            "saying the branch does not exist.") { table =>
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

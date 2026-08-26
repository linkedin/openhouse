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

// The RTAS interaction family. Each case composes CREATE OR REPLACE TABLE AS SELECT with another
// table state or another operation: an evolved schema, a snapshot reference, a maintenance
// procedure, or a REST lock. The cases run on parquet and orc.
trait RtasInteractionScenarios extends RtasScenarioKit {
  import Rows._

  def interactionRtasCases(format: String): List[Plan.Case] = {
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)(),
      description = s"Three seed rows in a $format table.")
    val replacePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)()
        .sql("enableReplace")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('replace.enabled'='true')")(),
      description = s"Three seed rows in a $format table with replace.enabled set to true.")
    val userPropertyPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', " +
            "'replace.enabled'='true', 'user.key'='v1')")()
        .insert(3)(),
      description = s"Three seed rows in a $format table with replace.enabled set to true and a " +
        "user property user.key=v1.")
    val retentionPolicyPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"PARTITIONED BY (${Core.datePartition.columnName}) " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', 'replace.enabled'='true')")()
        .insert(3)()
        .sql("setRetention")(table =>
          s"ALTER TABLE $table SET POLICY " +
            s"(RETENTION = 30d ON COLUMN ${Core.datePartition.columnName} " +
            "WHERE pattern = 'yyyy-MM-dd-HH')")(),
      description = s"Three seed rows in a $format table partitioned by datepartition, with " +
        "replace.enabled set to true and a 30-day retention policy on datepartition.")

    List(
      replacePreparation.test(
        "interact.rtas.historyPreserved",
        "CREATE OR REPLACE TABLE AS SELECT keeps the pre-replace snapshot in history: two " +
          "snapshots exist afterward and the pre-replace one still reads 3 rows.") { table =>
        val preReplaceSnapshotId = snapshotIds(table.spark, table.name).last
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        val snapshotCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}.snapshots")
          .collect()(0)
          .getLong(0)
        val historicalRowCount = table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} " +
              s"VERSION AS OF $preReplaceSnapshotId")
          .collect()(0)
          .getLong(0)

        assert(
          snapshotCount == 2,
          s"replace should retain two snapshots, got $snapshotCount")
        assert(
          historicalRowCount == 3,
          s"pre-replace snapshot should contain 3 rows, got $historicalRowCount")
      },
      replacePreparation.test(
        "interact.rtas.restoreRejected",
        "Rolling back to a snapshot from before CREATE OR REPLACE TABLE AS SELECT is rejected " +
          "because it is not an ancestor of the current snapshot.") { table =>
        val preReplaceSnapshotId = snapshotIds(table.spark, table.name).last
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        val exception = Check.intercept[ValidationException](
          table.spark.sql(
            "CALL openhouse.system.rollback_to_snapshot(" +
              s"'${catalogRelative(table.name)}', $preReplaceSnapshotId)"))

        assert(
          exception.getMessage.contains("not an ancestor"),
          "rollback across replacement should reject the old lineage")
      },
      replacePreparation.test(
        "interact.rtas.setCurrentRecovery",
        "set_current_snapshot to a pre-replace snapshot recovers the pre-replace 3 rows.") { table =>
        val preReplaceSnapshotId = snapshotIds(table.spark, table.name).last
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        table.spark.sql(
          "CALL openhouse.system.set_current_snapshot(" +
            s"'${catalogRelative(table.name)}', $preReplaceSnapshotId)")
        val recoveredRowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)

        assert(
          recoveredRowCount == 3,
          s"set_current_snapshot should recover 3 rows, got $recoveredRowCount")
      },
      replacePreparation.test(
        "interact.rtas.writeAfter",
        "A table replaced by CREATE OR REPLACE TABLE AS SELECT accepts an insert immediately " +
          "afterward, and the row count reflects both the replacement's rows and the new insert.") { table =>
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        val rowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)

        assert(
          rowCount == 3,
          s"replaced table should contain 3 rows after insert, got $rowCount")
      },
      replacePreparation.test(
        "interact.rtas.partitionSpecChange",
        "CREATE OR REPLACE TABLE AS SELECT with a new PARTITIONED BY clause replaces the " +
          "partition specification and preserves all 3 rows.") { table =>
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"PARTITIONED BY (${Core.datePartition.columnName}) " +
            s"AS SELECT * FROM ${table.name}")
        val description = table.spark
          .sql(s"DESCRIBE TABLE ${table.name}")
          .collect()
          .toSeq
        val rowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)

        assert(
          description.exists(_.getString(0) == "# Partition Information") &&
            description.count(
              _.getString(0) == Core.datePartition.columnName) == 2,
          "RTAS should replace the partition specification")
        assert(
          rowCount == 3,
          s"partition-spec replacement should preserve 3 rows, got $rowCount")
      },
      basePreparation.test(
        "interact.rtas.dropsColumn",
        "CREATE OR REPLACE TABLE AS SELECT with a narrower column list projects a separate table " +
          "down to those two columns while preserving all 3 rows.") { table =>
        val sideTable = s"${table.name}_dropcol"
        table.spark.sql(s"DROP TABLE IF EXISTS $sideTable")
        try {
          table.spark.sql(
            s"CREATE TABLE $sideTable USING $dataSource " +
              "TBLPROPERTIES ('replace.enabled'='true') " +
              s"AS SELECT * FROM ${table.name}")
          table.spark.sql(
            s"CREATE OR REPLACE TABLE $sideTable USING $dataSource AS " +
              s"SELECT ${Core.long0.columnName}, ${Core.string0.columnName} " +
              s"FROM $sideTable")
          val columns = table.spark
            .sql(s"SELECT * FROM $sideTable LIMIT 1")
            .columns
            .toSeq
          val rowCount = table.spark
            .sql(s"SELECT count(*) FROM $sideTable")
            .collect()(0)
            .getLong(0)

          assert(
            columns == Seq(Core.long0.columnName, Core.string0.columnName),
            s"RTAS should project the table to two columns, got $columns")
          assert(
            rowCount == 3,
            s"column-drop RTAS should preserve 3 rows, got $rowCount")
        } finally {
          table.spark.sql(s"DROP TABLE IF EXISTS $sideTable")
        }
      },
      userPropertyPreparation.test(
        "interact.rtas.props.userSurvival",
        "CREATE OR REPLACE TABLE AS SELECT with no TBLPROPERTIES clause preserves the existing " +
          "user.key and replace.enabled properties.") { table =>
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        val properties = tableProps(table.spark, table.name)

        assert(
          properties.get("user.key").contains("v1"),
          s"user.key did not survive RTAS: ${properties.get("user.key")}")
        assert(
          properties.get("replace.enabled").contains("true"),
          "replace.enabled did not survive RTAS")
      },
      userPropertyPreparation.test(
        "interact.rtas.props.statementWins",
        "CREATE OR REPLACE TABLE AS SELECT with a TBLPROPERTIES clause overrides the matching " +
          "existing property while properties absent from the statement survive unchanged.") { table =>
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            "TBLPROPERTIES ('user.key'='v2') " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        val properties = tableProps(table.spark, table.name)

        assert(
          properties.get("user.key").contains("v2"),
          s"statement property should win, got ${properties.get("user.key")}")
        assert(
          properties.get("replace.enabled").contains("true"),
          "properties omitted from RTAS should survive")
      },
      replacePreparation.test(
        "interact.rtas.props.createDefaulting",
        "CREATE OR REPLACE TABLE AS SELECT with write.format.default=orc sets that property, " +
          "keeps format-version at 2, and the replaced table remains writable.") { table =>
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            "TBLPROPERTIES ('write.format.default'='orc') " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        val properties = tableProps(table.spark, table.name)

        assert(
          properties.get("write.format.default").contains("orc"),
          "RTAS should set write.format.default to orc")
        assert(
          properties.get("format-version").forall(_ == "2"),
          s"format-version drifted: ${properties.get("format-version")}")

        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        assert(
          table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0) == 3,
          "RTAS table using ORC should remain writable")
      },
      retentionPolicyPreparation.test(
        "interact.rtas.props.preservesRetentionPolicy",
        "CREATE OR REPLACE TABLE AS SELECT with a new partition spec preserves the table's UUID " +
          "and its retention policy.") { table =>
        val tableUuidBefore = tableProps(table.spark, table.name)
          .getOrElse("openhouse.tableUUID", "<absent>")
        val policiesBefore = tableProps(table.spark, table.name)
          .getOrElse("policies", "<absent>")
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"PARTITIONED BY (${Core.datePartition.columnName}) " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        val properties = tableProps(table.spark, table.name)

        assert(
          properties.getOrElse("openhouse.tableUUID", "<absent>") ==
            tableUuidBefore,
          "table UUID should survive RTAS")
        assert(
          properties.getOrElse("policies", "<absent>") == policiesBefore,
          "RTAS should preserve the retention policy")
      },
      replacePreparation
        .test(
          "interact.rtas.withBranch",
          "CREATE OR REPLACE TABLE AS SELECT while a branch exists is rejected because branching " +
            "is enabled, and both main and the branch remain exactly as they were before the " +
            "attempt.") { table =>
          table.spark.sql(
            s"ALTER TABLE ${table.name} CREATE BRANCH keepbr")
          table.spark.sql(
            s"INSERT INTO ${table.name}.branch_keepbr VALUES " +
              "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
          val mainStateBefore = table.state
          val branchRowCountBefore = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name} VERSION AS OF 'keepbr'")
            .collect()(0)
            .getLong(0)
          val exception = Check.intercept[BadRequestException](
            table.spark.sql(
              s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
                s"AS SELECT * FROM ${table.name} " +
                s"WHERE ${Core.long0.columnName} <= 2"))
          val branchRowCountAfter = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name} VERSION AS OF 'keepbr'")
            .collect()(0)
            .getLong(0)

          assert(
            exception.getMessage.contains("while branching is enabled"),
            s"msg: ${exception.getMessage.take(160)}")
          assert(
            table.state == mainStateBefore,
            "rejected RTAS should not change the main table")
          assert(
            branchRowCountAfter == branchRowCountBefore,
            "rejected RTAS should not change the branch")
        }
        .copy(knownBugReason = Some(
          "The guide documents CREATE OR REPLACE TABLE AS SELECT as incompatible with an " +
            "existing branch. The current product accepts the statement. This case keeps the " +
            "documented rejection as the contract so the gap is visible; it is skipped until the " +
            "product enforces the rejection.")))
  }

  // The REST lock has no SQL surface, so this case runs directly against a Ctx like the other
  // control-plane cases. The lock must reject both a normal write and RTAS.
  def interactRtasOnLockedTable(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_lockrtas"
    val Array(db, tbl) = table.stripPrefix("openhouse.").split("\\.", 2)
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(coreCreateParquet(table))
    spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, 3)}")
    spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('replace.enabled'='true')")
    try {
      val (lockStatus, lockBody) = Rest.post(ctx, s"/v1/databases/$db/tables/$tbl/lock", """{"locked":true}""")
      assert(lockStatus >= 200 && lockStatus < 300, s"lock POST failed: $lockStatus $lockBody")
      val blocked = Check.intercept[Exception](spark.sql(
        s"UPDATE $table SET ${Core.string0.columnName} = 'x' WHERE ${Core.long0.columnName} = 1"))
      assert(Exceptions.causeChain(blocked).exists(t => Option(t.getMessage).exists(_.toLowerCase.contains("locked"))),
        s"lock not enforced on UPDATE: ${blocked.getMessage.take(160)}")
      val rowCountBefore = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)
      val snapshotCountBefore =
        spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0)
      val replaceFailure = Check.intercept[BadRequestException](
        spark.sql(
          s"CREATE OR REPLACE TABLE $table USING $dataSource " +
            s"AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2"))

      assert(
        replaceFailure.getMessage.toLowerCase.contains("locked"),
        s"RTAS rejection did not identify the lock: ${replaceFailure.getMessage.take(160)}")
      assert(
        spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == rowCountBefore,
        "rejected RTAS changed the table rows")
      assert(
        spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0) ==
          snapshotCountBefore,
        "rejected RTAS committed a snapshot")
    } finally {
      Rest.delete(ctx, s"/v1/databases/$db/tables/$tbl/lock")
      spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  val interactionContextCases: List[Plan.Case] =
    List(
      Plan.Case(
        "interact.rtas.onLockedTable @ embedded",
        interactRtasOnLockedTable,
        description = "While a table is REST-locked, both UPDATE and CREATE OR REPLACE TABLE AS " +
          "SELECT are rejected, and the table keeps the same rows and snapshots."))
}

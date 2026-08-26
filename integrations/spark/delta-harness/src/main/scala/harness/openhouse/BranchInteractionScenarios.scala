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

// The branch interaction family. Each case composes a branch or a write-audit-publish staged commit
// with another table state or another operation, so the cases show how branch routing behaves
// alongside DDL, snapshot references and maintenance. The cases run on parquet and orc.
trait BranchInteractionScenarios extends BranchScenarioKit {
  import Rows._

  def interactionBranchCases(format: String): List[Plan.Case] = {
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)(),
      description = s"Three seed rows in a $format table.")
    val twoSnapshotPreparation = TablePreparation(
      format,
      coreTwoSnapshots(format),
      description = s"Five seed rows across two snapshots in a $format table.")
    val wapPreparation = TablePreparation(
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
      twoSnapshotPreparation.test(
        "interact.branch.ttBeforeBranchPoint",
        "After branching and writing to the branch, a snapshot ID or timestamp from before the " +
          "branch point still resolves to the pre-branch 3 rows, both on main and while " +
          "spark.wap.branch selects the branch.") { table =>
        val snapshots = snapshotIds(table.spark, table.name)
        val firstCommitTimestamp = table.spark
          .sql(
            s"SELECT CAST(committed_at AS STRING) FROM ${table.name}.snapshots " +
              "ORDER BY committed_at LIMIT 1")
          .collect()(0)
          .getString(0)
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
            "('write.wap.enabled'='true')")
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH tb")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_tb VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")

        assert(
          table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name} VERSION AS OF 'tb'")
            .collect()(0)
            .getLong(0) == 6,
          "branch head should contain 6 rows")
        assert(
          table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name} " +
                s"VERSION AS OF ${snapshots.head}")
            .collect()(0)
            .getLong(0) == 3,
          "snapshot ID should resolve before the branch point")

        table.spark.conf.set("spark.wap.branch", "tb")
        try {
          assert(
            table.spark
              .sql(
                s"SELECT count(*) FROM ${table.name} " +
                  s"TIMESTAMP AS OF '$firstCommitTimestamp'")
              .collect()(0)
              .getLong(0) == 3,
            "explicit timestamp should override spark.wap.branch")
          assert(
            table.spark
              .sql(
                s"SELECT count(*) FROM ${table.name} " +
                  s"VERSION AS OF ${snapshots.head}")
              .collect()(0)
              .getLong(0) == 3,
            "explicit snapshot ID should override spark.wap.branch")
        } finally {
          table.spark.conf.unset("spark.wap.branch")
        }
      },
      basePreparation.test(
        "interact.branch.mainDdlImmediate",
        "ALTER TABLE ADD COLUMN changes the schema seen from a branch immediately, an old-arity " +
          "insert into the branch fails afterward, and a new-arity insert matching the added " +
          "column succeeds.") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
            "('write.wap.enabled'='true')")
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH mb")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_mb VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        table.spark.sql(
          s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
        val branchColumns = table.spark
          .sql(
            s"SELECT * FROM ${table.name} VERSION AS OF 'mb' LIMIT 1")
          .columns
          .toSeq

        assert(
          branchColumns.contains("extra_col"),
          s"main DDL should change the table-global schema: $branchColumns")

        val exception = Check.intercept[AnalysisException](
          table.spark.sql(
            s"INSERT INTO ${table.name}.branch_mb VALUES " +
              "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')"))
        assert(
          exception.getMessage.toLowerCase.contains("not enough data columns"),
          "old-arity branch writer should fail after main DDL")

        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_mb VALUES " +
            "(CAST(8 AS BIGINT), 8, 'row-8', 8.5, true, " +
            "'2024-01-08-07', 44)")
        assert(
          table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name} VERSION AS OF 'mb'")
            .collect()(0)
            .getLong(0) == 5,
          "new-arity branch write should succeed after main DDL")
      },
      twoSnapshotPreparation.test(
        "interact.branch.expireProtectsRefs",
        "Snapshot expiration after writes on both main and a branch keeps both ref heads, drops " +
          "the intermediate snapshots, and leaves both main and the branch fully readable.") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
            "('write.wap.enabled'='true')")
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH eb")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_eb VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
        assert(
          table.spark
            .sql(s"SELECT count(*) FROM ${table.name}.snapshots")
            .collect()(0)
            .getLong(0) == 4,
          "expected four snapshots before expiration")

        table.spark.sql(
          "CALL openhouse.system.expire_snapshots(" +
            s"table => '${catalogRelative(table.name)}', " +
            "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
            "retain_last => 1)")
        val refs = table.spark
          .sql(s"SELECT name FROM ${table.name}.refs")
          .collect()
          .map(_.getString(0))
          .toSet
        val snapshotCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}.snapshots")
          .collect()(0)
          .getLong(0)
        val branchRowCount = table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'eb'")
          .collect()(0)
          .getLong(0)
        val mainRowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)

        assert(refs == Set("main", "eb"), s"refs changed: $refs")
        assert(
          snapshotCount == 2,
          s"expiration should retain two ref heads, got $snapshotCount")
        assert(
          branchRowCount == 6,
          s"branch should remain readable with 6 rows, got $branchRowCount")
        assert(
          mainRowCount == 6,
          s"main should remain readable with 6 rows, got $mainRowCount")
      },
      twoSnapshotPreparation.test(
        "interact.branch.rollbackWhileWapConf",
        "Calling rollback_to_snapshot while spark.wap.branch selects a branch still rolls back " +
          "main, leaving the branch's own rows unaffected.") { table =>
        val firstSnapshotId = snapshotIds(table.spark, table.name).head
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
            "('write.wap.enabled'='true')")
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH rb")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_rb VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        table.spark.conf.set("spark.wap.branch", "rb")
        try {
          table.spark.sql(
            "CALL openhouse.system.rollback_to_snapshot(" +
              s"'${catalogRelative(table.name)}', $firstSnapshotId)")
        } finally {
          table.spark.conf.unset("spark.wap.branch")
        }
        val mainRowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)
        val branchRowCount = table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'rb'")
          .collect()(0)
          .getLong(0)

        assert(
          mainRowCount == 3,
          s"rollback should target main and restore 3 rows, got $mainRowCount")
        assert(
          branchRowCount == 6,
          s"rollback should leave branch at 6 rows, got $branchRowCount")
      },
      twoSnapshotPreparation.test(
        "interact.restore.expireAfterRollback",
        "After rolling back to the first snapshot, expiring snapshots removes the rolled-past " +
          "snapshot and keeps the current 3 rows readable, but time travel to that expired " +
          "snapshot now fails.") { table =>
        val snapshots = snapshotIds(table.spark, table.name)
        table.spark.sql(
          "CALL openhouse.system.rollback_to_snapshot(" +
            s"'${catalogRelative(table.name)}', ${snapshots.head})")
        table.spark.sql(
          "CALL openhouse.system.expire_snapshots(" +
            s"table => '${catalogRelative(table.name)}', " +
            "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
            "retain_last => 1)")
        val snapshotCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}.snapshots")
          .collect()(0)
          .getLong(0)
        val rowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)

        assert(
          snapshotCount == 1,
          s"rolled-past snapshot should expire, got $snapshotCount snapshots")
        assert(
          rowCount == 3,
          s"rollback should preserve 3 current rows, got $rowCount")

        val exception = Check.intercept[Exception](
          table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name} " +
                s"VERSION AS OF ${snapshots(1)}")
            .collect())
        assert(
          Exceptions.causeChain(exception).exists(error =>
            Option(error.getMessage)
              .exists(_.toLowerCase.contains("snapshot"))),
          "time travel to the expired rolled-past snapshot should fail")
      },
      basePreparation.test(
        "interact.branch.expireMerge.spuriousReject",
        "Expiring snapshots after two writes to a branch removes the intermediate branch " +
          "snapshot but keeps the branch fully readable; fast_forward onto that punctured " +
          "ancestry is rejected, and main stays consistent whether or not a cherry-pick recovery " +
          "succeeds.") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH mb")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_mb VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_mb VALUES " +
            "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}.snapshots") == "3",
          "expected parent and two branch snapshots")

        table.spark.sql(
          "CALL openhouse.system.expire_snapshots(" +
            s"table => '${catalogRelative(table.name)}', " +
            "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
            "retain_last => 1)")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}.snapshots") == "2",
          "expiration should remove the intermediate branch snapshot")
        val refs = table.spark
          .sql(s"SELECT name FROM ${table.name}.refs")
          .collect()
          .map(_.getString(0))
          .toSet
        assert(refs == Set("main", "mb"), s"refs changed: $refs")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'mb'") == "5",
          "branch should remain readable after expiration")

        val exception = Check.intercept[Exception](
          table.spark.sql(
            "CALL openhouse.system.fast_forward(" +
              s"'${catalogRelative(table.name)}', 'main', 'mb')"))
        assert(
          Option(exception.getMessage).exists(_.contains("not an ancestor")),
          "fast_forward should reject the punctured branch ancestry")

        val branchHeadSnapshotId = table.spark
          .sql(
            s"SELECT snapshot_id FROM ${table.name}.refs WHERE name = 'mb'")
          .collect()(0)
          .getLong(0)
        val cherryPickOutcome =
          try {
            table.spark.sql(
              "CALL openhouse.system.cherrypick_snapshot(" +
                s"'${catalogRelative(table.name)}', " +
                s"${branchHeadSnapshotId}L)")
            s"SUCCEEDED: main now ${countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name}")} rows"
          } catch {
            case exception: Throwable =>
              s"REJECTED ${exception.getClass.getName} :: " +
                Option(exception.getMessage).getOrElse("").take(160)
          }
        println(
          s"DIAG expireMerge.cherrypickFallback: $cherryPickOutcome")
        val mainRowCount = countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name}").toLong

        assert(
          mainRowCount == 3 || mainRowCount == 4,
          s"main should remain consistent, got $mainRowCount rows")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'mb'") == "5",
          "branch data should remain available for copy-out recovery")
      },
      wapPreparation.test(
        "interact.branch.expireMerge.stagedWapLoss",
        "Snapshot expiration removes an unreferenced staged WAP snapshot, and publishing that " +
          "wap_id afterward fails while main remains at its original 3 rows.") { table =>
        table.spark.conf.set("spark.wap.id", "w2")
        try {
          table.spark.sql(
            s"INSERT INTO ${table.name} VALUES " +
              "(CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01')")
        } finally {
          table.spark.conf.unset("spark.wap.id")
        }
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}.snapshots " +
              "WHERE summary['wap.id'] = 'w2'") == "1",
          "WAP write should create one staged snapshot")

        table.spark.sql(
          "CALL openhouse.system.expire_snapshots(" +
            s"table => '${catalogRelative(table.name)}', " +
            "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
            "retain_last => 1)")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}.snapshots " +
              "WHERE summary['wap.id'] = 'w2'") == "0",
          "expiration should remove the unreferenced staged snapshot")

        val exception = Check.intercept[Exception](
          table.spark.sql(
            "CALL openhouse.system.publish_changes(" +
              s"table => '${catalogRelative(table.name)}', wap_id => 'w2')"))
        println(
          "DIAG stagedWapLoss.publish: " +
            s"${exception.getClass.getName} :: " +
            Option(exception.getMessage).getOrElse("").take(180))
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "3",
          "main should remain unchanged after staged snapshot loss")
      })
  }

  // A table created with write.wap.enabled and replace.enabled both set. The case reads those flags
  // back, then creates a branch and confirms the replace path is refused while the branch exists.
  def interactionBranchFlagCases(format: String): List[Plan.Case] = {
    val flagPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', " +
            "'write.wap.enabled'='true', 'replace.enabled'='true')")()
        .insert(3)(),
      description = s"Three seed rows in a $format table with write.wap.enabled and " +
        "replace.enabled both set to true at create time.")

    List(
      flagPreparation.test(
        "interact.flags.wapReplaceAtCreate",
        "WAP and replace flags set at CREATE time are active, and a subsequent RTAS is rejected " +
          "while a branch exists and WAP is enabled.") { table =>
        val properties = tableProps(table.spark, table.name)
        assert(
          properties.get("write.wap.enabled").contains("true") &&
            properties.get("replace.enabled").contains("true"),
          "WAP and replace flags should be active when set at CREATE")

        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH cb")
        val exception = Check.intercept[BadRequestException](
          table.spark.sql(
            s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
              s"AS SELECT * FROM ${table.name}"))
        assert(
          exception.getMessage.contains("while WAP"),
          "RTAS should reject a table with WAP enabled at CREATE")
      })
  }
}

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

trait InteractionScenarios extends ScenarioKit {
  import Rows._


  private def interactionDdlCases(format: String): List[Plan.Case] = {
    val preparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())

    List(
      preparation.test("interact.ddl.ttAfterAddColumn") { table =>
        val seedSnapshotId = snapshotIds(table.spark, table.name).last
        table.spark.sql(
          s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES $extraColInsert9")
        val currentColumns = table.spark
          .sql(s"SELECT * FROM ${table.name} LIMIT 1")
          .columns
          .toSeq
        val historicalColumns = table.spark
          .sql(
            s"SELECT * FROM ${table.name} " +
              s"VERSION AS OF $seedSnapshotId LIMIT 1")
          .columns
          .toSeq
        val historicalRowCount = table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} " +
              s"VERSION AS OF $seedSnapshotId")
          .collect()(0)
          .getLong(0)

        assert(
          currentColumns.contains("extra_col"),
          s"current read is missing the evolved column: $currentColumns")
        assert(
          !historicalColumns.contains("extra_col") &&
            historicalColumns.size == Core.tableColumns.size,
          s"time travel should use the snapshot schema: $historicalColumns")
        assert(
          historicalRowCount == 3,
          s"pre-DDL snapshot should contain 3 rows, got $historicalRowCount")
      },
      preparation.test("interact.ddl.restoreAfterAddColumn") { table =>
        val seedSnapshotId = snapshotIds(table.spark, table.name).last
        table.spark.sql(
          s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES $extraColInsert9")
        table.spark.sql(
          "CALL openhouse.system.rollback_to_snapshot(" +
            s"'${catalogRelative(table.name)}', $seedSnapshotId)")
        val currentColumns = table.spark
          .sql(s"SELECT * FROM ${table.name} LIMIT 1")
          .columns
          .toSeq
        val currentRowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)
        val nonNullEvolvedValueCount = table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} " +
              "WHERE extra_col IS NOT NULL")
          .collect()(0)
          .getLong(0)

        assert(
          currentColumns.contains("extra_col"),
          s"rollback should retain the evolved schema: $currentColumns")
        assert(
          currentRowCount == 3,
          s"rollback should restore 3 rows, got $currentRowCount")
        assert(
          nonNullEvolvedValueCount == 0,
          "rolled-back rows should read the evolved column as null")

        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES $extraColInsert10")
        assert(
          table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0) == 4,
          "the rolled-back table should accept evolved-schema writes")
      },
      preparation.test("interact.ddl.dropColAfterData") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES $extraColInsert9")
        val exception = Check.intercept[BadRequestException](
          table.spark.sql(
            s"ALTER TABLE ${table.name} DROP COLUMN extra_col"))

        assert(
          exception.getMessage.contains("not found in newSchema"),
          s"drop rejection message changed: ${exception.getMessage.take(200)}")
        assert(
          table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name} WHERE extra_col = 42")
            .collect()(0)
            .getLong(0) == 1,
          "rejected drop should leave the column data readable")

        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES $extraColInsert10")
        assert(
          table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0) == 5,
          "rejected drop should leave the table writable")
      })
  }

  private def interactionRtasCases(format: String): List[Plan.Case] = {
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())
    val replacePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)()
        .sql("enableReplace")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('replace.enabled'='true')")())
    val userPropertyPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', " +
            "'replace.enabled'='true', 'user.key'='v1')")()
        .insert(3)())
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
            "WHERE pattern = 'yyyy-MM-dd-HH')")())

    List(
      replacePreparation.test("interact.rtas.historyPreserved") { table =>
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
      replacePreparation.test("interact.rtas.restoreRejected") { table =>
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
      replacePreparation.test("interact.rtas.setCurrentRecovery") { table =>
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
      replacePreparation.test("interact.rtas.writeAfter") { table =>
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
      replacePreparation.test("interact.rtas.partitionSpecChange") { table =>
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
      basePreparation.test("interact.rtas.dropsColumn") { table =>
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
        "interact.rtas.props.userSurvival") { table =>
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
        "interact.rtas.props.statementWins") { table =>
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
        "interact.rtas.props.createDefaulting") { table =>
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
        "interact.rtas.props.reservedPlane") { table =>
        val tableUuidBefore = tableProps(table.spark, table.name)
          .getOrElse("openhouse.tableUUID", "<absent>")
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"PARTITIONED BY (${Core.datePartition.columnName}) " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        val properties = tableProps(table.spark, table.name)
        val policiesAfter = properties.get("policies")

        assert(
          properties.getOrElse("openhouse.tableUUID", "<absent>") ==
            tableUuidBefore,
          "table UUID should survive RTAS")
        assert(
          policiesAfter.forall(policy =>
            !policy.toLowerCase.contains("retention")),
          s"RTAS should currently remove the retention policy: $policiesAfter")
      },
      replacePreparation.test("interact.rtas.withBranch") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH keepbr")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_keepbr VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        val refs = table.spark
          .sql(s"SELECT name FROM ${table.name}.refs")
          .collect()
          .map(_.getString(0))
          .toSet
        val branchRowCount = table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'keepbr'")
          .collect()(0)
          .getLong(0)

        assert(
          refs.contains("keepbr"),
          s"branch ref did not survive RTAS: $refs")
        assert(
          branchRowCount == 4,
          s"branch should retain 4 rows after RTAS, got $branchRowCount")
      })
  }

  private def interactionBranchCases(format: String): List[Plan.Case] = {
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())
    val twoSnapshotPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)()
        .sql("insertMore")(table =>
          s"INSERT INTO $table VALUES " +
            "(CAST(4 AS BIGINT), 4, 'row-4', 4.5, true, '2024-01-04-03'), " +
            "(CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')")())
    val wapPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)()
        .sql("enableWap")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")())

    List(
      twoSnapshotPreparation.test(
        "interact.branch.ttBeforeBranchPoint") { table =>
        val snapshots = snapshotIds(table.spark, table.name)
        val firstCommitTimestamp = table.spark
          .sql(
            s"SELECT committed_at FROM ${table.name}.snapshots " +
              "ORDER BY committed_at LIMIT 1")
          .collect()(0)
          .getTimestamp(0)
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
      basePreparation.test("interact.branch.mainDdlImmediate") { table =>
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
        "interact.branch.expireProtectsRefs") { table =>
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
        "interact.branch.rollbackWhileWapConf") { table =>
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
        "interact.restore.expireAfterRollback") { table =>
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
        "interact.branch.expireMerge.spuriousReject") { table =>
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
        "interact.branch.expireMerge.stagedWapLoss") { table =>
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

  private def interactionMiscellaneousCases(
      format: String): List[Plan.Case] = {
    val flagPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', " +
            "'write.wap.enabled'='true', 'replace.enabled'='true')")()
        .insert(3)())
    val oneFilePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .sql("seed")(table =>
          s"INSERT INTO $table SELECT /*+ COALESCE(1) */ * FROM " +
            s"(${RowGenerator.valuesClause(Core, 3)}) AS seed")())
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())

    List(
      flagPreparation.test("interact.flags.wapReplaceAtCreate") { table =>
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
      },
      oneFilePreparation.test("interact.mor.alterToMor") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
            "('write.delete.mode'='merge-on-read')")
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
        val deleteFileCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}.all_delete_files")
          .collect()(0)
          .getLong(0)
        val rowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)

        assert(
          deleteFileCount == 1,
          s"ALTER-to-MoR should create one delete file, got $deleteFileCount")
        assert(
          rowCount == 2,
          s"ALTER-to-MoR delete should leave 2 rows, got $rowCount")
      },
      basePreparation.test("interact.maint.compactEvolved") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES $extraColInsert9")
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES $extraColInsert10")
        table.spark.sql(
          "CALL openhouse.system.rewrite_data_files(" +
            s"table => '${catalogRelative(table.name)}')")
        val rowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)
        val evolvedValueCount = table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} " +
              "WHERE extra_col IN (42, 43)")
          .collect()(0)
          .getLong(0)
        val nullValueCount = table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} WHERE extra_col IS NULL")
          .collect()(0)
          .getLong(0)

        assert(
          rowCount == 5,
          s"compaction should preserve 5 rows, got $rowCount")
        assert(
          evolvedValueCount == 2,
          s"compaction should preserve two evolved values, got $evolvedValueCount")
        assert(
          nullValueCount == 3,
          s"pre-evolution rows should remain null, got $nullValueCount")
      })
  }

  val interactionCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      interactionDdlCases(format) ++
        interactionRtasCases(format) ++
        interactionBranchCases(format) ++
        interactionMiscellaneousCases(format)
    }

  // G2 characterization needs the REST lock (no SQL surface) → Ctx-based like controlPlane.
  // Sanity-checks the lock DOES block a normal write, then demonstrates RTAS sails through it.
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
      // G2: the replace branches never reach the isTableLocked check — RTAS replaces a LOCKED table.
      spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2,
        "G2 characterization: RTAS bypassed the lock (if a locked-table rejection landed here, G2 is FIXED — update AUDIT-FINDINGS)")
    } finally {
      Rest.delete(ctx, s"/v1/databases/$db/tables/$tbl/lock")
      spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  val interactionContextCases: List[Plan.Case] =
    List(
      Plan.Case(
        "interact.rtas.onLockedTable @ embedded",
        interactRtasOnLockedTable))

  // ═══ Surface-completion axis: queued follow-ups + untested Iceberg surface ═══════════════════


}

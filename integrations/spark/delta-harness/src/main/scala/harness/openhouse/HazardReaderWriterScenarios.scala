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

trait HazardReaderWriterScenarios extends ScenarioKit {
  import Rows._

  private def cowCreate(t: String, fmt: String): String =
    s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$fmt')"
  private def cowCreate(t: String): String = cowCreate(t, "parquet")
  private def morCreate(t: String, fmt: String): String =
    s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES (${morPropsFmt(fmt)})"

  val readerWriterCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val cowPreparation = TablePreparation(
        format,
        TableTest(Core)
          .sql("create")(table => cowCreate(table, format))()
          .insert(3)())
      val morPreparation = TablePreparation(
        format,
        TableTest(Core)
          .sql("create")(table => morCreate(table, format))()
          .insert(3)())

      List(
        cowPreparation.test("readerWriter.changelog.append") { table =>
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
        },
        morPreparation.test("readerWriter.changelog.append.mor") { table =>
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

          println(s"DIAG changelog.append.mor: $changeTypes")
          assert(
            changeTypes.getOrElse("INSERT", 0L) == 1 &&
              !changeTypes.contains("DELETE"),
            s"MoR append changelog must contain one INSERT and no DELETE: $changeTypes")
        },
        cowPreparation.test("readerWriter.changelog.overwrite") { table =>
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
            changeTypes.values.sum >= 1,
            s"overwrite changelog must be non-empty: $changeTypes")
        },
        morPreparation.test("readerWriter.changelog.overwrite.mor") { table =>
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

          println(s"DIAG changelog.overwrite.mor: $changeTypes")
          assert(
            changeTypes.values.sum >= 1,
            s"MoR overwrite changelog must be non-empty: $changeTypes")
        },
        cowPreparation.test("readerWriter.changelog.delete") { table =>
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
        },
        morPreparation.test("readerWriter.changelog.delete.mor") { table =>
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

          println(s"DIAG changelog.delete.mor: $changeTypes")
          assert(
            changeTypes.getOrElse("DELETE", 0L) == 1 &&
              !changeTypes.contains("INSERT"),
            s"MoR delete changelog must contain one DELETE and no INSERT: $changeTypes")
        },
        cowPreparation.test("readerWriter.changelog.update") { table =>
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
            changeTypes.getOrElse("DELETE", 0L) >= 1 &&
              changeTypes.getOrElse("INSERT", 0L) >= 1,
            s"update changelog must decompose to DELETE and INSERT: $changeTypes")
        },
        morPreparation.test("readerWriter.changelog.update.mor") { table =>
          val seedSnapshotId = snapshotIds(table.spark, table.name).head
          table.spark.sql(
            s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'upd' " +
              s"WHERE ${Core.long0.columnName} = 2")
          val exception = Check.intercept[Exception] {
            val view = table.spark
              .sql(
                "CALL openhouse.system.create_changelog_view(" +
                  s"table => '${catalogRelative(table.name)}', " +
                  s"options => map('start-snapshot-id', '$seedSnapshotId'))")
              .collect()(0)
              .getString(0)
            table.spark.sql(s"SELECT * FROM $view").collect()
          }

          assert(
            Exceptions.causeChain(exception).exists(error =>
              Option(error.getMessage)
                .exists(_.contains("Delete files are currently not supported"))),
            "MoR update changelog should reject position-delete files")
          println(
            "DIAG changelog.update.mor: " +
              "REJECTED (delete files unsupported in changelog scans)")
        },
        cowPreparation.test("readerWriter.changelog.merge") { table =>
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
            changeTypes.values.sum >= 1,
            s"merge changelog must be non-empty: $changeTypes")
        },
        morPreparation.test("readerWriter.changelog.merge.mor") { table =>
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
          val exception = Check.intercept[Exception] {
            val view = table.spark
              .sql(
                "CALL openhouse.system.create_changelog_view(" +
                  s"table => '${catalogRelative(table.name)}', " +
                  s"options => map('start-snapshot-id', '$seedSnapshotId'))")
              .collect()(0)
              .getString(0)
            table.spark.sql(s"SELECT * FROM $view").collect()
          }

          assert(
            Exceptions.causeChain(exception).exists(error =>
              Option(error.getMessage)
                .exists(_.contains("Delete files are currently not supported"))),
            "MoR merge changelog should reject position-delete files")
          println(
            "DIAG changelog.merge.mor: " +
              "REJECTED (delete files unsupported in changelog scans)")
        },
        cowPreparation.test("readerWriter.incremental.append") { table =>
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
        },
        cowPreparation.test("readerWriter.incremental.delete") { table =>
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
            addedRowCount >= 0,
            s"delete incremental scan returned $addedRowCount")
        },
        cowPreparation.test("readerWriter.incremental.overwrite") { table =>
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
            addedRowCount >= 0,
            s"overwrite incremental scan returned $addedRowCount")
        },
        cowPreparation.test("readerWriter.incremental.update") { table =>
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
            addedRowCount >= 0,
            s"update incremental scan returned $addedRowCount")
        },
        cowPreparation.test("readerWriter.stream.append") { table =>
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
        },
        cowPreparation.test("readerWriter.stream.deleteRejected") { table =>
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
        })
    }

  private def localizedHazardCases(format: String): List[Plan.Case] = {
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table => cowCreate(table, format))()
        .insert(3)())
    val taggedReplacePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table => cowCreate(table, format))()
        .insert(3)()
        .sql("enableReplace")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('replace.enabled'='true')")()
        .sql("tagPii")(table =>
          s"ALTER TABLE $table MODIFY COLUMN " +
            s"${Core.string0.columnName} SET TAG = (PII)")())
    val partitionedPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"PARTITIONED BY (${Core.datePartition.columnName}) " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())
    val twoSnapshotPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table => cowCreate(table, format))()
        .insert(3)()
        .sql("insertMore")(table =>
          s"INSERT INTO $table VALUES " +
            "(CAST(4 AS BIGINT), 4, 'row-4', 4.5, true, '2024-01-04-03'), " +
            "(CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')")())
    val wapPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table => cowCreate(table, format))()
        .insert(3)()
        .sql("enableWap")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")())

    List(
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
      },
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
      },
      taggedReplacePreparation.test(
        "hazard.rtas.wipesColumnTags") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} ALTER COLUMN " +
            s"${Core.string0.columnName} COMMENT 'contains-pii'")
        val policiesBefore =
          tableProps(table.spark, table.name).getOrElse("policies", "")
        assert(
          policiesBefore.toLowerCase.contains("pii") ||
            policiesBefore.toLowerCase.contains("columntags"),
          s"PII tag was not stored before RTAS: $policiesBefore")

        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        val policiesAfter =
          tableProps(table.spark, table.name).getOrElse("policies", "")
        val comment = table.spark
          .sql(s"DESCRIBE TABLE ${table.name}")
          .collect()
          .find(_.getString(0) == Core.string0.columnName)
          .map(_.getString(2))
          .getOrElse("")

        assert(
          !policiesAfter.toLowerCase.contains("pii"),
          s"PII column tag survived RTAS: $policiesAfter")
        println(
          s"DIAG rtas.columnComment after replace: '$comment' " +
            "(was 'contains-pii')")
      },
      partitionedPreparation.test(
        "hazard.retentionBranch.defended") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH rbb")
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} <= 2")
        table.spark.sql(
          "CALL openhouse.system.expire_snapshots(" +
            s"table => '${catalogRelative(table.name)}', " +
            "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
            "retain_last => 1)")
        table.spark.sql(
          "CALL openhouse.system.remove_orphan_files(" +
            s"table => '${catalogRelative(table.name)}', " +
            "older_than => TIMESTAMP '2020-01-01 00:00:00')")

        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'rbb'") == "3",
          "branch should remain readable after retention cleanup")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "1",
          "main should reflect the retention-shaped delete")
      },
      twoSnapshotPreparation.test("hazard.rename.consumers") { table =>
        val snapshots = snapshotIds(table.spark, table.name)
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH rnb")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_rnb VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        val renamedTable = s"${table.name}_rn"
        table.spark.sql(
          s"ALTER TABLE ${table.name} RENAME TO $renamedTable")
        try {
          assert(
            countOf(
              table.spark,
              s"SELECT count(*) FROM $renamedTable " +
                "VERSION AS OF 'rnb'") == "6",
            "branch should survive table rename")
          assert(
            countOf(
              table.spark,
              s"SELECT count(*) FROM $renamedTable " +
                s"VERSION AS OF ${snapshots.head}") == "3",
            "time travel should survive table rename")

          table.spark.sql(
            s"INSERT INTO $renamedTable VALUES " +
              "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
          assert(
            countOf(
              table.spark,
              s"SELECT count(*) FROM $renamedTable") == "6",
            "renamed table should remain writable")
        } finally {
          table.spark.sql(
            s"ALTER TABLE $renamedTable RENAME TO ${table.name}")
        }
      },
      wapPreparation.test("hazard.wapToggle.branchesSurvive") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH wtb")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_wtb VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
            "('write.wap.enabled'='false')")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_wtb VALUES " +
            "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")

        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'wtb'") == "5",
          "named branch should survive disabling WAP")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "3",
          "branch writes should leave main unchanged")
      },
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
      })
  }

  val hazardCases: List[Plan.Case] =
    List("parquet", "orc").flatMap(localizedHazardCases)

  // H4 — lock starves maintenance (needs the REST lock → Ctx-based). The same gate G2 shows the
  // replace path SKIPS is hit by every maintenance commit: upkeep is blocked, replacement is not.
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

  val hazardContextCases: List[Plan.Case] =
    List(
      Plan.Case(
        "hazard.lock.starvesMaintenance @ embedded",
        hazardLockStarvesMaintenance))

}

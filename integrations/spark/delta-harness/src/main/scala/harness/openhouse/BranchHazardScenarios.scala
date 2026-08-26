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

// The branch hazard families. Each case creates a named branch and then runs an operation that could
// disturb it: a retention policy, a table rename, or turning write.wap.enabled off and on again. The
// cases run on parquet and orc.
trait BranchHazardScenarios extends BranchScenarioKit { this: HazardReaderWriterScenarios =>
  import Rows._

  def hazardBranchCases(format: String): List[Plan.Case] = {
    val partitionedPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"PARTITIONED BY (${Core.datePartition.columnName}) " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)(),
      description = s"Three seed rows in a $format table partitioned by datepartition.")
    val twoSnapshotPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table => cowCreate(table, format))()
        .insert(3)()
        .sql("insertMore")(table =>
          s"INSERT INTO $table VALUES " +
            "(CAST(4 AS BIGINT), 4, 'row-4', 4.5, true, '2024-01-04-03'), " +
            "(CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')")(),
      description = s"Five seed rows across two snapshots in a copy-on-write $format table.")
    val wapPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table => cowCreate(table, format))()
        .insert(3)()
        .sql("enableWap")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")(),
      description = s"Three seed rows in a $format table with write.wap.enabled set to true.")

    List(
      partitionedPreparation.test(
        "hazard.retentionBranch.defended",
        "After a branch is created, main is trimmed by DELETE, and snapshot expiration plus " +
          "orphan-file removal run, the branch still reads its 3 rows and main reflects the " +
          "trimmed row count.") { table =>
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
      twoSnapshotPreparation.test(
        "hazard.rename.consumers",
        "After ALTER TABLE RENAME, both a pre-existing branch and pre-existing time travel to an " +
          "old snapshot remain readable under the new name, and the renamed table still accepts " +
          "writes.") { table =>
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
      wapPreparation.test(
        "hazard.wapToggle.branchesSurvive",
        "A named branch keeps accumulating its own rows across write.wap.enabled being turned " +
          "off, while main stays at its original 3 rows.") { table =>
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
      })
  }
}

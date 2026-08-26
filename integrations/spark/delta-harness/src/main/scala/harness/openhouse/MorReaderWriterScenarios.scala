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

// The merge-on-read reader and writer families. Each case is the merge-on-read counterpart of a
// copy-on-write changelog case: the same operation runs on a format-version 2 table whose delete,
// update and merge modes are merge-on-read, so the changelog it produces is read from the position
// delete files that mutation wrote. The cases run on parquet and orc.
trait MorReaderWriterScenarios extends MorScenarioKit {
  import Rows._

  private def morCreate(t: String, fmt: String): String =
    s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES (${morPropsFmt(fmt)})"

  private def morPreparation(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table => morCreate(table, format))()
        .insert(3)(),
      description = s"Three seed rows in a merge-on-read $format table.")

  // The changelog view over an append on a merge-on-read table.
  def morReaderWriterChangelogAppendCases(format: String): List[Plan.Case] =
    List(
      morPreparation(format).test(
        "readerWriter.changelog.append.mor",
        "On a merge-on-read table, a changelog view over an appended row reports exactly one " +
          "INSERT and no DELETE.") { table =>
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
      })

  // The changelog view over an INSERT OVERWRITE on a merge-on-read table.
  def morReaderWriterChangelogOverwriteCases(format: String): List[Plan.Case] =
    List(
      morPreparation(format).test(
        "readerWriter.changelog.overwrite.mor",
        "On a merge-on-read table, a changelog view over an INSERT OVERWRITE that drops one row " +
          "reports exactly that row as a DELETE.") { table =>
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
          changeTypes == Map("DELETE" -> 1L),
          s"MoR overwrite changelog must contain the one removed row: $changeTypes")
      })

  // The changelog view over a position-delete DELETE.
  def morReaderWriterChangelogDeleteCases(format: String): List[Plan.Case] =
    List(
      morPreparation(format).test(
        "readerWriter.changelog.delete.mor",
        "On a merge-on-read table, a changelog view over a DELETE reports exactly one DELETE and " +
          "no INSERT.") { table =>
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
      })

  // The changelog view over a merge-on-read UPDATE.
  def morReaderWriterChangelogUpdateCases(format: String): List[Plan.Case] =
    List(
      morPreparation(format).test(
        "readerWriter.changelog.update.mor",
        "On a merge-on-read table, reading a changelog view over an UPDATE is rejected because " +
          "position-delete files are not supported in changelog scans.") { table =>
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
      })

  // The changelog view over a merge-on-read MERGE.
  def morReaderWriterChangelogMergeCases(format: String): List[Plan.Case] =
    List(
      morPreparation(format).test(
        "readerWriter.changelog.merge.mor",
        "On a merge-on-read table, reading a changelog view over a MERGE is rejected because " +
          "position-delete files are not supported in changelog scans.") { table =>
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
      })
}

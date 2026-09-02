package harness

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.StructType

/**
 * Partition transforms: which PARTITIONED BY transforms the catalog accepts at table creation, the partition field
 * each accepted transform produces, and the partition specifications it rejects.
 *
 * Operations: CREATE TABLE PARTITIONED BY each of identity, bucket, truncate, years, months, days and hours followed
 * by a read of the partitions metadata table; CREATE TABLE PARTITIONED BY the rejected void transform, the rejected
 * days transform over a date column, and a column the table does not declare.
 *
 * Preparation axes: for the accepted and rejected transforms, a TypesTable in each of the two columnar formats seeded
 * with three rows whose timestamps fall in three distinct hours, days, months and years; for the rejected partition
 * column, the standard seeded core table in the same two formats.
 *
 * Case families: ten families contributing 20 cases, 14 accepted transforms and 6 rejections.
 */
trait ScenarioPartitionTransform extends ScenarioKit {

  /** Every partition-transform case, one file format at a time. */
  lazy val partitionTransformCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      acceptedTransforms.map {
        case (caseName, transform, partitionField, expectedPartitionCount) =>
          acceptedTransformCase(format, caseName, transform, partitionField, expectedPartitionCount)
      } ++
        rejectedTransforms.map {
          case (caseName, transform, expectedMessage) =>
            rejectedTransformCase(format, caseName, transform, expectedMessage)
        }
    } ++ preparedCoreFormats.map(partitionByNonExistentColumnCase)

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  // A fully valued TypesTable row whose date and timestamp columns both come from `timestamp`, so one row lands in one
  // partition of every time-based transform.
  private def partitionRow(id: Long, str: String, timestamp: String): String =
    s"(CAST($id AS BIGINT), ${id.toInt}, ${id}.5, " +
      s"CAST(${id}.50 AS decimal(10,2)), '$str', CAST('bin-$id' AS binary), " +
      s"DATE '${timestamp.take(10)}', TIMESTAMP '$timestamp', TIMESTAMP_NTZ '$timestamp')"

  /**
   * One accepted partition transform: a table PARTITIONED BY that transform reports a single partition field with the
   * expected name in its partitions metadata table, and the three seeded rows land in the expected number of distinct
   * partitions. The transform, its partition field name, and that partition count are the parameters.
   */
  private def acceptedTransformCase(
      format: String,
      caseName: String,
      transform: String,
      partitionField: String,
      expectedPartitionCount: Int): Plan.Case =
    TablePreparation(
      format,
      TableTest(TypesTable)
        .sql("create")(table =>
          s"CREATE TABLE $table (${TypesTable.columnDefinitions}) " +
            s"USING $dataSource PARTITIONED BY ($transform) " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .sql("insertPartitionRows")(table =>
          s"INSERT INTO $table VALUES " +
            partitionRow(1, "aa-1", "2023-12-31 23:00:00") + ", " +
            partitionRow(2, "bb-2", "2024-01-01 00:00:00") + ", " +
            partitionRow(3, "cc-3", "2024-02-01 01:00:00"))(view =>
          assert(
            view.after.size == view.before.size + 3,
            s"expected three partition test rows, got ${view.after.size}")))
      .test(caseName) { table =>
        val partitionTable = table.spark.table(s"${table.name}.partitions")
        val partitionFields = partitionTable.schema("partition").dataType
          .asInstanceOf[StructType]
          .fieldNames
          .toSeq

        assert(
          partitionFields == Seq(partitionField),
          s"expected partition field $partitionField, got ${partitionFields.mkString(", ")}")
        assert(
          partitionTable.count() == expectedPartitionCount,
          s"expected $expectedPartitionCount partitions for $transform")
      }

  /**
   * One rejected partition transform: CREATE TABLE PARTITIONED BY that transform fails with a RuntimeException
   * carrying the expected message, and the scratch table it would have created is gone. The transform and the expected
   * message are the parameters.
   */
  private def rejectedTransformCase(
      format: String,
      caseName: String,
      transform: String,
      expectedMessage: String): Plan.Case =
    TablePreparation(
      format,
      TableTest(TypesTable)
        .sql("create")(table =>
          s"CREATE TABLE $table (${TypesTable.columnDefinitions}) " +
            s"USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")())
      .test(caseName) { table =>
        val scratchTable = table.name + "_x"

        withCleanupStatement(table.spark.sql(_), s"DROP TABLE IF EXISTS $scratchTable") {
          val exception = Check.intercept[RuntimeException](
            table.spark.sql(
              s"CREATE TABLE $scratchTable " +
                s"(${TypesTable.columnDefinitions}) " +
                s"USING $dataSource PARTITIONED BY ($transform) " +
                s"TBLPROPERTIES ('write.format.default'='$format')"))

          assert(exception.getMessage.contains(expectedMessage))
        }
      }

  /**
   * CREATE TABLE PARTITIONED BY a column the table does not declare is rejected with an AnalysisException naming that
   * column, and the scratch table it would have created is gone.
   */
  private def partitionByNonExistentColumnCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("partition.byNonExistentColumn.rejected") { table =>
      val scratchTable = table.name + "_x"

      withCleanupStatement(table.spark.sql(_), s"DROP TABLE IF EXISTS $scratchTable") {
        val exception = Check.intercept[AnalysisException](
          table.spark.sql(
            s"CREATE TABLE $scratchTable ($columnDefinitions) " +
              s"USING $dataSource PARTITIONED BY (no_such_column) " +
              s"TBLPROPERTIES ('write.format.default'='${preparation.label}')"))

        assert(exception.getMessage.contains("no_such_column"))
      }
    }

  // The accepted transforms: the case name, the PARTITIONED BY clause, the partition field the catalog derives, and
  // the number of distinct partitions the three seeded rows land in.
  private val acceptedTransforms: List[(String, String, String, Int)] =
    List(
      ("partition.identity", "id", "id", 3),
      ("partition.bucket", "bucket(4, id)", "id_bucket", 2),
      ("partition.truncate", "truncate(2, str)", "str_trunc", 3),
      ("partition.years", "years(ts)", "ts_year", 2),
      ("partition.months", "months(ts)", "ts_month", 3),
      ("partition.days", "days(ts)", "ts_day", 3),
      ("partition.hours", "hours(ts)", "ts_hour", 3))

  // The rejected transforms: the case name, the PARTITIONED BY clause, and the message the rejection carries.
  private val rejectedTransforms: List[(String, String, String)] =
    List(
      ("partition.void.rejected", "void(n)", "not supported"),
      ("partition.dateDay.rejected", "days(dt)", "Unsupported column"))

}

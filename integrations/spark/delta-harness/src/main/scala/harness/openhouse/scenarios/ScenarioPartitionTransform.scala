package harness

import org.apache.spark.sql.AnalysisException

import scala.collection.JavaConverters._

/**
 * Partition transforms: which PARTITIONED BY transforms the catalog accepts at table creation, the exact Iceberg
 * partition field each accepted transform installs, the partition values the seeded rows land on, and the partition
 * specifications the catalog rejects while leaving no table behind.
 *
 * Operations: CREATE TABLE PARTITIONED BY each of identity, bucket, truncate, years, months, days and hours followed
 * by an inspection of the persisted partition spec and the partitions metadata table; CREATE TABLE PARTITIONED BY the
 * rejected void transform, the rejected days transform over a date column, and a column the table does not declare.
 *
 * Preparation axes: for the accepted and rejected transforms, a TypesTable in each of the two columnar formats seeded
 * with three rows whose timestamps fall in three distinct hours, days, months and years; for the rejected partition
 * column, the standard seeded core table in the same two formats.
 *
 * Case families: ten families contributing 20 cases, 14 accepted transforms and 6 rejections.
 */
trait ScenarioPartitionTransform extends CatalogDdlSupport {

  /** Every partition-transform case, one file format at a time. */
  lazy val partitionTransformCases: List[TestCase] =
    fileFormats.flatMap { format =>
      acceptedTransforms.map(transform => acceptedTransformCase(format, transform)) ++
        rejectedTransforms.map {
          case (caseName, transform, expectedMessage) =>
            rejectedTransformCase(format, caseName, transform, expectedMessage)
        }
    } ++ preparedCoreFormats.map(partitionByNonExistentColumnCase)

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  // One accepted transform: the case name, the PARTITIONED BY clause, the source column the catalog derives the
  // partition from, the Iceberg transform string it installs, the partition field name it generates, and the
  // (partition value, record count) rows the three seeded rows land on, ordered by partition value.
  private final case class AcceptedTransform(
    caseName: String,
    partitionClause: String,
    sourceColumn: String,
    transformName: String,
    partitionField: String,
    expectedPartitions: List[(String, Long)])

  // A fully valued TypesTable row whose date and timestamp columns both come from `timestamp`, so one row lands in one
  // partition of every time-based transform. The three seeded rows fall in adjacent hours, days, months and years, so
  // the partition values distinguish one time transform from the next.
  private def partitionRow(id: Long, str: String, timestamp: String): String =
    s"(CAST($id AS BIGINT), ${id.toInt}, ${id}.5, " +
      s"CAST(${id}.50 AS decimal(10,2)), '$str', CAST('bin-$id' AS binary), " +
      s"DATE '${timestamp.take(10)}', TIMESTAMP '$timestamp', TIMESTAMP_NTZ '$timestamp')"

  /**
   * One accepted partition transform: a table PARTITIONED BY that transform installs exactly one partition field, whose
   * source column, Iceberg transform and generated field name match the expectation, and the three seeded rows land on
   * the expected partition values with the expected record counts.
   */
  private def acceptedTransformCase(format: String, transform: AcceptedTransform): TestCase =
    TablePreparation(
      format,
      TableTest(TypesTable)
        .sql("create")(table =>
          s"CREATE TABLE $table (${TypesTable.columnDefinitions}) " +
            s"USING $dataSource PARTITIONED BY (${transform.partitionClause}) " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .sql("insertPartitionRows")(table =>
          s"INSERT INTO $table VALUES " +
            partitionRow(1, "aa-1", "2023-12-31 23:00:00") + ", " +
            partitionRow(2, "bb-2", "2024-01-01 00:00:00") + ", " +
            partitionRow(3, "cc-3", "2024-02-01 01:00:00"))(view =>
          assert(
            view.after.size == view.before.size + 3,
            s"expected three partition test rows, got ${view.after.size}")))
      .test(transform.caseName) { table =>
        val icebergTable = icebergTableOf(table.spark, table.name)
        val specFields = icebergTable.spec().fields().asScala.toList

        assert(
          specFields.size == 1,
          s"expected one partition field, got ${specFields.map(_.name())}")
        val field = specFields.head
        val sourceColumn = icebergTable.schema().findColumnName(field.sourceId())

        assert(
          sourceColumn == transform.sourceColumn,
          s"expected partition source column ${transform.sourceColumn}, got $sourceColumn")
        assert(
          field.transform().toString == transform.transformName,
          s"expected transform ${transform.transformName}, got ${field.transform()}")
        assert(
          field.name() == transform.partitionField,
          s"expected partition field ${transform.partitionField}, got ${field.name()}")

        val actualPartitions = table.spark
          .sql(
            s"SELECT CAST(partition.${transform.partitionField} AS STRING) AS v, record_count " +
              s"FROM ${table.name}.partitions ORDER BY partition.${transform.partitionField}")
          .collect()
          .toSeq
          .map(row => (row.getString(0), row.getLong(1)))

        assert(
          actualPartitions == transform.expectedPartitions,
          s"expected partitions ${transform.expectedPartitions}, got $actualPartitions")
      }

  /**
   * One rejected partition transform: CREATE TABLE PARTITIONED BY that transform fails with a RuntimeException
   * carrying the expected message, and the scratch table it would have created never resolves. The scratch name
   * extends the prepared table's generated unique suffix, and cleanup drops only that name. The transform and the
   * expected message are the parameters.
   */
  private def rejectedTransformCase(
      format: String,
      caseName: String,
      transform: String,
      expectedMessage: String): TestCase =
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

          assert(
            exception.getMessage.contains(expectedMessage),
            s"unexpected message: ${exception.getMessage.take(160)}")
          Check.intercept[AnalysisException](
            table.spark.sql(s"SELECT 1 FROM $scratchTable LIMIT 1"))
        }
      }

  /**
   * CREATE TABLE PARTITIONED BY a column the table does not declare is rejected with an AnalysisException naming that
   * column, and the scratch table it would have created never resolves.
   */
  private def partitionByNonExistentColumnCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("partition.byNonExistentColumn.rejected") { table =>
      val scratchTable = table.name + "_x"

      withCleanupStatement(table.spark.sql(_), s"DROP TABLE IF EXISTS $scratchTable") {
        val exception = Check.intercept[AnalysisException](
          table.spark.sql(
            s"CREATE TABLE $scratchTable ($columnDefinitions) " +
              s"USING $dataSource PARTITIONED BY (no_such_column) " +
              s"TBLPROPERTIES ('write.format.default'='${preparation.label}')"))

        assert(
          exception.getMessage.contains("no_such_column"),
          s"unexpected message: ${exception.getMessage.take(160)}")
        Check.intercept[AnalysisException](
          table.spark.sql(s"SELECT 1 FROM $scratchTable LIMIT 1"))
      }
    }

  // The accepted transforms, keyed to the Iceberg partition field and partition values they install. The bucket and
  // time transforms store integer ordinals, so the pinned values are the exact ordinals the catalog computed.
  private val acceptedTransforms: List[AcceptedTransform] =
    List(
      AcceptedTransform(
        "partition.identity", "id", "id", "identity", "id",
        List(("1", 1L), ("2", 1L), ("3", 1L))),
      AcceptedTransform(
        "partition.bucket", "bucket(16, id)", "id", "bucket[16]", "id_bucket",
        List(("3", 1L), ("4", 2L))),
      AcceptedTransform(
        "partition.truncate", "truncate(2, str)", "str", "truncate[2]", "str_trunc",
        List(("aa", 1L), ("bb", 1L), ("cc", 1L))),
      AcceptedTransform(
        "partition.years", "years(ts)", "ts", "year", "ts_year",
        List(("53", 1L), ("54", 2L))),
      AcceptedTransform(
        "partition.months", "months(ts)", "ts", "month", "ts_month",
        List(("647", 1L), ("648", 1L), ("649", 1L))),
      AcceptedTransform(
        "partition.days", "days(ts)", "ts", "day", "ts_day",
        List(("2023-12-31", 1L), ("2024-01-01", 1L), ("2024-02-01", 1L))),
      AcceptedTransform(
        "partition.hours", "hours(ts)", "ts", "hour", "ts_hour",
        List(("473351", 1L), ("473352", 1L), ("474097", 1L))))

  // The rejected transforms: the case name, the PARTITIONED BY clause, and the message the rejection carries.
  private val rejectedTransforms: List[(String, String, String)] =
    List(
      ("partition.void.rejected", "void(n)", "not supported"),
      ("partition.dateDay.rejected", "days(dt)", "Unsupported column"))

}

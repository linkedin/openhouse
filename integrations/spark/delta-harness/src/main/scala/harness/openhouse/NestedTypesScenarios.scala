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

trait NestedTypesScenarios extends ScenarioKit {
  import Rows._

  // Nested and complex types (NestedTable).

  /** One unpartitioned nested-column table per file format. */
  val nestedLayouts: List[Layout] =
    List("parquet", "orc", "avro").map(format => Layout(s"nested-unpartitioned/$format", table =>
      s"CREATE TABLE $table (${NestedTable.columnDefinitions}) USING $dataSource TBLPROPERTIES ('write.format.default'='$format')"))

  /** Creates the nested-column table under `layout`, then seeds `numberOfRows` rows. */
  def createAndSeedNested(layout: Layout, numberOfRows: Int): TableTest[NestedTable.type] =
    TableTest(NestedTable).sql("create")(layout.create)().insert(numberOfRows)()

  /**
   * Selecting the top-level id alongside struct, array, map and nested-struct fields reads back exactly the seeded
   * values for all 3 rows.
   */
  private def nestedRoundtripCase(preparation: TablePreparation[NestedTable.type]): Plan.Case =
    preparation.test("nested.roundtrip") { table =>
      val actual = table.spark
        .sql(
          s"SELECT id, s.x, s.y, arr, m['k'], nested.inner.z " +
            s"FROM ${table.name} ORDER BY id")
        .collect()
        .toSeq
        .map(row =>
          (
            row.getLong(0),
            row.getInt(1),
            row.getString(2),
            row.getSeq[Int](3),
            row.getInt(4),
            row.getInt(5)))
      val expected = (1 to 3).map { value =>
        (
          value.toLong,
          value,
          s"row-$value",
          Seq(value, value + 1),
          value,
          value)
      }

      assert(actual == expected)
    }

  /** Selecting only a nested struct field (s.x) returns just that field's values for all 3 rows, in id order. */
  private def nestedProjectFieldCase(preparation: TablePreparation[NestedTable.type]): Plan.Case =
    preparation.test("nested.projectField") { table =>
      val actual = table.spark
        .sql(s"SELECT s.x FROM ${table.name} ORDER BY id")
        .collect()
        .toSeq
        .map(_.getInt(0))

      assert(actual == Seq(1, 2, 3))
    }

  /** Filtering WHERE s.x = 2 on a nested struct field returns only the matching row's id. */
  private def nestedFilterNestedFieldCase(
      preparation: TablePreparation[NestedTable.type]): Plan.Case =
    preparation.test("nested.filterNestedField") { table =>
      val actual = table.spark
        .sql(s"SELECT id FROM ${table.name} WHERE s.x = 2 ORDER BY id")
        .collect()
        .toSeq
        .map(_.getLong(0))

      assert(actual == Seq(2L))
    }

  /** UPDATE SET s.x = 99 WHERE id = 2 changes only that row's nested field and leaves every other row unchanged. */
  private def nestedUpdateStructFieldCase(
      preparation: TablePreparation[NestedTable.type]): Plan.Case =
    preparation.test("nested.updateStructField") { table =>
      table.spark.sql(
        s"UPDATE ${table.name} SET s.x = 99 WHERE id = 2")

      assert(
        table.spark
          .sql(s"SELECT s.x FROM ${table.name} WHERE id = 2")
          .collect()(0)
          .getInt(0) == 99)
      assert(
        table.spark
          .sql(s"SELECT s.x FROM ${table.name} WHERE id = 1")
          .collect()(0)
          .getInt(0) == 1)
    }

  /**
   * MERGE WHEN NOT MATCHED THEN INSERT with a fully nested source row adds a 4th row whose nested struct field reads
   * back as inserted.
   */
  private def nestedMergeInsertCase(preparation: TablePreparation[NestedTable.type]): Plan.Case =
    preparation.test("nested.mergeInsert") { table =>
      table.spark.sql(
        s"""MERGE INTO ${table.name} target USING (
                    SELECT * FROM VALUES
                      (
                        CAST(4 AS BIGINT),
                        named_struct('x', 4, 'y', 'row-4'),
                        array(4, 5),
                        map('k', 4),
                        named_struct('inner', named_struct('z', 4)))
                    AS source(id, s, arr, m, nested)
                  ) source ON target.id = source.id
                  WHEN NOT MATCHED THEN INSERT *""")

      val ids = table.spark
        .sql(s"SELECT id FROM ${table.name} ORDER BY id")
        .collect()
        .toSeq
        .map(_.getLong(0))

      assert(ids == Seq(1L, 2L, 3L, 4L))
      assert(
        table.spark
          .sql(s"SELECT s.x FROM ${table.name} WHERE id = 4")
          .collect()(0)
          .getInt(0) == 4)
    }

  /** DELETE WHERE s.x = 2 filtering on a nested struct field removes only the matching row, leaving ids 1 and 3. */
  private def nestedDeleteByNestedFieldCase(
      preparation: TablePreparation[NestedTable.type]): Plan.Case =
    preparation
      .test("nested.deleteByNestedField") { table =>
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE s.x = 2")

        val ids = table.spark
          .sql(s"SELECT id FROM ${table.name} ORDER BY id")
          .collect()
          .toSeq
          .map(_.getLong(0))

        assert(ids == Seq(1L, 3L))
      }
      .copy(knownBugReason = Some(
        "DELETE on a nested struct field crashes in the Spark and Iceberg row-level " +
          "rewrite."))

  /**
   * Inserting a row with NULL struct, empty array and empty map reads back a null struct and an empty array for that
   * row.
   */
  private def nestedNullValuesCase(preparation: TablePreparation[NestedTable.type]): Plan.Case =
    preparation.test("nested.nullValues") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES (" +
          "CAST(4 AS BIGINT), " +
          "CAST(NULL AS struct<x:int,y:string>), " +
          "CAST(array() AS array<int>), " +
          "CAST(map() AS map<string,int>), " +
          "CAST(NULL AS struct<inner:struct<z:int>>))")

      val insertedRow = table.spark
        .sql(s"SELECT id, s, arr FROM ${table.name} WHERE id = 4")
        .collect()(0)

      assert(insertedRow.isNullAt(1))
      assert(insertedRow.getSeq[Int](2).isEmpty)
    }

  /**
   * The nested-type cases. Each preparation holds three seed rows with struct, array, map and doubly-nested struct
   * fields in one unpartitioned nested layout.
   */
  val nestedCases: List[Plan.Case] =
    nestedLayouts
      .map(layout =>
        TablePreparation(
          layout.label,
          createAndSeedNested(layout, 3)))
      .flatMap { preparation =>
        List(
          nestedRoundtripCase(preparation),
          nestedProjectFieldCase(preparation),
          nestedFilterNestedFieldCase(preparation),
          nestedUpdateStructFieldCase(preparation),
          nestedMergeInsertCase(preparation),
          nestedDeleteByNestedFieldCase(preparation),
          nestedNullValuesCase(preparation))
      }

  // Type-edge coverage (TypesTable).

  /** One unpartitioned scalar-type table per file format. */
  val typesLayouts: List[Layout] =
    List("parquet", "orc", "avro").map(format => Layout(s"types-unpartitioned/$format", table =>
      s"CREATE TABLE $table (${TypesTable.columnDefinitions}) USING $dataSource TBLPROPERTIES ('write.format.default'='$format')"))

  /** Creates the scalar-type table under `layout`, then seeds `numberOfRows` rows. */
  def createAndSeedTypes(layout: Layout, numberOfRows: Int): TableTest[TypesTable.type] =
    TableTest(TypesTable).sql("create")(layout.create)().insert(numberOfRows)()

  // A full valued row for TypesTable with the given id; individual tests override specific columns.
  private def typesRow(id: Long, n: String, x: String, dec: String, str: String): String =
    s"(CAST($id AS BIGINT), $n, $x, $dec, $str, CAST('b' AS binary), DATE '2024-01-01', " +
      s"TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:00:00')"

  private def partitionRow(id: Long, str: String, timestamp: String): String =
    s"(CAST($id AS BIGINT), ${id.toInt}, ${id}.5, " +
      s"CAST(${id}.50 AS decimal(10,2)), '$str', CAST('bin-$id' AS binary), " +
      s"DATE '${timestamp.take(10)}', TIMESTAMP '$timestamp', TIMESTAMP_NTZ '$timestamp')"

  /**
   * Selecting id, n, x, dec and str for the first seeded row reads back the exact long, int, double, decimal and string
   * values that were seeded.
   */
  private def typesRoundtripCase(preparation: TablePreparation[TypesTable.type]): Plan.Case =
    preparation.test("types.roundtrip") { table =>
      val row = table.spark
        .sql(
          s"SELECT id, n, x, dec, str FROM ${table.name} WHERE id = 1")
        .collect()(0)

      assert(
        row.getLong(0) == 1L &&
          row.getInt(1) == 1 &&
          row.getDouble(2) == 1.5)
      assert(
        row.getDecimal(3).compareTo(
          new java.math.BigDecimal("1.50")) == 0)
      assert(row.getString(4) == "row-1")
    }

  /**
   * Inserting a row with every non-key column NULL reads back as null for the int, double, string, timestamp and
   * timestamp_ntz columns.
   */
  private def typesNullsCase(preparation: TablePreparation[TypesTable.type]): Plan.Case =
    preparation.test("types.nulls") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES (" +
          "CAST(10 AS BIGINT), NULL, NULL, NULL, NULL, " +
          "NULL, NULL, NULL, NULL)")

      val row = table.spark
        .sql(
          s"SELECT n, x, str, ts, tsntz FROM ${table.name} WHERE id = 10")
        .collect()(0)

      assert((0 to 4).forall(row.isNullAt))
    }

  /** Inserting rows with double('NaN') and double('Infinity') reads back as NaN and positive infinity respectively. */
  private def typesSpecialFloatsCase(preparation: TablePreparation[TypesTable.type]): Plan.Case =
    preparation.test("types.specialFloats") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          s"${typesRow(11, "0", "double('NaN')", "CAST(0 AS decimal(10,2))", "'x'")}, " +
          s"${typesRow(12, "0", "double('Infinity')", "CAST(0 AS decimal(10,2))", "'y'")}")

      assert(
        table.spark
          .sql(s"SELECT x FROM ${table.name} WHERE id = 11")
          .collect()(0)
          .getDouble(0)
          .isNaN)
      assert(
        table.spark
          .sql(s"SELECT x FROM ${table.name} WHERE id = 12")
          .collect()(0)
          .getDouble(0)
          .isInfinite)
    }

  /**
   * Inserting a row at Long.MaxValue, Int.MaxValue and a max-precision decimal reads those boundary values back
   * unchanged.
   */
  private def typesBoundariesCase(preparation: TablePreparation[TypesTable.type]): Plan.Case =
    preparation.test("types.boundaries") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          typesRow(
            Long.MaxValue,
            Int.MaxValue.toString,
            "0.0",
            "CAST(99999999.99 AS decimal(10,2))",
            "'max'"))

      val row = table.spark
        .sql(
          s"SELECT id, n, dec FROM ${table.name} WHERE str = 'max'")
        .collect()(0)

      assert(
        row.getLong(0) == Long.MaxValue &&
          row.getInt(1) == Int.MaxValue)
      assert(
        row.getDecimal(2).compareTo(
          new java.math.BigDecimal("99999999.99")) == 0)
    }

  /** Inserting rows with a unicode string and an empty string reads each back unchanged. */
  private def typesUnicodeAndEmptyCase(preparation: TablePreparation[TypesTable.type]): Plan.Case =
    preparation.test("types.unicodeAndEmpty") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          s"${typesRow(13, "0", "0.0", "CAST(0 AS decimal(10,2))", "'\u65e5\u672c\u8a9e \uD83C\uDF89'")}, " +
          s"${typesRow(14, "0", "0.0", "CAST(0 AS decimal(10,2))", "''")}")

      assert(
        table.spark
          .sql(s"SELECT str FROM ${table.name} WHERE id = 13")
          .collect()(0)
          .getString(0) == "\u65e5\u672c\u8a9e \uD83C\uDF89")
      assert(
        table.spark
          .sql(s"SELECT str FROM ${table.name} WHERE id = 14")
          .collect()(0)
          .getString(0) == "")
    }

  /**
   * The type-edge cases. Each preparation holds three seed rows covering the int, double, decimal, string, binary,
   * date, timestamp and timestamp_ntz columns in one unpartitioned types layout.
   */
  val typesCases: List[Plan.Case] =
    typesLayouts
      .map(layout =>
        TablePreparation(
          layout.label,
          createAndSeedTypes(layout, 3)))
      .flatMap { preparation =>
        List(
          typesRoundtripCase(preparation),
          typesNullsCase(preparation),
          typesSpecialFloatsCase(preparation),
          typesBoundariesCase(preparation),
          typesUnicodeAndEmptyCase(preparation))
      }

  // Partition transforms and evolution.

  /**
   * One supported partition transform: a table PARTITIONED BY that transform reports a single partition field with the
   * expected name in its partitions metadata table, and the seeded rows land in the expected number of distinct
   * partitions. The transform, its partition field name, and that partition count are the parameters.
   */
  private def supportedPartitionTransformCase(
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
        val partitionTable =
          table.spark.table(s"${table.name}.partitions")
        val partitionFields = partitionTable.schema("partition").dataType
          .asInstanceOf[org.apache.spark.sql.types.StructType]
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
   * One rejected partition transform: CREATE TABLE PARTITIONED BY that transform fails with a RuntimeException carrying
   * the expected message, and no scratch table is left behind. The transform and the expected message are the
   * parameters.
   */
  private def rejectedPartitionTransformCase(
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
        val exception = Check.intercept[RuntimeException](
          table.spark.sql(
            s"CREATE TABLE $scratchTable " +
              s"(${TypesTable.columnDefinitions}) " +
              s"USING $dataSource PARTITIONED BY ($transform) " +
              s"TBLPROPERTIES ('write.format.default'='$format')"))

        table.spark.sql(s"DROP TABLE IF EXISTS $scratchTable")
        assert(exception.getMessage.contains(expectedMessage))
      }

  /** The supported and the rejected partition transforms, in parquet and in orc. */
  val partitionTransformCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val supported = List(
        ("partition.identity", "id", "id", 3),
        ("partition.bucket", "bucket(4, id)", "id_bucket", 2),
        ("partition.truncate", "truncate(2, str)", "str_trunc", 3),
        ("partition.years", "years(ts)", "ts_year", 2),
        ("partition.months", "months(ts)", "ts_month", 3),
        ("partition.days", "days(ts)", "ts_day", 3),
        ("partition.hours", "hours(ts)", "ts_hour", 3))
        .map {
          case (caseName, transform, partitionField, expectedPartitionCount) =>
            supportedPartitionTransformCase(format, caseName, transform, partitionField, expectedPartitionCount)
        }
      val rejected = List(
        ("partition.void.rejected", "void(n)", "not supported"),
        (
          "partition.dateDay.rejected",
          "days(dt)",
          "Unsupported column"))
        .map {
          case (caseName, transform, expectedMessage) =>
            rejectedPartitionTransformCase(format, caseName, transform, expectedMessage)
        }

      supported ++ rejected
    }

  /**
   * On three seed rows in an unpartitioned table in the given file format, ALTER TABLE ADD PARTITION FIELD is rejected
   * with an exception stating that evolution of table partitioning is unsupported, which leaves recreating the table as
   * the way to change partitioning.
   */
  private def partitionEvolutionAddRejectedCase(format: String): Plan.Case =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())
      .test("partition.evolutionAdd.rejected") { table =>
        val exception = Check.intercept[Exception](
          table.spark.sql(
            s"ALTER TABLE ${table.name} ADD PARTITION FIELD ${Core.date0.columnName}"))

        assert(
          exception.getMessage.contains("Evolution of table partitioning"))
      }

  /**
   * On three seed rows in a table partitioned by the date column in the given file format, ALTER TABLE DROP PARTITION
   * FIELD is rejected with an exception stating that evolution of table partitioning is unsupported.
   */
  private def partitionEvolutionDropRejectedCase(format: String): Plan.Case =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"PARTITIONED BY (${Core.date0.columnName}) " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())
      .test("partition.evolutionDrop.rejected") { table =>
        val exception = Check.intercept[Exception](
          table.spark.sql(
            s"ALTER TABLE ${table.name} DROP PARTITION FIELD ${Core.date0.columnName}"))

        assert(
          exception.getMessage.contains("Evolution of table partitioning"))
      }

  /** The rejected partition-evolution statements, in parquet and in orc. */
  val partitionEvolutionCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      List(
        partitionEvolutionAddRejectedCase(format),
        partitionEvolutionDropRejectedCase(format))
    }

}

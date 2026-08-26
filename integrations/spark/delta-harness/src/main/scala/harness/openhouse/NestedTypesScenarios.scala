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
  val nestedLayouts: List[Layout] =
    List("parquet", "orc", "avro").map(format => Layout(s"nested-unpartitioned/$format", table =>
      s"CREATE TABLE $table (${NestedTable.columnDefinitions}) USING $dataSource TBLPROPERTIES ('write.format.default'='$format')"))

  def createAndSeedNested(layout: Layout, numberOfRows: Int): TableTest[NestedTable.type] =
    TableTest(NestedTable).sql("create")(layout.create)().insert(numberOfRows)()

  val nestedCases: List[Plan.Case] =
    nestedLayouts
      .map(layout =>
        TablePreparation(
          layout.label,
          createAndSeedNested(layout, 3),
          description = s"Three seed rows with nested struct, array, map and doubly-nested " +
            s"struct fields in an unpartitioned ${layout.label.split('/').last} table."))
      .flatMap { preparation =>
        List(
          preparation.test(
            "nested.roundtrip",
            "Selecting the top-level id alongside struct, array, map and nested-struct fields " +
              "reads back exactly the seeded values for all 3 rows.") { table =>
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
          },
          preparation.test(
            "nested.projectField",
            "Selecting only a nested struct field (s.x) returns just that field's values for " +
              "all 3 rows, in id order.") { table =>
            val actual = table.spark
              .sql(s"SELECT s.x FROM ${table.name} ORDER BY id")
              .collect()
              .toSeq
              .map(_.getInt(0))

            assert(actual == Seq(1, 2, 3))
          },
          preparation.test(
            "nested.filterNestedField",
            "Filtering WHERE s.x = 2 on a nested struct field returns only the matching row's " +
              "id.") { table =>
            val actual = table.spark
              .sql(s"SELECT id FROM ${table.name} WHERE s.x = 2 ORDER BY id")
              .collect()
              .toSeq
              .map(_.getLong(0))

            assert(actual == Seq(2L))
          },
          preparation.test(
            "nested.updateStructField",
            "UPDATE SET s.x = 99 WHERE id = 2 changes only that row's nested field, leaving " +
              "other rows' nested fields untouched.") { table =>
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
          },
          preparation.test(
            "nested.mergeInsert",
            "MERGE WHEN NOT MATCHED THEN INSERT with a fully nested source row adds a 4th row " +
              "whose nested struct field reads back as inserted.") { table =>
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
          },
          preparation
            .test(
              "nested.deleteByNestedField",
              "DELETE WHERE s.x = 2 filtering on a nested struct field removes only the matching " +
                "row, leaving ids 1 and 3.") { table =>
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
                "rewrite.")),
          preparation.test(
            "nested.nullValues",
            "Inserting a row with NULL struct, empty array and empty map reads back a null " +
              "struct and an empty array for that row.") { table =>
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
          })
      }

  // Type-edge coverage (TypesTable).
  val typesLayouts: List[Layout] =
    List("parquet", "orc", "avro").map(format => Layout(s"types-unpartitioned/$format", table =>
      s"CREATE TABLE $table (${TypesTable.columnDefinitions}) USING $dataSource TBLPROPERTIES ('write.format.default'='$format')"))

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

  val typesCases: List[Plan.Case] =
    typesLayouts
      .map(layout =>
        TablePreparation(
          layout.label,
          createAndSeedTypes(layout, 3),
          description = "Three seed rows covering int, double, decimal, string, binary, date, " +
            s"timestamp and timestamp_ntz columns in an unpartitioned ${layout.label.split('/').last} table."))
      .flatMap { preparation =>
        List(
          preparation.test(
            "types.roundtrip",
            "Selecting id, n, x, dec and str for the first seeded row reads back the exact " +
              "long, int, double, decimal and string values that were seeded.") { table =>
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
          },
          preparation.test(
            "types.nulls",
            "Inserting a row with every non-key column NULL reads back as null for the int, " +
              "double, string, timestamp and timestamp_ntz columns.") { table =>
            table.spark.sql(
              s"INSERT INTO ${table.name} VALUES (" +
                "CAST(10 AS BIGINT), NULL, NULL, NULL, NULL, " +
                "NULL, NULL, NULL, NULL)")

            val row = table.spark
              .sql(
                s"SELECT n, x, str, ts, tsntz FROM ${table.name} WHERE id = 10")
              .collect()(0)

            assert((0 to 4).forall(row.isNullAt))
          },
          preparation.test(
            "types.specialFloats",
            "Inserting rows with double('NaN') and double('Infinity') reads back as NaN and " +
              "positive infinity respectively.") { table =>
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
          },
          preparation.test(
            "types.boundaries",
            "Inserting a row at Long.MaxValue, Int.MaxValue and a max-precision decimal reads " +
              "those boundary values back unchanged.") { table =>
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
          },
          preparation.test(
            "types.unicodeAndEmpty",
            "Inserting rows with a unicode string and an empty string reads each back " +
              "unchanged.") { table =>
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
          })
      }

  // Partition transforms and evolution.
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
                    s"expected three partition test rows, got ${view.after.size}")),
              description = s"Three rows in a $format table partitioned by $transform.")
              .test(
                caseName,
                s"With PARTITIONED BY ($transform), the partitions metadata table reports a " +
                  s"single partition field named $partitionField and $expectedPartitionCount " +
                  "distinct partitions for the seeded rows.") { table =>
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
        }
      val rejected = List(
        ("partition.void.rejected", "void(n)", "not supported"),
        (
          "partition.dateDay.rejected",
          "days(dt)",
          "Unsupported column"))
        .map {
          case (caseName, transform, expectedMessage) =>
            TablePreparation(
              format,
              TableTest(TypesTable)
                .sql("create")(table =>
                  s"CREATE TABLE $table (${TypesTable.columnDefinitions}) " +
                    s"USING $dataSource " +
                    s"TBLPROPERTIES ('write.format.default'='$format')")(),
              description = s"An unpartitioned, unseeded $format table.")
              .test(
                caseName,
                s"CREATE TABLE PARTITIONED BY ($transform) is rejected with a RuntimeException " +
                  s"whose message contains '$expectedMessage', and no scratch table is left " +
                  "behind.") { table =>
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
        }

      supported ++ rejected
    }

  // Partition evolution is not supported: ALTER TABLE ADD or DROP PARTITION FIELD is rejected with
  // a 400 response telling the caller to recreate the table. These cases capture that rejection.
  val partitionEvolutionCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      List(
        TablePreparation(
          format,
          TableTest(Core)
            .sql("create")(table =>
              s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
                s"TBLPROPERTIES ('write.format.default'='$format')")()
            .insert(3)(),
          description = s"Three seed rows in an unpartitioned $format table.")
          .test(
            "partition.evolutionAdd.rejected",
            "ALTER TABLE ADD PARTITION FIELD is rejected with an exception stating partition " +
              "evolution is not supported.") { table =>
            val exception = Check.intercept[Exception](
              table.spark.sql(
                s"ALTER TABLE ${table.name} ADD PARTITION FIELD datepartition"))

            assert(
              exception.getMessage.contains("Evolution of table partitioning"))
          },
        TablePreparation(
          format,
          TableTest(Core)
            .sql("create")(table =>
              s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
                "PARTITIONED BY (datepartition) " +
                s"TBLPROPERTIES ('write.format.default'='$format')")()
            .insert(3)(),
          description = s"Three seed rows in a $format table partitioned by datepartition.")
          .test(
            "partition.evolutionDrop.rejected",
            "ALTER TABLE DROP PARTITION FIELD is rejected with an exception stating partition " +
              "evolution is not supported.") { table =>
            val exception = Check.intercept[Exception](
              table.spark.sql(
                s"ALTER TABLE ${table.name} DROP PARTITION FIELD datepartition"))

            assert(
              exception.getMessage.contains("Evolution of table partitioning"))
          })
    }


}

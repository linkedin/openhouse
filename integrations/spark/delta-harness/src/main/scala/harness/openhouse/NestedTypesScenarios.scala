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

  // ── nested / complex types (NestedTable) ───────────────────────────────────────────────
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
          createAndSeedNested(layout, 3)))
      .flatMap { preparation =>
        List(
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
          },
          preparation.test("nested.projectField") { table =>
            val actual = table.spark
              .sql(s"SELECT s.x FROM ${table.name} ORDER BY id")
              .collect()
              .toSeq
              .map(_.getInt(0))

            assert(actual == Seq(1, 2, 3))
          },
          preparation.test("nested.filterNestedField") { table =>
            val actual = table.spark
              .sql(s"SELECT id FROM ${table.name} WHERE s.x = 2 ORDER BY id")
              .collect()
              .toSeq
              .map(_.getLong(0))

            assert(actual == Seq(2L))
          },
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
          },
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
          },
          preparation.test("nested.deleteByNestedField") { table =>
            table.spark.sql(
              s"DELETE FROM ${table.name} WHERE s.x = 2")

            val ids = table.spark
              .sql(s"SELECT id FROM ${table.name} ORDER BY id")
              .collect()
              .toSeq
              .map(_.getLong(0))

            assert(ids == Seq(1L, 3L))
          },
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
          })
      }

  // ── type-edge coverage (TypesTable) ─────────────────────────────────────────────────────
  val typesLayouts: List[Layout] =
    List("parquet", "orc", "avro").map(format => Layout(s"types-unpartitioned/$format", table =>
      s"CREATE TABLE $table (${TypesTable.columnDefinitions}) USING $dataSource TBLPROPERTIES ('write.format.default'='$format')"))

  def createAndSeedTypes(layout: Layout, numberOfRows: Int): TableTest[TypesTable.type] =
    TableTest(TypesTable).sql("create")(layout.create)().insert(numberOfRows)()

  // A full valued row for TypesTable with the given id; individual tests override specific columns.
  private def typesRow(id: Long, n: String, x: String, dec: String, str: String): String =
    s"(CAST($id AS BIGINT), $n, $x, $dec, $str, CAST('b' AS binary), DATE '2024-01-01', " +
      s"TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:00:00')"

  val typesCases: List[Plan.Case] =
    typesLayouts
      .map(layout =>
        TablePreparation(
          layout.label,
          createAndSeedTypes(layout, 3)))
      .flatMap { preparation =>
        List(
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
          },
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
          },
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
          },
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
          },
          preparation.test("types.unicodeAndEmpty") { table =>
            table.spark.sql(
              s"INSERT INTO ${table.name} VALUES " +
                s"${typesRow(13, "0", "0.0", "CAST(0 AS decimal(10,2))", "'日本語 🎉'")}, " +
                s"${typesRow(14, "0", "0.0", "CAST(0 AS decimal(10,2))", "''")}")

            assert(
              table.spark
                .sql(s"SELECT str FROM ${table.name} WHERE id = 13")
                .collect()(0)
                .getString(0) == "日本語 🎉")
            assert(
              table.spark
                .sql(s"SELECT str FROM ${table.name} WHERE id = 14")
                .collect()(0)
                .getString(0) == "")
          })
      }

  // ── partition transforms + evolution ────────────────────────────────────────────────────
  // Each transform test is self-contained: create partitioned by the transform, seed, and verify
  // the rows roundtrip and a partition spec is registered.
  val partitionTransformCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      val supported = List(
        "partition.identity" -> "id",
        "partition.bucket" -> "bucket(4, id)",
        "partition.truncate" -> "truncate(2, str)",
        "partition.years" -> "years(ts)",
        "partition.months" -> "months(ts)",
        "partition.days" -> "days(ts)",
        "partition.hours" -> "hours(ts)")
        .map {
          case (caseName, transform) =>
            TablePreparation(
              format,
              TableTest(TypesTable)
                .sql("create")(table =>
                  s"CREATE TABLE $table (${TypesTable.columnDefinitions}) " +
                    s"USING $dataSource PARTITIONED BY ($transform) " +
                    s"TBLPROPERTIES ('write.format.default'='$format')")()
                .insert(3)())
              .test(caseName) { table =>
                assert(table.rows.size == 3)
                assert(
                  table.spark
                    .sql(s"SELECT * FROM ${table.name}.partitions")
                    .collect()
                    .nonEmpty)
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
        }

      supported ++ rejected
    }

  // OpenHouse contract: partition evolution is NOT supported — ALTER … ADD/DROP PARTITION FIELD is
  // rejected with a 400 telling you to recreate the table. Captured as negative tests.
  val partitionEvolutionCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      List(
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
            .insert(3)())
          .test("partition.evolutionDrop.rejected") { table =>
            val exception = Check.intercept[Exception](
              table.spark.sql(
                s"ALTER TABLE ${table.name} DROP PARTITION FIELD datepartition"))

            assert(
              exception.getMessage.contains("Evolution of table partitioning"))
          })
    }


}

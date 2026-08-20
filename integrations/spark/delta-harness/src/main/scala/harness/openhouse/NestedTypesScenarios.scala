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

  // Read every nested column back and check the seeded values roundtrip.
  val nestedRoundtrip: TableTest[NestedTable.type] =
    TableTest(NestedTable).check("nested.roundtrip") { view =>
      val got = view.spark.sql(s"SELECT id, s.x, s.y, arr, m['k'], nested.inner.z FROM ${view.table} ORDER BY id").collect().toSeq
      val actual = got.map(r => (r.getLong(0), r.getInt(1), r.getString(2), r.getSeq[Int](3), r.getInt(4), r.getInt(5)))
      assert(actual == (1 to 3).map(i => (i.toLong, i, s"row-$i", Seq(i, i + 1), i, i)))
    }

  val nestedProjectField: TableTest[NestedTable.type] =
    TableTest(NestedTable).check("nested.projectField") { view =>
      val xs = view.spark.sql(s"SELECT s.x FROM ${view.table} ORDER BY id").collect().map(_.getInt(0)).toSeq
      assert(xs == Seq(1, 2, 3))
    }

  val nestedFilterField: TableTest[NestedTable.type] =
    TableTest(NestedTable).check("nested.filterNestedField") { view =>
      val ids = view.spark.sql(s"SELECT id FROM ${view.table} WHERE s.x = 2 ORDER BY id").collect().map(_.getLong(0)).toSeq
      assert(ids == Seq(2L))
    }

  // Update a nested struct field.
  val nestedUpdateStructField: TableTest[NestedTable.type] =
    TableTest(NestedTable).sql("nested.updateStructField")(table => s"UPDATE $table SET s.x = 99 WHERE id = 2") { view =>
      assert(view.spark.sql(s"SELECT s.x FROM ${view.table} WHERE id = 2").collect()(0).getInt(0) == 99)
      assert(view.spark.sql(s"SELECT s.x FROM ${view.table} WHERE id = 1").collect()(0).getInt(0) == 1)
    }

  val nestedMergeInsert: TableTest[NestedTable.type] =
    TableTest(NestedTable).sql("nested.mergeInsert")(table =>
      s"""MERGE INTO $table tgt USING (
            SELECT * FROM VALUES
              (CAST(4 AS BIGINT), named_struct('x', 4, 'y', 'row-4'), array(4, 5), map('k', 4), named_struct('inner', named_struct('z', 4)))
            AS v(id, s, arr, m, nested)
          ) src ON tgt.id = src.id
          WHEN NOT MATCHED THEN INSERT *""") { view =>
      val ids = view.spark.sql(s"SELECT id FROM ${view.table} ORDER BY id").collect().map(_.getLong(0)).toSeq
      assert(ids == Seq(1L, 2L, 3L, 4L))
      assert(view.spark.sql(s"SELECT s.x FROM ${view.table} WHERE id = 4").collect()(0).getInt(0) == 4)
    }

  val nestedDeleteByField: TableTest[NestedTable.type] =
    TableTest(NestedTable).sql("nested.deleteByNestedField")(table => s"DELETE FROM $table WHERE s.x = 2") { view =>
      val ids = view.spark.sql(s"SELECT id FROM ${view.table} ORDER BY id").collect().map(_.getLong(0)).toSeq
      assert(ids == Seq(1L, 3L))
    }

  // Insert a row with a null struct and empty array/map.
  val nestedNullValues: TableTest[NestedTable.type] =
    TableTest(NestedTable).sql("nested.nullValues")(table =>
      s"INSERT INTO $table VALUES (CAST(4 AS BIGINT), CAST(NULL AS struct<x:int,y:string>), " +
        s"CAST(array() AS array<int>), CAST(map() AS map<string,int>), CAST(NULL AS struct<inner:struct<z:int>>))") { view =>
      val row4 = view.spark.sql(s"SELECT id, s, arr FROM ${view.table} WHERE id = 4").collect()(0)
      assert(row4.isNullAt(1))                 // s is null
      assert(row4.getSeq[Int](2).isEmpty)      // arr is empty
    }

  val nestedOperations: List[(String, TableTest[NestedTable.type])] = List(
    "nested.roundtrip"          -> nestedRoundtrip,
    "nested.projectField"       -> nestedProjectField,
    "nested.filterNestedField"  -> nestedFilterField,
    "nested.updateStructField"  -> nestedUpdateStructField,
    "nested.mergeInsert"        -> nestedMergeInsert,
    "nested.deleteByNestedField" -> nestedDeleteByField,
    "nested.nullValues"         -> nestedNullValues
  )

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

  val typesRoundtrip: TableTest[TypesTable.type] =
    TableTest(TypesTable).check("types.roundtrip") { view =>
      val r = view.spark.sql(s"SELECT id, n, x, dec, str FROM ${view.table} WHERE id = 1").collect()(0)
      assert(r.getLong(0) == 1L && r.getInt(1) == 1 && r.getDouble(2) == 1.5)
      assert(r.getDecimal(3).compareTo(new java.math.BigDecimal("1.50")) == 0)
      assert(r.getString(4) == "row-1")
    }

  val typesNulls: TableTest[TypesTable.type] =
    TableTest(TypesTable).sql("types.nulls")(table =>
      s"INSERT INTO $table VALUES (CAST(10 AS BIGINT), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)") { view =>
      val r = view.spark.sql(s"SELECT n, x, str, ts, tsntz FROM ${view.table} WHERE id = 10").collect()(0)
      assert((0 to 4).forall(r.isNullAt))
    }

  val typesSpecialFloats: TableTest[TypesTable.type] =
    TableTest(TypesTable).sql("types.specialFloats")(table =>
      s"INSERT INTO $table VALUES ${typesRow(11, "0", "double('NaN')", "CAST(0 AS decimal(10,2))", "'x'")}, " +
        s"${typesRow(12, "0", "double('Infinity')", "CAST(0 AS decimal(10,2))", "'y'")}") { view =>
      assert(view.spark.sql(s"SELECT x FROM ${view.table} WHERE id = 11").collect()(0).getDouble(0).isNaN)
      assert(view.spark.sql(s"SELECT x FROM ${view.table} WHERE id = 12").collect()(0).getDouble(0).isInfinite)
    }

  val typesBoundaries: TableTest[TypesTable.type] =
    TableTest(TypesTable).sql("types.boundaries")(table =>
      s"INSERT INTO $table VALUES " +
        s"${typesRow(9223372036854775807L, "2147483647", "0.0", "CAST(99999999.99 AS decimal(10,2))", "'max'")}") { view =>
      val r = view.spark.sql(s"SELECT id, n, dec FROM ${view.table} WHERE str = 'max'").collect()(0)
      assert(r.getLong(0) == Long.MaxValue && r.getInt(1) == Int.MaxValue)
      assert(r.getDecimal(2).compareTo(new java.math.BigDecimal("99999999.99")) == 0)
    }

  val typesUnicodeAndEmpty: TableTest[TypesTable.type] =
    TableTest(TypesTable).sql("types.unicodeAndEmpty")(table =>
      s"INSERT INTO $table VALUES ${typesRow(13, "0", "0.0", "CAST(0 AS decimal(10,2))", "'日本語 🎉'")}, " +
        s"${typesRow(14, "0", "0.0", "CAST(0 AS decimal(10,2))", "''")}") { view =>
      assert(view.spark.sql(s"SELECT str FROM ${view.table} WHERE id = 13").collect()(0).getString(0) == "日本語 🎉")
      assert(view.spark.sql(s"SELECT str FROM ${view.table} WHERE id = 14").collect()(0).getString(0) == "")
    }

  val typesOperations: List[(String, TableTest[TypesTable.type])] = List(
    "types.roundtrip"       -> typesRoundtrip,
    "types.nulls"           -> typesNulls,
    "types.specialFloats"   -> typesSpecialFloats,
    "types.boundaries"      -> typesBoundaries,
    "types.unicodeAndEmpty" -> typesUnicodeAndEmpty
  )

  // ── partition transforms + evolution ────────────────────────────────────────────────────
  // Each transform test is self-contained: create partitioned by the transform, seed, and verify
  // the rows roundtrip and a partition spec is registered.
  def partitionTransform(transform: String): TableTest[TypesTable.type] =
    TableTest(TypesTable)
      .sql("create")(table =>
        s"CREATE TABLE $table (${TypesTable.columnDefinitions}) USING $dataSource PARTITIONED BY ($transform) " +
          s"TBLPROPERTIES ('write.format.default'='$seedFmt')")()
      .insert(3)()
      .check("verify") { view =>
        assert(view.after.size == 3)
        assert(view.spark.sql(s"SELECT * FROM ${view.table}.partitions").collect().nonEmpty)
      }

  // A CREATE with an unsupported partition transform is rejected. Run it on a scratch name so the
  // pipeline's managed (valid) table still exists for snapshotting.
  private def partitionTransformRejected(label: String, transform: String, expectMessage: String): TableTest[TypesTable.type] =
    TableTest(TypesTable)
      .sql("create")(table => s"CREATE TABLE $table (${TypesTable.columnDefinitions}) USING $dataSource TBLPROPERTIES ('write.format.default'='$seedFmt')")()
      .step(label) { (spark, table) =>
        val scratch = table + "_x"
        val error = Check.intercept[RuntimeException](spark.sql(
          s"CREATE TABLE $scratch (${TypesTable.columnDefinitions}) USING $dataSource PARTITIONED BY ($transform) TBLPROPERTIES ('write.format.default'='$seedFmt')"))
        spark.sql(s"DROP TABLE IF EXISTS $scratch")
        assert(error.getMessage.contains(expectMessage))
      }()

  val partitionTransforms: List[(String, TableTest[TypesTable.type])] = List(
    "partition.identity"        -> partitionTransform("id"),
    "partition.bucket"          -> partitionTransform("bucket(4, id)"),
    "partition.truncate"        -> partitionTransform("truncate(2, str)"),
    "partition.years"           -> partitionTransform("years(ts)"),
    "partition.months"          -> partitionTransform("months(ts)"),
    "partition.days"            -> partitionTransform("days(ts)"),
    "partition.hours"           -> partitionTransform("hours(ts)"),
    // OpenHouse contract: these transforms are rejected (negative tests).
    "partition.void.rejected"   -> partitionTransformRejected("partition.void.rejected", "void(n)", "not supported"),
    "partition.dateDay.rejected" -> partitionTransformRejected("partition.dateDay.rejected", "days(dt)", "Unsupported column")
  )

  // OpenHouse contract: partition evolution is NOT supported — ALTER … ADD/DROP PARTITION FIELD is
  // rejected with a 400 telling you to recreate the table. Captured as negative tests.
  val partitionEvolutionAddRejected: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(table => s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$seedFmt')")()
      .insert(3)()
      .step("partition.evolutionAdd.rejected") { (spark, table) =>
        val error = Check.intercept[Exception](spark.sql(s"ALTER TABLE $table ADD PARTITION FIELD datepartition"))
        assert(error.getMessage.contains("Evolution of table partitioning"))
      }()

  val partitionEvolutionDropRejected: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(table => s"CREATE TABLE $table ($columnDefinitions) USING $dataSource PARTITIONED BY (datepartition) TBLPROPERTIES ('write.format.default'='$seedFmt')")()
      .insert(3)()
      .step("partition.evolutionDrop.rejected") { (spark, table) =>
        val error = Check.intercept[Exception](spark.sql(s"ALTER TABLE $table DROP PARTITION FIELD datepartition"))
        assert(error.getMessage.contains("Evolution of table partitioning"))
      }()

  val partitionEvolution: List[(String, TableTest[CoreTable.type])] = List(
    "partition.evolutionAdd.rejected"  -> partitionEvolutionAddRejected,
    "partition.evolutionDrop.rejected" -> partitionEvolutionDropRejected
  )


}

package harness

import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.sql.Date
import java.time.{Instant, LocalDateTime}

/**
 * Scalar data types: representative round-trip, null, numeric-boundary, special-floating-value, and string behavior
 * for the typed scalar table.
 *
 * Operations: a round trip of the seeded long, int, double, decimal and string values; an INSERT of an all-null row;
 * an INSERT of the special double values NaN and Infinity; an INSERT at the long, int and decimal boundaries; and an
 * INSERT of a unicode string and an empty string.
 *
 * Preparation axes: one unpartitioned TypesTable layout per file format, each seeded with three rows covering every
 * scalar column.
 *
 * Case families: five families over two layouts, contributing 10 cases.
 */
trait ScenarioDataType extends ScenarioKit {

  /** Every scalar-type case, one layout at a time. */
  lazy val dataTypeCases: List[TestCase] =
    preparedTypesTables.flatMap(preparation =>
      List(
        roundtripCase(preparation),
        nullsCase(preparation),
        specialFloatsCase(preparation),
        boundariesCase(preparation),
        unicodeAndEmptyCase(preparation)))

  /** One unpartitioned scalar-type table per file format. */
  lazy val typesLayouts: List[Layout] =
    fileFormats.map(format =>
      Layout(
        s"types-unpartitioned/$format",
        table =>
          s"CREATE TABLE $table (${TypesTable.columnDefinitions}) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')"))

  /** One preparation per scalar-type layout: the table is created, then seeded with three fully valued rows. */
  lazy val preparedTypesTables: List[TablePreparation[TypesTable.type]] =
    typesLayouts.map(layout =>
      TablePreparation(
        layout.label,
        TableTest(TypesTable).sql("create")(layout.create)().insert(standardSeedRowCount)()))

  // A fully valued TypesTable row with the given ID; each case supplies the columns it is about.
  private def typesRow(id: Long, n: String, x: String, dec: String, str: String): String =
    s"(CAST($id AS BIGINT), $n, $x, $dec, $str, CAST('b' AS binary), DATE '2024-01-01', " +
      s"TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:00:00')"

  /**
   * Selecting the first seeded row reads back every scalar value exactly.
   */
  private def roundtripCase(preparation: TablePreparation[TypesTable.type]): TestCase =
    preparation.test("types.roundtrip") { table =>
      val row = table.spark
        .sql(
          s"SELECT id, n, x, dec, str, bin, dt, ts, tsntz FROM ${table.name} WHERE id = 1")
        .collect()(0)

      assert(
        row.getLong(0) == 1L &&
          row.getInt(1) == 1 &&
          row.getDouble(2) == 1.5,
        s"unexpected numeric values: ${row.toSeq}")
      assert(
        row.getDecimal(3).compareTo(new BigDecimal("1.50")) == 0,
        s"unexpected decimal value: ${row.getDecimal(3)}")
      assert(row.getString(4) == "row-1", s"unexpected string value: ${row.getString(4)}")
      assert(
        java.util.Arrays.equals(
          row.getAs[Array[Byte]](5),
          "bin-1".getBytes(StandardCharsets.UTF_8)),
        s"unexpected binary value: ${row.getAs[Array[Byte]](5).mkString("[", ",", "]")}")
      assert(
        row.getDate(6) == Date.valueOf("2024-01-01"),
        s"unexpected date value: ${row.getDate(6)}")
      assert(
        row.getTimestamp(7).toInstant == Instant.parse("2024-01-01T00:00:00Z"),
        s"unexpected timestamp value: ${row.getTimestamp(7)}")
      assert(
        row.getAs[LocalDateTime](8) == LocalDateTime.of(2024, 1, 1, 0, 0),
        s"unexpected timestamp_ntz value: ${row.getAs[LocalDateTime](8)}")
    }

  /**
   * Inserting a row with every non-key column NULL reads every non-key column back as null.
   */
  private def nullsCase(preparation: TablePreparation[TypesTable.type]): TestCase =
    preparation.test("types.nulls") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES (" +
          "CAST(10 AS BIGINT), NULL, NULL, NULL, NULL, " +
          "NULL, NULL, NULL, NULL)")

      val row = table.spark
        .sql(
          s"SELECT n, x, dec, str, bin, dt, ts, tsntz FROM ${table.name} WHERE id = 10")
        .collect()(0)

      assert((0 until 8).forall(row.isNullAt))
    }

  /** Inserting rows with double('NaN') and double('Infinity') reads back as NaN and positive infinity respectively. */
  private def specialFloatsCase(preparation: TablePreparation[TypesTable.type]): TestCase =
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
          .getDouble(0) == Double.PositiveInfinity)
    }

  /**
   * Inserting a row at Long.MaxValue, Int.MaxValue and a max-precision decimal reads those boundary values back
   * unchanged.
   */
  private def boundariesCase(preparation: TablePreparation[TypesTable.type]): TestCase =
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
        row.getDecimal(2).compareTo(new BigDecimal("99999999.99")) == 0)
    }

  /** Inserting rows with a unicode string and an empty string reads each back unchanged. */
  private def unicodeAndEmptyCase(preparation: TablePreparation[TypesTable.type]): TestCase =
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
}

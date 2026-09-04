package harness

import java.math.BigDecimal

/**
 * Scalar data types: how the long, int, double, decimal, string, binary, date, timestamp and timestamp_ntz columns
 * round trip, and how the catalog stores the edge values of each one.
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
   * Selecting id, n, x, dec and str for the first seeded row reads back the exact long, int, double, decimal and
   * string values that were seeded.
   */
  private def roundtripCase(preparation: TablePreparation[TypesTable.type]): TestCase =
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
        row.getDecimal(3).compareTo(new BigDecimal("1.50")) == 0)
      assert(row.getString(4) == "row-1")
    }

  /**
   * Inserting a row with every non-key column NULL reads back as null for the int, double, string, timestamp and
   * timestamp_ntz columns.
   */
  private def nullsCase(preparation: TablePreparation[TypesTable.type]): TestCase =
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
          .getDouble(0)
          .isInfinite)
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

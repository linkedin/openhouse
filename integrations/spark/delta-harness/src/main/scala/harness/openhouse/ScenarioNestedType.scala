package harness

/**
 * Nested and complex types: struct, array, map and struct-in-struct columns, the reads and writes that address their
 * fields, and the schema changes the catalog allows inside a struct.
 *
 * Operations: a full round trip of every nested column, projection of a struct field, a filter on a struct field, an
 * UPDATE of a struct field, a MERGE that inserts a fully nested row, a DELETE filtered on a struct field, an INSERT of
 * null and empty nested values, ADD COLUMN of a new struct field, and the rejected DROP COLUMN of an existing struct
 * field.
 *
 * Preparation axes: one unpartitioned NestedTable layout per file format, each seeded with three rows carrying struct,
 * array, map and doubly-nested struct values; plus the standard seeded core table in Parquet and ORC for the two
 * struct-evolution families, which build and drop their own side table.
 *
 * Case families: nine families contributing 25 cases, 21 on the nested layouts and 4 on the standard formats.
 */
trait ScenarioNestedType extends ScenarioKit {

  /** Every nested-type case: the reads and writes on the nested layouts, then the struct-evolution cases. */
  lazy val nestedTypeCases: List[Plan.Case] =
    preparedNestedTables.flatMap(preparation =>
      List(
        roundtripCase(preparation),
        projectFieldCase(preparation),
        filterNestedFieldCase(preparation),
        updateStructFieldCase(preparation),
        mergeInsertCase(preparation),
        deleteByNestedFieldCase(preparation),
        nullValuesCase(preparation))) ++
      preparedCoreFormats.flatMap(preparation =>
        List(
          addStructFieldCase(preparation),
          dropStructFieldRejectedCase(preparation)))

  /** One unpartitioned nested-column table per file format. */
  lazy val nestedLayouts: List[Layout] =
    fileFormats.map(format =>
      Layout(
        s"nested-unpartitioned/$format",
        table =>
          s"CREATE TABLE $table (${NestedTable.columnDefinitions}) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')"))

  /** One preparation per nested layout: the table is created, then seeded with three nested rows. */
  lazy val preparedNestedTables: List[TablePreparation[NestedTable.type]] =
    nestedLayouts.map(layout =>
      TablePreparation(
        layout.label,
        TableTest(NestedTable).sql("create")(layout.create)().insert(standardSeedRowCount)()))

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * Selecting the top-level id alongside struct, array, map and nested-struct fields reads back exactly the seeded
   * values for all 3 rows.
   */
  private def roundtripCase(preparation: TablePreparation[NestedTable.type]): Plan.Case =
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
      val expected = (1 to standardSeedRowCount).map { value =>
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
  private def projectFieldCase(preparation: TablePreparation[NestedTable.type]): Plan.Case =
    preparation.test("nested.projectField") { table =>
      val actual = table.spark
        .sql(s"SELECT s.x FROM ${table.name} ORDER BY id")
        .collect()
        .toSeq
        .map(_.getInt(0))

      assert(actual == Seq(1, 2, 3))
    }

  /** Filtering WHERE s.x = 2 on a nested struct field returns only the matching row's id. */
  private def filterNestedFieldCase(preparation: TablePreparation[NestedTable.type]): Plan.Case =
    preparation.test("nested.filterNestedField") { table =>
      val actual = table.spark
        .sql(s"SELECT id FROM ${table.name} WHERE s.x = 2 ORDER BY id")
        .collect()
        .toSeq
        .map(_.getLong(0))

      assert(actual == Seq(2L))
    }

  /** UPDATE SET s.x = 99 WHERE id = 2 changes only that row's nested field and leaves every other row unchanged. */
  private def updateStructFieldCase(preparation: TablePreparation[NestedTable.type]): Plan.Case =
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
  private def mergeInsertCase(preparation: TablePreparation[NestedTable.type]): Plan.Case =
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
  private def deleteByNestedFieldCase(preparation: TablePreparation[NestedTable.type]): Plan.Case =
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
  private def nullValuesCase(preparation: TablePreparation[NestedTable.type]): Plan.Case =
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
   * On a side table, ADD COLUMN of a new nested struct field null-fills it for the existing row and accepts a new row
   * that sets the field.
   */
  private def addStructFieldCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("nested.addStructField") { table =>
      val sideTable = s"${table.name}_nst"
      withOwnedTable(table.spark.sql(_), sideTable)(
        table.spark.sql(
          s"CREATE TABLE $sideTable " +
            s"(id BIGINT, s STRUCT<x: INT, y: STRING>) USING $dataSource")) {
        table.spark.sql(
          s"INSERT INTO $sideTable VALUES " +
            "(CAST(1 AS BIGINT), named_struct('x', 1, 'y', 'a'))")
        table.spark.sql(
          s"ALTER TABLE $sideTable ADD COLUMN s.w INT")
        assert(
          countOf(table.spark, s"SELECT count(*) FROM $sideTable WHERE s.w IS NULL") == "1",
          "new nested field should null-fill the existing row")

        table.spark.sql(
          s"INSERT INTO $sideTable VALUES " +
            "(CAST(2 AS BIGINT), " +
            "named_struct('x', 2, 'y', 'b', 'w', 9))")
        assert(
          countOf(table.spark, s"SELECT count(*) FROM $sideTable WHERE s.w = 9") == "1",
          "new nested field should be writable")
      }
    }

  /**
   * On a side table, ALTER TABLE DROP COLUMN of a nested struct field is rejected with an exception, and the field
   * remains readable afterward.
   */
  private def dropStructFieldRejectedCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("nested.dropStructField.rejected") { table =>
      val sideTable = s"${table.name}_nsd"
      withOwnedTable(table.spark.sql(_), sideTable)(
        table.spark.sql(
          s"CREATE TABLE $sideTable " +
            s"(id BIGINT, s STRUCT<x: INT, y: STRING>) USING $dataSource")) {
        table.spark.sql(
          s"INSERT INTO $sideTable VALUES " +
            "(CAST(1 AS BIGINT), named_struct('x', 1, 'y', 'a'))")
        Check.intercept[Exception](
          table.spark.sql(
            s"ALTER TABLE $sideTable DROP COLUMN s.x"))

        assert(
          table.spark
            .sql(s"SELECT s.x FROM $sideTable")
            .collect()(0)
            .getInt(0) == 1,
          "rejected nested drop should leave the field readable")
      }
    }

}

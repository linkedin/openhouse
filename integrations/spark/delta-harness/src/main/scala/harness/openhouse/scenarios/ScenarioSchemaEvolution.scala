package harness

import org.apache.spark.sql.AnalysisException
import org.apache.iceberg.exceptions.BadRequestException

/**
 * Schema evolution on the core table: the shape a CREATE TABLE statement produces, and the column additions, type
 * changes, reorderings and nullability changes the catalog accepts or rejects afterwards.
 *
 * Operations: read the created schema, ADD COLUMN in its single, multiple, commented and positioned forms, ALTER
 * COLUMN TYPE to widen an int and to widen a decimal, ALTER COLUMN FIRST to reorder, ALTER COLUMN DROP NOT NULL,
 * RENAME COLUMN, and the rejected forms DROP COLUMN, DROP COLUMN over written data, ALTER COLUMN TYPE to a narrower
 * type and ALTER COLUMN SET NOT NULL.
 *
 * Preparation axes: the four unseeded core layouts for the created-schema family; the four seeded core layouts for
 * the evolution families; the standard seeded table in Parquet and ORC for the rejection families and for the
 * families that build their own side table.
 *
 * Case families: 14 families contributing 42 cases, 4 created-schema, 24 evolution, and 14 rejection or side-table
 * cases.
 */
trait ScenarioSchemaEvolution extends SchemaTableFixtures {

  /** Every schema-evolution case: the created schema, then the accepted changes, then the boundaries. */
  lazy val schemaEvolutionCases: List[TestCase] =
    createdSchemaCases ++ schemaChangeCases ++ schemaBoundaryCases

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * The created table's schema is exactly CoreTable's columns, in declaration order and with their declared types, and
   * the table holds no rows.
   */
  private def createdSchemaCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.create") { table =>
      val actual = table.spark
        .table(table.name)
        .schema
        .fields
        .toList
        .map(field => field.name -> field.dataType.simpleString)
      val expected = Core.tableColumns.toList.map(column => (column.columnName, column.sqlType))

      assert(actual == expected, s"schema is $actual")
      assert(table.rows.isEmpty, "a table that was never seeded holds no rows")
    }

  /** ADD COLUMN adds the column to the schema, the existing rows read null for it, and the row count is unchanged. */
  private def addColumnSingleCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.addColumn.single") { table =>
      table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN added_int int")

      val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)
      val nullCount = table.spark
        .sql(s"SELECT count(*) FROM ${table.name} WHERE added_int IS NULL")
        .collect()(0)
        .getLong(0)

      assert(columnNames.contains("added_int"), s"added_int missing: $columnNames")
      assert(
        nullCount == table.preparedRows.size,
        s"existing rows should read null for added_int: $nullCount != ${table.preparedRows.size}")
      assert(table.rows.size == table.preparedRows.size, "ADD COLUMN changed the row count")
    }

  /** ADD COLUMNS with two columns in one statement adds both to the schema and leaves the row count unchanged. */
  private def addColumnMultipleCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.addColumn.multiple") { table =>
      table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMNS (added_a int, added_b string)")

      val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)

      assert(
        columnNames.contains("added_a") && columnNames.contains("added_b"),
        s"added columns missing: $columnNames")
      assert(table.rows.size == table.preparedRows.size, "ADD COLUMNS changed the row count")
    }

  /** ADD COLUMN ... COMMENT stores the comment on the added column and the reader sees it. */
  private def addColumnCommentCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.addColumn.comment") { table =>
      table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN added_c int COMMENT 'a note'")

      val addedColumn = table.spark
        .table(table.name)
        .schema
        .fields
        .find(_.name == "added_c")
        .getOrElse(throw new AssertionError("added_c missing"))

      assert(
        addedColumn.getComment().contains("a note"),
        s"comment not stored: ${addedColumn.getComment()}")
    }

  /** ADD COLUMN ... AFTER foo_col_long places the added column directly after that column in the schema. */
  private def addColumnPositionCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.addColumn.position") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} ADD COLUMN added_after int AFTER ${Core.long0.columnName}")

      val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)

      assert(
        columnNames.indexOf("added_after") == columnNames.indexOf(Core.long0.columnName) + 1,
        s"added_after not after long0: $columnNames")
    }

  /**
   * ALTER COLUMN foo_col_int TYPE bigint widens the column in the schema and the already-written values read back
   * unchanged.
   */
  private def alterColumnTypeWidenCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.alterColumn.typeWiden") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} ALTER COLUMN ${Core.int0.columnName} TYPE bigint")

      val liveColumns = table.spark.table(table.name).schema.fields.toSeq
        .map(field => field.name -> field.dataType.simpleString)
        .toMap
      val values = table.spark
        .sql(
          s"SELECT ${Core.int0.columnName} FROM ${table.name} ORDER BY ${Core.long0.columnName}")
        .collect()
        .toSeq
        .map(_.getLong(0))

      assert(
        liveColumns.get(Core.int0.columnName).contains("bigint"),
        s"int0 not widened: ${liveColumns.get(Core.int0.columnName)}")
      assert(values == Seq(1L, 2L, 3L), s"values not preserved after widening: $values")
    }

  /**
   * RENAME COLUMN renames the column in the schema: the new name is present, the old name is gone, and the row count
   * is unchanged.
   */
  private def renameColumnCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation
      .test("schema.renameColumn") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN to_rename int")
        table.spark.sql(s"ALTER TABLE ${table.name} RENAME COLUMN to_rename TO renamed_col")

        val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)

        assert(
          columnNames.contains("renamed_col") && !columnNames.contains("to_rename"),
          s"RENAME COLUMN silently no-oped: $columnNames")
        assert(table.rows.size == table.preparedRows.size, "RENAME COLUMN changed the row count")
      }
      .copy(knownBugReason = Some(
        "RENAME COLUMN is a silent no-op because server-side schema casing normalization " +
          "restores the old name."))

  /** ALTER TABLE DROP COLUMN is rejected with a BadRequestException naming the column that would be dropped. */
  private def dropColumnRejectedCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.dropColumn.rejected") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} DROP COLUMN ${Core.int0.columnName}"))

      assert(
        exception.getMessage.contains("not found in newSchema"),
        s"unexpected message: ${exception.getMessage.take(160)}")
      assert(
        exception.getMessage.contains(Core.int0.columnName),
        s"message should name the dropped column: ${exception.getMessage.take(160)}")
    }

  /**
   * DROP COLUMN on a column that holds data is rejected, the column's data remains readable, and the table remains
   * writable.
   */
  private def dropColumnWithDataRejectedCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.dropColumn.withData.rejected") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColumnRowNine")
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} DROP COLUMN extra_col"))

      assert(
        exception.getMessage.contains("not found in newSchema"),
        s"drop rejection message changed: ${exception.getMessage.take(200)}")
      assert(
        queryCount(
          table.spark,
          s"SELECT count(*) FROM ${table.name} WHERE extra_col = 42") == "1",
        "rejected drop should leave the column data readable")

      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColumnRowTen")
      assert(
        queryCount(table.spark, s"SELECT count(*) FROM ${table.name}") == "5",
        "rejected drop should leave the table writable")
    }

  /**
   * ALTER TABLE ALTER COLUMN to a narrower type (bigint to int) is rejected with an AnalysisException about the
   * unsupported column change.
   */
  private def alterColumnNarrowTypeRejectedCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.alterColumn.narrowType.rejected") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} ALTER COLUMN ${Core.long0.columnName} TYPE int"))

      assert(
        exception.getMessage.contains("NOT_SUPPORTED_CHANGE_COLUMN"),
        s"unexpected message: ${exception.getMessage.take(160)}")
    }

  /**
   * ALTER TABLE ALTER COLUMN SET NOT NULL on a nullable column is rejected with an AnalysisException about the
   * nullable-to-non-nullable change.
   */
  private def alterColumnSetNotNullRejectedCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.alterColumn.setNotNull.rejected") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} ALTER COLUMN ${Core.string0.columnName} SET NOT NULL"))

      assert(
        exception.getMessage.contains("Cannot change nullable column to non-nullable"),
        s"unexpected message: ${exception.getMessage.take(160)}")
    }

  /** On a side table, dropping NOT NULL from a column allows a subsequent insert of a null value for that column. */
  private def alterColumnDropNotNullCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.alterColumn.dropNotNull") { table =>
      val sideTable = s"${table.name}_nn"
      withOwnedTable(table.spark.sql(_), sideTable)(
        table.spark.sql(
          s"CREATE TABLE $sideTable " +
            s"(id BIGINT, req INT NOT NULL) USING $dataSource")) {
        table.spark.sql(
          s"ALTER TABLE $sideTable ALTER COLUMN req DROP NOT NULL")
        table.spark.sql(
          s"INSERT INTO $sideTable VALUES (CAST(1 AS BIGINT), NULL)")
        assert(
          queryCount(table.spark, s"SELECT count(*) FROM $sideTable WHERE req IS NULL") == "1",
          "relaxing NOT NULL should allow a null write")
      }
    }

  /**
   * On a side table, widening a decimal column's precision preserves the original row and accepts a new row whose
   * value only fits the wider precision.
   */
  private def alterColumnDecimalWidenCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.alterColumn.decimalWiden") { table =>
      val sideTable = s"${table.name}_dec"
      withOwnedTable(table.spark.sql(_), sideTable)(
        table.spark.sql(
          s"CREATE TABLE $sideTable " +
            s"(id BIGINT, dec DECIMAL(10,2)) USING $dataSource")) {
        table.spark.sql(
          s"INSERT INTO $sideTable VALUES " +
            "(CAST(1 AS BIGINT), CAST(12345678.99 AS DECIMAL(10,2)))")
        table.spark.sql(
          s"ALTER TABLE $sideTable ALTER COLUMN dec TYPE DECIMAL(12,2)")
        table.spark.sql(
          s"INSERT INTO $sideTable VALUES " +
            "(CAST(2 AS BIGINT), CAST(1234567890.99 AS DECIMAL(12,2)))")
        assert(
          queryCount(table.spark, s"SELECT count(*) FROM $sideTable") == "2",
          "decimal widening should preserve old and new values")
      }
    }

  /** ALTER TABLE ALTER COLUMN ... FIRST moves that column to the front of the schema while preserving all 3 rows. */
  private def alterColumnReorderFirstCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("schema.alterColumn.reorderFirst") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} " +
          s"ALTER COLUMN ${Core.string0.columnName} FIRST")
      val columns = table.spark
        .sql(s"SELECT * FROM ${table.name} LIMIT 1")
        .columns
        .toSeq

      assert(
        columns.head == Core.string0.columnName,
        s"FIRST should move the column to the front: $columns")
      assert(
        queryCount(table.spark, s"SELECT count(*) FROM ${table.name}") == "3",
        "column reorder should preserve the rows")
    }

  /** The created-schema case on every unseeded core layout. */
  private val createdSchemaCases: List[TestCase] =
    preparedEmptyCoreTables.map(createdSchemaCase)

  /** The accepted schema changes on every seeded core layout. */
  private val schemaChangeCases: List[TestCase] =
    preparedCoreTables.flatMap { preparation =>
      List(
        addColumnSingleCase(preparation),
        addColumnMultipleCase(preparation),
        addColumnCommentCase(preparation),
        addColumnPositionCase(preparation),
        alterColumnTypeWidenCase(preparation),
        renameColumnCase(preparation))
    }

  /** The rejected schema changes and the side-table schema changes, in each columnar format. */
  private val schemaBoundaryCases: List[TestCase] =
    preparedCoreFormats.flatMap { preparation =>
      List(
        dropColumnRejectedCase(preparation),
        dropColumnWithDataRejectedCase(preparation),
        alterColumnNarrowTypeRejectedCase(preparation),
        alterColumnSetNotNullRejectedCase(preparation),
        alterColumnDropNotNullCase(preparation),
        alterColumnDecimalWidenCase(preparation),
        alterColumnReorderFirstCase(preparation))
    }

}

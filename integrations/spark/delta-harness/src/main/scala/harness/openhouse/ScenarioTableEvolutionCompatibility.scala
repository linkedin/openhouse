package harness

import org.apache.iceberg.{NullOrder, SortDirection}
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * One alteration a table can carry into the follow-up operations: the case-ID prefix its preparations contribute, the
 * preparation step label it runs under, and the function that applies the ALTER and proves it persisted without moving
 * data or the main branch before any follow-up runs on it.
 */
private[harness] final case class TableAlteration(
  casePrefix: String,
  stepLabel: String,
  applyAndValidate: (SparkSession, String) => Unit
)

/**
 * Table evolution compatibility: after a table has been altered, the reads, writes, snapshot operations and
 * maintenance procedures that worked before the alteration still work, and each still leaves the table in the exact
 * state it claims to.
 *
 * Operations: INSERT INTO after the alteration, row-level DELETE after the alteration, a VERSION AS OF read of the
 * pre-alteration snapshot, rollback_to_snapshot back to that snapshot, expire_snapshots over a history the case
 * builds, and rewrite_data_files over a multi-file baseline the case builds.
 *
 * Preparation axes: the four Parquet and ORC core layouts (each format crossed with unpartitioned and
 * date-partitioned), each seeded with the standard rows and then altered in one of four ways: ADD COLUMN cc int,
 * widening foo_col_int from int to bigint, WRITE ORDERED BY foo_col_long, or setting write.distribution-mode to
 * range. Each alteration is a metadata change that keeps the single seed snapshot, and each preparation validates that
 * its alteration persisted before contributing its six cases. That is 16 preparations.
 *
 * Case families: six families over 16 preparations, contributing 96 cases.
 */
trait ScenarioTableEvolutionCompatibility extends ScenarioKit {

  /** Every follow-up operation on every altered preparation, one preparation at a time. */
  lazy val tableEvolutionCompatibilityCases: List[TestCase] =
    alteredTablePreparations.flatMap(preparation =>
      List(
        insertCase(preparation),
        deleteCase(preparation),
        timeTravelCase(preparation),
        rollbackCase(preparation),
        expireSnapshotsCase(preparation),
        rewriteDataFilesCase(preparation)))

  /**
   * One preparation per Parquet and ORC layout and per alteration: the table is created, seeded with the standard
   * rows, then altered, and the alteration step validates that the change persisted. Each alteration carries its own
   * step label and case-ID prefix, so a case ID names the alteration it ran after. The case list walks this order, so
   * a preparation's six families sit together before the next preparation begins.
   */
  lazy val alteredTablePreparations: List[TablePreparation[CoreTable.type]] =
    layouts.flatMap { layout =>
      alterations.map { alteration =>
        TablePreparation(
          layout.label,
          create(layout)
            .insert(standardSeedRowCount)()
            .step(alteration.stepLabel)(alteration.applyAndValidate)(),
          alteration.casePrefix)
      }
    }

  // --- the alterations and the checks that prove each one persisted ---

  /** The core column shape a fresh seeded table reads back as: each core column name and its catalog type. */
  private val coreColumnShape: List[(String, String)] =
    Core.tableColumns.map(column => (column.columnName, column.sqlType)).toList

  /** The four alterations the follow-up operations run after, in the order the case list applies them. */
  private val alterations: List[TableAlteration] =
    List(
      TableAlteration(
        "afterAddColumn:",
        "addColumn",
        (spark, table) =>
          alterMetadataOnly(spark, table)(s"ALTER TABLE $table ADD COLUMN cc int") { rowsBefore =>
            assert(
              columnShapeOf(spark.table(table).schema) == coreColumnShape :+ ("cc" -> "int"),
              "ADD COLUMN cc appends an int column to the core schema")
            assert(
              countOf(spark, s"SELECT count(*) FROM $table WHERE cc IS NOT NULL") == "0",
              "the seeded rows read null for the added column")
            assert(
              inKeyOrder(PreparedTable.currentRows(spark, table, Core)) == inKeyOrder(rowsBefore),
              "ADD COLUMN keeps the exact seed rows in key order")
          }),
      TableAlteration(
        "afterTypeWiden:",
        "widenIntColumnToBigint",
        (spark, table) =>
          alterMetadataOnly(spark, table)(
            s"ALTER TABLE $table ALTER COLUMN ${Core.int0.columnName} TYPE bigint") { _ =>
            val widenedShape = coreColumnShape.map {
              case (name, _) if name == Core.int0.columnName => (name, "bigint")
              case column                                    => column
            }
            assert(
              columnShapeOf(spark.table(table).schema) == widenedShape,
              "widening makes foo_col_int a bigint in the schema")
            val widenedValues = spark
              .sql(s"SELECT ${Core.int0.columnName} FROM $table ORDER BY ${Core.long0.columnName}")
              .collect()
              .toSeq
              .map(_.getLong(0))
            assert(
              widenedValues == Seq(1L, 2L, 3L),
              s"widening preserves the seeded int values, found $widenedValues")
          }),
      TableAlteration(
        "afterWriteOrder:",
        "writeOrderedByLongKey",
        (spark, table) =>
          alterMetadataOnly(spark, table)(
            s"ALTER TABLE $table WRITE ORDERED BY ${Core.long0.columnName}") { rowsBefore =>
            assert(
              tableProps(spark, table).get("write.distribution-mode").contains("range"),
              "WRITE ORDERED BY persists a range write distribution")
            val icebergTable = Spark3Util.loadIcebergTable(spark, table)
            val sortFields = icebergTable.sortOrder().fields()
            assert(
              sortFields.size() == 1,
              s"WRITE ORDERED BY persists exactly one sort field, found ${sortFields.size()}")
            val sortField = sortFields.get(0)
            val longKeyFieldId = icebergTable.schema().findField(Core.long0.columnName).fieldId()
            assert(
              sortField.sourceId() == longKeyFieldId,
              s"the sort field is on ${Core.long0.columnName}, found source id ${sortField.sourceId()}")
            assert(
              sortField.direction() == SortDirection.ASC,
              s"the sort field is ascending, found ${sortField.direction()}")
            assert(
              sortField.nullOrder() == NullOrder.NULLS_FIRST,
              s"the sort field orders nulls first, found ${sortField.nullOrder()}")
            assert(
              inKeyOrder(PreparedTable.currentRows(spark, table, Core)) == inKeyOrder(rowsBefore),
              "setting a write order keeps the exact seed rows in key order")
          }),
      TableAlteration(
        "afterDistributionMode:",
        "setRangeDistributionMode",
        (spark, table) =>
          alterMetadataOnly(spark, table)(
            s"ALTER TABLE $table SET TBLPROPERTIES ('write.distribution-mode'='range')") { rowsBefore =>
            assert(
              tableProps(spark, table).get("write.distribution-mode").contains("range"),
              "the write.distribution-mode property persists as range")
            assert(
              inKeyOrder(PreparedTable.currentRows(spark, table, Core)) == inKeyOrder(rowsBefore),
              "setting the distribution mode keeps the exact seed rows in key order")
          }))

  /**
   * Runs a metadata-only ALTER and proves it moved neither data nor the branch: the snapshot count and the active main
   * snapshot are the same after as before. It captures the pre-alter rows and the active main snapshot first, runs the
   * statement, checks both invariants, and hands the pre-alter rows to `assertEffect`, which proves the alteration
   * itself persisted. Each preparation runs this on its own fresh table, so the captured state is per execution.
   */
  private def alterMetadataOnly(spark: SparkSession, table: String)(alterStatement: String)(
      assertEffect: Seq[Row] => Unit): Unit = {
    val snapshotCountBefore = PreparedTable.snapshotCount(spark, table)
    val activeSnapshotBefore = activeSnapshotId(spark, table)
    val rowsBefore = PreparedTable.currentRows(spark, table, Core)

    spark.sql(alterStatement)

    assert(
      PreparedTable.snapshotCount(spark, table) == snapshotCountBefore,
      "a metadata-only alteration commits no new snapshot")
    assert(
      activeSnapshotId(spark, table) == activeSnapshotBefore,
      "a metadata-only alteration leaves main pointing at the same snapshot")
    assertEffect(rowsBefore)
  }

  // --- the follow-up operations, each asserting exact rows, schema and active-snapshot semantics ---

  /**
   * A plain INSERT lands on the table after the alteration: it duplicates the key-1 row, leaves every other row and
   * the schema in place, and commits exactly one new snapshot.
   */
  private def insertCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("insert") { table =>
      val schemaBefore = columnShapeOf(table.spark.table(table.name).schema)
      val rowsBefore = table.rows
      val snapshotsBefore = table.snapshotCount
      val activeBefore = activeSnapshotId(table.spark, table.name)
      val duplicatedKeyOne = rowsBefore.filter(row => Rows.TypedRow(row).get(Core.long0) == 1L)

      table.spark.sql(
        s"INSERT INTO ${table.name} SELECT * FROM ${table.name} WHERE ${Core.long0.columnName} = 1")

      assert(
        table.rows == inKeyOrder(rowsBefore ++ duplicatedKeyOne),
        "insert duplicates the key-1 row and preserves the rest")
      assert(
        columnShapeOf(table.spark.table(table.name).schema) == schemaBefore,
        "insert leaves the schema unchanged")
      assert(
        table.snapshotCount == snapshotsBefore + 1,
        "insert commits exactly one new snapshot")
      assert(
        activeSnapshotId(table.spark, table.name) != activeBefore,
        "insert moves main to a new snapshot")
    }

  /**
   * A row-level DELETE lands on the table after the alteration: it removes exactly key 2, leaves the schema in place,
   * and commits exactly one new snapshot.
   */
  private def deleteCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("delete") { table =>
      val schemaBefore = columnShapeOf(table.spark.table(table.name).schema)
      val rowsBefore = table.rows
      val snapshotsBefore = table.snapshotCount
      val activeBefore = activeSnapshotId(table.spark, table.name)

      table.spark.sql(s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 2")

      assert(
        table.rows == rowsBefore.filter(row => Rows.TypedRow(row).get(Core.long0) != 2L),
        "delete removes exactly key 2 and preserves the rest")
      assert(
        columnShapeOf(table.spark.table(table.name).schema) == schemaBefore,
        "delete leaves the schema unchanged")
      assert(
        table.snapshotCount == snapshotsBefore + 1,
        "delete commits exactly one new snapshot")
      assert(
        activeSnapshotId(table.spark, table.name) != activeBefore,
        "delete moves main to a new snapshot")
    }

  /**
   * The seed snapshot from before the alteration is still readable through VERSION AS OF: it carries the
   * pre-alteration core schema and the exact three seed rows, and reading it commits nothing and leaves main where it
   * was.
   */
  private def timeTravelCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("timeTravel") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      val snapshotsBefore = table.snapshotCount
      val activeBefore = activeSnapshotId(table.spark, table.name)
      val historical = table.spark.sql(
        s"SELECT * FROM ${table.name} VERSION AS OF $seedSnapshotId ORDER BY ${Core.long0.columnName}")
      val historicalRows = historical.collect().toSeq

      assert(
        columnShapeOf(historical.schema) == coreColumnShape,
        "the seed snapshot carries the pre-alteration core schema")
      assert(
        historicalRows.map(row => Rows.TypedRow(row).get(Core.long0)) == Seq(1L, 2L, 3L),
        "the seed snapshot holds the three seed keys")
      assert(
        historicalRows.map(row => Rows.TypedRow(row).get(Core.string0)) ==
          Seq("row-1", "row-2", "row-3"),
        "the seed snapshot holds the seed string values")
      assert(
        activeBefore == seedSnapshotId,
        "the seed snapshot is the table's single, current snapshot")
      assert(
        table.snapshotCount == snapshotsBefore &&
          activeSnapshotId(table.spark, table.name) == activeBefore,
        "reading history commits nothing and leaves main where it was")
    }

  /**
   * rollback_to_snapshot back to the seed snapshot undoes an INSERT made after the alteration: main points at the
   * requested seed snapshot again, and the table holds the exact seed rows under the current schema.
   */
  private def rollbackCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rollback") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      val seedRows = table.rows
      val schemaBefore = columnShapeOf(table.spark.table(table.name).schema)
      val snapshotsBefore = table.snapshotCount

      table.spark.sql(
        s"INSERT INTO ${table.name} SELECT * FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
      assert(
        table.snapshotCount == snapshotsBefore + 1,
        "the case builds a snapshot to roll back from")

      table.spark.sql(
        "CALL openhouse.system.rollback_to_snapshot(" +
          s"'${catalogRelative(table.name)}', $seedSnapshotId)")

      assert(
        activeSnapshotId(table.spark, table.name) == seedSnapshotId,
        "rollback points main at the requested seed snapshot")
      assert(
        table.rows == seedRows,
        "rollback restores the exact seed rows")
      assert(
        columnShapeOf(table.spark.table(table.name).schema) == schemaBefore,
        "rollback leaves the current schema in place")
    }

  /**
   * expire_snapshots over a history the case builds retains fewer snapshots than it started with, keeps the current
   * snapshot, and preserves the exact current rows.
   */
  private def expireSnapshotsCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("expireSnapshots") { table =>
      duplicateKeyOne(table)
      duplicateKeyOne(table)
      val rowsBeforeExpire = table.rows
      val snapshotsBeforeExpire = table.snapshotCount
      val activeBeforeExpire = activeSnapshotId(table.spark, table.name)

      assert(
        snapshotsBeforeExpire > 1,
        s"the case builds a multi-snapshot history to expire, found $snapshotsBeforeExpire")
      table.spark.sql(
        "CALL openhouse.system.expire_snapshots(" +
          s"table => '${catalogRelative(table.name)}', " +
          "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
          "retain_last => 1)")

      assert(
        table.snapshotCount < snapshotsBeforeExpire,
        s"expire shrinks the retained snapshot set, found $snapshotsBeforeExpire -> ${table.snapshotCount}")
      assert(
        activeSnapshotId(table.spark, table.name) == activeBeforeExpire,
        "expire keeps the current snapshot")
      assert(
        table.rows == rowsBeforeExpire,
        "expire preserves the exact current rows")
    }

  /**
   * rewrite_data_files over a multi-file baseline the case builds compacts the data files into fewer, commits a new
   * snapshot, and preserves the exact current rows.
   */
  private def rewriteDataFilesCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rewriteDataFiles") { table =>
      duplicateKeyOne(table)
      duplicateKeyOne(table)
      val filesBefore = countOf(table.spark, s"SELECT count(*) FROM ${table.name}.files").toLong
      val rowsBeforeRewrite = table.rows
      val snapshotsBeforeRewrite = table.snapshotCount

      assert(
        filesBefore >= 2,
        s"the case builds a multi-file baseline to rewrite, found $filesBefore files")
      table.spark.sql(
        "CALL openhouse.system.rewrite_data_files(" +
          s"table => '${catalogRelative(table.name)}', " +
          "options => map('min-input-files', '2'))")

      val filesAfter = countOf(table.spark, s"SELECT count(*) FROM ${table.name}.files").toLong
      assert(
        filesAfter < filesBefore,
        s"rewrite compacts the data files, found $filesBefore -> $filesAfter")
      assert(
        table.rows == rowsBeforeRewrite,
        "rewrite preserves the exact current rows")
      assert(
        table.snapshotCount == snapshotsBeforeRewrite + 1,
        "rewrite commits exactly one new snapshot")
    }

  // --- shared helpers the follow-ups and alterations read the table through ---

  /** Appends another copy of the key-1 row through its own commit, so a case builds a data file and a snapshot. */
  private def duplicateKeyOne(table: PreparedTable[CoreTable.type]): Unit =
    table.spark.sql(
      s"INSERT INTO ${table.name} SELECT * FROM ${table.name} WHERE ${Core.long0.columnName} = 1")

  /** The column name and catalog type of every field in `schema`, in schema order. */
  private def columnShapeOf(schema: StructType): List[(String, String)] =
    schema.fields.map(field => (field.name, field.dataType.catalogString)).toList

  /** The snapshot the table's main branch currently points at, read from the refs metadata table. */
  private def activeSnapshotId(spark: SparkSession, table: String): Long =
    spark.sql(s"SELECT snapshot_id FROM $table.refs WHERE name = 'main'").collect()(0).getLong(0)

}

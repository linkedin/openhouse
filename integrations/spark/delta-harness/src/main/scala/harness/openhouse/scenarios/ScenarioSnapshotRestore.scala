package harness

import org.apache.spark.sql.SparkSession

/**
 * Snapshot restore: returning a table to an earlier snapshot, and what the restored table keeps.
 *
 * Operations: rollback_to_snapshot and set_current_snapshot back to the seed snapshot, and rollback_to_snapshot back
 * to a pre-evolution snapshot after ADD COLUMN and an insert into the new column.
 *
 * Preparation axes: in each of the two columnar formats, the two-snapshot core table for the two restore procedures,
 * and the standard seeded core table for the schema-evolution family.
 *
 * Case families: three families contributing 6 cases.
 */
trait ScenarioSnapshotRestore extends HistoryTableFixtures {

  /** The standard seed as its restored (key, string) rows, so a restore proves it recovered exactly these rows. */
  private val seedKeyStrings: Seq[(Long, String)] =
    Seq((1L, "row-1"), (2L, "row-2"), (3L, "row-3"))

  /** Every snapshot-restore case, one file format at a time. */
  lazy val snapshotRestoreCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        restoreToSeedCase(
          preparedTwoSnapshotTable(format),
          "restore.rollbackToSnapshot",
          "rollback_to_snapshot"),
        restoreToSeedCase(
          preparedTwoSnapshotTable(format),
          "restore.setCurrentSnapshot",
          "set_current_snapshot"),
        afterAddColumnCase(preparedStandardTable(format)))
    }

  // --- the shared helpers and case bodies the surface above composes ---

  /** The snapshot the main branch points at, read from the refs metadata table, which names one snapshot per branch. */
  private def mainSnapshotId(spark: SparkSession, table: String): Long =
    spark
      .sql(s"SELECT snapshot_id FROM $table.refs WHERE name = 'main'")
      .collect()
      .toSeq
      .map(_.getLong(0)) match {
      case Seq(snapshotId) => snapshotId
      case mainSnapshotIds =>
        throw new AssertionError(s"main names one snapshot, found $mainSnapshotIds")
    }

  /** The column names the table exposes now, in declaration order. */
  private def columnNamesOf(table: PreparedTable[CoreTable.type]): Seq[String] =
    table.spark.sql(s"SELECT * FROM ${table.name} LIMIT 1").columns.toSeq

  /** The (key, string) rows the table currently holds, in ascending key order. */
  private def keyStringsOf(table: PreparedTable[CoreTable.type]): Seq[(Long, String)] =
    table.rows.map(row => (Rows.TypedRow(row).get(Core.long0), Rows.TypedRow(row).get(Core.string0)))

  /** The long keys the table currently holds, in ascending order. */
  private def keysOf(table: PreparedTable[CoreTable.type]): Seq[Long] =
    table.rows.map(row => Rows.TypedRow(row).get(Core.long0))

  /**
   * Restoring to the first snapshot through `procedure` points main back at that snapshot, keeps the seed schema,
   * recovers exactly the seed rows, and leaves the table accepting a follow-up insert that lands the seed plus the new
   * key.
   */
  private def restoreToSeedCase(
      preparation: TablePreparation[CoreTable.type],
      caseName: String,
      procedure: String): TestCase =
    preparation.test(caseName) { table =>
      val seedSnapshot = snapshotIds(table.spark, table.name).head

      table.spark.sql(
        s"CALL openhouse.system.$procedure('${catalogRelativeTableName(table.name)}', $seedSnapshot)")

      assert(
        mainSnapshotId(table.spark, table.name) == seedSnapshot,
        "main points at the requested snapshot")
      assert(
        columnNamesOf(table) == Core.columnNames,
        s"the restored table keeps the seed schema: ${columnNamesOf(table)}")
      assert(
        keyStringsOf(table) == seedKeyStrings,
        s"the restore recovers exactly the seed rows: ${keyStringsOf(table)}")

      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6L, "row-6")}")
      assert(
        keysOf(table) == Seq(1L, 2L, 3L, 6L),
        s"the restored table accepts a follow-up insert: ${keysOf(table)}")
    }

  /**
   * Rolling back to the pre-evolution snapshot after ADD COLUMN and an insert points main back at that snapshot, keeps
   * the evolved schema, recovers exactly the seed rows that read null for the new column, and leaves the table
   * accepting a write into the evolved column.
   */
  private def afterAddColumnCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("restore.afterAddColumn") { table =>
      val seedSnapshot = mainSnapshotId(table.spark, table.name)
      table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      table.spark.sql(s"INSERT INTO ${table.name} VALUES $extraColumnRowNine")
      table.spark.sql(
        "CALL openhouse.system.rollback_to_snapshot(" +
          s"'${catalogRelativeTableName(table.name)}', $seedSnapshot)")

      assert(
        mainSnapshotId(table.spark, table.name) == seedSnapshot,
        "main points at the requested pre-evolution snapshot")
      assert(
        columnNamesOf(table) == Core.columnNames :+ "extra_col",
        s"the rollback keeps the evolved schema: ${columnNamesOf(table)}")
      assert(
        keyStringsOf(table) == seedKeyStrings,
        s"the rollback recovers exactly the seed rows: ${keyStringsOf(table)}")
      assert(
        queryCount(
          table.spark,
          s"SELECT count(*) FROM ${table.name} WHERE extra_col IS NOT NULL") == "0",
        "the recovered seed rows read the evolved column as null")

      table.spark.sql(s"INSERT INTO ${table.name} VALUES $extraColumnRowTen")
      assert(
        keysOf(table) == Seq(1L, 2L, 3L, 10L),
        s"the rolled-back table accepts an evolved-schema insert: ${keysOf(table)}")
    }

}

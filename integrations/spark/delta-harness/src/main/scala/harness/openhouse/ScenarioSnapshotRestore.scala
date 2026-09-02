package harness

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
trait ScenarioSnapshotRestore extends ScenarioKit {

  /** Every snapshot-restore case, one file format at a time. */
  lazy val snapshotRestoreCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        rollbackToSnapshotCase(preparedTwoSnapshotTable(format)),
        setCurrentSnapshotCase(preparedTwoSnapshotTable(format)),
        afterAddColumnCase(preparedStandardTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** rollback_to_snapshot to the first snapshot restores the 3 rows the seed commit wrote. */
  private def rollbackToSnapshotCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("restore.rollbackToSnapshot") { table =>
      val firstSnapshotId = snapshotIds(table.spark, table.name).head

      table.spark.sql(
        "CALL openhouse.system.rollback_to_snapshot(" +
          s"'${catalogRelative(table.name)}', $firstSnapshotId)")

      assert(table.rows.size == 3)
    }

  /** set_current_snapshot to the first snapshot restores the 3 rows the seed commit wrote. */
  private def setCurrentSnapshotCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("restore.setCurrentSnapshot") { table =>
      val firstSnapshotId = snapshotIds(table.spark, table.name).head

      table.spark.sql(
        "CALL openhouse.system.set_current_snapshot(" +
          s"'${catalogRelative(table.name)}', $firstSnapshotId)")

      assert(table.rows.size == 3)
    }

  /**
   * Rolling back to the pre-evolution snapshot after ADD COLUMN and an insert keeps the evolved schema, restores 3
   * rows that read null for the new column, and leaves the table accepting writes into that column.
   */
  private def afterAddColumnCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("restore.afterAddColumn") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).last
      table.spark.sql(
        s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColInsert9")
      table.spark.sql(
        "CALL openhouse.system.rollback_to_snapshot(" +
          s"'${catalogRelative(table.name)}', $seedSnapshotId)")
      val currentColumns = table.spark
        .sql(s"SELECT * FROM ${table.name} LIMIT 1")
        .columns
        .toSeq

      assert(
        currentColumns.contains("extra_col"),
        s"rollback should retain the evolved schema: $currentColumns")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "3",
        "rollback should restore 3 rows")
      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name} WHERE extra_col IS NOT NULL") == "0",
        "rolled-back rows should read the evolved column as null")

      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColInsert10")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "4",
        "the rolled-back table should accept evolved-schema writes")
    }

}

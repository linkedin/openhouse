package harness

/**
 * Incremental read: a scan bounded by a start and an end snapshot returns the rows the snapshots in that range
 * appended, and nothing else.
 *
 * Operations: an incremental scan across an append, across a row-level DELETE, across an INSERT OVERWRITE that only
 * removes rows, across an UPDATE, and across the second commit of a two-snapshot history.
 *
 * Preparation axes: in each of the two columnar formats, the standard seeded core table for the four
 * operation-bounded scans, and the two-snapshot core table for the scan between the two seeded commits.
 *
 * Case families: five families contributing 10 cases.
 */
trait IncrementalReadScenarios extends ScenarioKit {

  /** Every incremental-read case, one file format at a time. */
  lazy val incrementalReadCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        incrementalCase(
          preparedStandardTable(format),
          "incrementalRead.append",
          table =>
            s"INSERT INTO $table VALUES " +
              "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')",
          1),
        incrementalCase(
          preparedStandardTable(format),
          "incrementalRead.delete",
          table => s"DELETE FROM $table WHERE ${Core.long0.columnName} = 1",
          0),
        incrementalCase(
          preparedStandardTable(format),
          "incrementalRead.overwrite",
          table =>
            s"INSERT OVERWRITE $table SELECT * FROM $table " +
              s"WHERE ${Core.long0.columnName} <= 2",
          0),
        incrementalCase(
          preparedStandardTable(format),
          "incrementalRead.update",
          table =>
            s"UPDATE $table SET ${Core.string0.columnName} = 'upd' " +
              s"WHERE ${Core.long0.columnName} = 2",
          0),
        betweenSnapshotsCase(preparedTwoSnapshotTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** The number of rows an incremental scan between the two snapshot IDs returns. */
  private def incrementalRowCount(
      table: PreparedTable[CoreTable.type],
      startSnapshotId: Long,
      endSnapshotId: Long): Long =
    table.spark.read
      .format("iceberg")
      .option("start-snapshot-id", startSnapshotId)
      .option("end-snapshot-id", endSnapshotId)
      .load(table.name)
      .count()

  /**
   * Running the statement against a seeded table and scanning from the seed snapshot to the snapshot the statement
   * committed returns exactly `expectedRowCount` rows.
   */
  private def incrementalCase(
      preparation: TablePreparation[CoreTable.type],
      caseName: String,
      statement: String => String,
      expectedRowCount: Long): Plan.Case =
    preparation.test(caseName) { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(statement(table.name))
      val currentSnapshotId = snapshotIds(table.spark, table.name).last
      val addedRowCount = incrementalRowCount(table, seedSnapshotId, currentSnapshotId)

      assert(
        addedRowCount == expectedRowCount,
        s"$caseName returned $addedRowCount rows, expected $expectedRowCount")
    }

  /** An incremental read spanning both snapshots of the two-snapshot table returns the 2 rows the second one added. */
  private def betweenSnapshotsCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("incrementalRead.betweenSnapshots") { table =>
      val snapshots = snapshotIds(table.spark, table.name)
      val addedRowCount = incrementalRowCount(table, snapshots(0), snapshots(1))

      assert(addedRowCount == 2, s"the second commit added 2 rows, scan returned $addedRowCount")
    }

}

package harness

import org.apache.spark.sql.SparkSession

/**
 * Incremental read: a scan bounded by a start and an end snapshot returns the rows the snapshots in that range
 * appended, and nothing else.
 *
 * Operations: an incremental scan across an append returns the appended row; across a row-level DELETE, an INSERT
 * OVERWRITE that only removes rows, and an UPDATE it returns no rows, because each of those commits an active snapshot
 * that appends nothing; and a scan across the second commit of a two-snapshot history returns that commit's two rows.
 *
 * Preparation axes: in each of the two columnar formats, the standard seeded core table for the four
 * operation-bounded scans, and the two-snapshot core table for the scan between the two seeded commits.
 *
 * Case families: five families contributing 10 cases.
 */
trait ScenarioIncrementalRead extends ScenarioKit {

  /** Every incremental-read case, one file format at a time. */
  lazy val incrementalReadCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        appendCase(preparedStandardTable(format)),
        removeRowsCase(
          preparedStandardTable(format),
          "incrementalRead.delete",
          table => s"DELETE FROM $table WHERE ${Core.long0.columnName} = 1",
          List(2L, 3L)),
        removeRowsCase(
          preparedStandardTable(format),
          "incrementalRead.overwrite",
          table =>
            s"INSERT OVERWRITE $table SELECT * FROM $table " +
              s"WHERE ${Core.long0.columnName} <= 2",
          List(1L, 2L)),
        updateCase(preparedStandardTable(format)),
        betweenSnapshotsCase(preparedTwoSnapshotTable(format)))
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

  /** The long keys an incremental scan between the two snapshot IDs returns, in ascending order. */
  private def incrementalKeys(
      table: PreparedTable[CoreTable.type],
      startSnapshotId: Long,
      endSnapshotId: Long): Seq[Long] =
    table.spark.read
      .format("iceberg")
      .option("start-snapshot-id", startSnapshotId)
      .option("end-snapshot-id", endSnapshotId)
      .load(table.name)
      .collect()
      .toSeq
      .map(row => Rows.TypedRow(row).get(Core.long0))
      .sorted

  /** The long keys the table currently holds, in ascending order. */
  private def keysOf(table: PreparedTable[CoreTable.type]): Seq[Long] =
    table.rows.map(row => Rows.TypedRow(row).get(Core.long0))

  /**
   * Appending a row commits a new active snapshot, leaves the seed plus that row, and an incremental scan from the
   * seed to the new snapshot returns exactly the appended key.
   */
  private def appendCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("incrementalRead.append") { table =>
      val seedSnapshot = mainSnapshotId(table.spark, table.name)
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      val appendSnapshot = mainSnapshotId(table.spark, table.name)

      assert(appendSnapshot != seedSnapshot, "the append commits a new active snapshot")
      assert(keysOf(table) == Seq(1L, 2L, 3L, 6L), s"the append leaves the seed plus key 6: ${keysOf(table)}")
      assert(
        incrementalKeys(table, seedSnapshot, appendSnapshot) == Seq(6L),
        "the incremental scan returns exactly the appended row")
    }

  /**
   * Running `statement` against the seed commits a new active snapshot and leaves exactly `expectedRemainingKeys`,
   * while an incremental scan from the seed to the new snapshot returns no rows, because the commit appends nothing.
   */
  private def removeRowsCase(
      preparation: TablePreparation[CoreTable.type],
      caseName: String,
      statement: String => String,
      expectedRemainingKeys: List[Long]): TestCase =
    preparation.test(caseName) { table =>
      val seedSnapshot = mainSnapshotId(table.spark, table.name)
      table.spark.sql(statement(table.name))
      val currentSnapshot = mainSnapshotId(table.spark, table.name)

      assert(currentSnapshot != seedSnapshot, s"$caseName commits a new active snapshot")
      assert(
        keysOf(table) == expectedRemainingKeys,
        s"$caseName leaves keys $expectedRemainingKeys, found ${keysOf(table)}")
      assert(
        incrementalKeys(table, seedSnapshot, currentSnapshot).isEmpty,
        s"$caseName appends nothing, so the incremental scan returns no rows")
    }

  /**
   * An UPDATE commits a new active snapshot, keeps all three keys and changes only key 2's string value, while an
   * incremental scan from the seed to the new snapshot returns no rows, because the commit appends nothing.
   */
  private def updateCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("incrementalRead.update") { table =>
      val seedSnapshot = mainSnapshotId(table.spark, table.name)
      table.spark.sql(
        s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'upd' " +
          s"WHERE ${Core.long0.columnName} = 2")
      val currentSnapshot = mainSnapshotId(table.spark, table.name)
      val keyStrings = table.rows.map(row =>
        (Rows.TypedRow(row).get(Core.long0), Rows.TypedRow(row).get(Core.string0)))

      assert(currentSnapshot != seedSnapshot, "the update commits a new active snapshot")
      assert(
        keyStrings == Seq((1L, "row-1"), (2L, "upd"), (3L, "row-3")),
        s"the update changes only key 2's string value, found $keyStrings")
      assert(
        incrementalKeys(table, seedSnapshot, currentSnapshot).isEmpty,
        "the update appends nothing, so the incremental scan returns no rows")
    }

  /** An incremental read spanning both snapshots of the two-snapshot table returns keys 4 and 5, the second commit. */
  private def betweenSnapshotsCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("incrementalRead.betweenSnapshots") { table =>
      val history = snapshotIds(table.spark, table.name)
      assert(history.size == 2, s"the two-snapshot table has two commits, found ${history.size}")
      assert(
        history.last == mainSnapshotId(table.spark, table.name),
        "the newest commit is the one the main branch points at")

      assert(
        incrementalKeys(table, history(0), history(1)) == Seq(4L, 5L),
        "the second commit appended keys 4 and 5")
    }

}

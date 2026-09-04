package harness

import java.util.concurrent.TimeUnit
import org.apache.spark.sql.SparkSession

/**
 * Changelog: the row-level change feed `create_changelog_view` reports for a snapshot range, and what it reports once
 * the start of that range has been expired.
 *
 * Operations: the five reusable changelog operations `ChangelogSupport` owns (an append, an INSERT OVERWRITE that
 * drops one row, a row-level DELETE, an UPDATE, and a MERGE that updates one row and inserts another), each followed
 * by a changelog view opened at the seed snapshot; a changelog view over an append-only history with no start
 * snapshot; and a changelog view whose start point has been expired, named by snapshot id and by two timestamps.
 *
 * Preparation axes: in each of the two columnar formats, the standard seeded core table for the five operations and
 * for the expired-start family, and the two-snapshot core table for the append-only history family. The operations
 * are data `ChangelogSupport` holds, so this general scenario crosses them with the standard seeded table while the
 * replace-table layer crosses the same operations with its own replace preparations.
 *
 * Case families: three families contributing 14 cases, 10 operation cases, 2 append-only history cases and 2
 * expired-start cases.
 */
trait ScenarioChangelog extends ChangelogSupport {

  /** Every changelog case, one file format at a time. */
  lazy val changelogCases: List[TestCase] =
    fileFormats.flatMap { format =>
      changelogOperationCasesFor(List(preparedStandardTable(format))) ++
        List(
          appendOnlyHistoryCase(preparedTwoSnapshotTable(format)),
          expiredStartRejectedCase(preparedStandardTable(format)))
    }

  // --- the shared helpers and case bodies the surface above composes ---

  /** The commit time of one snapshot, in epoch milliseconds, read from the snapshots metadata table. */
  private def commitTimeOf(table: PreparedTable[CoreTable.type], snapshotId: Long): Long =
    table.spark
      .sql(s"SELECT committed_at FROM ${table.name}.snapshots WHERE snapshot_id = $snapshotId")
      .collect()(0)
      .getTimestamp(0)
      .getTime

  /** The name of a changelog view over `table`, opened at the commit time `startTimestampMillis`. */
  private def changelogViewFromStartTime(
      table: PreparedTable[CoreTable.type],
      startTimestampMillis: Long): String =
    table.spark
      .sql(
        "CALL openhouse.system.create_changelog_view(" +
          s"table => '${catalogRelative(table.name)}', " +
          s"options => map('start-timestamp', '$startTimestampMillis'))")
      .collect()(0)
      .getString(0)

  private def waitForNextSnapshotTimestamp(spark: SparkSession, table: String): Unit = {
    val previousTimestamp = spark
      .sql(s"SELECT committed_at FROM $table.snapshots ORDER BY committed_at DESC LIMIT 1")
      .collect()(0)
      .getTimestamp(0)
      .getTime
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)

    while (
      System.currentTimeMillis() <= previousTimestamp &&
      System.nanoTime() < deadline) {
      Thread.sleep(1L)
    }

    assert(
      System.currentTimeMillis() > previousTimestamp,
      s"clock did not advance beyond snapshot timestamp $previousTimestamp")
  }

  /** create_changelog_view over an append-only history reports 5 changes, all of change type INSERT. */
  private def appendOnlyHistoryCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("changelog.appendOnlyHistory") { table =>
      val view = table.spark
        .sql(
          "CALL openhouse.system.create_changelog_view(" +
            s"table => '${catalogRelative(table.name)}')")
        .collect()(0)
        .getString(0)
      val actualChangeCounts = changeCounts(table, view)

      assert(
        actualChangeCounts == Map("INSERT" -> 5L),
        s"append-only changelog should report five inserts: $actualChangeCounts")
    }

  /**
   * Expiring the start of a changelog range makes that range unreadable, and the engine rejects each way of naming the
   * expired start. The case stages three commits with distinct commit times, expires every snapshot except the
   * newest, and confirms the retained history is exactly that newest snapshot. It then opens a changelog view from
   * three expired start points and pins each precise rejection: the expired start snapshot id raises an
   * IllegalArgumentException that names the start snapshot as outside the current ancestry, and a start timestamp
   * before the history or inside the expired range raises an IllegalStateException that reports no snapshot older than
   * the requested time.
   */
  private def expiredStartRejectedCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("changelog.expiredStartRejected") { table =>
      waitForNextSnapshotTimestamp(table.spark, table.name)
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      waitForNextSnapshotTimestamp(table.spark, table.name)
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
      val history = snapshotIds(table.spark, table.name)
      assert(history.size == 3, s"the changelog range spans three commits, found ${history.size}")
      val expiredStartSnapshot = history.head
      val currentSnapshot = history.last
      val firstCommitTime = commitTimeOf(table, history.head)
      val middleCommitTime = commitTimeOf(table, history(1))

      table.spark.sql(
        "CALL openhouse.system.expire_snapshots(" +
          s"table => '${catalogRelative(table.name)}', " +
          "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
          "retain_last => 1)")
      assert(
        snapshotIds(table.spark, table.name) == List(currentSnapshot),
        s"expire_snapshots(retain_last => 1) keeps only the newest snapshot $currentSnapshot")

      val expiredIdRejection = Check.intercept[IllegalArgumentException](
        changeCounts(table, changelogViewFrom(table, expiredStartSnapshot)))
      assert(
        expiredIdRejection.getMessage.contains("is not a parent ancestor of end snapshot") &&
          expiredIdRejection.getMessage.contains(expiredStartSnapshot.toString),
        "the expired start snapshot is named as outside the current ancestry: " +
          expiredIdRejection.getMessage.take(200))

      val beforeHistoryRejection = Check.intercept[IllegalStateException](
        changeCounts(table, changelogViewFromStartTime(table, firstCommitTime - 1000L)))
      assert(
        beforeHistoryRejection.getMessage.contains("Cannot find snapshot older than"),
        "a start before the history reports no snapshot older than the requested time: " +
          beforeHistoryRejection.getMessage.take(200))

      val insideExpiredRangeRejection = Check.intercept[IllegalStateException](
        changeCounts(table, changelogViewFromStartTime(table, middleCommitTime - 1L)))
      assert(
        insideExpiredRangeRejection.getMessage.contains("Cannot find snapshot older than"),
        "a start inside the expired range reports no snapshot older than the requested time: " +
          insideExpiredRangeRejection.getMessage.take(200))
    }

}

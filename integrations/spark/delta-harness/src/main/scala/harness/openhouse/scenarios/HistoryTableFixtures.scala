package harness

import java.util.concurrent.TimeUnit
import org.apache.spark.sql.SparkSession

/**
 * Multi-snapshot and evolved-row fixtures used by history scenarios.
 */
trait HistoryTableFixtures extends RtasTableFixtures {

  /**
   * An unpartitioned core table holding the standard seed and a second two-row append in distinct commit
   * milliseconds.
   */
  protected def preparedTwoSnapshotTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      createCoreTable(coreLayout(format, format, ""))
        .insert(standardSeedRowCount)()
        .step("waitForNextSnapshotTimestamp")(waitForNextSnapshotTimestamp)()
        .sql("insertRowsFourAndFive")(table =>
          s"INSERT INTO $table VALUES " +
            "(CAST(4 AS BIGINT), 4, 'row-4', 4.5, true, '2024-01-04-03'), " +
            "(CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')")())

  protected val extraColumnRowNine =
    "(CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01', 42)"
  protected val extraColumnRowTen =
    "(CAST(10 AS BIGINT), 10, 'row-10', 10.5, true, '2024-01-10-01', 43)"

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
}

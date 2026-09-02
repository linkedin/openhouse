package harness

import java.nio.file.Files
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger

/**
 * Structured streaming: reading a table as a stream, writing a stream into a table, resuming a stream across a
 * restart, and the snapshot histories a resumed stream rejects.
 *
 * Operations: a streaming read into a memory sink; a streaming append of two rows through the iceberg write-stream
 * format; a streaming read into a destination table, restarted after an append; the same restart after a DELETE
 * snapshot; and the same restart after the checkpoint's offset snapshot has been expired.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats. The three restart families
 * create and drop their own destination table in the same format.
 *
 * Case families: five families contributing 10 cases.
 */
trait StreamingScenarios extends ScenarioKit {

  /** Every streaming case, one file format at a time. */
  lazy val streamingCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        readCase(preparedStandardTable(format)),
        writeCase(preparedStandardTable(format)),
        readAcrossRestartCase(preparedStandardTable(format), format),
        deleteSnapshotRejectedCase(preparedStandardTable(format), format),
        expiredCheckpointCase(preparedStandardTable(format), format))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  // Runs one AvailableNow batch of a streaming read of `source` into `destination`, resuming from `checkpoint`. Each
  // call returns after the batch has been committed, so the caller can assert on the destination and then run again.
  private def streamOneBatch(
      table: PreparedTable[CoreTable.type],
      destination: String,
      checkpoint: String): Unit = {
    val query = table.spark.readStream
      .table(table.name)
      .writeStream
      .format("iceberg")
      .outputMode("append")
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint)
      .toTable(destination)
    assert(query.awaitTermination(120000), "stream did not finish")
    query.stop()
  }

  /**
   * A Spark structured streaming read of the table, run in AvailableNow batch mode, delivers all 3 seed rows to a
   * memory sink within 120 seconds.
   */
  private def readCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("streaming.read") { table =>
      val checkpoint = Files.createTempDirectory("ck-read").toString
      val sink = s"memsink_${System.nanoTime}"
      val query = table.spark.readStream
        .table(table.name)
        .writeStream
        .format("memory")
        .queryName(sink)
        .trigger(Trigger.AvailableNow())
        .option("checkpointLocation", checkpoint)
        .start()

      assert(
        query.awaitTermination(120000),
        "streaming read did not finish in 120 seconds")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM $sink") == "3",
        "streaming read should deliver the three seed rows")
    }

  /**
   * A Spark structured streaming append of two rows through the iceberg write-stream format lands both rows, growing
   * the table from 3 to 5 rows.
   */
  private def writeCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("streaming.write") { table =>
      import table.spark.implicits._
      implicit val sqlContext: SQLContext = table.spark.sqlContext
      val memoryStream = MemoryStream[Long]
      memoryStream.addData(100L, 101L)
      val rows = memoryStream.toDF().selectExpr(
        s"value AS ${Core.long0.columnName}",
        s"CAST(value AS INT) AS ${Core.int0.columnName}",
        s"concat('row-', value) AS ${Core.string0.columnName}",
        s"CAST(value AS DOUBLE) AS ${Core.double0.columnName}",
        s"true AS ${Core.boolean0.columnName}",
        s"'2024-01-01-00' AS ${Core.date0.columnName}")
      val checkpoint = Files.createTempDirectory("ck-write").toString
      val query = rows.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .toTable(table.name)

      query.processAllAvailable()
      query.stop()
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "5",
        "streaming write should append two rows")
    }

  /**
   * A streaming read of the table delivers the seed rows on first run and the newly inserted row after restart, into a
   * destination table.
   */
  private def readAcrossRestartCase(
      preparation: TablePreparation[CoreTable.type],
      format: String): Plan.Case =
    preparation.test("streaming.readAcrossRestart") { table =>
      val destination = s"${table.name}_s"
      val checkpoint = Files.createTempDirectory("ck-restart").toString

      withOwnedTable(table.spark.sql(_), destination)(
        table.spark.sql(coreCreate(destination, format))) {
        streamOneBatch(table, destination, checkpoint)
        assert(
          countOf(table.spark, s"SELECT count(*) FROM $destination") == "3",
          "initial stream did not deliver the seed")
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        streamOneBatch(table, destination, checkpoint)
        assert(
          countOf(table.spark, s"SELECT count(*) FROM $destination") == "4",
          "stream restart did not deliver the appended row")
      }
    }

  /**
   * An append-only stream restarted after a DELETE snapshot was written fails, with an error mentioning delete or
   * overwrite.
   */
  private def deleteSnapshotRejectedCase(
      preparation: TablePreparation[CoreTable.type],
      format: String): Plan.Case =
    preparation.test("streaming.deleteSnapshot.rejected") { table =>
      val destination = s"${table.name}_sd"
      val checkpoint = Files.createTempDirectory("ck-delete").toString

      withOwnedTable(table.spark.sql(_), destination)(
        table.spark.sql(coreCreate(destination, format))) {
        streamOneBatch(table, destination, checkpoint)
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
        val exception =
          Check.intercept[Exception](streamOneBatch(table, destination, checkpoint))

        assert(
          Exceptions.causeChain(exception).exists(error =>
            Option(error.getMessage).exists(message =>
              message.toLowerCase.contains("delete") ||
                message.toLowerCase.contains("overwrite"))),
          "an append-only stream rejects a delete snapshot: " +
            s"${exception.getClass.getSimpleName} ${Option(exception.getMessage).getOrElse("").take(140)}")
      }
    }

  /**
   * A streaming read that resumes after its earliest offset snapshot has been expired fails, with an error naming the
   * expired or missing snapshot.
   */
  private def expiredCheckpointCase(
      preparation: TablePreparation[CoreTable.type],
      format: String): Plan.Case =
    preparation.test("streaming.expiredCheckpoint") { table =>
      val destination = s"${table.name}_sink"
      val checkpoint = Files.createTempDirectory("ck-expired").toString

      withOwnedTable(table.spark.sql(_), destination)(
        table.spark.sql(coreCreate(destination, format))) {
        streamOneBatch(table, destination, checkpoint)
        assert(
          countOf(table.spark, s"SELECT count(*) FROM $destination") == "3",
          "initial stream should deliver the seed")

        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        streamOneBatch(table, destination, checkpoint)
        assert(
          countOf(table.spark, s"SELECT count(*) FROM $destination") == "4",
          "control restart should deliver one incremental row")

        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
        table.spark.sql(
          "CALL openhouse.system.expire_snapshots(" +
            s"table => '${catalogRelative(table.name)}', " +
            "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
            "retain_last => 1)")
        val exception =
          Check.intercept[Exception](streamOneBatch(table, destination, checkpoint))

        assert(
          Exceptions.causeChain(exception).exists(error =>
            Option(error.getMessage).exists(message =>
              message.contains("expired or removed") ||
                message.contains("Cannot load current offset") ||
                message.contains("Cannot find snapshot"))),
          "stream restart should report the expired checkpoint offset")
      }
    }

}

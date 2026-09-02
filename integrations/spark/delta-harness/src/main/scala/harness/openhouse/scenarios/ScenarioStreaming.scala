package harness

import java.nio.file.{Files, Path}
import java.util.Comparator

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

/**
 * Structured streaming: reading a table as a stream, writing a stream into a table, resuming a stream across a
 * restart, and the snapshot histories a resumed stream rejects.
 *
 * Operations: a streaming read into a memory sink; a streaming append of two rows through the iceberg write-stream
 * format; a streaming read into a destination table, restarted after an append; the same restart after a DELETE
 * snapshot; and the same restart after the checkpoint's offset snapshot has been expired.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats. The three restart families
 * create and drop their own destination table in the same format through the owned-table boundary.
 *
 * Ownership: every case runs inside an owned checkpoint directory that is deleted on every outcome, and every started
 * streaming query is stopped on every outcome. A checkpoint-deletion or query-stop failure rides along suppressed on
 * a primary failure and surfaces on a clean body, following the shared lifecycle contract. The destination tables the
 * restart families build are owned and dropped through the owned-table boundary.
 *
 * Case families: five families contributing 10 cases.
 */
trait ScenarioStreaming extends ScenarioKit {

  /** Every streaming case, one file format at a time. */
  lazy val streamingCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        readCase(preparedStandardTable(format)),
        writeCase(preparedStandardTable(format)),
        readAcrossRestartCase(preparedStandardTable(format), format),
        deleteSnapshotRejectedCase(preparedStandardTable(format), format),
        expiredCheckpointCase(preparedStandardTable(format), format))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * Creates a fresh checkpoint directory, runs `use` against it, and deletes the directory on every outcome. The
   * deletion walks the tree in a stream it closes, so a case that finishes or fails leaves the checkpoint directory
   * gone. A deletion failure rides along suppressed on a body failure and surfaces on a clean body.
   */
  private def withOwnedCheckpoint(namePrefix: String)(use: String => Unit): Unit = {
    val checkpoint = Files.createTempDirectory(namePrefix)
    OwnedTableLifecycle.withCleanup(deleteRecursively(checkpoint))(use(checkpoint.toString))
  }

  /** Deletes `directory` and everything under it, walking the tree deepest-first in a stream that is closed. */
  private def deleteRecursively(directory: Path): Unit =
    if (Files.exists(directory)) {
      val entries = Files.walk(directory)
      try {
        entries.sorted(Comparator.reverseOrder[Path]()).forEach(path => Files.deleteIfExists(path))
      } finally {
        entries.close()
      }
    }

  /** Runs `use` against `query` and stops the query on every outcome, following the shared lifecycle contract. */
  private def withStoppedQuery(query: StreamingQuery)(use: => Unit): Unit =
    OwnedTableLifecycle.withCleanup(query.stop())(use)

  // Runs one AvailableNow batch of a streaming read of `source` into `destination`, resuming from `checkpoint`. The
  // query is stopped on every outcome. Each call returns after the batch has been committed, so the caller can assert
  // on the destination and then run again.
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
    withStoppedQuery(query) {
      assert(query.awaitTermination(120000), "stream did not finish")
    }
  }

  /**
   * A Spark structured streaming read of the table, run in AvailableNow batch mode, delivers all 3 seed rows to a
   * memory sink within 120 seconds.
   */
  private def readCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("streaming.read") { table =>
      withOwnedCheckpoint("ck-read") { checkpoint =>
        val sink = s"memsink_${System.nanoTime}"
        val query = table.spark.readStream
          .table(table.name)
          .writeStream
          .format("memory")
          .queryName(sink)
          .trigger(Trigger.AvailableNow())
          .option("checkpointLocation", checkpoint)
          .start()

        withStoppedQuery(query) {
          assert(
            query.awaitTermination(120000),
            "streaming read did not finish in 120 seconds")
          assert(
            countOf(table.spark, s"SELECT count(*) FROM $sink") == "3",
            "streaming read should deliver the three seed rows")
        }
      }
    }

  /**
   * A Spark structured streaming append of two rows through the iceberg write-stream format lands both rows, growing
   * the table from 3 to 5 rows.
   */
  private def writeCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("streaming.write") { table =>
      withOwnedCheckpoint("ck-write") { checkpoint =>
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
        val query = rows.writeStream
          .format("iceberg")
          .outputMode("append")
          .option("checkpointLocation", checkpoint)
          .toTable(table.name)

        withStoppedQuery(query) {
          query.processAllAvailable()
          assert(
            countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "5",
            "streaming write should append two rows")
        }
      }
    }

  /**
   * A streaming read of the table delivers the seed rows on first run and the newly inserted row after restart, into a
   * destination table.
   */
  private def readAcrossRestartCase(
      preparation: TablePreparation[CoreTable.type],
      format: String): TestCase =
    preparation.test("streaming.readAcrossRestart") { table =>
      withOwnedCheckpoint("ck-restart") { checkpoint =>
        val destination = s"${table.name}_s"

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
    }

  /**
   * An append-only stream restarted after a DELETE snapshot was written fails, with an error mentioning delete or
   * overwrite.
   */
  private def deleteSnapshotRejectedCase(
      preparation: TablePreparation[CoreTable.type],
      format: String): TestCase =
    preparation.test("streaming.deleteSnapshot.rejected") { table =>
      withOwnedCheckpoint("ck-delete") { checkpoint =>
        val destination = s"${table.name}_sd"

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
    }

  /**
   * A streaming read that resumes after its earliest offset snapshot has been expired fails, with an error naming the
   * expired or missing snapshot.
   */
  private def expiredCheckpointCase(
      preparation: TablePreparation[CoreTable.type],
      format: String): TestCase =
    preparation.test("streaming.expiredCheckpoint") { table =>
      withOwnedCheckpoint("ck-expired") { checkpoint =>
        val destination = s"${table.name}_sink"

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

}

package harness

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

/**
 * Concurrency: two writers racing on one table. Every write either commits or fails with a typed commit-conflict
 * exception, and the table the race leaves behind is consistent with the writes that committed.
 *
 * Operations: two threads each running three single-row INSERTs against the same table, and two threads each running
 * an UPDATE of the same row to a different value.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats. The concurrency helpers are
 * feature neutral, so a feature layer reuses them for its own table mode.
 *
 * Case families: two families contributing 4 cases.
 */
trait ConcurrencyScenarios extends ScenarioKit {

  /** Every concurrency case, one file format at a time. */
  lazy val concurrencyCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        appendAppendCase(preparedStandardTable(format)),
        updateUpdateCase(preparedStandardTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * Runs every function on its own daemon thread, releases them together, and waits up to three minutes for all of
   * them. Returns the throwables the threads raised, plus one for each thread still running at the deadline.
   */
  protected def runConcurrently(functions: Seq[() => Unit]): Seq[Throwable] = {
    val errors = new ConcurrentLinkedQueue[Throwable]()
    val start = new CountDownLatch(1)
    val threads = functions.zipWithIndex.map { case (function, index) =>
      val thread = new Thread(
        () =>
          try {
            start.await()
            function()
          } catch {
            case interrupted: InterruptedException =>
              Thread.currentThread().interrupt()
              errors.add(interrupted)
            case throwable: Throwable =>
              errors.add(throwable)
          },
        s"delta-harness-concurrent-$index")
      thread.setDaemon(true)
      thread
    }
    threads.foreach(_.start())
    start.countDown()

    val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3)
    threads.foreach { thread =>
      val remainingNanos = deadline - System.nanoTime()
      if (remainingNanos > 0) {
        TimeUnit.NANOSECONDS.timedJoin(thread, remainingNanos)
      }
    }

    threads.filter(_.isAlive).foreach { thread =>
      errors.add(
        new AssertionError(s"${thread.getName} did not complete within 3 minutes"))
      thread.interrupt()
    }
    errors.toArray(Array.empty[Throwable]).toSeq
  }

  /** A commit conflict the catalog reports through one of its typed commit, validation or transport exceptions. */
  protected def isTypedCommitConflict(throwable: Throwable): Boolean =
    Exceptions.causeChain(throwable).exists { cause =>
      val className = cause.getClass.getName
      className.contains("CommitFailed") ||
        className.contains("CommitStateUnknown") ||
        className.contains("Validation") ||
        className.contains("BadRequest") ||
        className.contains("WebClientResponse")
    }

  /**
   * Two threads concurrently insert 3 rows each; every insert either commits or fails with a typed commit-conflict
   * exception, and the final row count matches 3 plus the number of inserts that actually committed.
   */
  private def appendAppendCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("concurrency.appendAppend") { table =>
      val failureCount = new AtomicInteger(0)
      def writer(base: Int): () => Unit = () =>
        (0 until 3).foreach { offset =>
          val value = base + offset
          try {
            table.spark.sql(
              s"INSERT INTO ${table.name} VALUES " +
                s"(CAST($value AS BIGINT), $value, 'row-c', 1.5, true, '2024-01-09-01')")
          } catch {
            case exception: Throwable =>
              assert(
                isTypedCommitConflict(exception),
                "concurrent append failed with an untyped error: " +
                  s"${exception.getClass.getName}")
              failureCount.incrementAndGet()
          }
        }
      val threadErrors = runConcurrently(Seq(writer(100), writer(200)))
      val expectedRowCount = 3 + 6 - failureCount.get

      assert(
        threadErrors.isEmpty,
        s"writer thread failed outside the insert loop: $threadErrors")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == expectedRowCount.toString,
        s"expected $expectedRowCount rows after ${failureCount.get} of 6 inserts hit a conflict")
    }

  /**
   * Two threads concurrently UPDATE the same row to different values; the row count stays at 3, and the final value is
   * one of the two competing updates or the original seed value, with any failure being a typed commit conflict.
   */
  private def updateUpdateCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("concurrency.updateUpdate") { table =>
      def updater(value: String): () => Unit = () =>
        try {
          table.spark.sql(
            s"UPDATE ${table.name} SET ${Core.string0.columnName} = '$value' " +
              s"WHERE ${Core.long0.columnName} = 2")
        } catch {
          case exception: Throwable =>
            assert(
              isTypedCommitConflict(exception),
              "concurrent update failed with an untyped error: " +
                s"${exception.getClass.getName}")
        }
      val threadErrors = runConcurrently(Seq(updater("AAA"), updater("BBB")))
      val finalValue = table.spark
        .sql(
          s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} = 2")
        .collect()(0)
        .getString(0)

      assert(
        threadErrors.isEmpty,
        s"updater thread failed with a non-conflict error: $threadErrors")
      assert(
        finalValue == "AAA" || finalValue == "BBB" || finalValue == "row-2",
        s"concurrent updates produced a torn value: $finalValue")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "3",
        "concurrent updates should leave the row count at 3")
    }

}

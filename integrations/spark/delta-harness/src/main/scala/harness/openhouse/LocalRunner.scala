package harness

import java.util.concurrent.{Callable, Executors, TimeUnit}
import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
 * The local runner: the `harness.Main` launch class the `runOpenHouse` task starts, plus the retry policy it executes
 * each case under. This file is compiled into the `local` source set only, so the published portable library carries
 * the catalog and the framework without a run loop of its own.
 */
object Runner {
  val MaxAttempts = 3

  /** Runs a case, retrying only a transient-infrastructure failure. */
  def execute(testCase: TestCase, context: Ctx): (Outcome, Int) = {
    @tailrec def attempt(attemptIndex: Int): (Outcome, Int) = {
      val outcome =
        try {
          testCase.run(context.copy(spark = context.spark.newSession()))
          Outcome.Passed
        }
        catch { case NonFatal(throwable) => Outcome.Failed(throwable) }
      outcome match {
        case failure: Outcome.Failed
            if failure.retryable && attemptIndex + 1 < MaxAttempts =>
          attempt(attemptIndex + 1)
        case terminal =>
          (terminal, attemptIndex + 1)
      }
    }
    attempt(0)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val (server, spark, tablesUri, authorizationToken) = OpenHouseEnv.start()
    var runFailure: Option[Throwable] = None
    try {
      spark.sparkContext.setLogLevel("ERROR")
      val ctx = Ctx(spark, "openhouse.dbMatrix", tablesUri, authorizationToken)

      // Each command-line argument is an include substring. A case runs when its ID contains every provided substring.
      // An empty argument list selects the full catalog.
      val filters = args.toList
      val cases = ScenarioCatalog.cases.filter(testCase =>
        filters.forall(testCase.id.contains))

      val header =
        if (filters.isEmpty) {
          "all cases"
        } else {
          s"filter ${filters.mkString(", ")} -> ${cases.size} cases"
        }
      println(s"\n=== delta-harness :: localized cases @ OpenHouse catalog ($header) ===\n")

      // Each case owns a fresh table. Worker tasks use separate Spark sessions over the shared Spark context, and
      // results are printed in catalog order.
      val parallelism = sys.env.get("HARNESS_PARALLELISM").map(_.toInt)
        .getOrElse(math.max(1, Runtime.getRuntime.availableProcessors()))
      println(s"parallelism: $parallelism worker sessions\n")

      def runOne(testCase: TestCase): (TestCase, (Outcome, Int)) =
        testCase.embeddedSkipReason
          .map(reason => s"embedded limitation: $reason")
          .orElse(testCase.bugReason) match {
          case Some(reason) =>
            (testCase, (Outcome.Skipped(reason): Outcome, 0))
          case None =>
            (testCase, Runner.execute(testCase, ctx))
        }

      val results =
        if (parallelism <= 1) {
          cases.map(runOne)
        } else {
          val pool = Executors.newFixedThreadPool(parallelism)
          try {
            val futures = cases.map(testCase =>
              pool.submit(
                new Callable[(TestCase, (Outcome, Int))] {
                  def call(): (TestCase, (Outcome, Int)) = runOne(testCase)
                }))
            futures.map(_.get(60, TimeUnit.MINUTES))
          } finally {
            pool.shutdownNow()
          }
        }

      results.foreach { case (testCase, (outcome, attempts)) =>
        val note = outcome match {
          case failure: Outcome.Failed =>
            s"  (${failure.reason}${if (failure.retryable) " [retryable]" else ""})"
          case Outcome.Skipped(reason) =>
            s"  ($reason)"
          case Outcome.Passed =>
            ""
        }
        println(f"${outcome.label}%-4s ${testCase.id}%-52s try=$attempts$note")
      }

      val failed =
        results.count { case (_, (outcome, _)) => outcome.isInstanceOf[Outcome.Failed] }
      val skipped =
        results.count { case (_, (outcome, _)) => outcome.isInstanceOf[Outcome.Skipped] }
      val passed = results.size - failed - skipped
      println(f"\n$passed passed, $skipped skipped, $failed failed  (${results.size} cases)")

      if (failed > 0 || passed == 0) {
        throw new AssertionError(
          s"delta harness finished with $passed passed, $skipped skipped, and $failed failed cases")
      }
    } catch {
      case failure: Throwable =>
        runFailure = Some(failure)
        throw failure
    } finally {
      val cleanupFailures =
        List[() => Unit](
          () => spark.stop(),
          () => server.stop())
          .flatMap { cleanup =>
            try {
              cleanup()
              None
            } catch {
              case failure: Throwable => Some(failure)
            }
          }

      runFailure match {
        case Some(failure) =>
          cleanupFailures.foreach(failure.addSuppressed)
        case None =>
          cleanupFailures.headOption.foreach { failure =>
            cleanupFailures.drop(1).foreach(failure.addSuppressed)
            throw failure
          }
      }
    }
  }
}

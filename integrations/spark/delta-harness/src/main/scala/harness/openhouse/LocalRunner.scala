package harness

import java.util.concurrent.{Callable, Executors, TimeUnit}

/**
 * The `harness.Main` launch class used by the embedded OpenHouse Gradle tasks. This file is compiled into the `local`
 * source set only, so the published portable library carries the catalog and retry policy without a launch loop.
 */
object Main {
  private val FoundationCatalogArgument = "--catalog=foundation"

  def main(args: Array[String]): Unit = {
    val (server, spark) = OpenHouseEnv.start()
    var runFailure: Option[Throwable] = None
    try {
      spark.sparkContext.setLogLevel("ERROR")
      val ctx = Ctx(spark, "openhouse.dbMatrix")

      val (catalogArguments, filters) = args.toList.partition(_.startsWith("--catalog="))
      val selectedCatalog = catalogArguments match {
        case Nil =>
          Catalog.cases
        case List(FoundationCatalogArgument) =>
          Catalog.foundationContributions.flatMap { case (_, contribution) => contribution }
        case unsupported =>
          throw new HarnessConfigurationException(
            s"supported catalog selection: $FoundationCatalogArgument; received ${unsupported.mkString(", ")}")
      }
      val cases = selectedCatalog.filter(testCase =>
        filters.forall(testCase.id.contains))

      val header =
        if (catalogArguments.nonEmpty && filters.isEmpty) {
          "foundation catalog"
        } else if (filters.isEmpty) {
          "full catalog"
        } else {
          s"filter ${filters.mkString(", ")} -> ${cases.size} cases"
        }
      println(s"\n=== delta-harness :: scenario cases @ OpenHouse catalog ($header) ===\n")

      // Each case owns a fresh table. Worker tasks use separate Spark sessions over the shared Spark context, and
      // results are printed in catalog order.
      val parallelism =
        RunnerConfiguration.parallelism(sys.env, Runtime.getRuntime.availableProcessors())
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
            s"  (${failure.reason})"
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

package harness

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.iceberg.exceptions.BadRequestException
import org.apache.iceberg.exceptions.ValidationException
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

/** Runs a case, retrying only a transient-infrastructure failure. */
object Runner {
  val MaxAttempts = 3

  def execute(testCase: Plan.Case, context: Ctx): (Outcome, Int) = {
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

/** Boots the embedded OpenHouse server and wires a SparkSession to the OpenHouse catalog. */
object OpenHouseEnv {
  import com.linkedin.openhouse.tablestest.OpenHouseLocalServer

  private def authToken(): String =
    Option(getClass.getClassLoader.getResourceAsStream("dummy.token"))
      .map(is => scala.io.Source.fromInputStream(is, "UTF-8").mkString.trim)
      .getOrElse("default-token")

  private def wireCatalog(builder: SparkSession.Builder, name: String, uri: String, token: String): SparkSession.Builder =
    builder
      .config(s"spark.sql.catalog.$name", "org.apache.iceberg.spark.SparkCatalog")
      .config(s"spark.sql.catalog.$name.catalog-impl", "com.linkedin.openhouse.spark.OpenHouseCatalog")
      .config(s"spark.sql.catalog.$name.uri", uri)
      .config(s"spark.sql.catalog.$name.cluster", "local-cluster")
      .config(s"spark.sql.catalog.$name.auth-token", token)

  def start(): (OpenHouseLocalServer, SparkSession, String, String) = {
    // The embedded server uses Hibernate to create its H2 schema. Hibernate owns initialization for this process, so
    // classpath SQL initialization stays disabled.
    System.setProperty("spring.sql.init.mode", "never")
    System.setProperty("spring.jpa.hibernate.ddl-auto", "create-drop")

    val server = new OpenHouseLocalServer()
    server.start()
    try {
      val uri = s"http://localhost:${server.getPort}"
      val token = authToken()

      val base = SparkSession.builder()
        .appName("delta-harness-openhouse")
        .master("local[2]")
        .config("spark.sql.extensions",
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
            "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")

      val wired =
        Seq("openhouse", "default_iceberg")
          .foldLeft(base)(wireCatalog(_, _, uri, token))
      (server, wired.getOrCreate(), uri, token)
    } catch {
      case startupFailure: Throwable =>
        try {
          server.stop()
        } catch {
          case cleanupFailure: Throwable =>
            startupFailure.addSuppressed(cleanupFailure)
        }
        throw startupFailure
    }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val (server, spark, restUri, restToken) = OpenHouseEnv.start()
    var runFailure: Option[Throwable] = None
    try {
      spark.sparkContext.setLogLevel("ERROR")
      val ctx = Ctx(spark, "openhouse.dbMatrix", restUri, restToken)

      // Each command-line argument is an include substring. A case runs when its ID contains every provided substring.
      // An empty argument list selects the full catalog.
      val filters = args.toList
      val cases = Plan.cases.filter(testCase =>
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

      def runOne(testCase: Plan.Case): (Plan.Case, (Outcome, Int)) =
        testCase.embeddedSkipReason
          .map(reason => s"embedded limitation: $reason")
          .orElse(Plan.bugReason(testCase)) match {
          case Some(reason) =>
            (testCase, (Outcome.Skipped(reason): Outcome, 0))
          case None =>
            (testCase, Runner.execute(testCase, ctx))
        }

      val results =
        if (parallelism <= 1) {
          cases.map(runOne)
        } else {
          val pool = java.util.concurrent.Executors.newFixedThreadPool(parallelism)
          try {
            val futures = cases.map(testCase =>
              pool.submit(
                new java.util.concurrent.Callable[(Plan.Case, (Outcome, Int))] {
                  def call(): (Plan.Case, (Outcome, Int)) = runOne(testCase)
                }))
            futures.map(_.get(60, java.util.concurrent.TimeUnit.MINUTES))
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

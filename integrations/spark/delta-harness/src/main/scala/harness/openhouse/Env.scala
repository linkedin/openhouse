package harness

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.iceberg.exceptions.BadRequestException
import org.apache.iceberg.exceptions.ValidationException
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

/** Runs a case, retrying only a transient-infrastructure failure. */
object Runner {
  val MaxAttempts = 3

  def execute(c: Plan.Case, ctx: Ctx): (Outcome, Int) = {
    @tailrec def attempt(n: Int): (Outcome, Int) = {
      val outcome =
        try { c.run(ctx); Outcome.Passed }
        catch { case NonFatal(t) => Outcome.Failed(t) }
      outcome match {
        case f: Outcome.Failed if f.retryable && n + 1 < MaxAttempts => attempt(n + 1)
        case terminal                                                => (terminal, n + 1)
      }
    }
    attempt(0)
  }
}

// Boot app for the REAL House Table Service as a 2nd Spring context in-JVM (HTS-embed, Option A).
// Mirrors services/.../e2e/SpringH2HtsApplication's annotation set (test-scope, so replicated here).
// Security auto-config is excluded (spring-security-web is only partially present on the harness
// classpath, and the harness runs unauthenticated) — exactly as the tables boot does.
// internal.catalog.mapper is intentionally NOT scanned (a client-side concern needing FileIOManager;
// the HTS server does not use it). Proven by HtsBootProbe.
@org.springframework.boot.autoconfigure.SpringBootApplication(
  exclude = Array(
    classOf[org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration],
    classOf[org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration]))
@org.springframework.context.annotation.ComponentScan(basePackages = Array(
  "com.linkedin.openhouse.housetables.api",
  "com.linkedin.openhouse.housetables.dto.mapper",
  "com.linkedin.openhouse.housetables.controller",
  "com.linkedin.openhouse.housetables.services",
  "com.linkedin.openhouse.common.exception.handler",
  "com.linkedin.openhouse.common.audit",
  "com.linkedin.openhouse.housetables.repository",
  "com.linkedin.openhouse.housetables.properties",
  "com.linkedin.openhouse.housetables.config",
  "com.linkedin.openhouse.cluster.configs",
  "com.linkedin.openhouse.cluster.storage"))
@org.springframework.boot.autoconfigure.domain.EntityScan(
  basePackages = Array("com.linkedin.openhouse.housetables.model"))
class HtsBootApp

/** Boots the embedded real House Table Service (H2, MySQL-mode) as its own Spring context. */
object HtsEnv {
  import org.springframework.boot.builder.SpringApplicationBuilder
  import org.springframework.boot.web.context.WebServerApplicationContext
  import org.springframework.context.ConfigurableApplicationContext

  /** @return (context, base-uri) for the embedded HTS. */
  def start(): (ConfigurableApplicationContext, String) = {
    val root = System.getProperty("java.io.tmpdir") + "/hts-embed"
    val ctx = new SpringApplicationBuilder(classOf[HtsBootApp])
      .properties(
        "server.port=0",
        "cluster.storage.root-path=" + root,
        "cluster.tables.allowed-client-name-values=trino,spark")
      .run()
    val port = ctx.asInstanceOf[WebServerApplicationContext].getWebServer.getPort
    (ctx, s"http://localhost:$port")
  }
}

/** Boots the embedded OpenHouse server and wires a SparkSession to the OpenHouse catalog. */
object OpenHouseEnv {
  import com.linkedin.openhouse.tablestest.OpenHouseLocalServer
  import org.springframework.context.ConfigurableApplicationContext

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

  def start(): (OpenHouseLocalServer, SparkSession, String, String, Option[ConfigurableApplicationContext]) = {
    // HTS-embed (Option A): when HARNESS_REAL_HTS=1, boot the real House Table Service as a 2nd
    // Spring context, point the embedded tables server's HouseTableRepositoryImpl at it via
    // cluster.housetables.base-uri, and disable the @Primary in-memory stub (openhouse.htsStub.enabled
    // =false) so the real HTTP client is the sole HouseTableRepository. Default (flag unset) keeps the
    // stub — the existing green baseline is always reproducible.
    val realHts = sys.env.get("HARNESS_REAL_HTS").contains("1")
    val htsCtxOpt: Option[ConfigurableApplicationContext] =
      if (realHts) {
        // Boot the HTS context FIRST, while no spring.sql.init.mode System property is set, so it
        // uses its own application.properties (spring.sql.init.mode=always) and runs schema.sql +
        // data.sql on its MySQL-mode H2. The tables-context suppression props below are set AFTER
        // this returns (the HTS context is already fully refreshed), so they don't affect HTS.
        val (ctx, htsUri) = HtsEnv.start()
        HtsAdmin.htsUri = htsUri   // enables the undrop preparation axis (Phase 4)
        System.setProperty("cluster.housetables.base-uri", htsUri)
        System.setProperty("openhouse.htsStub.enabled", "false")
        println(s">> REAL HTS mode: embedded HTS at $htsUri (stub disabled)")
        Some(ctx)
      } else None

    // ALWAYS (both stub and real-HTS modes): housetables-lib.jar is on the harness classpath
    // unconditionally (print-cp.init.gradle pulls it in for the real-HTS path). Its root
    // data.sql/schema.sql are MySQL-dialect and would be auto-run by the TABLES context's H2
    // (non-MySQL mode) → INSERT IGNORE syntax error. The tables side ships no SQL scripts and relies
    // on Hibernate auto-DDL, so (i) never run classpath SQL init for it, and (ii) make auto-DDL
    // explicit (the stray schema.sql otherwise flips Spring Boot's embedded-H2 ddl-auto default to
    // `none`, leaving the tables server's own H2 tables — feature-toggle status/rules — missing).
    // In real-HTS mode this runs AFTER HtsEnv.start(), so the HTS schema (which needs init) is safe.
    System.setProperty("spring.sql.init.mode", "never")
    System.setProperty("spring.jpa.hibernate.ddl-auto", "create-drop")

    val server = new OpenHouseLocalServer()
    server.start()
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

    val wired = Seq("openhouse", "default_iceberg").foldLeft(base)(wireCatalog(_, _, uri, token))
    (server, wired.getOrCreate(), uri, token, htsCtxOpt)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val (server, spark, restUri, restToken, htsCtxOpt) = OpenHouseEnv.start()
    spark.sparkContext.setLogLevel("ERROR")
    HtsAdmin.tablesUri = restUri; HtsAdmin.token = restToken   // undrop restore path (Phase 4)
    val ctx = Ctx(spark, "openhouse.dbMatrix", restUri, restToken)

    // Each command-line arg is an include-substring; a case runs only if its id contains ALL of
    // them (AND). No args = run everything.
    val filters = args.toList
    def selected(id: String): Boolean = filters.forall(id.contains)
    val cases = Plan.cases.filter(c => selected(c.id))

    val header = if (filters.isEmpty) "all cases" else s"filter ${filters.mkString(", ")} -> ${cases.size} cases"
    println(s"\n=== delta-harness :: localized cases @ OpenHouse catalog ($header) ===\n")

    // Known-bug cases are tagged (Plan.knownBugs) and reported SKIP rather than run — deferred,
    // not passing. Everything else executes.
    //
    // Cases are independent (each owns its table via the atomic counter), so they run on a worker
    // pool. Each worker task gets its OWN SparkSession (spark.newSession(): separate SQLConf —
    // isolating the session-global state some tests mutate, e.g. spark.wap.branch/wap.id and
    // changelog temp views — over the shared SparkContext). Results are collected and printed in
    // the original case order, so output is identical to a sequential run.
    // HARNESS_PARALLELISM overrides; <=1 falls back to the sequential path.
    val parallelism = sys.env.get("HARNESS_PARALLELISM").map(_.toInt)
      .getOrElse(math.max(1, Runtime.getRuntime.availableProcessors()))
    println(s"parallelism: $parallelism worker sessions\n")

    def runOne(c: Plan.Case): (String, (Outcome, Int)) =
      Plan.bugReason(c.id) match {
        case Some(reason) => (c.id, (Outcome.Skipped(reason): Outcome, 0))
        case None         => (c.id, Runner.execute(c, ctx.copy(spark = ctx.spark.newSession())))
      }

    val results =
      if (parallelism <= 1) cases.map(runOne)
      else {
        val pool = java.util.concurrent.Executors.newFixedThreadPool(parallelism)
        try {
          val futures = cases.map(c => pool.submit(new java.util.concurrent.Callable[(String, (Outcome, Int))] {
            def call(): (String, (Outcome, Int)) = runOne(c)
          }))
          futures.map(_.get(60, java.util.concurrent.TimeUnit.MINUTES))
        } finally pool.shutdown()
      }

    results.foreach { case (id, (outcome, attempts)) =>
      val note = outcome match {
        case f: Outcome.Failed       => s"  (${f.reason}${if (f.retryable) " [retryable]" else ""})"
        case Outcome.Skipped(reason) => s"  ($reason)"
        case Outcome.Passed          => ""
      }
      println(f"${outcome.label}%-4s ${id}%-52s try=$attempts$note")
    }

    val failed = results.count { case (_, (outcome, _)) => outcome.isInstanceOf[Outcome.Failed] }
    val skipped = results.count { case (_, (outcome, _)) => outcome.isInstanceOf[Outcome.Skipped] }
    val passed = results.size - failed - skipped
    println(f"\n$passed passed, $skipped skipped, $failed failed  (${results.size} cases)")
    if (passed == 0) println("WARNING: no case actually passed (empty selection or all skipped) — reporting failure")

    try spark.stop() catch { case _: Throwable => () }
    try server.stop() catch { case _: Throwable => () }
    htsCtxOpt.foreach(ctx => try ctx.close() catch { case _: Throwable => () })
    // A run that validated nothing (0 cases, or everything skipped) is NOT success.
    System.exit(if (failed == 0 && passed > 0) 0 else 1)
  }
}

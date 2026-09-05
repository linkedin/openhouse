package harness

import org.apache.spark.sql.SparkSession
import scala.io.Source

/**
 * Embedded environment wiring: boots the in-process OpenHouse server and hands back a SparkSession pointed at its
 * catalog. This file is compiled into the `local` source set only, because it pulls in the server test fixtures that
 * the published portable library leaves out.
 */
object OpenHouseEnv {
  import com.linkedin.openhouse.tablestest.OpenHouseLocalServer

  private def authToken(): String =
    Option(getClass.getClassLoader.getResourceAsStream("dummy.token"))
      .map(tokenStream => Source.fromInputStream(tokenStream, "UTF-8").mkString.trim)
      .getOrElse("default-token")

  private def wireCatalog(
      builder: SparkSession.Builder,
      name: String,
      uri: String,
      token: String): SparkSession.Builder =
    builder
      .config(s"spark.sql.catalog.$name", "org.apache.iceberg.spark.SparkCatalog")
      .config(
        s"spark.sql.catalog.$name.catalog-impl",
        "com.linkedin.openhouse.spark.OpenHouseCatalog")
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

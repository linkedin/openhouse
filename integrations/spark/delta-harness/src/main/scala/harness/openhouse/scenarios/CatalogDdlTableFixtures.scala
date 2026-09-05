package harness

import org.apache.spark.sql.SparkSession

/**
 * Iceberg metadata and side-table lifecycle fixtures used by catalog DDL scenarios.
 */
trait CatalogDdlTableFixtures extends RtasTableFixtures {

  protected final def icebergTableOf(spark: SparkSession, table: String): org.apache.iceberg.Table =
    org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, table)

  /** Runs a side-table operation and drops the table after the operation completes. */
  protected def withOwnedTable(runStatement: String => Unit, table: String)(
      create: => Unit)(use: => Unit): Unit =
    OwnedTableLifecycle.withOwnership(runStatement(s"DROP TABLE IF EXISTS $table")) {
      markTableCreated =>
        create
        markTableCreated()
        use
    }

  /** Runs an operation and then executes the supplied cleanup statement on every outcome. */
  protected def withCleanupStatement(runStatement: String => Unit, cleanupStatement: String)(
      use: => Unit): Unit =
    OwnedTableLifecycle.withCleanup(runStatement(cleanupStatement))(use)
}

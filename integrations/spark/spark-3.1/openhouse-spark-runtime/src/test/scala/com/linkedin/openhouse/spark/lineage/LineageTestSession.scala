package com.linkedin.openhouse.spark.lineage

import org.apache.spark.sql.SparkSession

import java.nio.file.Files

/**
 * Shared Spark session for the lineage tests.
 *
 * Tables live in a throwaway Iceberg Hadoop warehouse under the `local` catalog, which mirrors how
 * OpenHouse tables are exposed to Spark (a DataSourceV2 catalog) without requiring the /tables
 * service to be up.
 */
object LineageTestSession {

  lazy val spark: SparkSession = {
    val warehouse = Files.createTempDirectory("openhouse-lineage-test").toString
    val session = SparkSession
      .builder()
      .master("local[1]")
      .appName("openhouse-lineage-test")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
          "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", warehouse)
      .config("spark.sql.warehouse.dir", warehouse + "/session")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    createTables(session)
    session
  }

  private def createTables(spark: SparkSession): Unit = {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.db.orders (
        order_id BIGINT,
        customer_id BIGINT,
        product_id BIGINT,
        quantity INT,
        unit_price DOUBLE,
        discount DOUBLE,
        region STRING,
        order_date DATE
      ) USING iceberg
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.db.customers (
        customer_id BIGINT,
        customer_name STRING,
        email STRING,
        country STRING,
        tier STRING
      ) USING iceberg
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.db.products (
        product_id BIGINT,
        product_name STRING,
        category STRING,
        list_price DOUBLE
      ) USING iceberg
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.db.order_facts (
        order_id BIGINT,
        customer_id BIGINT,
        revenue DOUBLE,
        region STRING
      ) USING iceberg
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.db.customer_360 (
        customer_id BIGINT,
        customer_name STRING,
        lifetime_revenue DOUBLE,
        order_count BIGINT,
        load_id STRING
      ) USING iceberg
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.db.customer_updates (
        customer_id BIGINT,
        customer_name STRING,
        revenue DOUBLE
      ) USING iceberg
    """)
  }
}

/** Convenience helpers shared by the lineage test classes. */
trait LineageTestHelpers {

  protected def spark: SparkSession = LineageTestSession.spark

  /** Analyses (but does not execute) `sql` and returns the extracted lineage. */
  protected def lineageOf(sql: String): SqlLineage =
    SqlLineageExtractor
      .extractFromSql(spark, sql)
      .getOrElse(throw new AssertionError(s"No lineage extracted for: $sql"))

  protected def tableNames(lineage: SqlLineage): Set[String] =
    lineage.inputTables.map(_.qualifiedName).toSet

  protected def sourceNames(lineage: SqlLineage, column: String): Set[String] =
    lineage.sourcesOf(column).map(_.qualifiedName).toSet

  protected def conditionColumns(lineage: SqlLineage, kind: String): Set[String] =
    lineage.conditions.filter(_.kind == kind).flatMap(_.columns).map(_.qualifiedName).toSet

  protected def columnNames(lineage: SqlLineage): Seq[String] = lineage.columnLineage.map(_.column)
}

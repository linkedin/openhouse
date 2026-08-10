package com.linkedin.openhouse.spark.lineage

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.nio.file.Files

/**
 * Runs the lineage extractor - compiled once against Spark 3.1 and repackaged into this artifact -
 * against Spark 3.5.
 *
 * Several plan nodes changed shape between the two releases (`CreateTableAsSelect` swapped its
 * constructor, `MergeIntoTable` gained an argument, CTEs are no longer inlined at analysis time), so
 * this suite is the guard that the accessor-based extraction in `PlanAccessors` really is
 * version-independent.
 */
class SqlLineageExtractorTestSpark3_5 {

  private def spark: SparkSession = Spark35LineageTestSession.spark

  private def lineageOf(sql: String): SqlLineage =
    SqlLineageExtractor
      .extractFromSql(spark, sql)
      .getOrElse(throw new AssertionError(s"No lineage extracted for: $sql"))

  private def tableNames(lineage: SqlLineage): Set[String] =
    lineage.inputTables.map(_.qualifiedName).toSet

  private def sourceNames(lineage: SqlLineage, column: String): Set[String] =
    lineage.sourcesOf(column).map(_.qualifiedName).toSet

  private def conditionColumns(lineage: SqlLineage, kind: String): Set[String] =
    lineage.conditions.filter(_.kind == kind).flatMap(_.columns).map(_.qualifiedName).toSet

  private def transformationOf(lineage: SqlLineage, column: String): String =
    lineage.columnLineage
      .find(_.column == column)
      .map(_.transformation)
      .getOrElse(throw new AssertionError(s"No column lineage for: $column"))

  @Test
  def selectLineageWorksOnSpark35(): Unit = {
    val lineage = lineageOf("""
      SELECT o.order_id, c.customer_name, o.quantity * o.unit_price AS revenue
      FROM local.db.orders o
      JOIN local.db.customers c ON o.customer_id = c.customer_id
      WHERE c.tier = 'GOLD'""")

    assertEquals(Set("local.db.orders", "local.db.customers"), tableNames(lineage))
    assertEquals(
      Set("local.db.orders.quantity", "local.db.orders.unit_price"),
      sourceNames(lineage, "revenue"))
    assertEquals(Set("local.db.customers.tier"), conditionColumns(lineage, ConditionKind.Filter))
    assertEquals(
      Set("local.db.orders.customer_id", "local.db.customers.customer_id"),
      conditionColumns(lineage, ConditionKind.Join))
  }

  @Test
  def ctasTargetIsResolvedDespiteTheChangedNodeSignature(): Unit = {
    val lineage = lineageOf("""
      CREATE TABLE local.db.ctas_target_35 USING iceberg AS
      SELECT order_id, quantity * unit_price AS revenue FROM local.db.orders""")

    assertEquals(LineageOperation.CreateTableAsSelect, lineage.operation)
    assertEquals(Some("local.db.ctas_target_35"), lineage.outputTable.map(_.qualifiedName))
    assertEquals(
      Set("local.db.orders.quantity", "local.db.orders.unit_price"),
      sourceNames(lineage, "revenue"))
  }

  @Test
  def insertIntoMapsOntoTargetColumnsOnSpark35(): Unit = {
    val lineage = lineageOf("""
      INSERT INTO local.db.order_facts
      SELECT order_id, customer_id, quantity * unit_price, region FROM local.db.orders""")

    assertEquals(LineageOperation.InsertInto, lineage.operation)
    assertEquals(Some("local.db.order_facts"), lineage.outputTable.map(_.qualifiedName))
    assertEquals(Seq("order_id", "customer_id", "revenue", "region"), lineage.columnLineage.map(_.column))
    assertEquals(
      Set("local.db.orders.quantity", "local.db.orders.unit_price"),
      sourceNames(lineage, "revenue"))
  }

  @Test
  def mergeIsReadThroughAccessorsDespiteTheExtraArgument(): Unit = {
    val lineage = lineageOf("""
      MERGE INTO local.db.customer_360 t
      USING local.db.customer_updates s
      ON t.customer_id = s.customer_id
      WHEN MATCHED THEN UPDATE SET t.lifetime_revenue = t.lifetime_revenue + s.revenue
      WHEN NOT MATCHED THEN
        INSERT (customer_id, customer_name, lifetime_revenue, order_count, load_id)
        VALUES (s.customer_id, s.customer_name, s.revenue, 1, 'merge')""")

    assertEquals(LineageOperation.Merge, lineage.operation)
    assertEquals(Some("local.db.customer_360"), lineage.outputTable.map(_.qualifiedName))
    assertEquals(
      Set("local.db.customer_360", "local.db.customer_updates"),
      tableNames(lineage))
    assertEquals(
      Set("local.db.customer_360.lifetime_revenue", "local.db.customer_updates.revenue"),
      sourceNames(lineage, "lifetime_revenue"))
    // Every branch that can assign the column is folded into one entry, exactly as on Spark 3.1:
    // matched UPDATE, not-matched INSERT and the untouched carry-over of the existing row.
    assertEquals(
      Set("(lifetime_revenue + revenue)", "lifetime_revenue", "revenue"),
      transformationOf(lineage, "lifetime_revenue").split(" \\| ").toSet)

    // Spark 3.4+ rewrites MERGE into a join before lineage sees it, so the ON predicate arrives as a
    // JOIN condition rather than the MERGE_ON that the un-rewritten Spark 3.1 node reports. Either
    // way the same key columns are attributed.
    assertEquals(
      Set("local.db.customer_360.customer_id", "local.db.customer_updates.customer_id"),
      conditionColumns(lineage, ConditionKind.Join))
  }

  @Test
  def rewrittenRowLevelWritesDoNotLeakInternalRowTrackingColumns(): Unit = {
    // The rewrite appends `_file` / `_pos` metadata columns to the query so Spark can locate the
    // rows it replaces. They are not table columns and must not show up as lineage.
    val lineage = lineageOf("DELETE FROM local.db.order_facts WHERE region = 'APAC'")

    assertEquals(
      Seq("order_id", "customer_id", "revenue", "region"),
      lineage.columnLineage.map(_.column))
  }

  @Test
  def cteReferencesAreLinkedBackToTheirDefinition(): Unit = {
    // Spark 3.4+ keeps CTEs as CTERelationDef/CTERelationRef instead of inlining them at analysis.
    val lineage = lineageOf("""
      WITH big_orders AS (
        SELECT customer_id, quantity * unit_price AS revenue
        FROM local.db.orders WHERE quantity > 100
      )
      SELECT c.customer_name, SUM(b.revenue) AS lifetime_revenue
      FROM big_orders b JOIN local.db.customers c ON b.customer_id = c.customer_id
      GROUP BY c.customer_name""")

    assertEquals(Set("local.db.orders", "local.db.customers"), tableNames(lineage))
    assertEquals(
      Set("local.db.orders.quantity", "local.db.orders.unit_price"),
      sourceNames(lineage, "lifetime_revenue"))
    // The filter inside the CTE body is indirect lineage of the outer statement.
    assertEquals(
      Set("local.db.orders.quantity"),
      conditionColumns(lineage, ConditionKind.Filter))
  }

  @Test
  def updateAndDeleteReportTargetAndPredicate(): Unit = {
    val update =
      lineageOf("UPDATE local.db.order_facts SET revenue = revenue * 1.1 WHERE region = 'EU'")
    assertEquals(LineageOperation.Update, update.operation)
    assertEquals(Some("local.db.order_facts"), update.outputTable.map(_.qualifiedName))
    // Copy-on-write turns the UPDATE into a full rewrite of the matching files, so every column is
    // reported: the assigned one through its formula, the rest as a conditional carry-over. The
    // predicate column is a source of all of them, which is what actually decides their new value.
    assertEquals(
      Set("local.db.order_facts.revenue", "local.db.order_facts.region"),
      sourceNames(update, "revenue"))
    assertTrue(
      transformationOf(update, "revenue").contains("revenue * "),
      s"expected the assignment formula, got: ${transformationOf(update, "revenue")}")
    assertEquals(
      Set("local.db.order_facts.region"),
      sourceNames(update, "order_id") -- Set("local.db.order_facts.order_id"))

    val delete = lineageOf("DELETE FROM local.db.order_facts WHERE region = 'APAC'")
    assertEquals(LineageOperation.Delete, delete.operation)
    assertEquals(Some("local.db.order_facts"), delete.outputTable.map(_.qualifiedName))
    assertEquals(
      Set("local.db.order_facts.region"),
      conditionColumns(delete, ConditionKind.Filter))
  }
}

private object Spark35LineageTestSession {

  lazy val spark: SparkSession = {
    val warehouse = Files.createTempDirectory("openhouse-lineage-test-35").toString
    val session = SparkSession
      .builder()
      .master("local[1]")
      .appName("openhouse-lineage-test-3.5")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
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
        order_id BIGINT, customer_id BIGINT, quantity INT, unit_price DOUBLE, region STRING
      ) USING iceberg""")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.db.customers (
        customer_id BIGINT, customer_name STRING, tier STRING
      ) USING iceberg""")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.db.order_facts (
        order_id BIGINT, customer_id BIGINT, revenue DOUBLE, region STRING
      ) USING iceberg""")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.db.customer_360 (
        customer_id BIGINT, customer_name STRING, lifetime_revenue DOUBLE,
        order_count BIGINT, load_id STRING
      ) USING iceberg""")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.db.customer_updates (
        customer_id BIGINT, customer_name STRING, revenue DOUBLE
      ) USING iceberg""")
  }
}

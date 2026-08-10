package com.linkedin.openhouse.spark.lineage

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Showcase of the lineage that can be recovered from a Spark SQL statement.
 *
 * Every test states a SQL shape and asserts exactly which table- and column-level facts the
 * extractor is able to report for it: which tables were read, which table was written, which
 * upstream columns each output column is derived from, through which expression, and which columns
 * only influenced row selection.
 *
 * The statements are analysed, never executed, so none of them mutates the test warehouse.
 */
class SqlLineageExtractorTest extends LineageTestHelpers {

  // ---------------------------------------------------------------------------------------------
  // Reads
  // ---------------------------------------------------------------------------------------------

  @Test
  def plainSelectReportsInputTableAndIdentityColumns(): Unit = {
    val lineage = lineageOf("SELECT order_id, quantity FROM local.db.orders")

    assertEquals(LineageOperation.Select, lineage.operation)
    assertEquals(None, lineage.outputTable)
    assertEquals(Set("local.db.orders"), tableNames(lineage))
    assertEquals(Seq("order_id", "quantity"), columnNames(lineage))
    assertEquals(Set("local.db.orders.order_id"), sourceNames(lineage, "order_id"))
    assertTrue(lineage.columnLineageFor("order_id").exists(_.isIdentity))
  }

  @Test
  def selectStarExpandsToEveryColumnOfTheTable(): Unit = {
    val lineage = lineageOf("SELECT * FROM local.db.customers")

    assertEquals(
      Seq("customer_id", "customer_name", "email", "country", "tier"),
      columnNames(lineage))
    assertEquals(Set("local.db.customers.email"), sourceNames(lineage, "email"))
  }

  @Test
  def aliasKeepsTargetNameWhileSourceStaysTheOriginalColumn(): Unit = {
    val lineage = lineageOf("SELECT customer_name AS full_name FROM local.db.customers")

    assertEquals(Seq("full_name"), columnNames(lineage))
    assertEquals(Set("local.db.customers.customer_name"), sourceNames(lineage, "full_name"))
    assertEquals(TransformationType.Identity, lineage.columnLineage.head.transformationType)
  }

  // ---------------------------------------------------------------------------------------------
  // Column-level derivation
  // ---------------------------------------------------------------------------------------------

  @Test
  def computedColumnReportsEveryContributingColumnAndTheFormula(): Unit = {
    val lineage = lineageOf(
      "SELECT quantity * unit_price * (1 - discount) AS net_revenue FROM local.db.orders")

    val revenue = lineage.columnLineage.head
    assertEquals("net_revenue", revenue.column)
    assertEquals(TransformationType.Expression, revenue.transformationType)
    assertEquals(
      Set(
        "local.db.orders.quantity",
        "local.db.orders.unit_price",
        "local.db.orders.discount"),
      revenue.sources.map(_.qualifiedName).toSet)
    assertTrue(
      revenue.transformation.contains("quantity") &&
        revenue.transformation.contains("unit_price") &&
        revenue.transformation.contains("discount"),
      s"formula should name its inputs but was '${revenue.transformation}'")
  }

  @Test
  def caseExpressionCollectsColumnsFromEveryBranch(): Unit = {
    val lineage = lineageOf("""
      SELECT CASE WHEN quantity > 100 THEN 'BULK'
                  WHEN unit_price > 500 THEN 'PREMIUM'
                  ELSE region END AS bucket
      FROM local.db.orders""")

    assertEquals(
      Set("local.db.orders.quantity", "local.db.orders.unit_price", "local.db.orders.region"),
      sourceNames(lineage, "bucket"))
    assertTrue(lineage.columnLineage.head.transformation.startsWith("CASE WHEN"))
  }

  @Test
  def literalColumnHasNoUpstreamColumn(): Unit = {
    val lineage = lineageOf("SELECT order_id, 'batch-42' AS load_id FROM local.db.orders")

    val loadId = lineage.columnLineageFor("load_id").get
    assertEquals(TransformationType.Literal, loadId.transformationType)
    assertTrue(loadId.sources.isEmpty)
    assertEquals("'batch-42'", loadId.transformation)
  }

  @Test
  def aggregateColumnsAreClassifiedAndGroupingKeysReportedAsIndirectLineage(): Unit = {
    val lineage = lineageOf("""
      SELECT region,
             SUM(quantity * unit_price) AS revenue,
             COUNT(DISTINCT customer_id) AS buyers
      FROM local.db.orders
      GROUP BY region""")

    assertEquals(
      TransformationType.Aggregation,
      lineage.columnLineageFor("revenue").get.transformationType)
    assertEquals(
      Set("local.db.orders.quantity", "local.db.orders.unit_price"),
      sourceNames(lineage, "revenue"))
    assertEquals(Set("local.db.orders.customer_id"), sourceNames(lineage, "buyers"))
    assertTrue(lineage.columnLineageFor("buyers").get.transformation.contains("count"))
    assertEquals(Set("local.db.orders.region"), conditionColumns(lineage, ConditionKind.GroupBy))
  }

  @Test
  def windowFunctionReportsPartitionAndOrderColumnsAsSources(): Unit = {
    val lineage = lineageOf("""
      SELECT order_id,
             ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
      FROM local.db.orders""")

    val rn = lineage.columnLineageFor("rn").get
    assertEquals(TransformationType.Window, rn.transformationType)
    assertEquals(
      Set("local.db.orders.customer_id", "local.db.orders.order_date"),
      rn.sources.map(_.qualifiedName).toSet)
    assertTrue(rn.transformation.contains("row_number"))
  }

  @Test
  def nestedSubqueryIsFlattenedIntoASingleFormula(): Unit = {
    val lineage = lineageOf("""
      SELECT t.total * 2 AS doubled
      FROM (SELECT quantity * unit_price AS total FROM local.db.orders) t""")

    assertEquals(
      Set("local.db.orders.quantity", "local.db.orders.unit_price"),
      sourceNames(lineage, "doubled"))
    val formula = lineage.columnLineage.head.transformation
    assertTrue(
      formula.contains("quantity") && formula.contains("unit_price") && formula.contains("2"),
      s"intermediate alias should be inlined but was '$formula'")
  }

  @Test
  def unionMergesSourcesOfBothBranchesIntoOneColumn(): Unit = {
    val lineage = lineageOf("""
      SELECT customer_id, country FROM local.db.customers
      UNION ALL
      SELECT customer_id, region FROM local.db.orders""")

    assertEquals(Set("local.db.customers", "local.db.orders"), tableNames(lineage))
    assertEquals(
      Set("local.db.customers.country", "local.db.orders.region"),
      sourceNames(lineage, "country"))
  }

  // ---------------------------------------------------------------------------------------------
  // Multi-table reads and indirect lineage
  // ---------------------------------------------------------------------------------------------

  @Test
  def joinReportsEveryInputTableAndTheJoinKeys(): Unit = {
    val lineage = lineageOf("""
      SELECT o.order_id, p.category, o.quantity * p.list_price AS list_revenue
      FROM local.db.orders o
      JOIN local.db.products p ON o.product_id = p.product_id
      JOIN local.db.customers c ON o.customer_id = c.customer_id
      WHERE c.tier = 'GOLD' AND p.category = 'BOOKS'""")

    assertEquals(
      Set("local.db.orders", "local.db.products", "local.db.customers"),
      tableNames(lineage))
    assertEquals(Set("local.db.products.category"), sourceNames(lineage, "category"))
    assertEquals(
      Set("local.db.orders.quantity", "local.db.products.list_price"),
      sourceNames(lineage, "list_revenue"))

    assertEquals(
      Set(
        "local.db.orders.product_id",
        "local.db.products.product_id",
        "local.db.orders.customer_id",
        "local.db.customers.customer_id"),
      conditionColumns(lineage, ConditionKind.Join))
  }

  @Test
  def filterColumnsAreReportedSeparatelyFromProjectedColumns(): Unit = {
    val lineage =
      lineageOf("SELECT order_id FROM local.db.orders WHERE region = 'US' AND quantity > 10")

    assertEquals(Set("local.db.orders.order_id"), sourceNames(lineage, "order_id"))
    assertEquals(
      Set("local.db.orders.region", "local.db.orders.quantity"),
      conditionColumns(lineage, ConditionKind.Filter))
  }

  @Test
  def subqueryInWhereClauseContributesItsTableAndFilterColumns(): Unit = {
    val lineage = lineageOf("""
      SELECT order_id FROM local.db.orders
      WHERE customer_id IN (SELECT customer_id FROM local.db.customers WHERE tier = 'GOLD')""")

    assertEquals(Set("local.db.orders", "local.db.customers"), tableNames(lineage))
    assertTrue(
      conditionColumns(lineage, ConditionKind.Filter).contains("local.db.customers.tier"),
      "filter columns inside the subquery must be captured")
  }

  @Test
  def cteLineageResolvesThroughToTheBaseTables(): Unit = {
    val lineage = lineageOf("""
      WITH big_orders AS (
        SELECT customer_id, quantity * unit_price AS revenue
        FROM local.db.orders WHERE quantity > 100
      )
      SELECT c.customer_name, SUM(b.revenue) AS lifetime_revenue
      FROM big_orders b JOIN local.db.customers c ON b.customer_id = c.customer_id
      GROUP BY c.customer_name""")

    assertEquals(Set("local.db.orders", "local.db.customers"), tableNames(lineage))
    assertEquals(Set("local.db.customers.customer_name"), sourceNames(lineage, "customer_name"))
    assertEquals(
      Set("local.db.orders.quantity", "local.db.orders.unit_price"),
      sourceNames(lineage, "lifetime_revenue"))
    assertTrue(
      conditionColumns(lineage, ConditionKind.Filter).contains("local.db.orders.quantity"),
      "a filter inside the CTE must still be reported")
  }

  @Test
  def selfJoinKeepsBothSidesPointingAtTheSameTable(): Unit = {
    val lineage = lineageOf("""
      SELECT a.order_id, b.order_id AS previous_order_id
      FROM local.db.orders a JOIN local.db.orders b ON a.customer_id = b.customer_id""")

    assertEquals(Set("local.db.orders"), tableNames(lineage))
    assertEquals(Set("local.db.orders.order_id"), sourceNames(lineage, "order_id"))
    assertEquals(Set("local.db.orders.order_id"), sourceNames(lineage, "previous_order_id"))
  }

  // ---------------------------------------------------------------------------------------------
  // Writes
  // ---------------------------------------------------------------------------------------------

  @Test
  def insertIntoMapsQueryColumnsOntoTargetTableColumns(): Unit = {
    val lineage = lineageOf("""
      INSERT INTO local.db.order_facts
      SELECT order_id, customer_id, quantity * unit_price, region FROM local.db.orders""")

    assertEquals(LineageOperation.InsertInto, lineage.operation)
    assertEquals(Some("local.db.order_facts"), lineage.outputTable.map(_.qualifiedName))
    assertEquals(Set("local.db.orders"), tableNames(lineage))
    // Column names come from the target table, not from the SELECT list.
    assertEquals(Seq("order_id", "customer_id", "revenue", "region"), columnNames(lineage))
    assertEquals(
      Set("local.db.orders.quantity", "local.db.orders.unit_price"),
      sourceNames(lineage, "revenue"))
    assertTrue(lineage.columnLineageFor("region").exists(_.isIdentity))
  }

  @Test
  def insertOverwriteIsDistinguishedFromInsertInto(): Unit = {
    val lineage = lineageOf("""
      INSERT OVERWRITE local.db.order_facts
      SELECT order_id, customer_id, quantity * unit_price, region
      FROM local.db.orders WHERE region = 'US'""")

    assertEquals(LineageOperation.InsertOverwrite, lineage.operation)
    assertEquals(Some("local.db.order_facts"), lineage.outputTable.map(_.qualifiedName))
    assertEquals(Set("local.db.orders.region"), conditionColumns(lineage, ConditionKind.Filter))
  }

  @Test
  def ctasReportsTheNewTableAndTheColumnsItIsBuiltFrom(): Unit = {
    val lineage = lineageOf("""
      CREATE TABLE local.db.ctas_target USING iceberg AS
      SELECT o.order_id,
             c.customer_name,
             o.quantity * o.unit_price AS revenue
      FROM local.db.orders o
      JOIN local.db.customers c ON o.customer_id = c.customer_id
      WHERE c.country = 'US'""")

    assertEquals(LineageOperation.CreateTableAsSelect, lineage.operation)
    assertEquals(Some("local.db.ctas_target"), lineage.outputTable.map(_.qualifiedName))
    assertEquals(Set("local.db.orders", "local.db.customers"), tableNames(lineage))
    assertEquals(Seq("order_id", "customer_name", "revenue"), columnNames(lineage))
    assertEquals(Set("local.db.customers.customer_name"), sourceNames(lineage, "customer_name"))
    assertEquals(
      Set("local.db.orders.quantity", "local.db.orders.unit_price"),
      sourceNames(lineage, "revenue"))
    assertEquals(Set("local.db.customers.country"), conditionColumns(lineage, ConditionKind.Filter))
  }

  @Test
  def mergeReportsPerColumnAssignmentsFromEveryBranch(): Unit = {
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
    // A MERGE reads the target as well as the source.
    assertEquals(
      Set("local.db.customer_360", "local.db.customer_updates"),
      tableNames(lineage))
    assertEquals(
      Set("local.db.customer_360.lifetime_revenue", "local.db.customer_updates.revenue"),
      sourceNames(lineage, "lifetime_revenue"))
    assertEquals(
      Set("local.db.customer_360.customer_id", "local.db.customer_updates.customer_id"),
      conditionColumns(lineage, ConditionKind.MergeOn))
  }

  @Test
  def updateReportsTheAssignedColumnAndThePredicate(): Unit = {
    val lineage =
      lineageOf("UPDATE local.db.order_facts SET revenue = revenue * 1.1 WHERE region = 'EU'")

    assertEquals(LineageOperation.Update, lineage.operation)
    assertEquals(Some("local.db.order_facts"), lineage.outputTable.map(_.qualifiedName))
    val revenue = lineage.columnLineageFor("revenue").get
    assertEquals(Set("local.db.order_facts.revenue"), revenue.sources.map(_.qualifiedName).toSet)
    assertEquals(TransformationType.Expression, revenue.transformationType)
    assertEquals(
      Set("local.db.order_facts.region"),
      conditionColumns(lineage, ConditionKind.Filter))
  }

  @Test
  def deleteReportsOnlyTheTargetTableAndThePredicate(): Unit = {
    val lineage = lineageOf("DELETE FROM local.db.order_facts WHERE region = 'APAC'")

    assertEquals(LineageOperation.Delete, lineage.operation)
    assertEquals(Some("local.db.order_facts"), lineage.outputTable.map(_.qualifiedName))
    assertTrue(lineage.columnLineage.isEmpty)
    assertEquals(
      Set("local.db.order_facts.region"),
      conditionColumns(lineage, ConditionKind.Filter))
  }

  // ---------------------------------------------------------------------------------------------
  // Statements that carry no lineage, and serialization
  // ---------------------------------------------------------------------------------------------

  @Test
  def statementsWithoutTablesProduceNoLineage(): Unit = {
    assertTrue(SqlLineageExtractor.extractFromSql(spark, "SHOW NAMESPACES IN local").isEmpty)
    assertTrue(SqlLineageExtractor.extractFromSql(spark, "SELECT 1").isEmpty)
  }

  @Test
  def lineageSerializesToSingleLineJsonForEventEmission(): Unit = {
    val lineage = lineageOf("""
      INSERT INTO local.db.order_facts
      SELECT order_id, customer_id, quantity * unit_price, region FROM local.db.orders""")

    val json = lineage.toJson
    assertFalse(json.contains("\n"), "payload must be a single line")
    assertTrue(json.contains("\"operation\":\"INSERT_INTO\""))
    assertTrue(json.contains("\"outputTable\":\"local.db.order_facts\""))
    assertTrue(json.contains("\"inputTables\":[\"local.db.orders\"]"))
    assertTrue(json.contains("\"local.db.orders.unit_price\""))
  }
}

package com.linkedin.openhouse.spark.lineage

import org.apache.log4j.{Level, Logger, PatternLayout, WriterAppender}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.io.StringWriter

/**
 * Shows how lineage is captured for statements that actually run, which is how it would be wired in
 * a cluster:
 * {{{
 *   --conf spark.sql.queryExecutionListeners=com.linkedin.openhouse.spark.lineage.OpenhouseLineageListener
 * }}}
 *
 * The listener hands every statement to a [[LineageSink]]. Here the sink collects events in memory
 * so they can be asserted on; in production it would be swapped for the Kafka publisher, and by
 * default it is [[LogLineageSink]], which writes the lineage to the driver log.
 */
class OpenhouseLineageListenerTest extends LineageTestHelpers {

  private val AwaitTimeoutMs = 30000L

  @Test
  def listenerCapturesLineageOfExecutedWriteStatements(): Unit = {
    withListener { sink =>
      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.listener_source
        (id BIGINT, amount DOUBLE, region STRING) USING iceberg""")
      spark.sql("INSERT INTO local.db.listener_source VALUES (1, 10.0, 'US'), (2, 20.0, 'EU')")
      sink.clear()

      spark.sql("""
        CREATE TABLE local.db.listener_target USING iceberg AS
        SELECT id, amount * 2 AS double_amount FROM local.db.listener_source WHERE region = 'US'""")

      val lineage = await(sink, _.outputTable.exists(_.qualifiedName == "local.db.listener_target"))
      assertEquals(LineageOperation.CreateTableAsSelect, lineage.operation)
      assertEquals(Set("local.db.listener_source"), tableNames(lineage))
      assertEquals(
        Set("local.db.listener_source.amount"),
        sourceNames(lineage, "double_amount"))
      assertEquals(
        Set("local.db.listener_source.region"),
        conditionColumns(lineage, ConditionKind.Filter))
    }
  }

  @Test
  def listenerCapturesLineageOfExecutedReadStatements(): Unit = {
    withListener { sink =>
      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.listener_source
        (id BIGINT, amount DOUBLE, region STRING) USING iceberg""")
      sink.clear()

      spark.sql("SELECT region, SUM(amount) AS total FROM local.db.listener_source GROUP BY region")
        .collect()

      val lineage = await(sink, l => tableNames(l).contains("local.db.listener_source"))
      assertEquals(LineageOperation.Select, lineage.operation)
      assertEquals(Set("local.db.listener_source.amount"), sourceNames(lineage, "total"))
      assertEquals(
        TransformationType.Aggregation,
        lineage.columnLineageFor("total").get.transformationType)
    }
  }

  @Test
  def defaultSinkWritesOneJsonLinePerStatementToTheLog(): Unit = {
    val lineage = lineageOf("""
      INSERT INTO local.db.order_facts
      SELECT order_id, customer_id, quantity * unit_price, region FROM local.db.orders""")

    val captured = new StringWriter()
    val appender = new WriterAppender(new PatternLayout("%m%n"), captured)
    val logger = Logger.getLogger(classOf[LogLineageSink])
    val originalLevel = logger.getLevel
    logger.addAppender(appender)
    logger.setLevel(Level.INFO)
    try {
      new LogLineageSink().emit(lineage)
    } finally {
      logger.removeAppender(appender)
      logger.setLevel(originalLevel)
    }

    val logged = captured.toString
    assertTrue(logged.contains("openhouse-lineage"), s"nothing logged, got '$logged'")
    assertTrue(logged.contains("\"outputTable\":\"local.db.order_facts\""))
    assertTrue(logged.contains("local.db.orders.unit_price"))
    assertEquals(1, logged.trim.split("\n").length, "lineage must be a single log line")
  }

  @Test
  def aFailingSinkNeverBreaksTheQueryThatProducedIt(): Unit = {
    val exploding = new LineageSink {
      override def emit(lineage: SqlLineage): Unit = throw new IllegalStateException("kafka down")
    }
    val listener = OpenhouseLineageListener.register(spark, exploding)
    try {
      val rows = spark.sql("SELECT 1 AS one FROM local.db.orders LIMIT 1").collect()
      assertNotNull(rows)
    } finally {
      OpenhouseLineageListener.unregister(spark, listener)
    }
  }

  private def withListener(body: InMemoryLineageSink => Unit): Unit = {
    val sink = new InMemoryLineageSink
    val listener = OpenhouseLineageListener.register(spark, sink)
    try {
      body(sink)
    } finally {
      OpenhouseLineageListener.unregister(spark, listener)
    }
  }

  /** The listener bus is asynchronous, so poll until the expected event shows up. */
  private def await(sink: InMemoryLineageSink, predicate: SqlLineage => Boolean): SqlLineage = {
    val deadline = System.currentTimeMillis() + AwaitTimeoutMs
    while (System.currentTimeMillis() < deadline) {
      sink.events.find(predicate) match {
        case Some(lineage) => return lineage
        case None => Thread.sleep(50)
      }
    }
    throw new AssertionError(
      s"no matching lineage captured; saw ${sink.events.map(_.toJson).mkString("\n")}")
  }
}

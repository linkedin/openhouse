package com.linkedin.openhouse.spark.lineage

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.slf4j.LoggerFactory

import scala.util.Try

/**
 * Captures table- and column-level lineage for every statement executed in a Spark session and
 * hands it to a [[LineageSink]].
 *
 * Enable it declaratively, which is the mechanism Spark uses to instantiate listeners at session
 * start:
 * {{{
 *   spark.sql.queryExecutionListeners=com.linkedin.openhouse.spark.lineage.OpenhouseLineageListener
 * }}}
 *
 * or programmatically, which is convenient in a shell or a test:
 * {{{
 *   val sink = new InMemoryLineageSink
 *   OpenhouseLineageListener.register(spark, sink)
 * }}}
 *
 * The listener fires only for statements that actually run, so `spark.sql("SELECT ...")` alone
 * produces nothing until an action is invoked. Use [[SqlLineageExtractor.extractFromSql]] to analyse
 * SQL text without executing it.
 */
class OpenhouseLineageListener(sink: LineageSink) extends QueryExecutionListener {

  private val log = LoggerFactory.getLogger(classOf[OpenhouseLineageListener])

  def this() = this(new LogLineageSink)

  def this(conf: SparkConf) = this(new LogLineageSink)

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    capture(qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    capture(qe)

  private def capture(qe: QueryExecution): Unit = {
    try {
      SqlLineageExtractor.extract(qe.analyzed, sqlTextOf(qe)).foreach(sink.emit)
    } catch {
      // Lineage capture must never break the query that produced it.
      case e: Throwable => log.warn("Failed to extract OpenHouse lineage", e)
    }
  }

  /**
   * Recovers the original statement text.
   *
   * Spark 3.4 onwards keeps it on the plan's `Origin`; on older releases the only generally
   * available source is the job description, which JDBC/Thrift clients and
   * `SparkContext.setJobDescription` populate.
   */
  private def sqlTextOf(qe: QueryExecution): Option[String] = {
    val fromOrigin = Try {
      val origin = qe.analyzed.origin
      val method = origin.getClass.getMethod("sqlText")
      method.invoke(origin).asInstanceOf[Option[String]]
    }.toOption.flatten
    fromOrigin.orElse {
      Option(qe.sparkSession)
        .flatMap(session => Option(session.sparkContext.getLocalProperty("spark.job.description")))
    }
  }
}

object OpenhouseLineageListener {

  /** Registers a listener on an existing session and returns it so it can be unregistered later. */
  def register(spark: SparkSession, sink: LineageSink = new LogLineageSink)
      : OpenhouseLineageListener = {
    val listener = new OpenhouseLineageListener(sink)
    spark.listenerManager.register(listener)
    listener
  }

  def unregister(spark: SparkSession, listener: OpenhouseLineageListener): Unit =
    spark.listenerManager.unregister(listener)
}

package com.linkedin.openhouse.spark.lineage

import org.slf4j.LoggerFactory

/**
 * Destination for extracted lineage.
 *
 * Production deployments emit lineage as Kafka events; this interface is the seam where such a
 * publisher plugs in. [[LogLineageSink]] is the default and simply writes to the driver log.
 */
trait LineageSink {
  def emit(lineage: SqlLineage): Unit
}

/** Writes lineage to the driver log: one JSON line, plus a readable block at DEBUG level. */
class LogLineageSink extends LineageSink {

  private val log = LoggerFactory.getLogger(classOf[LogLineageSink])

  override def emit(lineage: SqlLineage): Unit = {
    log.info("openhouse-lineage {}", lineage.toJson)
    if (log.isDebugEnabled) {
      log.debug("\n{}", lineage.toPrettyString)
    }
  }
}

/** Keeps lineage in memory. Intended for tests and for interactive exploration in a shell. */
class InMemoryLineageSink extends LineageSink {

  private val collected = new java.util.concurrent.ConcurrentLinkedQueue[SqlLineage]()

  override def emit(lineage: SqlLineage): Unit = collected.add(lineage)

  def events: Seq[SqlLineage] = {
    val iterator = collected.iterator()
    val builder = Seq.newBuilder[SqlLineage]
    while (iterator.hasNext) {
      builder += iterator.next()
    }
    builder.result()
  }

  def last: Option[SqlLineage] = events.lastOption

  def clear(): Unit = collected.clear()
}

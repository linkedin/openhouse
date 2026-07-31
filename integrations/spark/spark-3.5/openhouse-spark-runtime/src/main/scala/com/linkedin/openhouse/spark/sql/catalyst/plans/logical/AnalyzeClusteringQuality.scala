package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LeafCommand
import org.apache.spark.sql.types.StringType

/**
 * The logical plan of `ANALYZE TABLE t COMPUTE CLUSTERING QUALITY`.
 *
 * A read-only probe (no commit, no property write) that reports how well a table is clustered to
 * its CURRENT key selection, using only what OPTIMIZE persists (`optimize.cluster.state`) plus
 * manifest metrics (`t.files`). Output rows: `(metric, dimension, value)` where `dimension` is set
 * only for per-key depth rows.
 */
case class AnalyzeClusteringQuality(tableName: Seq[String]) extends LeafCommand {

  override lazy val output: Seq[Attribute] = Seq(
    AttributeReference("metric", StringType, nullable = false)(),
    AttributeReference("dimension", StringType, nullable = true)(),
    AttributeReference("value", StringType, nullable = false)())

  override def simpleString(maxFields: Int): String = {
    s"AnalyzeClusteringQuality: ${tableName}"
  }
}

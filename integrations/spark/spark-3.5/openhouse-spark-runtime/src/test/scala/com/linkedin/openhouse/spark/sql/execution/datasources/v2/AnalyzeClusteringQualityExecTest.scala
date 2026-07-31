package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.OptimizeTable.ClusterInterval
import com.linkedin.openhouse.spark.sql.execution.datasources.v2.AnalyzeClusteringQualityExec._

class AnalyzeClusteringQualityExecTest {

  @Test
  def metricExprAccessesPerFileMetricQuotingKey(): Unit = {
    assertEquals("readable_metrics.ts.lower_bound", metricExpr("ts", "lower_bound"))
    assertEquals("readable_metrics.`my-col`.upper_bound", metricExpr("my-col", "upper_bound"))
  }

  @Test
  def coveragePredicateNoIntervalsIsFalse(): Unit = {
    assertEquals("false", coveragePredicate("lo", "hi", Seq.empty, "INT"))
  }

  @Test
  def coveragePredicateBoundedChecksBothSidesCastToKeyType(): Unit = {
    val p = coveragePredicate("lo", "hi",
      Seq(ClusterInterval("c", "ts", "sort", Some("5"), "20")), "INT")
    assertEquals("((hi <= CAST('20' AS INT)) AND (lo > CAST('5' AS INT)))", p)
  }

  @Test
  def coveragePredicateUnboundedBelowDropsLowerCheck(): Unit = {
    val p = coveragePredicate("lo", "hi",
      Seq(ClusterInterval("c", "ts", "sort", None, "20")), "INT")
    assertEquals("((hi <= CAST('20' AS INT)) AND true)", p)
  }

  @Test
  def coveragePredicateMultipleIntervalsAreOred(): Unit = {
    val p = coveragePredicate("lo", "hi", Seq(
      ClusterInterval("c", "ts", "sort", None, "10"),
      ClusterInterval("c", "ts", "sort", Some("10"), "20")), "INT")
    assertTrue(p.contains(" OR "))
    assertTrue(p.startsWith("((hi <= CAST('10' AS INT))"))
  }
}

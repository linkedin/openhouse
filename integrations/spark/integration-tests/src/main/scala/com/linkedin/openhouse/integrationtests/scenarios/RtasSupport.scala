package com.linkedin.openhouse.integrationtests

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.RuntimeConfig

/** Failure and outcome assertions shared by replacement cases and their focused contract tests. */
object RtasSupport {
  import ConcurrencySupport.{Committed, Conflicted, WriterOutcome}

  /** Runs a narrowing statement with checked casts and restores the caller's ANSI setting after every outcome. */
  def assertAnsiCastOverflow(configuration: RuntimeConfig)(statement: => Unit): Unit = {
    val previousSetting = configuration.getOption("spark.sql.ansi.enabled")
    try {
      configuration.set("spark.sql.ansi.enabled", "true")
      val failure = Check.intercept[Exception](statement)
      assert(
        Exceptions.causeChain(failure).exists {
          case overflow: ArithmeticException with SparkThrowable =>
            overflow.getErrorClass == "CAST_OVERFLOW"
          case _ => false
        },
        s"narrowing the out-of-range key must report CAST_OVERFLOW, found $failure")
    } finally {
      previousSetting match {
        case Some(value) => configuration.set("spark.sql.ansi.enabled", value)
        case None        => configuration.unset("spark.sql.ansi.enabled")
      }
    }
  }

  /** Checks the complete key result for each accepted replacement/append commit ordering. */
  def assertReplaceAppendOutcome(
      replacement: Option[WriterOutcome],
      append: Option[WriterOutcome],
      settledKeys: Seq[Long]): Unit = {
    val expectedKeySets = (replacement, append) match {
      case (Some(Committed), Some(Conflicted)) => Set(Set(1L, 2L))
      case (Some(Conflicted), Some(Committed)) => Set(Set(1L, 2L, 3L, 30L))
      case (Some(Committed), Some(Committed))  => Set(Set(1L, 2L), Set(1L, 2L, 30L))
      case recordedOutcome =>
        throw new AssertionError(
          s"one writer commits when a replace races an append, recorded $recordedOutcome")
    }
    assert(
      settledKeys.distinct.size == settledKeys.size && expectedKeySets.contains(settledKeys.toSet),
      s"writer outcomes ${(replacement, append)} require keys from $expectedKeySets exactly once, " +
        s"found $settledKeys")
  }
}

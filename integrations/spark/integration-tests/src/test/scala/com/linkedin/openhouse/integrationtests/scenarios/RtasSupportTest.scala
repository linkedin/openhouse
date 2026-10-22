package com.linkedin.openhouse.integrationtests

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.types.IntegerType
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{inOrder, mock, verify, when}

final class RtasSupportTest {
  import ConcurrencySupport.{Committed, Conflicted, WriterOutcome}

  @Test
  def checkedCastReportsOverflowAndRestoresExplicitSettings(): Unit = {
    Seq("false", "true").foreach { original =>
      val configuration = mock(classOf[RuntimeConfig])
      when(configuration.getOption("spark.sql.ansi.enabled")).thenReturn(Some(original))
      val changes = inOrder(configuration)

      RtasSupport.assertAnsiCastOverflow(configuration) {
        changes.verify(configuration).getOption("spark.sql.ansi.enabled")
        changes.verify(configuration).set("spark.sql.ansi.enabled", "true")
        Cast(Literal(3000000000L), IntegerType, None, ansiEnabled = true).eval()
      }

      changes.verify(configuration).set("spark.sql.ansi.enabled", original)
    }
  }

  @Test
  def wrappedOverflowRestoresAnAbsentSetting(): Unit = {
    val configuration = mock(classOf[RuntimeConfig])
    when(configuration.getOption("spark.sql.ansi.enabled")).thenReturn(None)
    val overflow = assertThrows(
      classOf[ArithmeticException],
      () => Cast(Literal(3000000000L), IntegerType, None, ansiEnabled = true).eval())

    RtasSupport.assertAnsiCastOverflow(configuration) {
      throw new Exception("Spark job failed", overflow)
    }

    verify(configuration).unset("spark.sql.ansi.enabled")
  }

  @Test
  def successfulUncheckedCastFailsTheContractAndRestoresConfiguration(): Unit = {
    val configuration = mock(classOf[RuntimeConfig])
    when(configuration.getOption("spark.sql.ansi.enabled")).thenReturn(Some("false"))
    assertEquals(
      -1294967296,
      Cast(Literal(3000000000L), IntegerType, None, ansiEnabled = false).eval())

    assertThrows(
      classOf[AssertionError],
      () =>
        RtasSupport.assertAnsiCastOverflow(configuration) {
          Cast(Literal(3000000000L), IntegerType, None, ansiEnabled = false).eval()
        })

    verify(configuration).set("spark.sql.ansi.enabled", "false")
  }

  @Test
  def unrelatedFailuresFailTheContractAndRestoreConfiguration(): Unit = {
    val otherSparkArithmeticFailure = new ArithmeticException("division by zero") with SparkThrowable {
      override def getErrorClass: String = "DIVIDE_BY_ZERO"
    }
    Seq(
      new Exception("unrelated service failure"),
      new ArithmeticException("ordinary arithmetic"),
      otherSparkArithmeticFailure,
      new AssertionError("statement failed")).foreach { failure =>
      val configuration = mock(classOf[RuntimeConfig])
      when(configuration.getOption("spark.sql.ansi.enabled")).thenReturn(None)

      assertThrows(
        classOf[AssertionError],
        () => RtasSupport.assertAnsiCastOverflow(configuration) { throw failure })

      verify(configuration).unset("spark.sql.ansi.enabled")
    }
  }

  @Test
  def acceptsEveryDocumentedCommitOrdering(): Unit = {
    Seq(
      (Committed, Conflicted, Seq(1L, 2L)),
      (Conflicted, Committed, Seq(1L, 2L, 3L, 30L)),
      (Committed, Committed, Seq(1L, 2L)),
      (Committed, Committed, Seq(1L, 2L, 30L))
    ).foreach { case (replacement, append, keys) =>
      RtasSupport.assertReplaceAppendOutcome(Some(replacement), Some(append), keys)
    }
  }

  @Test
  def rejectsMissingWriterOutcomesAndTwoConflicts(): Unit = {
    val outcomes: Seq[Option[WriterOutcome]] = Seq(None, Some(Committed), Some(Conflicted))
    outcomes
      .flatMap(replacement => outcomes.map(append => (replacement, append)))
      .filter { case (replacement, append) =>
        replacement.isEmpty || append.isEmpty ||
          (replacement.contains(Conflicted) && append.contains(Conflicted))
      }
      .foreach { case (replacement, append) =>
        assertThrows(
          classOf[AssertionError],
          () => RtasSupport.assertReplaceAppendOutcome(replacement, append, Seq(1L, 2L)))
      }
  }

  @Test
  def rejectsLostReplacementsDuplicatesAndIncorrectRows(): Unit = {
    Seq(
      (Committed, Committed, Seq(1L, 2L, 3L, 30L)),
      (Committed, Committed, Seq(1L, 2L, 2L)),
      (Committed, Committed, Seq(1L)),
      (Committed, Conflicted, Seq(1L, 2L, 30L)),
      (Conflicted, Committed, Seq(1L, 2L))
    ).foreach { case (replacement, append, keys) =>
      assertThrows(
        classOf[AssertionError],
        () => RtasSupport.assertReplaceAppendOutcome(Some(replacement), Some(append), keys))
    }
  }

  @Test
  def replacementCaseIdsAndSkipPoliciesRemainStable(): Unit = {
    assertEquals(52, Scenarios.rtasContractCases.size)
    Seq(
      "rtas.schema.incompatibleType.notSilentlyLossy" -> false,
      "rtas.concurrency.replaceVersusAppend" -> true
    ).foreach { case (prefix, skipped) =>
      val cases = Scenarios.rtasContractCases.filter(_.id.startsWith(prefix + " @ "))
      assertEquals(2, cases.size)
      assertTrue(cases.forall(_.knownBugReason.isDefined == skipped))
    }
  }
}

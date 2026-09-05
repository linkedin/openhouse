package harness

import java.net.{ConnectException, SocketException, SocketTimeoutException}
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

final class RunnerTest {
  private def context(spark: SparkSession): Ctx =
    Ctx(spark, "openhouse.test", mock(classOf[TableLockClient]))

  @Test
  def configuredParallelismRequiresAPositiveInteger(): Unit = {
    assertEquals(8, RunnerConfiguration.parallelism(Map.empty, 8))
    assertEquals(1, RunnerConfiguration.parallelism(Map.empty, 0))
    assertEquals(
      4,
      RunnerConfiguration.parallelism(Map("HARNESS_PARALLELISM" -> "4"), 8))

    List("0", "-1", "many").foreach { value =>
      val failure = assertThrows(
        classOf[HarnessConfigurationException],
        () =>
          RunnerConfiguration.parallelism(
            Map("HARNESS_PARALLELISM" -> value),
            availableProcessors = 8))
      assertEquals(
        s"HARNESS_PARALLELISM must be a positive integer; received '$value'",
        failure.getMessage)
    }
  }

  @Test
  def transientClassificationMatchesTheRetryContract(): Unit = {
    assertTrue(Exceptions.isTransientConnectionFailure(new SocketTimeoutException("timeout")))
    assertTrue(Exceptions.isTransientConnectionFailure(new ConnectException("refused")))
    assertTrue(Exceptions.isTransientConnectionFailure(new SocketException("Connection reset by peer")))
    assertTrue(
      Exceptions.isTransientConnectionFailure(
        new Exception("outer", new Exception("middle", new SocketTimeoutException("timeout")))))

    assertFalse(Exceptions.isTransientConnectionFailure(new SocketException("broken pipe")))
    assertFalse(Exceptions.isTransientConnectionFailure(new java.io.IOException("input failed")))
    assertFalse(Exceptions.isTransientConnectionFailure(new AssertionError("wrong rows")))
  }

  @Test
  def causeTraversalStopsAtCycles(): Unit = {
    val cyclicFailure = new Exception("cycle") {
      override def getCause: Throwable = this
    }

    assertEquals(List(cyclicFailure), Exceptions.causeChain(cyclicFailure))
  }

  @Test
  def runnerRetriesOnlyTransientSessionCreationFailures(): Unit = {
    val rootSpark = mock(classOf[SparkSession])
    val freshSpark = mock(classOf[SparkSession])
    val retryFailure =
      new RuntimeException("session creation failed", new SocketTimeoutException("retry"))
    when(rootSpark.newSession())
      .thenThrow(retryFailure)
      .thenReturn(freshSpark)
    var receivedSpark = Option.empty[SparkSession]
    val retryThenPass = TestCase("retry-then-pass", ctx => receivedSpark = Some(ctx.spark))

    assertEquals(
      (Outcome.Passed, 2),
      Runner.execute(retryThenPass, context(rootSpark)))
    assertSame(freshSpark, receivedSpark.get)

    val exhaustedSpark = mock(classOf[SparkSession])
    val exhaustedFailure =
      new RuntimeException("session creation failed", new SocketTimeoutException("exhausted"))
    when(exhaustedSpark.newSession()).thenThrow(exhaustedFailure)
    val caseRuns = new AtomicInteger()
    val (exhaustedOutcome, exhaustedCount) = Runner.execute(
      TestCase("exhaust-retries", _ => caseRuns.incrementAndGet()),
      context(exhaustedSpark))

    assertEquals(Runner.MaxAttempts, exhaustedCount)
    assertEquals(0, caseRuns.get())
    assertSame(exhaustedFailure, exhaustedOutcome.asInstanceOf[Outcome.Failed].cause)

    val terminalSpark = mock(classOf[SparkSession])
    val terminalFailure = new AssertionError("wrong rows")
    when(terminalSpark.newSession()).thenThrow(terminalFailure)
    val (terminalOutcome, terminalCount) = Runner.execute(
      TestCase("terminal", _ => ()),
      context(terminalSpark))

    assertEquals(1, terminalCount)
    assertSame(terminalFailure, terminalOutcome.asInstanceOf[Outcome.Failed].cause)
  }

  @Test
  def runnerNeverRetriesAfterTheCaseStarts(): Unit = {
    val rootSpark = mock(classOf[SparkSession])
    val freshSpark = mock(classOf[SparkSession])
    when(rootSpark.newSession()).thenReturn(freshSpark)

    val assertionAttempts = new AtomicInteger()
    val assertionFailure =
      new AssertionError("unexpected exception", new SocketTimeoutException("nested timeout"))
    val (assertionOutcome, assertionCount) = Runner.execute(
      TestCase(
        "assertion",
        _ => {
          assertionAttempts.incrementAndGet()
          throw assertionFailure
        }),
      context(rootSpark))

    assertEquals(1, assertionCount)
    assertEquals(1, assertionAttempts.get())
    assertSame(assertionFailure, assertionOutcome.asInstanceOf[Outcome.Failed].cause)

    val cleanupAttempts = new AtomicInteger()
    val cleanupFailure = new SocketTimeoutException("cleanup timeout")
    val (cleanupOutcome, cleanupCount) = Runner.execute(
      TestCase(
        "cleanup",
        _ => {
          cleanupAttempts.incrementAndGet()
          OwnedTableLifecycle.withCleanup(throw cleanupFailure)(())
        }),
      context(rootSpark))

    assertEquals(1, cleanupCount)
    assertEquals(1, cleanupAttempts.get())
    assertSame(cleanupFailure, cleanupOutcome.asInstanceOf[Outcome.Failed].cause)
  }
}

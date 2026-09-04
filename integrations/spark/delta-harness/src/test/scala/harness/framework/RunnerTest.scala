package harness

import java.net.{ConnectException, SocketException, SocketTimeoutException}
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

final class RunnerTest {
  private def context(): Ctx = {
    val spark = mock(classOf[SparkSession])
    when(spark.newSession()).thenReturn(spark)
    Ctx(spark, "openhouse.test", mock(classOf[TableLockClient]))
  }

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
    assertTrue(Exceptions.isTransient(new SocketTimeoutException("timeout")))
    assertTrue(Exceptions.isTransient(new ConnectException("refused")))
    assertTrue(Exceptions.isTransient(new SocketException("Connection reset by peer")))
    assertTrue(
      Exceptions.isTransient(
        new Exception("outer", new Exception("middle", new SocketTimeoutException("timeout")))))

    assertFalse(Exceptions.isTransient(new SocketException("broken pipe")))
    assertFalse(Exceptions.isTransient(new java.io.IOException("input failed")))
    assertFalse(Exceptions.isTransient(new AssertionError("wrong rows")))
  }

  @Test
  def causeTraversalStopsAtCycles(): Unit = {
    val cyclicFailure = new Exception("cycle") {
      override def getCause: Throwable = this
    }

    assertEquals(List(cyclicFailure), Exceptions.causeChain(cyclicFailure))
  }

  @Test
  def runnerRetriesOnlyTransientFailures(): Unit = {
    val retryAttempts = new AtomicInteger()
    val retryThenPass = TestCase(
      "retry-then-pass",
      _ =>
        if (retryAttempts.incrementAndGet() == 1) {
          throw new SocketTimeoutException("retry")
        })

    assertEquals((Outcome.Passed, 2), Runner.execute(retryThenPass, context()))

    val exhaustedAttempts = new AtomicInteger()
    val exhaustedFailure = new SocketTimeoutException("exhausted")
    val exhaustRetries = TestCase(
      "exhaust-retries",
      _ => {
        exhaustedAttempts.incrementAndGet()
        throw exhaustedFailure
      })
    val (exhaustedOutcome, exhaustedCount) = Runner.execute(exhaustRetries, context())

    assertEquals(Runner.MaxAttempts, exhaustedCount)
    assertEquals(Runner.MaxAttempts, exhaustedAttempts.get())
    assertSame(exhaustedFailure, exhaustedOutcome.asInstanceOf[Outcome.Failed].cause)

    val terminalAttempts = new AtomicInteger()
    val terminalFailure = new AssertionError("wrong rows")
    val terminal = TestCase(
      "terminal",
      _ => {
        terminalAttempts.incrementAndGet()
        throw terminalFailure
      })
    val (terminalOutcome, terminalCount) = Runner.execute(terminal, context())

    assertEquals(1, terminalCount)
    assertEquals(1, terminalAttempts.get())
    assertSame(terminalFailure, terminalOutcome.asInstanceOf[Outcome.Failed].cause)
  }
}

package com.linkedin.openhouse.integrationtests

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException
import org.apache.iceberg.exceptions.{BadRequestException, CommitFailedException, CommitStateUnknownException, ValidationException}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

final class ConcurrencySupportTest {
  @Test
  def recognizesDirectAndWrappedConfirmedConflicts(): Unit = {
    val conflict = new CommitFailedException("concurrent metadata update")
    assertTrue(ConcurrencySupport.isTypedCommitConflict(conflict))
    assertTrue(ConcurrencySupport.isTypedCommitConflict(new Exception("Spark wrapper", conflict)))
  }

  @Test
  def rejectsUnrelatedValidationAndTransportFailures(): Unit = {
    val forbidden = mock(classOf[WebClientResponseWithMessageException])
    when(forbidden.getStatusCode).thenReturn(403)
    val unavailable = mock(classOf[WebClientResponseWithMessageException])
    when(unavailable.getStatusCode).thenReturn(503)
    val failures = Seq(
      forbidden,
      unavailable,
      new BadRequestException("invalid property"),
      new ValidationException("incompatible schema"),
      new CommitStateUnknownException(new Exception("service unavailable")),
      new Exception("CommitFailed is only text"))

    failures.foreach { failure =>
      assertFalse(ConcurrencySupport.isTypedCommitConflict(failure), failure.toString)
      assertFalse(
        ConcurrencySupport.isTypedCommitConflict(new Exception("Spark wrapper", failure)),
        failure.toString)
    }
  }

  @Test
  def unknownCommitStateTakesPrecedenceOverNestedConflict(): Unit = {
    val unknown = new CommitStateUnknownException(new CommitFailedException("earlier attempt"))
    assertFalse(ConcurrencySupport.isTypedCommitConflict(unknown))
    assertFalse(ConcurrencySupport.isTypedCommitConflict(new Exception("Spark wrapper", unknown)))
    assertFalse(
      ConcurrencySupport.isTypedCommitConflict(
        new CommitFailedException(unknown, "outer attempt")))
  }

  @Test
  def concurrentFunctionsBothStartBeforeEitherCompletes(): Unit = {
    val started = new CountDownLatch(2)
    val completed = new AtomicInteger()
    val errors = ConcurrencySupport.runConcurrently(Seq.fill(2)(() => {
      started.countDown()
      assertTrue(started.await(5, TimeUnit.SECONDS), "both functions must start concurrently")
      completed.incrementAndGet()
      ()
    }))
    assertTrue(errors.isEmpty, errors.toString)
    assertEquals(2, completed.get())
    assertTrue(ConcurrencySupport.runConcurrently(Seq.empty).isEmpty)
  }

  @Test
  def unexpectedWriterFailuresReachTheConcurrentErrorResult(): Unit = {
    val forbidden = mock(classOf[WebClientResponseWithMessageException])
    when(forbidden.getStatusCode).thenReturn(403)
    val unknown = new CommitStateUnknownException(new Exception("indeterminate commit"))
    val failures = Seq(
      forbidden,
      unknown,
      new BadRequestException("invalid request"),
      new ValidationException("invalid schema"))
    val errors = ConcurrencySupport.runConcurrently(failures.map(failure => () => {
      try {
        throw failure
      } catch {
        case conflict if ConcurrencySupport.isTypedCommitConflict(conflict) => ()
      }
    }))
    assertEquals(failures.toSet, errors.toSet)
    assertEquals(failures.size, errors.size)
  }
}

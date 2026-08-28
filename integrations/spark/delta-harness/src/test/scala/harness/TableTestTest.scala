package harness

import org.junit.jupiter.api.Assertions.{
  assertEquals,
  assertFalse,
  assertNotEquals,
  assertSame,
  assertThrows,
  assertTrue
}
import org.junit.jupiter.api.Test

/**
 * Pins fresh table identity and ownership cleanup: generated names stay namespace-scoped and
 * unique across counter resets, cleanup starts after the ownership mark, and a cleanup failure is
 * suppressed behind the primary test failure.
 */
final class TableTestTest {
  @Test
  def generatedTableNamesAreDistinctAndNamespaceScoped(): Unit = {
    TableTest.seedCounter(0)
    val firstTable = TableTest.nextQualifiedTableName("test_namespace")
    TableTest.seedCounter(0)
    val secondTable = TableTest.nextQualifiedTableName("test_namespace")

    assertTrue(firstTable.startsWith("test_namespace.t_"))
    assertTrue(secondTable.startsWith("test_namespace.t_"))
    assertNotEquals(firstTable, secondTable)
  }

  @Test
  def failureBeforeOwnershipSkipsCleanup(): Unit = {
    val createFailure = new Exception("table already exists")
    var cleanupCalled = false

    val thrown = assertThrows(
      classOf[Exception],
      () =>
        OwnedTableLifecycle.withOwnership(cleanupCalled = true)(_ =>
          throw createFailure))

    assertSame(createFailure, thrown)
    assertFalse(cleanupCalled, "a failed create must leave the conflicting table intact")
  }

  @Test
  def successfulOwnershipRunsCleanupOnce(): Unit = {
    var cleanupCount = 0

    OwnedTableLifecycle.withOwnership(cleanupCount += 1)(markTableCreated =>
      markTableCreated())

    assertEquals(1, cleanupCount)
  }

  @Test
  def cleanupFailureIsSuppressedOnThePrimaryFailure(): Unit = {
    val testFailure = new Exception("test failed")
    val cleanupFailure = new Exception("cleanup failed")

    val thrown = assertThrows(
      classOf[Exception],
      () =>
        OwnedTableLifecycle.withOwnership(throw cleanupFailure) { markTableCreated =>
          markTableCreated()
          throw testFailure
        })

    assertSame(testFailure, thrown)
    assertEquals(List(cleanupFailure), thrown.getSuppressed.toList)
  }
}

package harness

import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows}
import org.junit.jupiter.api.Test

final class TableLifecycleTest {
  @Test
  def cleanupRunsOnlyForAnOwnedTable(): Unit = {
    var cleanupCount = 0
    val createFailure = new Exception("table already exists")

    val thrown = assertThrows(
      classOf[Exception],
      () =>
        OwnedTableLifecycle.withOwnership(cleanupCount += 1)(_ =>
          throw createFailure))

    assertSame(createFailure, thrown)
    assertEquals(0, cleanupCount)

    OwnedTableLifecycle.withOwnership(cleanupCount += 1)(markTableCreated =>
      markTableCreated())

    assertEquals(1, cleanupCount)
  }

  @Test
  def cleanupFailureIsSuppressedBehindTheBodyFailure(): Unit = {
    val bodyFailure = new Exception("test failed")
    val cleanupFailure = new Exception("cleanup failed")

    val thrown = assertThrows(
      classOf[Exception],
      () =>
        OwnedTableLifecycle.withOwnership(throw cleanupFailure) { markTableCreated =>
          markTableCreated()
          throw bodyFailure
        })

    assertSame(bodyFailure, thrown)
    assertEquals(List(cleanupFailure), thrown.getSuppressed.toList)
  }
}

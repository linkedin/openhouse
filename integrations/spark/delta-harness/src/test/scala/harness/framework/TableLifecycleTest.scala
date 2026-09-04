package harness

import java.util.concurrent.{Callable, Executors, TimeUnit}
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

  @Test
  def generatedTableNamesStayUniqueAcrossCounterResets(): Unit = {
    val pool = Executors.newFixedThreadPool(8)
    try {
      def generateNames(): List[String] = {
        TableTest.seedCounter(0)
        (1 to 500)
          .map(_ =>
            pool.submit(new Callable[String] {
              override def call(): String = TableTest.nextQualifiedTableName("openhouse.test")
            }))
          .map(_.get(30, TimeUnit.SECONDS))
          .toList
      }

      val names = generateNames() ++ generateNames()
      assertEquals(names.size, names.distinct.size)
    } finally {
      pool.shutdownNow()
    }
  }
}

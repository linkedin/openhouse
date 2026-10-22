package com.linkedin.openhouse.integrationtests

import java.util.concurrent.{Callable, Executors, TimeUnit}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{mock, when}

final class TableLifecycleTest {
  @ParameterizedTest
  @ValueSource(strings = Array("success", "create-validation", "preparation", "body"))
  def prepareDropsItsTableAfterEachOutcome(phase: String): Unit = {
    val spark = mock(classOf[SparkSession])
    val emptyRows = mock(classOf[Dataset[Row]])
    val snapshotRows = mock(classOf[Dataset[Row]])
    when(emptyRows.collect()).thenReturn(Array.empty[Row])
    when(snapshotRows.collect()).thenReturn(Array(Row(0L)))
    var createdTable = Option.empty[String]
    var tableExists = false
    var dropCount = 0
    when(spark.sql(anyString())).thenAnswer { invocation =>
      val statement = invocation.getArgument[String](0)
      if (statement.startsWith("CREATE TABLE ")) {
        createdTable = Some(statement.stripPrefix("CREATE TABLE "))
        tableExists = true
        emptyRows
      } else if (statement.startsWith("DROP TABLE IF EXISTS ")) {
        assertEquals(createdTable.get, statement.stripPrefix("DROP TABLE IF EXISTS "))
        tableExists = false
        dropCount += 1
        emptyRows
      } else if (statement.startsWith("SELECT count(*)")) {
        snapshotRows
      } else {
        emptyRows
      }
    }
    val expectedFailure = new AssertionError(s"injected $phase failure")
    val preparation = TableTest(CoreTable)
      .sql("create")(table => s"CREATE TABLE $table") { _ =>
        if (phase == "create-validation") throw expectedFailure
      }
      .step("prepare") { (_, _) =>
        if (phase == "preparation") throw expectedFailure
      }()

    val runPreparation = () =>
      preparation.prepare(TestContext(spark, "openhouse.test")) { table =>
        assertTrue(tableExists)
        assertEquals(createdTable.get, table.name)
        if (phase == "body") throw expectedFailure
      }

    if (phase == "success") {
      runPreparation()
    } else {
      assertSame(expectedFailure, assertThrows(classOf[AssertionError], () => runPreparation()))
    }
    assertTrue(createdTable.isDefined)
    assertFalse(tableExists)
    assertEquals(1, dropCount)
  }

  @Test
  def preparationBuildersAppendImmutableSteps(): Unit = {
    val empty = TableTest(CoreTable)
    val seeded = empty.insert(3)()
    val extended = seeded.step("custom")((_, _) => ())()

    assertTrue(empty.steps.isEmpty)
    assertEquals(Vector("insert(3)"), seeded.steps.map(_.label))
    assertEquals(Vector("insert(3)", "custom"), extended.steps.map(_.label))
    assertSame(CoreTable, extended.schema)
    assertSame(seeded.steps.head, extended.steps.head)
  }

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
  def cleanupFailureAfterSuccessfulBodyIsPrimary(): Unit = {
    val cleanupFailure = new Exception("cleanup failed")

    val thrown = assertThrows(
      classOf[Exception],
      () => OwnedTableLifecycle.withCleanup(throw cleanupFailure)(()))

    assertSame(cleanupFailure, thrown)
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

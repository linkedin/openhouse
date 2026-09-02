package harness

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import scala.collection.mutable.ListBuffer

/**
 * Pins how a preparation turns a test body into a catalog case: the ID it builds, the post-test hook every case from
 * that preparation runs, and the known-bug reason a DML test case carries into its cases. Building a case runs no SQL,
 * so these assertions need no Spark session.
 */
final class TablePreparationTest {
  private val emptyPreparation = TableTest(CoreTable)

  @Test
  def formatsCaseIdFromPrefixNameAndLabel(): Unit = {
    val preparation = TablePreparation("partitioned/orc", emptyPreparation, "prep.evolved:")

    val testCase = preparation.test("delete.byPredicate")(_ => ())

    assertEquals("prep.evolved:delete.byPredicate @ partitioned/orc", testCase.id)
  }

  @Test
  def formatsCaseIdWithoutAPrefixWhenThePreparationDeclaresNone(): Unit = {
    val preparation = TablePreparation("unpartitioned/parquet", emptyPreparation)

    assertEquals(
      "insert.into @ unpartitioned/parquet",
      preparation.test("insert.into")(_ => ()).id)
  }

  @Test
  def buildsACaseWithoutRunningItsBodyOrItsPostTestHook(): Unit = {
    val calls = ListBuffer.empty[String]
    val preparation = TablePreparation[CoreTable.type](
      "unpartitioned/parquet",
      emptyPreparation,
      afterTest = _ => calls += "afterTest")

    preparation.test("insert.into")(_ => calls += "body")

    assertTrue(calls.isEmpty, s"building a case ran $calls")
  }

  @Test
  def runsADmlTestCaseUnderTheIdOfThePreparationItIsGiven(): Unit = {
    val calls = ListBuffer.empty[String]
    val preparation = TablePreparation("unpartitioned/parquet", emptyPreparation)
    val dmlTestCase = DmlTestCase(
      "insert.into",
      (_: PreparedTable[CoreTable.type]) => calls += "insert.into")

    val testCase = dmlTestCase.runOn(preparation)

    assertEquals("insert.into @ unpartitioned/parquet", testCase.id)
    assertEquals(None, testCase.knownBugReason)
    assertTrue(calls.isEmpty, s"runOn ran the operation: $calls")
  }

  @Test
  def carriesTheKnownBugReasonOfADmlTestCaseIntoItsCase(): Unit = {
    val preparation = TablePreparation("partitioned/orc", emptyPreparation, "prep.ordered:")
    val dmlTestCase = DmlTestCase(
      "delete.byPartitionPredicate",
      (_: PreparedTable[CoreTable.type]) => (),
      knownBugReason = Some("the rewrite crashes on a write-ordered table"))

    val testCase = dmlTestCase.runOn(preparation)

    assertEquals("prep.ordered:delete.byPartitionPredicate @ partitioned/orc", testCase.id)
    assertEquals(Some("the rewrite crashes on a write-ordered table"), testCase.knownBugReason)
    assertEquals(
      Some("bug: the rewrite crashes on a write-ordered table"),
      Plan.bugReason(testCase))
  }
}

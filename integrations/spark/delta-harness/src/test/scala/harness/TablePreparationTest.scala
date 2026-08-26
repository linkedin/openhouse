package harness

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

final class TablePreparationTest {
  @Test
  def formatsCaseIdFromPrefixNameAndLabel(): Unit = {
    val preparation = TablePreparation(
      "partitioned/orc",
      TableTest(CoreTable),
      "prep.evolved:",
      description = "Three rows in an evolved ORC table.")

    val testCase = preparation.test(
      "delete.byPredicate",
      "DELETE removes the rows selected by its predicate.")(_ => ())

    assertEquals(
      "prep.evolved:delete.byPredicate @ partitioned/orc",
      testCase.id)
    assertEquals(
      "Three rows in an evolved ORC table.",
      testCase.preparationDescription)
    assertEquals(
      "DELETE removes the rows selected by its predicate.",
      testCase.description)
  }

  @Test
  def runsDescribedDmlCaseOnPreparation(): Unit = {
    val preparation = TablePreparation(
      "unpartitioned/parquet",
      TableTest(CoreTable),
      description = "Three rows in an unpartitioned Parquet table.")
    val dmlTestCase = DmlTestCase(
      "insert.append",
      "INSERT appends one row and commits one snapshot.",
      (_: PreparedTable[CoreTable.type]) => ())

    val testCase = dmlTestCase.runOn(preparation)

    assertEquals(
      "insert.append @ unpartitioned/parquet",
      testCase.id)
    assertEquals(dmlTestCase.description, testCase.description)
    assertEquals(preparation.description, testCase.preparationDescription)
  }
}

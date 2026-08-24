package harness

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

final class TablePreparationTest {
  @Test
  def formatsCaseIdFromPrefixNameAndLabel(): Unit = {
    val preparation = TablePreparation(
      "partitioned/orc",
      TableTest(CoreTable),
      "prep.evolved:")

    val testCase = preparation.test("delete.byPredicate")(_ => ())

    assertEquals(
      "prep.evolved:delete.byPredicate @ partitioned/orc",
      testCase.id)
  }
}

package harness

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

final class RowGeneratorTest {
  @Test
  def coreSeedLiteralsAreStable(): Unit = {
    assertEquals(
      "VALUES " +
        "(1, 1, 'row-1', 1.5, false, '2024-01-01-00'), " +
        "(2, 2, 'row-2', 2.5, true, '2024-01-01-01'), " +
        "(3, 3, 'row-3', 3.5, false, '2024-01-01-02')",
      RowGenerator.valuesClause(CoreTable, 3))
    assertEquals("2024-01-02-00", CoreTable.dateLiteral(25))
  }
}

package harness

import org.junit.jupiter.api.Assertions.{assertNotEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the table-name generator that gives every case its own table: each call mints a fresh UUID,
 * so two names differ even when the counter is reset between them, and every name stays inside the
 * namespace the caller asked for.
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
}

package com.linkedin.openhouse.integrationtests

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

final class RowGeneratorTest {
  @Test
  def descriptiveHandlesPreserveSqlColumnNamesAndSeedValues(): Unit = {
    assertEquals(
      Seq("id", "n", "x", "dec", "str", "bin", "dt", "ts", "tsntz"),
      TypesTable.columnNames)
    assertEquals(
      "VALUES (1, 1, 1.5, CAST(1.50 AS decimal(10,2)), 'row-1', CAST('bin-1' AS binary), " +
        "DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:00:00')",
      RowGenerator.valuesClause(TypesTable, 1))
    assertEquals(Seq("id", "s", "arr", "m", "nested"), NestedTable.columnNames)
    assertEquals(
      "VALUES (1, named_struct('x', 1, 'y', 'row-1'), array(1, 2), map('k', 1), " +
        "named_struct('inner', named_struct('z', 1)))",
      RowGenerator.valuesClause(NestedTable, 1))
  }

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

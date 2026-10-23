package com.linkedin.openhouse.integrationtests

import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}
import org.junit.jupiter.api.Test

final class TableTestFixturesTest {
  private object Fixtures extends TableTestFixtures {
    def changeAndSort(rows: Seq[Row]): Seq[Row] =
      inKeyOrder(rows.map(row => withColumnValue(row, Core.string0, "updated")))
  }

  @Test
  def rowTransformationsUseTheDeclaredColumnPositions(): Unit = {
    val rows = Seq(
      Row(3L, 3, "third", 3.5, false, "2024-01-01-02"),
      Row(1L, 1, "first", 1.5, false, "2024-01-01-00"))
    assertEquals(
      Seq(
        Row(1L, 1, "updated", 1.5, false, "2024-01-01-00"),
        Row(3L, 3, "updated", 3.5, false, "2024-01-01-02")),
      Fixtures.changeAndSort(rows))
    assertEquals("third", rows.head.getString(2))
  }

  @Test
  def foundationUsesOnlyUnpartitionedCoreLayouts(): Unit = {
    assertEquals(List("parquet", "orc"), Fixtures.coreLayouts.map(_.label))
    assertFalse(
      Fixtures.coreLayouts.exists(_.create("db.table").contains("PARTITIONED BY")))
  }
}

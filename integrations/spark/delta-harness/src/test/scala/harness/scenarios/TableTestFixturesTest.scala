package harness

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}
import org.junit.jupiter.api.Test

final class TableTestFixturesTest {
  private object Fixtures extends TableTestFixtures

  @Test
  def foundationUsesOnlyUnpartitionedCoreLayouts(): Unit = {
    assertEquals(List("parquet", "orc"), Fixtures.coreLayouts.map(_.label))
    assertFalse(
      Fixtures.coreLayouts.exists(_.create("db.table").contains("PARTITIONED BY")))
  }
}

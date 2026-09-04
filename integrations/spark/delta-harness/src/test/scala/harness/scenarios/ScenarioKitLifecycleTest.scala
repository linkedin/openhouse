package harness

import scala.collection.mutable.ArrayBuffer
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows}
import org.junit.jupiter.api.Test

final class ScenarioKitLifecycleTest {
  private object Kit extends ScenarioKit

  @Test
  def cleanupFailureAfterSuccessfulBodyIsPrimary(): Unit = {
    val cleanupFailure = new Exception("cleanup failed")

    val thrown = assertThrows(
      classOf[Exception],
      () => OwnedTableLifecycle.withCleanup(throw cleanupFailure)(()))

    assertSame(cleanupFailure, thrown)
  }

  @Test
  def ownedTableRunsTheExactLifecycleStatements(): Unit = {
    val statements = ArrayBuffer.empty[String]

    Kit.withOwnedTable(statements += _, "db.owned")(
      statements += "CREATE TABLE db.owned")(
      statements += "USE db.owned")

    assertEquals(
      List("CREATE TABLE db.owned", "USE db.owned", "DROP TABLE IF EXISTS db.owned"),
      statements.toList)
  }

  @Test
  def cleanupStatementRunsAfterARejectedOperation(): Unit = {
    val statements = ArrayBuffer.empty[String]
    val operationFailure = new Exception("rejected")

    val thrown = assertThrows(
      classOf[Exception],
      () =>
        Kit.withCleanupStatement(statements += _, "DROP TABLE IF EXISTS db.rejected")(
          throw operationFailure))

    assertSame(operationFailure, thrown)
    assertEquals(List("DROP TABLE IF EXISTS db.rejected"), statements.toList)
  }

  @Test
  def trackedRenameDropsTheAcceptedLiveName(): Unit = {
    val statements = ArrayBuffer.empty[String]

    Kit.withTrackedRename(statements += _, "db.original") { renameTo =>
      renameTo("db.renamed")
    }

    assertEquals(
      List(
        "ALTER TABLE db.original RENAME TO db.renamed",
        "DROP TABLE IF EXISTS db.renamed"),
      statements.toList)
  }
}

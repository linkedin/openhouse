package harness

import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import scala.collection.mutable.ListBuffer

/**
 * A `ScenarioKit` whose catalog statements are recorded instead of executed, so a test drives the real lifecycle
 * boundaries without a Spark session. `failingStatements` names the substrings whose statements throw, which is how a
 * test injects a create, rename or cleanup failure.
 */
private final class RecordingScenarioKit extends ScenarioKit {
  val statements = ListBuffer.empty[String]
  var failingStatements: List[String] = Nil

  val runStatement: String => Unit = statement => {
    statements += statement
    failingStatements
      .find(statement.contains)
      .foreach(failing => throw new IllegalStateException(s"statement rejected: $failing"))
  }

  def ownedTable(table: String)(create: => Unit)(use: => Unit): Unit =
    withOwnedTable(runStatement, table)(create)(use)

  def cleanupStatement(statement: String)(use: => Unit): Unit =
    withCleanupStatement(runStatement, statement)(use)

  def trackedRename(originalTable: String)(use: (String => Unit) => Unit): Unit =
    withTrackedRename(runStatement, originalTable)(use)

  def heldLock(lock: () => (Int, String), unlock: () => (Int, String))(
      use: (() => Unit) => Unit): Unit =
    withTableLock(lock, unlock)(use)
}

/**
 * Pins the lifecycle boundaries a case uses for an artifact it builds for itself: the owned table, the unconditional
 * cleanup around a rejected create, the rename tracker, and the lock. Every test drives the boundary in `ScenarioKit`
 * itself and injects the failure it is about, so a boundary that stopped cleaning up, cleaned up the wrong artifact,
 * or swallowed a failure is caught here.
 */
final class TableLifecycleTest {
  private val ok: () => (Int, String) = () => (200, "")

  @Test
  def anOwnedTableIsDroppedWhenItsCreateSucceeds(): Unit = {
    val kit = new RecordingScenarioKit

    kit.ownedTable("db.t_owned")(kit.runStatement("CREATE TABLE db.t_owned"))(
      kit.runStatement("SELECT 1"))

    assertEquals(
      List("CREATE TABLE db.t_owned", "SELECT 1", "DROP TABLE IF EXISTS db.t_owned"),
      kit.statements.toList)
  }

  @Test
  def anOwnedTableIsNotDroppedWhenItsCreateFails(): Unit = {
    val kit = new RecordingScenarioKit
    kit.failingStatements = List("CREATE TABLE db.t_conflict")

    val thrown = assertThrows(
      classOf[IllegalStateException],
      () =>
        kit.ownedTable("db.t_conflict")(kit.runStatement("CREATE TABLE db.t_conflict"))(
          kit.runStatement("SELECT 1")))

    assertTrue(thrown.getMessage.contains("CREATE TABLE db.t_conflict"))
    assertEquals(List("CREATE TABLE db.t_conflict"), kit.statements.toList)
  }

  @Test
  def anOwnedTableBodyFailureStaysPrimaryWhenItsDropAlsoFails(): Unit = {
    val kit = new RecordingScenarioKit
    kit.failingStatements = List("DROP TABLE IF EXISTS db.t_owned")
    val bodyFailure = new Exception("body failed")

    val thrown = assertThrows(
      classOf[Exception],
      () =>
        kit.ownedTable("db.t_owned")(kit.runStatement("CREATE TABLE db.t_owned"))(throw bodyFailure))

    assertSame(bodyFailure, thrown)
    assertEquals(1, thrown.getSuppressed.length)
    assertTrue(thrown.getSuppressed.head.getMessage.contains("DROP TABLE IF EXISTS db.t_owned"))
    assertEquals(
      List("CREATE TABLE db.t_owned", "DROP TABLE IF EXISTS db.t_owned"),
      kit.statements.toList)
  }

  @Test
  def anOwnedTableDropFailureSurfacesWhenTheBodySucceeds(): Unit = {
    val kit = new RecordingScenarioKit
    kit.failingStatements = List("DROP TABLE IF EXISTS db.t_owned")

    val thrown = assertThrows(
      classOf[IllegalStateException],
      () => kit.ownedTable("db.t_owned")(kit.runStatement("CREATE TABLE db.t_owned"))(()))

    assertTrue(thrown.getMessage.contains("DROP TABLE IF EXISTS db.t_owned"))
  }

  @Test
  def nestedOwnedTablesEachDropOnlyTheTableTheyCreated(): Unit = {
    val kit = new RecordingScenarioKit
    kit.failingStatements = List("CREATE TABLE db.t_inner")
    val outerCreate = "CREATE TABLE db.t_outer"

    val thrown = assertThrows(
      classOf[IllegalStateException],
      () =>
        kit.ownedTable("db.t_outer")(kit.runStatement(outerCreate)) {
          kit.ownedTable("db.t_inner")(kit.runStatement("CREATE TABLE db.t_inner"))(())
        })

    assertTrue(thrown.getMessage.contains("CREATE TABLE db.t_inner"))
    assertEquals(
      List(outerCreate, "CREATE TABLE db.t_inner", "DROP TABLE IF EXISTS db.t_outer"),
      kit.statements.toList)
  }

  @Test
  def aRejectedCreateIsCleanedUpWhateverTheRejectionDid(): Unit = {
    val scratchDrop = "DROP TABLE IF EXISTS db.t_scratch"

    // The rejection arrives as expected.
    val expectedKit = new RecordingScenarioKit
    expectedKit.failingStatements = List("CREATE TABLE db.t_scratch")
    expectedKit.cleanupStatement(scratchDrop) {
      Check.intercept[IllegalStateException](
        expectedKit.runStatement("CREATE TABLE db.t_scratch"))
    }
    assertEquals(List("CREATE TABLE db.t_scratch", scratchDrop), expectedKit.statements.toList)

    // The create unexpectedly succeeds, so the interception fails and the scratch table still goes.
    val unexpectedSuccessKit = new RecordingScenarioKit
    val successThrown = assertThrows(
      classOf[AssertionError],
      () =>
        unexpectedSuccessKit.cleanupStatement(scratchDrop) {
          Check.intercept[IllegalStateException](
            unexpectedSuccessKit.runStatement("CREATE TABLE db.t_scratch"))
        })
    assertTrue(successThrown.getMessage.contains("to be thrown"))
    assertEquals(
      List("CREATE TABLE db.t_scratch", scratchDrop),
      unexpectedSuccessKit.statements.toList)

    // The create throws a different type, so the interception fails and the scratch table still goes.
    val wrongTypeKit = new RecordingScenarioKit
    val wrongTypeThrown = assertThrows(
      classOf[AssertionError],
      () =>
        wrongTypeKit.cleanupStatement(scratchDrop) {
          Check.intercept[IllegalArgumentException](throw new IllegalStateException("other"))
        })
    assertTrue(wrongTypeThrown.getMessage.contains("but got"))
    assertEquals(List(scratchDrop), wrongTypeKit.statements.toList)

    // The assertion after the interception fails, and its failure stays primary over the cleanup failure.
    val assertionKit = new RecordingScenarioKit
    assertionKit.failingStatements = List(scratchDrop)
    val assertionFailure = new Exception("message assertion failed")
    val assertionThrown = assertThrows(
      classOf[Exception],
      () => assertionKit.cleanupStatement(scratchDrop)(throw assertionFailure))
    assertSame(assertionFailure, assertionThrown)
    assertEquals(1, assertionThrown.getSuppressed.length)
    assertEquals(List(scratchDrop), assertionKit.statements.toList)
  }

  @Test
  def aTrackedRenameLeavesNothingBehindUnderTheNameItLastAccepted(): Unit = {
    // The two names share no prefix, so an injected failure names exactly one of the two renames.
    val originalTable = "db.t_alpha"
    val renamedTable = "db.t_beta"
    val renameAway = s"ALTER TABLE $originalTable RENAME TO $renamedTable"
    val renameBack = s"ALTER TABLE $renamedTable RENAME TO $originalTable"

    // The case renames away and back, so the table ends under its original name and nothing is dropped.
    val kit = new RecordingScenarioKit
    kit.trackedRename(originalTable) { renameTo =>
      renameTo(renamedTable)
      renameTo(originalTable)
    }
    assertEquals(List(renameAway, renameBack), kit.statements.toList)

    // An assertion between the two renames fails, so the live name is the renamed one and that is what goes.
    val assertionKit = new RecordingScenarioKit
    val assertionFailure = new Exception("row count assertion failed")
    val assertionThrown = assertThrows(
      classOf[Exception],
      () =>
        assertionKit.trackedRename(originalTable) { renameTo =>
          renameTo(renamedTable)
          throw assertionFailure
        })
    assertSame(assertionFailure, assertionThrown)
    assertEquals(
      List(renameAway, s"DROP TABLE IF EXISTS $renamedTable"),
      assertionKit.statements.toList)

    // The rename back fails, so the table is still live under the renamed name and that is what goes.
    val renameBackKit = new RecordingScenarioKit
    renameBackKit.failingStatements = List(renameBack)
    val renameBackThrown = assertThrows(
      classOf[IllegalStateException],
      () =>
        renameBackKit.trackedRename(originalTable) { renameTo =>
          renameTo(renamedTable)
          renameTo(originalTable)
        })
    assertTrue(renameBackThrown.getMessage.contains(renameBack))
    assertEquals(
      List(renameAway, renameBack, s"DROP TABLE IF EXISTS $renamedTable"),
      renameBackKit.statements.toList)

    // The first rename fails, so the table never left its original name and the boundary drops nothing.
    val renameAwayKit = new RecordingScenarioKit
    renameAwayKit.failingStatements = List(renameAway)
    val renameAwayThrown = assertThrows(
      classOf[IllegalStateException],
      () => renameAwayKit.trackedRename(originalTable)(renameTo => renameTo(renamedTable)))
    assertTrue(renameAwayThrown.getMessage.contains(renameAway))
    assertEquals(List(renameAway), renameAwayKit.statements.toList)
  }

  @Test
  def aHeldLockIsReleasedOnceAndItsResponsesAreChecked(): Unit = {
    // The case releases the lock itself, so the boundary does not release it again.
    val releases = ListBuffer.empty[String]
    new RecordingScenarioKit().heldLock(ok, () => { releases += "release"; (200, "") }) { release =>
      release()
    }
    assertEquals(List("release"), releases.toList)

    // The case leaves the lock held, so the boundary releases it.
    releases.clear()
    new RecordingScenarioKit().heldLock(ok, () => { releases += "release"; (200, "") })(_ => ())
    assertEquals(List("release"), releases.toList)

    // A rejected lock request fails the case before the body runs.
    var bodyRan = false
    val lockThrown = assertThrows(
      classOf[AssertionError],
      () =>
        new RecordingScenarioKit()
          .heldLock(() => (503, "unavailable"), ok)(_ => bodyRan = true))
    assertTrue(lockThrown.getMessage.contains("lock request failed: 503"))
    assertTrue(!bodyRan, "the body should not run when the lock was refused")

    // A rejected release fails the case.
    val releaseThrown = assertThrows(
      classOf[AssertionError],
      () => new RecordingScenarioKit().heldLock(ok, () => (500, "boom"))(_ => ()))
    assertTrue(releaseThrown.getMessage.contains("unlock request failed: 500"))

    // A release failure rides along behind a body failure, and the boundary tries the release exactly once.
    releases.clear()
    val bodyFailure = new Exception("locked-write assertion failed")
    val bodyThrown = assertThrows(
      classOf[Exception],
      () =>
        new RecordingScenarioKit()
          .heldLock(ok, () => { releases += "release"; (500, "boom") })(_ => throw bodyFailure))
    assertSame(bodyFailure, bodyThrown)
    assertEquals(1, bodyThrown.getSuppressed.length)
    assertTrue(bodyThrown.getSuppressed.head.getMessage.contains("unlock request failed: 500"))
    assertEquals(List("release"), releases.toList)
  }
}

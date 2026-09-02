package harness

import java.util.concurrent.atomic.AtomicInteger

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the support this branch keeps for later feature layers: the changelog operations, the concurrency primitives,
 * and the kit's generic starting-state substrate. These assertions exercise each contract before a feature layer
 * integrates it.
 *
 * These Spark-free assertions inspect changelog data, plain JVM concurrency code, and preparation step lists.
 */
final class SupportContractTest {
  private val support = new ChangelogSupport {}

  private val expectedChangelogOperations = List(
    ("changelog.append", Map("INSERT" -> 1L)),
    ("changelog.overwrite", Map("DELETE" -> 1L)),
    ("changelog.delete", Map("DELETE" -> 1L)),
    ("changelog.update", Map("DELETE" -> 1L, "INSERT" -> 1L)),
    ("changelog.merge", Map("DELETE" -> 1L, "INSERT" -> 2L)))

  @Test
  def theChangelogOperationsAreTheOnesALaterLayerCrossesWithItsOwnPreparations(): Unit = {
    assertEquals(
      expectedChangelogOperations,
      support.changelogOperations.map(operation =>
        (operation.name, operation.expectedChangeCounts)))
  }

  @Test
  def everyChangelogOperationIsAStatementAgainstTheTableItIsGiven(): Unit = {
    support.changelogOperations.foreach { operation =>
      val statement = operation.statement("db.t_probe")

      assertTrue(
        statement.contains("db.t_probe"),
        s"${operation.name} does not address the table it is given: $statement")
      assertTrue(
        operation.expectedChangeCounts.values.forall(_ > 0L),
        s"${operation.name} expects a change type it does not produce")
    }
  }

  @Test
  def changelogOperationsCrossWithEveryPreparationTheyAreGiven(): Unit = {
    val preparations = support.preparedCoreFormats
    val cases = support.changelogOperationCasesFor(preparations)

    assertEquals(
      preparations.flatMap(preparation =>
        support.changelogOperations.map(operation =>
          s"${preparation.casePrefix}${operation.name} @ ${preparation.label}")),
      cases.map(_.id))
    assertTrue(
      ScenarioCatalog.foundationContributions
        .flatMap { case (_, contribution) => contribution }
        .forall(testCase => !testCase.id.startsWith("changelog.")),
      "changelog support must not contribute cases to the foundation catalog")
  }

  @Test
  def runConcurrentlyReleasesEveryFunctionAndReportsNothingWhenTheyAllSucceed(): Unit = {
    val completed = new AtomicInteger(0)
    val threadErrors =
      ConcurrencySupport.runConcurrently(Seq.fill(4)(() => completed.incrementAndGet()))

    assertTrue(threadErrors.isEmpty, s"a function failed unexpectedly: $threadErrors")
    assertEquals(4, completed.get)
  }

  @Test
  def runConcurrentlyReportsTheThrowableEveryFailingFunctionRaised(): Unit = {
    val threadErrors = ConcurrencySupport.runConcurrently(
      Seq(
        () => throw new IllegalStateException("first"),
        () => (),
        () => throw new IllegalStateException("second")))

    assertEquals(List("first", "second"), threadErrors.map(_.getMessage).sorted.toList)
  }

  @Test
  def aTypedCommitConflictIsRecognisedAnywhereInTheCauseChain(): Unit = {
    assertTrue(
      ConcurrencySupport.isTypedCommitConflict(
        new RuntimeException("outer", new CommitFailedProbe("inner"))),
      "a commit-conflict class name anywhere in the chain is a typed conflict")
    assertTrue(
      !ConcurrencySupport.isTypedCommitConflict(new IllegalArgumentException("plain")),
      "a failure whose chain names no commit, validation or transport class is untyped")
  }

  @Test
  def theGenericStartingStatesStayUsableForALaterLayer(): Unit = {
    val kit = new KitProbe

    assertTrue(
      kit.probeCoreCreate("db.t_probe", "orc").startsWith("CREATE TABLE db.t_probe ("),
      "coreCreate must build a CREATE for the table and format it is given")
    assertTrue(
      kit.probeCoreCreate("db.t_probe", "orc").contains("'write.format.default'='orc'"),
      "coreCreate must declare the format it is given")
    assertEquals(
      "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, false, '2024-01-01-00')",
      kit.probeCoreRow(7L, "row-7"))
    assertEquals(List("create"), kit.probeEmptyStandardTable("orc").preparation.steps.map(_.label).toList)
    assertEquals(
      List("create", "insert(3)", "waitForNextSnapshotTimestamp", "insertRowsFourAndFive"),
      kit.probeTwoSnapshotTable("parquet").preparation.steps.map(_.label).toList)
  }
}

/** Carries the commit-conflict class-name marker that the harness recognises. */
private final class CommitFailedProbe(message: String) extends Exception(message)

/**
 * Reads the kit's protected starting-state substrate the way a capability trait reads it, exercising the shared
 * contract in the foundation suite.
 */
private final class KitProbe extends ScenarioKit {
  def probeCoreCreate(table: String, format: String): String = coreCreate(table, format)
  def probeCoreRow(long: Long, tag: String): String = coreRow(long, tag)
  def probeEmptyStandardTable(format: String): TablePreparation[CoreTable.type] =
    preparedEmptyStandardTable(format)
  def probeTwoSnapshotTable(format: String): TablePreparation[CoreTable.type] =
    preparedTwoSnapshotTable(format)
}

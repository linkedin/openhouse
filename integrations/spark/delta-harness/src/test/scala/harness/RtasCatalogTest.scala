package harness

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the replace-table contribution this layer adds: its exact size, the fingerprint of its case IDs, the four
 * preparation axes its DML runs on, the reusable DML operations it covers, and the replace contracts it claims to
 * prove.
 *
 * Every assertion reads `rtasCases` and the RTAS families alone, so the frozen foundation keeps its own tests and a
 * sibling layer that adds its own contribution leaves this file as it is. The catalog invariants that apply to any
 * layer, namely ID uniqueness, alphabetical contribution ordering and the foundation-plus-extensions rule, are pinned
 * once in CaseCatalogTest.
 *
 * Reading the catalog builds the case list only; executing a case and starting Spark stay separate steps.
 */
final class RtasCatalogTest {
  private val expectedRtasCaseCount = 264
  private val expectedRtasSha256 =
    "90512741431e50ca77d4924dd7c8b789e3fa6512a23d5b746bc397a00e4532d1"

  /** The four replace preparations the reusable DML operations run on, in the order the layer builds them. */
  private val expectedRtasPreparationLabels = List(
    "unpartitioned/parquet",
    "partitioned/parquet",
    "unpartitioned/orc",
    "partitioned/orc")

  /**
   * Every replace contract this layer claims to prove, named by the case ID that proves it. The list is written out
   * here as its own literal, so the layer and this list are independent statements of the same coverage and any
   * drop, rename or reorder fails this test until the intended coverage is restated.
   */
  private val expectedContractCaseNames = List(
    "rtas.gate.enabled",
    "rtas.gate.disabled.rejected",
    "rtas.gate.replicationConflict.rejected",
    "rtas.sameShapeReplacement",
    "rtas.writeAfterReplace",
    "rtas.schema.addColumn",
    "rtas.schema.dropColumn",
    "rtas.schema.widenColumn",
    "rtas.schema.incompatibleType.notSilentlyLossy",
    "rtas.partition.specReplaced",
    "rtas.partition.changeAfterReplace",
    "rtas.property.userPropertyPreserved",
    "rtas.property.statementOverridesProperty",
    "rtas.policy.retentionPreserved",
    "rtas.policy.columnTagPreserved",
    "rtas.history.preReplaceTimeTravel",
    "rtas.history.rollbackRejected",
    "rtas.history.setCurrentSnapshotRecovers",
    "rtas.changelog.acrossBoundaryRejected",
    "rtas.incrementalRead.acrossBoundaryRejected",
    "rtas.rename.replaceThenRename",
    "rtas.rename.renameThenReplace",
    "rtas.sortOrder.changedAfterReplace",
    "rtas.sortOrder.removedAfterReplace",
    "rtas.identity.creatorPreserved",
    "rtas.concurrency.replaceVersusAppend")

  @Test
  def theReplaceContributionIsTheSizeAndShapeItIsPinnedTo(): Unit = {
    val caseIds = Scenarios.rtasCases.map(_.id)
    val actualSha256 = sha256(caseIds.mkString("\n"))

    assertEquals(
      expectedRtasCaseCount,
      caseIds.size,
      s"rtasCases changed; count=${caseIds.size}, sha256=$actualSha256")
    assertEquals(
      expectedRtasSha256,
      actualSha256,
      s"rtasCases changed; count=${caseIds.size}, sha256=$actualSha256")
    assertEquals(caseIds.distinct.size, caseIds.size, "replace case IDs must be unique")
    assertEquals(
      Scenarios.rtasDmlCases.map(_.id) ++ Scenarios.rtasContractCases.map(_.id),
      caseIds,
      "rtasCases is the DML axis followed by the replace contract")
  }

  @Test
  def theCatalogIntegratesTheReplaceContributionExactlyOnce(): Unit = {
    val replaceEntries = ScenarioCatalog.extensionContributions.filter {
      case (name, _) => name == "rtasCases"
    }

    assertEquals(
      1,
      replaceEntries.size,
      s"rtasCases is integrated once, found ${replaceEntries.size} entries")
    assertEquals(
      Scenarios.rtasCases.map(_.id),
      replaceEntries.head match { case (_, contribution) => contribution.map(_.id) },
      "the catalog integrates the very list the capability exposes")
  }

  @Test
  def theReplaceDmlAxisIsTheFourReplacePreparations(): Unit = {
    assertEquals(
      expectedRtasPreparationLabels,
      Scenarios.preparedRtasCoreTables.map(_.label))
    assertEquals(
      List("partitioned/parquet", "partitioned/orc"),
      Scenarios.preparedRtasPartitionedCoreTables.map(_.label))
    assertEquals(
      expectedRtasPreparationLabels,
      Scenarios.preparedNullStringRtasCoreTables.map(_.label),
      "the null-string preparations extend the same four replace preparations")
    assertTrue(
      (Scenarios.preparedRtasCoreTables ++ Scenarios.preparedRtasPartitionedCoreTables ++
        Scenarios.preparedNullStringRtasCoreTables)
        .forall(_.casePrefix == Scenarios.rtasCasePrefix),
      "every replace preparation marks its cases as running on a replaced table")
  }

  @Test
  def everyReplacePreparationReachesItsStartingStateThroughAReplace(): Unit = {
    val replaceStepLabels = List("prep.rtas", "prep.rtas.refresh")

    Scenarios.preparedRtasCoreTables.foreach { preparation =>
      assertEquals(
        List("create", s"insert(${Scenarios.standardSeedRowCount})") ++ replaceStepLabels,
        preparation.preparation.steps.map(_.label).toList,
        s"${preparation.label} creates, seeds, replaces and refreshes in that order")
    }
    Scenarios.preparedNullStringRtasCoreTables.foreach { preparation =>
      assertEquals(
        List("prep.nullStringRow"),
        preparation.preparation.steps.map(_.label).toList.takeRight(1),
        s"${preparation.label} ends by adding the null row the null-string operation reads")
    }
  }

  @Test
  def everyReusableDmlOperationRunsOnTheReplacePreparationsItAppliesTo(): Unit = {
    val coveredOperationNames = Scenarios.rtasDmlCases
      .map(_.id.stripPrefix(Scenarios.rtasCasePrefix).split(" @ ").head)
      .distinct
      .sorted
    val reusableOperationNames = (Scenarios.allDmlTestCases ++
      Scenarios.nullStringRowTestCases ++
      Scenarios.partitionedTableTestCases).map(_.id).distinct.sorted

    assertEquals(
      reusableOperationNames,
      coveredOperationNames,
      "every reusable DML operation runs on a replaced table")
    assertEquals(54, reusableOperationNames.size, "the reusable DML operation count changed")
    assertEquals(
      Scenarios.preparedRtasCoreTables.flatMap(preparation =>
        Scenarios.allDmlTestCases.map(testCase =>
          s"${preparation.casePrefix}${testCase.id} @ ${preparation.label}")),
      Scenarios.rtasCoreDmlCases.map(_.id),
      "the core replace bucket is its preparations crossed with every reusable operation")
    assertEquals(204, Scenarios.rtasCoreDmlCases.size)
    assertEquals(4, Scenarios.rtasNullStringDmlCases.size)
    assertEquals(4, Scenarios.rtasPartitionedDmlCases.size)
    assertEquals(212, Scenarios.rtasDmlCases.size)
  }

  @Test
  def everyReplaceContractHasAtLeastOneCaseInEveryColumnarFormat(): Unit = {
    val contractCaseNames = Scenarios.rtasContractCases
      .map(_.id.split(" @ ").head)
      .distinct

    assertEquals(
      expectedContractCaseNames,
      contractCaseNames,
      "the replace contract families changed")
    expectedContractCaseNames.foreach { contractCaseName =>
      assertEquals(
        Scenarios.fileFormats.map(format => s"$contractCaseName @ $format"),
        Scenarios.rtasContractCases.map(_.id).filter(_.startsWith(s"$contractCaseName @ ")),
        s"$contractCaseName runs in every columnar format")
    }
    assertEquals(
      expectedContractCaseNames.size * Scenarios.fileFormats.size,
      Scenarios.rtasContractCases.size)
  }

  @Test
  def everyReplaceCaseRunsOnAColumnarFormatInTheLandingMatrix(): Unit = {
    val preparationFormats = Scenarios.rtasCases
      .map(_.id.split(" @ ").last)
      .map(label => label.split("/").last)
      .distinct

    assertEquals(List("parquet", "orc"), Scenarios.fileFormats)
    assertEquals(
      Scenarios.fileFormats.sorted,
      preparationFormats.sorted,
      s"every replace case runs on a landing-matrix format, found $preparationFormats")
  }

  @Test
  def theReplaceSkipMetadataIsPinnedToTheKnownProductBugs(): Unit = {
    assertEquals(
      List(
        "rtas.concurrency.replaceVersusAppend @ orc",
        "rtas.concurrency.replaceVersusAppend @ parquet",
        "rtas.schema.incompatibleType.notSilentlyLossy @ orc",
        "rtas.schema.incompatibleType.notSilentlyLossy @ parquet"),
      Scenarios.rtasCases.collect {
        case testCase if testCase.knownBugReason.nonEmpty => testCase.id
      }.sorted,
      "the replace known-bug cases changed")
    assertTrue(
      Scenarios.rtasCases.forall(_.embeddedSkipReason.isEmpty),
      "every replace case reaches the embedded catalog")
  }

  private def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"$byte%02x")
      .mkString
}

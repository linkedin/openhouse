package harness

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the compatibility-and-streaming bundle this layer adds: its five capability contributions, and for each one the
 * exact size, the fingerprint of its case IDs, the operation and preparation axes it runs on, and the contracts it
 * claims to prove. It also pins the combined bundle size and fingerprint, and that the catalog integrates each
 * contribution exactly once as the very list the capability exposes.
 *
 * Every assertion reads the bundle's own contributions and filters `extensionContributions` to the bundle's names, so
 * the frozen foundation and the replace-table extension keep their own tests and a sibling layer that appends its own
 * contribution leaves this file passing. The catalog invariants that apply to any layer, namely ID uniqueness,
 * alphabetical contribution ordering and the foundation-plus-extensions rule, are pinned once in CaseCatalogTest.
 *
 * Reading the catalog builds the case list only; executing a case and starting Spark stay separate steps.
 */
final class CompatibilityStreamingCatalogTest {

  private val expectedConcurrencyCaseCount = 4
  private val expectedConcurrencySha256 =
    "f9a14d386fdb1f060503ff0c73fe0d71768e0fe22a844d48ce21d14ff7bb2248"

  private val expectedLockingCaseCount = 2
  private val expectedLockingSha256 =
    "b448df0793d18a98b9e1ab2bcfb36787d82ae82ab701da13f9b624584e81f749"

  private val expectedStreamingCaseCount = 10
  private val expectedStreamingSha256 =
    "b0aaf71e2512ceaba29069ea8940d7c5f9139fda8d5ac43dcf8ddb30b01de868"

  private val expectedTableEvolutionCaseCount = 96
  private val expectedTableEvolutionSha256 =
    "998d6e329c70f8c0200020f74aaf2e1736631a90a50df0d5bb7845c042009bdf"

  private val expectedWriterCaseCount = 2
  private val expectedWriterSha256 =
    "f5330fe65f02236a6c4fb1eba1a8c5ab59505a939180230bcdf4af50542cd94e"

  private val expectedBundleCaseCount = 114
  private val expectedBundleSha256 =
    "aa92e8d019290de7b7b30ed02b95cfe04f9d2490d2329f31f206b290efd59305"

  /** The bundle's five contributions, named as the catalog names them and in alphabetical order. */
  private val bundleContributions: List[(String, List[TestCase])] =
    List(
      "concurrencyCases"                 -> Scenarios.concurrencyCases,
      "lockingCases"                     -> Scenarios.lockingCases,
      "streamingCases"                   -> Scenarios.streamingCases,
      "tableEvolutionCompatibilityCases" -> Scenarios.tableEvolutionCompatibilityCases,
      "writerCompatibilityCases"         -> Scenarios.writerCompatibilityCases)

  /** Every bundle case, its five contributions concatenated in the order the catalog integrates them. */
  private def bundleCases: List[TestCase] =
    bundleContributions.flatMap { case (_, contribution) => contribution }

  @Test
  def eachBundleContributionIsTheSizeAndShapeItIsPinnedTo(): Unit = {
    val expected = List(
      ("concurrencyCases", expectedConcurrencyCaseCount, expectedConcurrencySha256),
      ("lockingCases", expectedLockingCaseCount, expectedLockingSha256),
      ("streamingCases", expectedStreamingCaseCount, expectedStreamingSha256),
      ("tableEvolutionCompatibilityCases", expectedTableEvolutionCaseCount, expectedTableEvolutionSha256),
      ("writerCompatibilityCases", expectedWriterCaseCount, expectedWriterSha256))

    val actual = bundleContributions.map { case (name, contribution) =>
      val caseIds = contribution.map(_.id)
      (name, caseIds.size, sha256(caseIds.mkString("\n")))
    }

    assertEquals(expected, actual, s"a bundle contribution changed; actual=$actual")
  }

  @Test
  def theBundleIsTheCombinedSizeAndShapeItIsPinnedTo(): Unit = {
    val caseIds = bundleCases.map(_.id)
    val actualSha256 = sha256(caseIds.mkString("\n"))

    assertEquals(
      expectedBundleCaseCount,
      caseIds.size,
      s"bundle changed; count=${caseIds.size}, sha256=$actualSha256")
    assertEquals(
      expectedBundleSha256,
      actualSha256,
      s"bundle changed; count=${caseIds.size}, sha256=$actualSha256")
    assertEquals(caseIds.distinct.size, caseIds.size, "bundle case IDs must be unique")
  }

  @Test
  def theCatalogIntegratesEveryBundleContributionExactlyOnce(): Unit = {
    bundleContributions.foreach { case (name, contribution) =>
      val integrated = ScenarioCatalog.extensionContributions.filter {
        case (integratedName, _) => integratedName == name
      }

      assertEquals(1, integrated.size, s"$name is integrated once, found ${integrated.size} entries")
      assertEquals(
        contribution.map(_.id),
        integrated.head match { case (_, cases) => cases.map(_.id) },
        s"the catalog integrates $name as the very list the capability exposes")
    }
  }

  @Test
  def theConcurrencyAxisRunsEveryRacingFamilyInEveryColumnarFormat(): Unit = {
    val expectedFamilies = List("concurrency.appendAppend", "concurrency.updateUpdate")

    assertEquals(
      expectedFamilies,
      familyNames(Scenarios.concurrencyCases),
      "the racing families changed")
    assertEachFamilyRunsInEveryFormat(expectedFamilies, Scenarios.concurrencyCases)
  }

  @Test
  def theLockingAxisIsTwoEmbeddedRestCases(): Unit = {
    assertEquals(
      List("lock.enforcement @ embedded", "lock.starvesMaintenance @ embedded"),
      Scenarios.lockingCases.map(_.id),
      "the lock cases changed; both drive the REST lock endpoint against the embedded server")
  }

  @Test
  def theStreamingAxisRunsEveryStreamingFamilyInEveryColumnarFormat(): Unit = {
    val expectedFamilies = List(
      "streaming.read",
      "streaming.write",
      "streaming.readAcrossRestart",
      "streaming.deleteSnapshot.rejected",
      "streaming.expiredCheckpoint")

    assertEquals(expectedFamilies, familyNames(Scenarios.streamingCases), "the streaming families changed")
    assertEachFamilyRunsInEveryFormat(expectedFamilies, Scenarios.streamingCases)
  }

  @Test
  def theTableEvolutionAxisIsEveryLayoutAndAlterationCrossedWithEveryFollowUp(): Unit = {
    val expectedPreparations = List(
      ("unpartitioned/parquet", "afterAddColumn:"),
      ("unpartitioned/parquet", "afterTypeWiden:"),
      ("unpartitioned/parquet", "afterWriteOrder:"),
      ("unpartitioned/parquet", "afterDistributionMode:"),
      ("partitioned/parquet", "afterAddColumn:"),
      ("partitioned/parquet", "afterTypeWiden:"),
      ("partitioned/parquet", "afterWriteOrder:"),
      ("partitioned/parquet", "afterDistributionMode:"),
      ("unpartitioned/orc", "afterAddColumn:"),
      ("unpartitioned/orc", "afterTypeWiden:"),
      ("unpartitioned/orc", "afterWriteOrder:"),
      ("unpartitioned/orc", "afterDistributionMode:"),
      ("partitioned/orc", "afterAddColumn:"),
      ("partitioned/orc", "afterTypeWiden:"),
      ("partitioned/orc", "afterWriteOrder:"),
      ("partitioned/orc", "afterDistributionMode:"))
    val expectedFollowUps =
      List("insert", "delete", "timeTravel", "rollback", "expireSnapshots", "rewriteDataFiles")

    assertEquals(
      expectedPreparations,
      Scenarios.alteredTablePreparations.map(preparation =>
        (preparation.label, preparation.casePrefix)),
      "the altered preparations changed")
    assertEquals(
      Scenarios.alteredTablePreparations.flatMap(preparation =>
        expectedFollowUps.map(followUp =>
          s"${preparation.casePrefix}$followUp @ ${preparation.label}")),
      Scenarios.tableEvolutionCompatibilityCases.map(_.id),
      "every follow-up operation runs on every altered preparation, one preparation at a time")
    assertEquals(
      expectedPreparations.size * expectedFollowUps.size,
      Scenarios.tableEvolutionCompatibilityCases.size)
  }

  @Test
  def theWriterCompatibilityAxisRunsTheExplicitColumnWriterInEveryColumnarFormat(): Unit = {
    val expectedFamilies = List("writerCompatibility.afterAddColumn")

    assertEquals(
      expectedFamilies,
      familyNames(Scenarios.writerCompatibilityCases),
      "the writer-compatibility family changed")
    assertEachFamilyRunsInEveryFormat(expectedFamilies, Scenarios.writerCompatibilityCases)
  }

  @Test
  def theBundleReachesTheEmbeddedCatalogWithNoKnownBugAndNoEmbeddedSkip(): Unit = {
    assertTrue(
      bundleCases.forall(_.knownBugReason.isEmpty),
      "the bundle claims no known product bug; every case is expected to pass on a conformant catalog")
    assertTrue(
      bundleCases.forall(_.embeddedSkipReason.isEmpty),
      "every bundle case reaches the embedded catalog; the streaming-checkpoint limits gate downstream")
  }

  /** The family name a case ID carries, stripping the ` @ <preparation label>` suffix. */
  private def familyNames(cases: List[TestCase]): List[String] =
    cases.map(_.id.split(" @ ").head).distinct

  private def assertEachFamilyRunsInEveryFormat(families: List[String], cases: List[TestCase]): Unit = {
    assertEquals(List("parquet", "orc"), Scenarios.fileFormats)
    families.foreach { family =>
      assertEquals(
        Scenarios.fileFormats.map(format => s"$family @ $format"),
        cases.map(_.id).filter(_.startsWith(s"$family @ ")),
        s"$family runs in every columnar format")
    }
  }

  private def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"$byte%02x")
      .mkString
}

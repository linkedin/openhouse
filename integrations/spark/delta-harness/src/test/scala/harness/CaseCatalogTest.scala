package harness

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the ordered catalog: its size, its fingerprint, the uniqueness of its IDs, the capability naming rule, and the
 * rule that every capability contributes exactly once. Reading the catalog does not execute a case or start Spark.
 */
final class CaseCatalogTest {
  private val expectedCaseCount = 1177
  private val expectedCatalogSha256 =
    "a10676c9fe0af5169459a0c9ad74eb2d005b7c7c63e2b7c7d4364bb7d8cc5bb9"

  /**
   * Every capability the standard catalog is built from, in the order Plan integrates them. This list is written out
   * here rather than derived from `Plan.contributions`, so adding, dropping, renaming or reordering a capability fails
   * this test until the intended catalog shape is restated.
   */
  private val expectedContributionNames = List(
    "accessControlCases",
    "changelogCases",
    "columnTagCases",
    "compactionPlanningCases",
    "concurrencyCases",
    "dataTypeCases",
    "dmlCases",
    "dmlValidationCases",
    "encryptionCases",
    "fileFormatCases",
    "fileReplicationCases",
    "incrementalReadCases",
    "lockingCases",
    "maintenanceCases",
    "metadataTableCases",
    "namespaceCases",
    "nestedTypeCases",
    "partitionEvolutionCases",
    "partitionTransformCases",
    "procedureCases",
    "renameCases",
    "scanPlanningCases",
    "schemaEvolutionCases",
    "snapshotRestoreCases",
    "sortOrderCases",
    "streamingCases",
    "tableEvolutionCompatibilityCases",
    "tablePropertyCases",
    "timeTravelCases",
    "writeDistributionCases",
    "writerCompatibilityCases")

  /**
   * Case-ID prefixes that name where a case came from rather than the capability it covers. Every case ID is owned by
   * the capability trait that defines it, so none of these appears in the catalog.
   */
  private val provenanceCaseIdPrefixes =
    List("fork.", "hazard.", "readerWriter.", "surface.", "interact.")

  @Test
  def orderedCaseCatalogMatchesBaseline(): Unit = {
    val caseIds = Plan.caseIds
    val actualCatalogSha256 = sha256(caseIds.mkString("\n"))
    val duplicateCaseIds = caseIds.groupBy(identity).collect {
      case (caseId, occurrences) if occurrences.size > 1 => caseId
    }.toList.sorted

    assertTrue(
      duplicateCaseIds.isEmpty,
      s"case IDs must be unique; duplicates=${duplicateCaseIds.mkString(", ")}")
    assertEquals(
      expectedCaseCount,
      caseIds.size,
      s"ordered case catalog changed; count=${caseIds.size}, sha256=$actualCatalogSha256")
    assertEquals(
      expectedCatalogSha256,
      actualCatalogSha256,
      s"ordered case catalog changed; count=${caseIds.size}, sha256=$actualCatalogSha256")
  }

  @Test
  def everyCaseIdNamesTheCapabilityItCovers(): Unit = {
    val provenanceNamedCaseIds =
      Plan.caseIds.filter(caseId => provenanceCaseIdPrefixes.exists(caseId.startsWith))

    assertTrue(
      provenanceNamedCaseIds.isEmpty,
      "a case ID names a capability, not the bucket it came from; " +
        s"offenders=${provenanceNamedCaseIds.mkString(", ")}")
  }

  @Test
  def theCatalogIntegratesExactlyTheIntendedCapabilities(): Unit = {
    val contributionNames = Plan.contributions.map { case (name, _) => name }

    assertEquals(
      expectedContributionNames,
      contributionNames,
      "Plan integrates a different set or order of capabilities than the catalog declares")
    assertEquals(
      expectedContributionNames.distinct.size,
      expectedContributionNames.size,
      "the declared capability list names a capability more than once")
  }

  @Test
  def eachCapabilityContributesExactlyOnceInOrder(): Unit = {
    val contributionNames = Plan.contributions.map { case (name, _) => name }

    assertEquals(
      contributionNames.distinct,
      contributionNames,
      s"a capability contribution is integrated more than once: $contributionNames")
    assertEquals(
      contributionNames.sorted,
      contributionNames,
      s"capability contributions are integrated in alphabetical order: $contributionNames")
    assertEquals(
      Plan.contributions.flatMap { case (_, contribution) => contribution.map(_.id) },
      Plan.caseIds,
      "the catalog is exactly its named contributions, concatenated in order")
    assertTrue(
      Plan.contributions.forall { case (_, contribution) => contribution.nonEmpty },
      "every named contribution supplies at least one case")
  }

  private def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"$byte%02x")
      .mkString
}

package harness

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the rules every integrated scenario set obeys: IDs are unique, the catalog is exactly
 * the foundation plus the extensions it names, it is those contributions concatenated in order, contributions are
 * named once and integrated alphabetically, every contribution supplies cases, and every case ID names its capability.
 *
 * This extension-stable test holds structural invariants while FoundationCatalogTest pins the exact set, size and
 * fingerprint of this branch's frozen foundation. Each later layer pins its contributions in a focused test. Reading
 * the catalog is a Spark-free operation.
 */
final class CaseCatalogTest {

  /**
   * Case-ID prefixes from the old provenance buckets. Every current case ID is owned by the capability trait that
   * defines it.
   */
  private val provenanceCaseIdPrefixes =
    List("fork.", "hazard.", "readerWriter.", "surface.", "interact.")

  @Test
  def everyCaseIdIsUnique(): Unit = {
    val caseIds = ScenarioCatalog.caseIds
    val duplicateCaseIds = caseIds.groupBy(identity).collect {
      case (caseId, occurrences) if occurrences.size > 1 => caseId
    }.toList.sorted

    assertTrue(
      duplicateCaseIds.isEmpty,
      s"case IDs must be unique; duplicates=${duplicateCaseIds.mkString(", ")}")
  }

  @Test
  def everyCaseIdNamesTheCapabilityItCovers(): Unit = {
    val provenanceNamedCaseIds =
      ScenarioCatalog.caseIds.filter(caseId => provenanceCaseIdPrefixes.exists(caseId.startsWith))

    assertTrue(
      provenanceNamedCaseIds.isEmpty,
      "a case ID names a capability, not the bucket it came from; " +
        s"offenders=${provenanceNamedCaseIds.mkString(", ")}")
  }

  @Test
  def eachCapabilityContributesExactlyOnceInAlphabeticalOrder(): Unit = {
    val contributionNames = ScenarioCatalog.contributions.map { case (name, _) => name }

    assertEquals(
      contributionNames.distinct,
      contributionNames,
      s"a capability contribution is integrated more than once: $contributionNames")
    assertEquals(
      contributionNames.sorted,
      contributionNames,
      s"capability contributions are integrated in alphabetical order: $contributionNames")
    assertTrue(
      ScenarioCatalog.contributions.forall { case (_, contribution) => contribution.nonEmpty },
      "every named contribution supplies at least one case")
  }

  @Test
  def theCatalogIsExactlyTheFoundationAndTheExtensionsItNames(): Unit = {
    val foundationNames = ScenarioCatalog.foundationContributions.map { case (name, _) => name }
    val extensionNames = ScenarioCatalog.extensionContributions.map { case (name, _) => name }
    val integratedNames = ScenarioCatalog.contributions.map { case (name, _) => name }
    val declaredCases = (ScenarioCatalog.foundationContributions ++
      ScenarioCatalog.extensionContributions).toMap

    assertTrue(
      foundationNames.intersect(extensionNames).isEmpty,
      "an extension names a contribution the foundation already owns: " +
        s"${foundationNames.intersect(extensionNames).mkString(", ")}")
    assertEquals(
      (foundationNames ++ extensionNames).sorted,
      integratedNames.sorted,
      "the catalog integrates a contribution that is neither a foundation nor an extension entry")
    ScenarioCatalog.contributions.foreach { case (name, contribution) =>
      assertEquals(
        declaredCases(name).map(_.id),
        contribution.map(_.id),
        s"$name is integrated as something other than the list the capability declares")
    }
  }

  @Test
  def theCatalogIsItsContributionsConcatenatedInOrder(): Unit = {
    val contributionOffsets = ScenarioCatalog.contributions
      .scanLeft(0) { case (offset, (_, contribution)) => offset + contribution.size }

    assertEquals(
      ScenarioCatalog.contributions.map { case (_, contribution) => contribution.size }.sum,
      ScenarioCatalog.caseIds.size,
      "the catalog holds exactly as many cases as its contributions supply")
    ScenarioCatalog.contributions.zip(contributionOffsets).foreach {
      case ((name, contribution), offset) =>
        assertEquals(
          contribution.map(_.id),
          ScenarioCatalog.caseIds.slice(offset, offset + contribution.size),
          s"$name does not occupy the slice of the catalog its position claims")
    }
  }
}

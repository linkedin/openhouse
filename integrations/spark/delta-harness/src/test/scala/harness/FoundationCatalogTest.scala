package harness

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the frozen foundation this branch owns: the eight capabilities in `ScenarioCatalog.foundationContributions`,
 * and for each one the exact number of cases it contributes and the fingerprint of the IDs it contributes, in order.
 *
 * Every assertion reads `foundationContributions` alone, never the complete catalog, so a later layer that fills in
 * `extensionContributions` leaves this file untouched and still passing. Each capability is pinned on its own line
 * against its own fingerprint, so a change to one of them fails only that line and names it. Uniqueness, ordering and
 * the contributions-concatenated rule are pinned once, for any catalog, in CaseCatalogTest.
 *
 * Reading the catalog is a Spark-free operation.
 */
final class FoundationCatalogTest {
  private val expectedFoundationCaseCount = 642

  /**
   * Every capability the frozen foundation is built from, in the order it declares them, with the number of cases it
   * contributes and the SHA-256 of its case IDs joined by newlines. This literal fixture makes every foundation
   * addition, removal, rename, reorder or resize require an explicit restatement.
   */
  private val expectedFoundationContributions = List(
    ("dataTypeCases", 10, "e676820cc791e8bbde8921a38d77049f980c55ce78c3a19b7c30a0c5694065e6"),
    ("dmlCases", 536, "0346a1b15adda474d19652c99a480228a61dedd376e4a5913b71a1c45e383e6f"),
    ("dmlValidationCases", 12, "ce7a969bef2060ff8b64333509f7ab40489a6c8b76b54354884c6468dc33a5ae"),
    ("fileFormatCases", 8, "97d22d197425a9156f8c1ef089d494d92f69a8ffcc09d487e5d8278070d98445"),
    ("nestedTypeCases", 18, "f2fcdc951e4c17e2d146c26e529f7ef3639534ea22c67dde795b325a7025ea70"),
    ("partitionEvolutionCases", 4, "bca457899d4108e353c31619603cec70888b7d3f4c35af67bbb27ebe9b1c4053"),
    ("schemaEvolutionCases", 42, "9609fab5bda329357ec4b582fbf225a47678c754e0698a8482c4d9412ddcceb8"),
    ("tablePropertyCases", 12, "b97d3f537b8d6740e00a6ae8fb8b88280de442fbc868342e5019fa0284434738"))

  private val expectedKnownBugCaseIds = List(
    "nested.deleteByNestedField @ nested-unpartitioned/orc",
    "nested.deleteByNestedField @ nested-unpartitioned/parquet",
    "prep.ordered:delete.byPartitionPredicate @ partitioned/orc",
    "prep.ordered:delete.byPartitionPredicate @ partitioned/parquet",
    "prep.ordered:delete.byPartitionPredicate @ unpartitioned/orc",
    "prep.ordered:delete.byPartitionPredicate @ unpartitioned/parquet",
    "schema.renameColumn @ partitioned/orc",
    "schema.renameColumn @ partitioned/parquet",
    "schema.renameColumn @ unpartitioned/orc",
    "schema.renameColumn @ unpartitioned/parquet")

  @Test
  def eachFoundationCapabilityContributesTheCasesItIsPinnedTo(): Unit = {
    val actualContributions =
      ScenarioCatalog.foundationContributions.map { case (name, contribution) =>
        (name, contribution.size, sha256(contribution.map(_.id).mkString("\n")))
      }

    assertEquals(
      expectedFoundationContributions.map { case (name, _, _) => name },
      actualContributions.map { case (name, _, _) => name },
      "the foundation declares a different set or order of capabilities than it is pinned to")
    expectedFoundationContributions.zip(actualContributions).foreach {
      case ((name, expectedCount, expectedSha256), (_, actualCount, actualSha256)) =>
        assertEquals(
          (expectedCount, expectedSha256),
          (actualCount, actualSha256),
          s"$name changed; count=$actualCount, sha256=$actualSha256")
    }
  }

  @Test
  def theFoundationIsTheSizeItIsPinnedTo(): Unit = {
    val foundationCaseCount =
      ScenarioCatalog.foundationContributions.map { case (_, contribution) => contribution.size }.sum

    assertEquals(
      expectedFoundationCaseCount,
      foundationCaseCount,
      s"foundation case count changed; count=$foundationCaseCount")
    assertEquals(
      expectedFoundationCaseCount,
      expectedFoundationContributions.map { case (_, count, _) => count }.sum,
      "the pinned per-capability counts do not add up to the pinned foundation total")
  }

  @Test
  def everyFoundationCaseRunsOnAColumnarFormatTheFoundationStandardizedOn(): Unit = {
    val preparationFormats = ScenarioCatalog.foundationContributions
      .flatMap { case (_, contribution) => contribution.map(_.id) }
      .map(caseId => caseId.split(" @ ").last)
      .map(label => label.split("/").last)
      .distinct

    assertEquals(List("parquet", "orc"), Scenarios.fileFormats)
    assertEquals(
      Scenarios.fileFormats.sorted,
      preparationFormats.sorted,
      s"a foundation case runs on a format outside the landing matrix: $preparationFormats")
  }

  @Test
  def theFoundationSkipMetadataIsPinnedToTheKnownProductBugs(): Unit = {
    val foundationCases =
      ScenarioCatalog.foundationContributions.flatMap { case (_, contribution) => contribution }

    assertEquals(
      expectedKnownBugCaseIds,
      foundationCases.collect {
        case testCase if testCase.knownBugReason.nonEmpty => testCase.id
      }.sorted,
      "the foundation known-bug cases changed")
    assertTrue(
      foundationCases.forall(_.embeddedSkipReason.isEmpty),
      "every foundation case reaches the embedded catalog")
  }

  private def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"$byte%02x")
      .mkString
}

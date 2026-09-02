package harness

import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the entry points a consumer outside this module was written against. Splitting the mixin object, the ordered
 * catalog and the case type into three names is an internal reorganisation, so a consumer that reads `Plan` and
 * `Scenarios` must keep compiling and keep answering the same values.
 *
 * Every reference below is written the way an external consumer writes it, so this file fails to compile if any of
 * those entry points is dropped, renamed or narrowed. The assertions prove the facade and catalog share one state.
 * Reading the catalog is a Spark-free operation.
 */
final class PublicSurfaceCompatibilityTest {

  @Test
  def planCaseStillNamesAndConstructsTheCaseType(): Unit = {
    val constructed: Plan.Case =
      Plan.Case("compat.probe", _ => (), knownBugReason = Some("probe"))

    assertEquals("compat.probe", constructed.id)
    assertEquals(Some("probe"), constructed.knownBugReason)
    assertEquals(None, constructed.embeddedSkipReason)
    assertTrue(
      (constructed: TestCase).isInstanceOf[TestCase],
      "Plan.Case must be the harness case type, not a separate copy of it")
  }

  @Test
  def planCaseStillMatchesAsAnExtractor(): Unit = {
    val matched = (Plan.Case("compat.probe", _ => ()): Plan.Case) match {
      case Plan.Case(id, _, None, None) => id
      case other                        => s"unmatched: $other"
    }

    assertEquals("compat.probe", matched)
  }

  @Test
  def planBugReasonStillPhrasesAKnownBugTheWayItAlwaysDid(): Unit = {
    val knownBug = Plan.Case("compat.bug", _ => (), knownBugReason = Some("the rewrite crashes"))
    val healthy = Plan.Case("compat.healthy", _ => ())

    assertEquals(Some("bug: the rewrite crashes"), Plan.bugReason(knownBug))
    assertEquals(None, Plan.bugReason(healthy))
  }

  @Test
  def planForwardsToTheCatalogRatherThanHoldingItsOwnCopy(): Unit = {
    assertEquals(ScenarioCatalog.caseIds, Plan.caseIds)
    assertEquals(ScenarioCatalog.cases.map(_.id), Plan.cases.map(_.id))
    assertEquals(Plan.cases.map(_.id), Plan.caseIds)
  }

  @Test
  def scenariosStillExposesTheConfigurationAConsumerOverrides(): Unit = {
    val originalDataSource = Scenarios.dataSource
    try {
      Scenarios.dataSource = "probe-source"

      assertEquals("probe-source", Scenarios.dataSource)
      assertTrue(
        Scenarios.layouts.head.create("db.t_probe").contains("USING probe-source"),
        "a CREATE statement must follow the data source the consumer set")
    } finally {
      Scenarios.dataSource = originalDataSource
    }

    assertEquals("iceberg", Scenarios.dataSource)
  }

  @Test
  def scenariosStillExposesTheCapabilityAndPreparationListsAConsumerReads(): Unit = {
    assertEquals(List("parquet", "orc"), Scenarios.fileFormats)
    assertEquals(4, Scenarios.layouts.size)
    assertEquals(4, Scenarios.preparedCoreTables.size)
    assertEquals(2, Scenarios.preparedCoreFormats.size)
    assertEquals(536, Scenarios.dmlCases.size)
    assertEquals(3, Scenarios.standardSeedRowCount)
    assertSame(
      Scenarios.dmlCases,
      ScenarioCatalog.foundationContributions.toMap.apply("dmlCases"),
      "the catalog must integrate the very list the capability exposes")
  }

  @Test
  def everyNamedContributionIsReadableFromTheScenariosObject(): Unit = {
    val contributionsFromScenariosObject: List[(String, List[TestCase])] = List(
      "dataTypeCases"           -> Scenarios.dataTypeCases,
      "dmlCases"                -> Scenarios.dmlCases,
      "dmlValidationCases"      -> Scenarios.dmlValidationCases,
      "fileFormatCases"         -> Scenarios.fileFormatCases,
      "nestedTypeCases"         -> Scenarios.nestedTypeCases,
      "partitionEvolutionCases" -> Scenarios.partitionEvolutionCases,
      "schemaEvolutionCases"    -> Scenarios.schemaEvolutionCases,
      "tablePropertyCases"      -> Scenarios.tablePropertyCases)

    assertEquals(
      contributionsFromScenariosObject.map { case (name, cases) => (name, cases.map(_.id)) },
      ScenarioCatalog.foundationContributions.map { case (name, cases) => (name, cases.map(_.id)) })
  }
}

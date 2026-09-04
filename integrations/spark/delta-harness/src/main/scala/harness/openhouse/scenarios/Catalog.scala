package harness

/**
 * Mixes every registered scenario with one shared kit instance. A scenario becomes runnable when this object mixes in
 * its trait and `Catalog` registers its case list.
 */
object Scenarios
    extends ScenarioDml
    with ScenarioDataType
    with ScenarioDmlValidation

/**
 * The ordered case catalog. Each named contribution owns its scenario body, preparation, assertions, and case IDs.
 * Extensions add one scenario mixin to `Scenarios` and one named case list to `extensionContributions`.
 */
object Catalog {

  /** The three scenario contributions that exercise the framework's core composition paths. */
  def foundationContributions: List[(String, List[TestCase])] =
    List(
      "dataTypeCases"      -> Scenarios.dataTypeCases,
      "dmlCases"           -> Scenarios.dmlCases,
      "dmlValidationCases" -> Scenarios.dmlValidationCases)

  /** Additional named scenario contributions supplied by a composed catalog. */
  def extensionContributions: List[(String, List[TestCase])] =
    List.empty

  /** Every capability contribution, named once, in the order the catalog integrates them. */
  def contributions: List[(String, List[TestCase])] =
    (foundationContributions ++ extensionContributions).sortBy { case (name, _) => name }

  /** The deterministic ordered case catalog. */
  def cases: List[TestCase] = contributions.flatMap { case (_, contribution) => contribution }

  /** The case IDs in catalog order. Reading them is a Spark-free catalog operation. */
  def caseIds: List[String] = cases.map(_.id)

}

/** Published facade for adapters that construct cases or enumerate the catalog through `Plan`. */
object Plan {

  /** The catalog's case type. */
  type Case = TestCase

  /** The case constructor and extractor exposed through the facade. */
  val Case: TestCase.type = TestCase

  /** The deterministic ordered case catalog. */
  def cases: List[TestCase] = Catalog.cases

  /** The case IDs in catalog order. */
  def caseIds: List[String] = Catalog.caseIds

  /** The skip reason a known bug produces, phrased so a run log explains the skip. */
  def bugReason(testCase: TestCase): Option[String] = testCase.bugReason

}

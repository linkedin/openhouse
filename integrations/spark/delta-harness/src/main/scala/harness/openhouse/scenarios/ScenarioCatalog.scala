package harness

/**
 * The single integration file: the object that mixes every scenario in, the ordered catalog built from it, and the
 * source entry points earlier consumers were written against.
 *
 * Adding a scenario is two lines here: one mixin on `Scenarios` and one named entry in
 * `ScenarioCatalog.extensionContributions`. Explicit registration makes this file the complete catalog definition.
 *
 * Mixing them here puts ScenarioKit first in the linearization, so its vals initialize before any capability's. This
 * object is what a scenario body, a preparation list and the harness configuration are read from, so it exposes the
 * shared kit surface (`dataSource`, `fileFormats`, the layout and preparation lists) alongside each capability's case
 * list.
 */
object Scenarios
    extends ScenarioDml
    with ScenarioRtas
    with ChangelogSupport

/**
 * The ordered catalog of scenario-owned test cases.
 *
 * The catalog is built from two explicit lists. `foundationContributions` is the focused behavior slice that proves
 * the execution framework; `extensionContributions` is where a later layer names the capabilities it adds.
 * `contributions` merges the two and sorts by contribution name, giving every layer a deterministic order independent
 * of list placement.
 *
 * A layer adds a capability through two append points: one mixin on `Scenarios` and one entry in
 * `extensionContributions`. It keeps its behavior and assertions in its own scenario source while the framework and
 * shared kit remain stable.
 *
 * Composition is all this object does: a scenario body, a preparation and a case ID all belong to the capability that
 * owns them.
 */
object ScenarioCatalog {

  /** The focused DML contract that proves the framework's end-to-end execution path. */
  def foundationContributions: List[(String, List[TestCase])] =
    List(
      "dmlCases" -> Scenarios.dmlCases)

  /**
   * The replace-table capability this layer adds to the canonical DML contract.
   */
  def extensionContributions: List[(String, List[TestCase])] =
    List(
      "rtasCases" -> Scenarios.rtasCases)

  /** Every capability contribution, named once, in the order the catalog integrates them. */
  def contributions: List[(String, List[TestCase])] =
    (foundationContributions ++ extensionContributions).sortBy { case (name, _) => name }

  /** The deterministic ordered case catalog. */
  def cases: List[TestCase] = contributions.flatMap { case (_, contribution) => contribution }

  /** The case IDs in catalog order. Reading them is a Spark-free catalog operation. */
  def caseIds: List[String] = cases.map(_.id)

}

/**
 * The entry point earlier consumers were written against. It preserves the source contract for `Plan.Case`,
 * `Plan.cases`, `Plan.caseIds` and `Plan.bugReason`.
 *
 * This stateless facade forwards every member to `ScenarioCatalog` or to the case itself, keeping one catalog state.
 * New code inside the harness reads `ScenarioCatalog` and `TestCase` directly.
 */
object Plan {

  /** The case type, which the harness now declares as `TestCase`. */
  type Case = TestCase

  /** The case constructor and extractor, so `Plan.Case(...)` still builds and matches a case. */
  val Case: TestCase.type = TestCase

  /** The deterministic ordered case catalog. */
  def cases: List[TestCase] = ScenarioCatalog.cases

  /** The case IDs in catalog order. */
  def caseIds: List[String] = ScenarioCatalog.caseIds

  /** The skip reason a known bug produces, phrased so a run log explains the skip. */
  def bugReason(testCase: TestCase): Option[String] = testCase.bugReason

}

package harness

/**
 * The ordered catalog of scenario-owned test cases.
 *
 * Every capability trait contributes exactly one case list. Plan names each contribution once, in alphabetical order
 * by contribution name, and concatenates them. Composition is all Plan does: a scenario body, a preparation and a case
 * ID all belong to the capability that owns them.
 */
object Plan {
  final case class Case(
    id:                 String,
    run:                Ctx => Unit,
    knownBugReason:     Option[String] = None,
    embeddedSkipReason: Option[String] = None
  )

  /** The deterministic ordered case catalog. Reading it does not execute a case or start Spark. */
  def caseIds: List[String] = cases.map(_.id)

  def bugReason(testCase: Case): Option[String] =
    testCase.knownBugReason.map(reason => s"bug: $reason")

  /** Every capability contribution, named once, in the order Plan integrates them. */
  def contributions: List[(String, List[Case])] =
    List(
      "accessControlCases"                -> Scenarios.accessControlCases,
      "changelogCases"                    -> Scenarios.changelogCases,
      "columnTagCases"                    -> Scenarios.columnTagCases,
      "compactionPlanningCases"           -> Scenarios.compactionPlanningCases,
      "concurrencyCases"                  -> Scenarios.concurrencyCases,
      "dataTypeCases"                     -> Scenarios.dataTypeCases,
      "dmlCases"                          -> Scenarios.dmlCases,
      "dmlValidationCases"                -> Scenarios.dmlValidationCases,
      "encryptionCases"                   -> Scenarios.encryptionCases,
      "fileFormatCases"                   -> Scenarios.fileFormatCases,
      "fileReplicationCases"              -> Scenarios.fileReplicationCases,
      "incrementalReadCases"              -> Scenarios.incrementalReadCases,
      "lockingCases"                      -> Scenarios.lockingCases,
      "maintenanceCases"                  -> Scenarios.maintenanceCases,
      "metadataTableCases"                -> Scenarios.metadataTableCases,
      "namespaceCases"                    -> Scenarios.namespaceCases,
      "nestedTypeCases"                   -> Scenarios.nestedTypeCases,
      "partitionEvolutionCases"           -> Scenarios.partitionEvolutionCases,
      "partitionTransformCases"           -> Scenarios.partitionTransformCases,
      "procedureCases"                    -> Scenarios.procedureCases,
      "renameCases"                       -> Scenarios.renameCases,
      "scanPlanningCases"                 -> Scenarios.scanPlanningCases,
      "schemaEvolutionCases"              -> Scenarios.schemaEvolutionCases,
      "snapshotRestoreCases"              -> Scenarios.snapshotRestoreCases,
      "sortOrderCases"                    -> Scenarios.sortOrderCases,
      "streamingCases"                    -> Scenarios.streamingCases,
      "tableEvolutionCompatibilityCases"  -> Scenarios.tableEvolutionCompatibilityCases,
      "tablePropertyCases"                -> Scenarios.tablePropertyCases,
      "timeTravelCases"                   -> Scenarios.timeTravelCases,
      "writeDistributionCases"            -> Scenarios.writeDistributionCases,
      "writerCompatibilityCases"          -> Scenarios.writerCompatibilityCases)

  def cases: List[Case] = contributions.flatMap { case (_, contribution) => contribution }
}

package harness

/** Mixes the scenario-owned case lists and shared preparation kits into one catalog source.
  *
  * The traits are listed bottom-up in the feature stack: the standard framework first, then RTAS,
  * then merge-on-read, then branch and write-audit-publish. A feature layer's traits extend that
  * layer's kit, so removing a layer's files and the traits below removes the layer entirely.
  */
object Scenarios
    extends DmlScenarios
    with NestedTypesScenarios
    with MaintControlScenarios
    with ForkScenarios
    with NegativeDdlScenarios
    with InteractionScenarios
    with SurfaceScenarios
    with HazardReaderWriterScenarios
    with ImplementationPinScenarios
    with RtasDmlScenarios
    with RtasDdlScenarios
    with RtasInteractionScenarios
    with RtasSurfaceScenarios
    with RtasHazardScenarios
    with MorDmlScenarios
    with MorMaintScenarios
    with MorReaderWriterScenarios
    with MorInteractionScenarios
    with MorSurfaceScenarios
    with MorForkScenarios
    with BranchDmlScenarios
    with BranchWapScenarios
    with BranchInteractionScenarios
    with BranchSurfaceScenarios
    with BranchHazardScenarios
    with BranchMorScenarios

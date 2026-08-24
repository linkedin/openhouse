package harness

/** Mixes the scenario-owned case lists and shared preparation kit into one catalog source. */
object Scenarios
    extends MorMaintScenarios
    with DmlScenarios
    with NestedTypesScenarios
    with MaintControlScenarios
    with ForkScenarios
    with BranchWapScenarios
    with NegativeDdlScenarios
    with InteractionScenarios
    with SurfaceScenarios
    with HazardReaderWriterScenarios

package harness

/** Mixes the standard scenario-owned case lists into one catalog source. */
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

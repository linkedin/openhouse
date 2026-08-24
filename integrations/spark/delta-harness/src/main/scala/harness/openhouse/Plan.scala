package harness

/** Defines the ordered catalog of scenario-owned test cases. */
object Plan {
  final case class Case(id: String, run: Ctx => Unit)

  /** The deterministic ordered case catalog. Reading it does not execute a case or start Spark. */
  def caseIds: List[String] = cases.map(_.id)

  // Known PRODUCT bugs: any case whose id contains the key is reported SKIP (bug: reason) instead
  // of failing the suite, and is tracked in BUGS.md. This is how we "tag a failing test and filter
  // it": a genuine bug is tagged here, deferred for follow-up, and never plowed past silently.
  val knownBugs: List[(String, String)] = List(
    // insert.explicitColumns is NO LONGER a bug tag — reclassified to a negative PIN (engine limitation,
    // not OpenHouse; code-verified). See insertExplicitColumns above and BUGS.md.
    "nested.deleteByNestedField" ->
      "DELETE WHERE <nested struct field> crashes with an internal optimizer NPE (SELECT/UPDATE on the same field work). Code-verified UPSTREAM: OpenHouse contributes no code to the row-level DELETE rewrite (owned by IcebergSparkSessionExtensions + Spark optimizer); the NPE is in the nested-field DELETE-rewrite plan. Needs a full stack capture before filing — see BUGS.md",
    "prep.ordered:delete.byPartitionPredicate" ->
      "DELETE by a partition predicate against a table created WITH a WRITE ORDERED BY clause throws an internal analyzer NPE, while the same DELETE on an unordered table (delete.byPartitionPredicate) succeeds. Code-verified UPSTREAM and the same family as nested.deleteByNestedField: OpenHouse contributes no code to the row-level DELETE rewrite (owned by IcebergSparkSessionExtensions plus the Spark optimizer), so the NPE lives in the ORDERED-BY DELETE-rewrite plan on Spark 3.5.2 / Iceberg 1.5.2. It reproduces identically on the embedded catalog and on the remote cluster, so it is gated in both environments. Needs a full stack capture before filing — see BUGS.md",
    "ddl.renameColumn" ->
      "RENAME COLUMN is a silent no-op. Code-verified GENUINE OpenHouse regression from #558 (commit 0ad4914): server-side normalizeSchemaCasingToTable rewrites every field's name to the table's spelling BY FIELD ID (BaseIcebergSchemaValidator:60-73), reverting the rename, and it runs BEFORE the sameSchema gate so validateWriteSchema (which would reject loudly) never fires. Fix: guard the normalizer with equalsIgnoreCase. Silent failure worse than the pre-#558 clean rejection — see BUGS.md",
    "ddl.encryption" ->
      "encryption KMS plugin is external/private (no impl/interface/mock in-repo); OSS leaves the encryption() hook un-wired and writes plaintext, so the intended-behavior assertion is deferred until the plugin is present — see DDL-TEST-PLAN.md / AUDIT-FINDINGS.md",
    "control.undrop" ->
      "undrop is SKIP under the DEFAULT stub path (HouseTableRepository is a @Primary in-memory stub; the public Tables DELETE hard-codes purge=true). Under HARNESS_REAL_HTS=1 the real embedded HTS is booted and undrop runs for real as the undrop:* battery + undropAdmin.* lifecycle (NOT SKIP) — see HTS-EMBED-PLAN.md / HTS-EMBED-IMPL.md / REST-FIDELITY-EVAL.md"
  )

  def bugReason(id: String): Option[String] =
    knownBugs.collectFirst { case (key, reason) if id.contains(key) => s"bug: $reason" }

  def cases: List[Case] =
    List(
      Scenarios.coreDmlCases,
      Scenarios.partitionedDmlCases,
      Scenarios.morDmlCases,
      Scenarios.deleteFileModeCases,
      Scenarios.nestedCases,
      Scenarios.typesCases,
      Scenarios.partitionTransformCases,
      Scenarios.partitionEvolutionCases,
      Scenarios.timeTravelCases,
      Scenarios.restoreRollbackCases,
      Scenarios.negativeCases,
      Scenarios.createSchemaCases,
      Scenarios.ddlSchemaCases,
      Scenarios.ddlNegativeCases,
      Scenarios.ddlPropertyCases,
      Scenarios.ddlMiscellaneousCases,
      Scenarios.ddlPolicyCases,
      Scenarios.ddlCtasRtasCases,
      Scenarios.ddlTagAclFeatureCases,
      Scenarios.ddlEncryptionCases,
      Scenarios.maintenanceCases,
      Scenarios.controlPlaneCases,
      Scenarios.branchingCases,
      Scenarios.interactionCases,
      Scenarios.interactionContextCases,
      Scenarios.surfaceCases,
      Scenarios.hazardCases,
      Scenarios.hazardContextCases,
      Scenarios.branchDmlCases,
      Scenarios.branchDdlCases,
      Scenarios.wapStagedCases,
      Scenarios.branchPartitionedDmlCases,
      Scenarios.branchMorDmlCases,
      Scenarios.rtasDmlCases,
      Scenarios.rtasPartitionedDmlCases,
      Scenarios.rtasMorDmlCases,
      Scenarios.morReadDmlCases,
      Scenarios.morCoexistCases,
      Scenarios.ddlConsumerCases,
      Scenarios.readerWriterCases,
      Scenarios.orderedDmlCases,
      Scenarios.evolvedDmlCases,
      Scenarios.undroppedDmlCases,
      Scenarios.undropAdminCases,
      Scenarios.maintenanceMorFoldCases,
      Scenarios.maintenanceMorMetaCases,
      Scenarios.undropInteractionCases,
      Scenarios.morHazardCases,
      Scenarios.morBranchMergeCases,
      Scenarios.encryptionPinCases,
      Scenarios.forkCases
    ).flatten
}

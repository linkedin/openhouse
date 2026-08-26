package harness

/** Defines the ordered catalog of scenario-owned test cases. */
object Plan {
  final case class Case(
    id:                     String,
    run:                    Ctx => Unit,
    description:            String,
    preparationDescription: String = "",
    knownBugReason:         Option[String] = None,
    embeddedSkipReason:     Option[String] = None
  ) {
    require(description.trim.nonEmpty, s"test case $id needs a description")
  }

  /** The deterministic ordered case catalog. Reading it does not execute a case or start Spark. */
  def caseIds: List[String] = cases.map(_.id)

  def bugReason(testCase: Case): Option[String] =
    testCase.knownBugReason.map(reason => s"bug: $reason")

  // The interaction, surface, reader/writer and hazard families are crossed with these two file
  // formats. Each family runs on one format before the next format starts, so the format loop is the
  // outer one and every contribution below keeps the catalog position it holds today.
  private val crossedFormats: List[String] = List("parquet", "orc")

  private def interactionContributions: List[Case] =
    crossedFormats.flatMap { format =>
      List(
        Scenarios.interactionDdlCases(format),
        Scenarios.interactionRtasCases(format),
        Scenarios.interactionBranchCases(format),
        Scenarios.interactionBranchFlagCases(format),
        Scenarios.interactionMorCases(format),
        Scenarios.interactionMiscellaneousCases(format)
      ).flatten
    }

  private def surfaceContributions: List[Case] =
    crossedFormats.flatMap { format =>
      List(
        Scenarios.surfaceBranchMaintenanceCases(format),
        Scenarios.surfaceMessageCases(format),
        Scenarios.surfaceBranchCases(format),
        Scenarios.surfaceReaderCases(format),
        Scenarios.surfaceRewriteProcedureCases(format),
        Scenarios.morSurfaceRewriteProcedureCases(format),
        Scenarios.surfaceBranchPublishCases(format),
        Scenarios.surfaceSnapshotProcedureCases(format),
        Scenarios.surfaceMetadataCases(format),
        Scenarios.morSurfaceMetadataCases(format),
        Scenarios.surfaceConcurrencyCases(format),
        Scenarios.surfaceRtasConcurrencyCases(format),
        Scenarios.surfaceSchemaCases(format),
        Scenarios.surfaceWriteCases(format),
        Scenarios.surfaceBranchWriteCases(format),
        Scenarios.surfacePinCases(format)
      ).flatten
    }

  private def hazardContributions: List[Case] =
    crossedFormats.flatMap { format =>
      List(
        Scenarios.hazardReaderCases(format),
        Scenarios.hazardRtasCases(format),
        Scenarios.hazardBranchCases(format),
        Scenarios.hazardWriterCases(format)
      ).flatten
    }

  private def readerWriterContributions: List[Case] =
    crossedFormats.flatMap { format =>
      List(
        Scenarios.readerWriterChangelogAppendCases(format),
        Scenarios.morReaderWriterChangelogAppendCases(format),
        Scenarios.readerWriterChangelogOverwriteCases(format),
        Scenarios.morReaderWriterChangelogOverwriteCases(format),
        Scenarios.readerWriterChangelogDeleteCases(format),
        Scenarios.morReaderWriterChangelogDeleteCases(format),
        Scenarios.readerWriterChangelogUpdateCases(format),
        Scenarios.morReaderWriterChangelogUpdateCases(format),
        Scenarios.readerWriterChangelogMergeCases(format),
        Scenarios.morReaderWriterChangelogMergeCases(format),
        Scenarios.readerWriterIncrementalAndStreamCases(format)
      ).flatten
    }

  // Every DDL-consumer family runs against one evolved preparation before the next preparation
  // starts, so the preparation loop is the outer one here.
  private def ddlConsumerContributions: List[Case] =
    Scenarios.ddlConsumerPreparations.flatMap { preparation =>
      List(
        Scenarios.ddlConsumerDataCases(preparation),
        Scenarios.branchDdlConsumerCases(preparation),
        Scenarios.ddlConsumerCompactionCases(preparation)
      ).flatten
    }

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
      Scenarios.layoutFormatCases,
      Scenarios.rtasLayoutFormatCases,
      Scenarios.branchLayoutFormatCases,
      Scenarios.morReadLayoutFormatCases,
      Scenarios.ddlSchemaCases,
      Scenarios.ddlNegativeCases,
      Scenarios.ddlPropertyCases,
      Scenarios.ddlMiscellaneousCases,
      Scenarios.ddlPolicyCases,
      Scenarios.ddlCtasRtasCases,
      Scenarios.ddlTagAclFeatureCases,
      Scenarios.maintenanceCases,
      Scenarios.controlPlaneCases,
      Scenarios.branchingCases
    ).flatten ++
      interactionContributions ++
      Scenarios.interactionContextCases ++
      surfaceContributions ++
      hazardContributions ++
      Scenarios.hazardContextCases ++
      List(
        Scenarios.branchDmlCases,
        Scenarios.branchDdlCases,
        Scenarios.wapStagedCases,
        Scenarios.branchPartitionedDmlCases,
        Scenarios.branchMorDmlCases,
        Scenarios.rtasDmlCases,
        Scenarios.rtasPartitionedDmlCases,
        Scenarios.rtasMorDmlCases,
        Scenarios.morReadDmlCases,
        Scenarios.morCoexistCases
      ).flatten ++
      ddlConsumerContributions ++
      readerWriterContributions ++
      List(
        Scenarios.orderedDmlCases,
        Scenarios.evolvedDmlCases,
        Scenarios.maintenanceMorFoldCases,
        Scenarios.maintenanceMorMetaCases,
        Scenarios.morHazardCases,
        Scenarios.morBranchMergeCases,
        Scenarios.encryptionPinCases,
        Scenarios.forkColumnDefaultAndDistributionCases,
        Scenarios.forkDeleteFileReplicationCases,
        Scenarios.forkFileAndCompactionCases
      ).flatten
}

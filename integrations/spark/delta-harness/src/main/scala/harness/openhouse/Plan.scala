package harness

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.iceberg.exceptions.BadRequestException
import org.apache.iceberg.exceptions.ValidationException
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

/** Assembles the run: every operation x every layout, plus create.schema per layout. */
object Plan {
  final case class Case(id: String, run: Ctx => Unit)

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

  def cases: List[Case] = {
    val dml = for {
      layout        <- Scenarios.layouts
      (name, op)    <- Scenarios.operations
    } yield Case(s"$name @ ${layout.label}", Scenarios.createAndSeed(layout, 3).andThen(op).run)

    val partitioned = for {
      layout        <- Scenarios.layouts.filter(_.label.startsWith("partitioned/"))
      (name, op)    <- Scenarios.partitionedOperations
    } yield Case(s"$name @ ${layout.label}", Scenarios.createAndSeed(layout, 3).andThen(op).run)

    // Merge-on-read: the same mutation operations, prepared on a MoR table.
    val mor = for {
      layout        <- Scenarios.morLayouts
      (name, op)    <- Scenarios.mutationOperations
    } yield Case(s"$name @ ${layout.label}", Scenarios.createAndSeed(layout, 3).andThen(op).run)

    // MoR discriminator: prove merge-on-read wrote delete files, and copy-on-write did not.
    val morVerify = Scenarios.morVerifyLayouts.map(layout =>
      Case(s"mor.writesDeleteFiles @ ${layout.label}", Scenarios.createAndSeedSingleFile(layout, 3).andThen(Scenarios.morWritesDeleteFiles).run))
    val cowVerify = Scenarios.cowVerifyLayouts.map(layout =>
      Case(s"cow.writesNoDeleteFiles @ ${layout.label}", Scenarios.createAndSeedSingleFile(layout, 3).andThen(Scenarios.cowWritesNoDeleteFiles).run))

    // Nested / complex types, on their own schema and layouts.
    val nested = for {
      layout        <- Scenarios.nestedLayouts
      (name, op)    <- Scenarios.nestedOperations
    } yield Case(s"$name @ ${layout.label}", Scenarios.createAndSeedNested(layout, 3).andThen(op).run)

    // Type-edge coverage, on TypesTable.
    val types = for {
      layout        <- Scenarios.typesLayouts
      (name, op)    <- Scenarios.typesOperations
    } yield Case(s"$name @ ${layout.label}", Scenarios.createAndSeedTypes(layout, 3).andThen(op).run)

    // Format multiplex. Blocks whose tables are seeded via the format-aware create helpers (coreCreateParquet
    // / coreCreate / propsCreate / the ddl inline creates now reading $seedFmt) run on parquet AND orc: any
    // table-creating op has a real format axis, and "format-inert" is a HYPOTHESIS this harness verifies, not
    // assumes. `crossFmt` sets the per-case seed format around each case (safe — cases are sequential per worker).
    val dataFormats     = List("parquet", "orc")
    def crossFmt[S <: Schema](block: List[(String, TableTest[S])]): List[Plan.Case] =
      for { f <- dataFormats; (name, t) <- block } yield Case(s"$name @ $f", ctx => Scenarios.withSeedFmt(f)(t.run(ctx)))

    // Partition transforms + evolution — multiplex (format is a hypothesis to verify, not assume).
    val partitionTransforms = crossFmt(Scenarios.partitionTransforms)
    val partitionEvolution  = crossFmt(Scenarios.partitionEvolution)

    val timeTravel      = for { f <- dataFormats; (name, t) <- Scenarios.timeTravelOps(f) }     yield Case(s"$name @ $f", t.run)
    val restoreRollback = for { f <- dataFormats; (name, t) <- Scenarios.restoreRollbackOps(f) } yield Case(s"$name @ $f", t.run)
    val maintenance     = for { f <- dataFormats; (name, t) <- Scenarios.maintenanceOps(f) }     yield Case(s"$name @ $f", t.run)
    val control         = Scenarios.controlPlane.map { case (name, f) => Case(s"$name @ embedded", f) }
    val forkColDefault  = Scenarios.forkColDefaultOps.map { case (name, f) => Case(name, f) }
    val forkPartitionDist = Scenarios.forkPartitionDistOps.map { case (name, f) => Case(name, f) }
    val forkDeleteFileReplication = Scenarios.forkDeleteFileReplicationOps.map { case (name, f) => Case(name, f) }
    val forkFileReplicationFactor = Scenarios.forkFileReplicationFactorOps.map { case (name, f) => Case(name, f) }
    val forkSplitSize   = Scenarios.forkSplitSizeOps.map { case (name, f) => Case(name, f) }
    val forkBinPackByLength = Scenarios.forkBinPackByLengthOps.map { case (name, f) => Case(name, f) }
    val forkCompactionOrder = Scenarios.forkCompactionOrderOps.map { case (name, f) => Case(name, f) }
    val branching       = crossFmt(Scenarios.branching)
    val branchDdl       = crossFmt(Scenarios.branchDdlOps)   // WAP mega-axis Stage B (G8 leak, systematic)
    val wapStaged       = crossFmt(Scenarios.wapStagedOps)   // WAP mega-axis Stage C (staged → publish)
    val interactions    = crossFmt(Scenarios.interactions) ++
      Scenarios.interactionCtxOps.map { case (name, f) => Case(s"$name @ embedded", f) }
    val surface         = crossFmt(Scenarios.surfaceOps)
    val hazards         = crossFmt(Scenarios.hazardOps) ++
      Scenarios.hazardCtxOps.map { case (name, f) => Case(s"$name @ embedded", f) }
    val readerWriter    = for { f <- dataFormats; (name, t) <- Scenarios.readerWriterOps(f) } yield Case(s"$name @ $f", t.run)
    val negatives       = crossFmt(Scenarios.negatives)
    val ddlNegatives    = crossFmt(Scenarios.ddlNegatives)
    val ddlProps        = crossFmt(Scenarios.ddlPropsOperations)
    val ddlMisc         = crossFmt(Scenarios.ddlMiscOperations)
    val ddlPolicy       = crossFmt(Scenarios.ddlPolicyOperations)
    val ddlCtasRtas     = crossFmt(Scenarios.ddlCtasRtasOperations)
    val ddlTagAcl       = crossFmt(Scenarios.ddlTagAclFeatureOperations)
    val ddlEncryption   = Scenarios.ddlEncryptionOperations.map { case (name, t) => Case(s"$name @ parquet", t.run) }

    // Phase 24 prep multipliers (full DML cross). Ordered prep × all operations; evolved prep ×
    // delete/update/read only (ADD COLUMN changes INSERT arity, breaking full-column inserts).
    val ddlPrepOrdered = for {
      layout     <- Scenarios.layouts
      (name, op) <- Scenarios.operations
    } yield Case(s"prep.ordered:$name @ ${layout.label}", Scenarios.createAndSeedOrdered(layout, 3).andThen(op).run)

    // delete/update/read only, and excluding ops that internally INSERT a full-column row
    // (delete.byNullCondition seeds a null row) — those hit the arity mismatch on the +1-column table.
    val ddlPrepEvolved = for {
      layout     <- Scenarios.layouts
      (name, op) <- Scenarios.operations.filter { case (n, _) =>
        (n.startsWith("delete.") || n.startsWith("update.") || n.startsWith("read.")) && !n.contains("byNullCondition") }
    } yield Case(s"prep.evolved:$name @ ${layout.label}", Scenarios.createAndSeedEvolved(layout, 3).andThen(op).run)

    // T axis — the whole DML catalog routed onto a BRANCH via spark.wap.branch (SURFACE-APPRAISAL
    // step 3). Format is vacuous for branches (refs never touch file encoding), so parquet only;
    // both partitionings kept (partitioning changes overwrite/dynamic-overwrite semantics on the
    // branch). Every op asserts its normal delta — now proving the op works branch-routed AND that
    // main is untouched (isolation). ~106 cases.
    // Format policy: ORC + Parquet (both), not parquet-only. Avro is intentionally NOT added to these
    // ref/metadata-routed blocks (branch/undrop/DDL-consumer) — the additive ask was ORC, and the
    // 3-format blocks keep Avro separately.
    val branchParquetLayouts = Scenarios.layouts.filter(l => l.label.endsWith("/parquet") || l.label.endsWith("/orc"))
    // WAP mega-axis Stage A — branch DML parity with the core CREATE path: all 6 layouts (incl avro) ×
    // operations, routed onto a branch, asserting branch delta + main isolation.
    val branchWap = for {
      layout     <- Scenarios.layouts
      (name, op) <- Scenarios.operations
    } yield Case(s"branchWap:$name @ ${layout.label}",
      Scenarios.createAndSeedOnBranch(layout, 3).andThen(op).andThen(Scenarios.branchMainIsolation).run)

    // Stage A — partition-only ops routed onto a branch (mirrors the core `partitioned` block).
    val branchWapPartitioned = for {
      layout     <- Scenarios.layouts.filter(_.label.startsWith("partitioned/"))
      (name, op) <- Scenarios.partitionedOperations
    } yield Case(s"branchWap:$name @ ${layout.label}",
      Scenarios.createAndSeedOnBranch(layout, 3).andThen(op).andThen(Scenarios.branchMainIsolation).run)

    // Branch × MoR — mutation ops routed onto a branch of a MoR table (cherry-pick rejects row-delete
    // snapshots). 3-format for parity with morLayouts (Stage A).
    val branchMorLayout = Scenarios.morLayouts.filter(_.label.startsWith("mor-unpartitioned/"))
    val branchWapMor = for {
      layout     <- branchMorLayout
      (name, op) <- Scenarios.mutationOperations
    } yield Case(s"branchWap:$name @ ${layout.label}",
      Scenarios.createAndSeedOnBranch(layout, 3).andThen(op).andThen(Scenarios.branchMainIsolation).run)

    // P axis (replace-lineage leg) — the whole DML catalog on an RTAS'd table (SURFACE-APPRAISAL
    // step 2). ~106 cases. (The undrop leg is gated on the embedded-HTS restructure — see
    // REST-FIDELITY-EVAL.md — so only the RTAS leg is runnable now.)
    val prepRtas = for {
      (label, partitionClause, fmt) <- Scenarios.rtasPrepShapes
      (name, op)                    <- Scenarios.operations
    } yield Case(s"prep.rtas:$name @ $label", Scenarios.createAndSeedRtas(partitionClause, 3, fmt).andThen(op).run)

    // RTAS full cross (Phase 28): partition-only ops on the partitioned RTAS shapes — mirrors the core
    // `partitioned` block (partitionedOperations × partitioned layouts) but on a replace-lineage base.
    val prepRtasPartitioned = for {
      (label, partitionClause, fmt) <- Scenarios.rtasPrepShapes.filter(_._1.startsWith("partitioned/"))
      (name, op)                    <- Scenarios.partitionedOperations
    } yield Case(s"prep.rtas:$name @ $label", Scenarios.createAndSeedRtas(partitionClause, 3, fmt).andThen(op).run)

    // RTAS × MoR — mutation ops on a replace-lineage MoR table. 3-format for parity with the core MoR
    // block (morLayouts = parquet/orc/avro), per the Phase-28 full cross.
    val prepRtasMor = for {
      fmt        <- List("parquet", "orc", "avro")
      (name, op) <- Scenarios.mutationOperations
    } yield Case(s"prep.rtasMor:$name @ mor-unpartitioned/$fmt",
      Scenarios.createAndSeedRtasMor("", 3, fmt).andThen(op).run)

    // P axis (drop→undrop leg) — the whole DML catalog on a table taken through a real HTS soft-delete
    // → restore round-trip (SURFACE-APPRAISAL). Requires the embedded real HTS (HARNESS_REAL_HTS=1);
    // empty otherwise. This is the surface-DOUBLING leg: every op re-verifies that the restored table
    // still behaves identically, i.e. that restore's destruction set does not intersect the feature's
    // state-dependency set. Undrop is metadata/ref reconstruction — file encoding is vacuous → parquet
    // layouts only (as with RTAS/branch).
    val undrop =
      if (HtsAdmin.enabled) for {
        layout     <- branchParquetLayouts
        (name, op) <- Scenarios.operations
      } yield Case(s"undrop:$name @ ${layout.label}",
        Scenarios.createAndSeedUndropped(layout, 3).andThen(op).run)
      else Nil

    // Undrop admin-lifecycle block (Phase 5) — soft-delete/list/restore/purge, real HTS only.
    val undropAdmin =
      if (HtsAdmin.enabled) Scenarios.undropAdminOps.map { case (name, run) => Case(name, run) }
      else Nil

    // Block 9 deepening: undrop 3-way compositions (branch/time-travel/schema survival), real HTS only.
    val undropInteract =
      if (HtsAdmin.enabled) Scenarios.undropInteractOps.map { case (name, run) => Case(name, run) }
      else Nil

    // DDL × consumer battery (task #3): each state-changing DDL, then each consumer must still work.
    // 4 DDL × 6 consumers × {unpartitioned, partitioned}/parquet = 48.
    val ddlConsumerBattery = for {
      layout          <- branchParquetLayouts
      (ddlName, prep) <- Scenarios.ddlPreps
      (conName, con)  <- Scenarios.ddlConsumers
    } yield Case(s"ddlConsume:$ddlName.$conName @ ${layout.label}", prep(layout).andThen(con).run)

    // MoR reads with a live position delete (closes the scan-path gap, step 1). Read/scan ops only —
    // they must apply the position delete at read time. Across formats (delete-file encoding differs).
    val morReadOps = Scenarios.operations.filter { case (n, _) => n.startsWith("read.") || n == "format.materialization" }
    val prepMorRead = for {
      layout     <- Scenarios.morVerifyLayouts   // single-file-friendly MoR layouts, per format
      (name, op) <- morReadOps
    } yield Case(s"prep.morRead:$name @ ${layout.label}", Scenarios.createAndSeedMorDeleted(layout, 3).andThen(op).run)

    // MoR delete-file COEXISTENCE (task #5 non-vacuous core): ops on a table that already carries a
    // live position delete. Format matters (delete-file encoding) → × 3 MoR formats.
    val morCoexist = for {
      layout     <- Scenarios.morVerifyLayouts
      (name, op) <- Scenarios.morCoexistOps
    } yield Case(s"$name @ ${layout.label}", Scenarios.createAndSeedMorDeleted(layout, 3).andThen(op).run)

    // Block 8 deepening: maintenance × MoR-with-live-delete. The delete-DECODE op (rewrite_data_files
    // fold) is format-relevant → × 3 MoR formats; metadata-only maintenance is format-vacuous → × 1.
    val maintenanceMorFold = for {
      layout     <- Scenarios.morVerifyLayouts
      (name, op) <- Scenarios.maintenanceMorFoldOps
    } yield Case(s"$name @ ${layout.label}", Scenarios.createAndSeedMorDeleted(layout, 3).andThen(op).run)
    val morParquetVerify = Scenarios.morVerifyLayouts.filter(l => l.label == "mor-verify/parquet" || l.label == "mor-verify/orc")
    val maintenanceMorMeta = for {
      layout     <- morParquetVerify
      (name, op) <- Scenarios.maintenanceMorMetaOps
    } yield Case(s"$name @ ${layout.label}", Scenarios.createAndSeedMorDeleted(layout, 3).andThen(op).run)

    // Block 10 deepening: MoR delete-file modality hazards (time-travel / rollback / expire). Snapshot
    // logic is format-vacuous → × 1 MoR layout.
    val morHazard = for {
      layout     <- morParquetVerify
      (name, op) <- Scenarios.morHazardOps
    } yield Case(s"$name @ ${layout.label}", Scenarios.createAndSeedMorDeleted(layout, 3).andThen(op).run)

    // MoR × branch MERGE: position deletes carried across fast_forward / cherry_pick / REPLACE BRANCH.
    // Single-file MoR seed so a branch DELETE is a real position delete; merge is format-vacuous → ×1.
    val morBranchMerge = for {
      layout     <- morParquetVerify
      (name, op) <- Scenarios.morBranchMergeOps
    } yield Case(s"$name @ ${layout.label}", Scenarios.createAndSeedSingleFile(layout, 3).andThen(op).run)

    // Encryption capability pin (characterization): OSS writes plaintext parquet (encryption un-wired).
    val encryptionPin = List(Case("surface.pin.dataPlaintext @ parquet", Scenarios.encryptionPlaintextPin.run))

    val creates = Scenarios.layouts.map { layout =>
      Case(s"create.schema @ ${layout.label}", Scenarios.createSchema(layout).run)
    }

    // DDL Phase 12: schema-evolution behaviors crossed with every layout.
    val ddlSchema = for {
      layout     <- Scenarios.layouts
      (name, op) <- Scenarios.ddlSchemaOperations
    } yield Case(s"$name @ ${layout.label}", Scenarios.createAndSeed(layout, 3).andThen(op).run)

    dml ++ partitioned ++ mor ++ morVerify ++ cowVerify ++ nested ++ types ++ partitionTransforms ++
      partitionEvolution ++ timeTravel ++ restoreRollback ++ negatives ++ creates ++ ddlSchema ++
      ddlNegatives ++ ddlProps ++ ddlMisc ++ ddlPolicy ++ ddlCtasRtas ++ ddlTagAcl ++ ddlEncryption ++
      maintenance ++ control ++ branching ++ interactions ++ surface ++ hazards ++ branchWap ++
      branchDdl ++ wapStaged ++ branchWapPartitioned ++ branchWapMor ++ prepRtas ++ prepRtasPartitioned ++ prepRtasMor ++ prepMorRead ++ morCoexist ++ ddlConsumerBattery ++
      readerWriter ++ ddlPrepOrdered ++ ddlPrepEvolved ++ undrop ++ undropAdmin ++
      maintenanceMorFold ++ maintenanceMorMeta ++ undropInteract ++ morHazard ++ morBranchMerge ++
      encryptionPin ++ forkColDefault ++ forkPartitionDist ++
      forkDeleteFileReplication ++ forkFileReplicationFactor ++ forkSplitSize ++
      forkBinPackByLength ++ forkCompactionOrder
  }
}

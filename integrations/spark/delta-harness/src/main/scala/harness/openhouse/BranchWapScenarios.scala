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

trait BranchWapScenarios extends ScenarioKit {
  import Rows._

  // ── Undrop 3-way compositions (Block 9, real HTS only) — restore's state-preservation, per feature ──
  // The undrop:* battery proves the whole op catalog works post-restore. These are pointed 3-way
  // chains that set up a SPECIFIC feature's state (branch / snapshot history / evolved schema),
  // destroy via soft-delete→restore, then consume that exact feature — the direct modality check that
  // restore's destruction set does not intersect refs / lineage / schema.

  // A pre-existing branch must survive the drop→undrop round-trip.
  def interactUndropBranchSurvives(ctx: Ctx): Unit = {
    val (table, db, tbl) = undropSeed(ctx, "t_ud_branch")
    ctx.spark.sql(s"ALTER TABLE $table CREATE BRANCH b")
    ctx.spark.sql(s"INSERT INTO $table.branch_b ${RowGenerator.valuesClause(Core, 2)}")   // branch diverges: 3+2=5
    softDeleteRestore(ctx, db, tbl)
    assert(ctx.spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "main row set changed across undrop")
    assert(ctx.spark.sql(s"SELECT count(*) FROM $table VERSION AS OF 'b'").collect()(0).getLong(0) == 5, "branch 'b' did not survive undrop")
    ctx.spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // Snapshot history (time travel) must survive restore.
  def interactUndropTimeTravelSurvives(ctx: Ctx): Unit = {
    val (table, db, tbl) = undropSeed(ctx, "t_ud_tt")
    val firstSnap = ctx.spark.sql(s"SELECT snapshot_id FROM $table.snapshots ORDER BY committed_at LIMIT 1").collect()(0).getLong(0)
    ctx.spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, 2)}")            // 2nd snapshot: 5 rows
    softDeleteRestore(ctx, db, tbl)
    assert(ctx.spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 5, "current state changed across undrop")
    assert(ctx.spark.sql(s"SELECT count(*) FROM $table VERSION AS OF $firstSnap").collect()(0).getLong(0) == 3,
      "pre-restore snapshot not time-travellable after undrop (lineage lost)")
    ctx.spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // Evolved schema must survive restore, and the restored table must still accept the evolved shape.
  def interactUndropSchemaSurvives(ctx: Ctx): Unit = {
    val (table, db, tbl) = undropSeed(ctx, "t_ud_schema")
    ctx.spark.sql(s"ALTER TABLE $table ADD COLUMN extra int")
    ctx.spark.sql(s"INSERT INTO $table VALUES (CAST(9 AS BIGINT), 9, 'row-9', 9.5, false, '2024-01-09-08', 99)")
    softDeleteRestore(ctx, db, tbl)
    assert(ctx.spark.sql(s"SELECT extra FROM $table WHERE ${Core.long0.columnName} = 9").collect()(0).getInt(0) == 99,
      "evolved column value lost across undrop")
    ctx.spark.sql(s"INSERT INTO $table VALUES (CAST(10 AS BIGINT), 10, 'row-10', 10.5, true, '2024-01-10-09', 100)")
    assert(ctx.spark.sql(s"SELECT count(*) FROM $table WHERE extra IS NOT NULL").collect()(0).getLong(0) == 2,
      "restored table did not accept the evolved schema for new writes")
    ctx.spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  val undropInteractOps: List[(String, Ctx => Unit)] = List(
    "interact.undrop.branchSurvives"    -> interactUndropBranchSurvives,
    "interact.undrop.timeTravelSurvives" -> interactUndropTimeTravelSurvives,
    "interact.undrop.schemaSurvives"    -> interactUndropSchemaSurvives
  )

  // ── Branching / WAP (format-agnostic → parquet only; behavior-focused, not matrixed) ─────────
  // A CoreTable row literal for branch writes (long,int,string,double,boolean,datepartition).

  // B1(a) direct branch ops (no WAP needed): write to t.branch_b, read it via VERSION AS OF 'b';
  // main stays isolated.
  val branchDirectIsolation: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("branch.direct.create")(t => s"ALTER TABLE $t CREATE BRANCH b")()
      .step("branch.direct.isolation") { (spark, table) =>
        spark.sql(s"INSERT INTO $table.branch_b VALUES ${coreRow(99, "branch")}")
        val onBranch = spark.sql(s"SELECT count(*) FROM $table VERSION AS OF 'b'").collect()(0).getLong(0)
        val onMain   = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)
        assert(onBranch == 4, s"branch b should have 4 rows, got $onBranch")
        assert(onMain == 3, s"main should be unchanged at 3, got $onMain")                // isolation
      }()

  // B1(b) spark.wap.branch conf: with write.wap.enabled, the conf routes BOTH reads and writes to the
  // branch transparently; unsetting reverts to main.
  val branchWapConfRouting: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("branch.wapconf.enable")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .sql("branch.wapconf.create")(t => s"ALTER TABLE $t CREATE BRANCH wapbr")()
      .step("branch.wapConf.routing") { (spark, table) =>
        spark.conf.set("spark.wap.branch", "wapbr")
        val onBranch =
          try {
            spark.sql(s"INSERT INTO $table VALUES ${coreRow(99, "wap")}")                 // routed to branch
            spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)             // reads branch
          } finally spark.conf.unset("spark.wap.branch")
        assert(onBranch == 4, s"on-branch read should see 4, got $onBranch")
        assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "main leaked")
      }()

  // B2 WAP stage → publish: a staged write (spark.wap.id) does NOT advance main; cherrypick publishes it.
  val wapStagePublish: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("wap.enable")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .step("wap.stagePublish") { (spark, table) =>
        spark.conf.set("spark.wap.id", "w1")
        try spark.sql(s"INSERT INTO $table VALUES ${coreRow(99, "staged")}")
        finally spark.conf.unset("spark.wap.id")
        assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "staged write leaked to main")
        val stagedId = spark.sql(s"SELECT snapshot_id FROM $table.snapshots WHERE summary['wap.id'] = 'w1'").collect()(0).getLong(0)
        spark.sql(s"CALL openhouse.system.cherrypick_snapshot('${catalogRelative(table)}', $stagedId)")
        assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 4, "publish did not advance main")
      }()

  // ── WAP mega-axis Stage C — staged-WAP write surface (stage → publish visibility) ────────────
  // The op is written as a STAGED snapshot (spark.wap.id): it must NOT advance main; assert main is
  // unchanged pre-publish, then cherrypick_snapshot PUBLISHES it and main reflects it. This is the
  // Phase-29 "T2 staged" target. Format-multiplexed by crossFmt (seedFmt-aware create).
  private def wapStagedWrite(label: String)(write: String => String)(preRows: Long, postRows: Long): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql(s"$label.enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .step(label) { (spark, table) =>
        spark.conf.set("spark.wap.id", "wS")
        try spark.sql(write(table)) finally spark.conf.unset("spark.wap.id")
        val mainPre = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)
        val stagedCount = spark.sql(s"SELECT count(*) FROM $table.snapshots WHERE summary['wap.id'] = 'wS'").collect()(0).getLong(0)
        println(s"DIAG $label: mainPreCount=$mainPre (expected $preRows) stagedSnapshots=$stagedCount")
        assert(mainPre == preRows,
          s"$label: staged write LEAKED to main pre-publish (main=$mainPre, expected $preRows)")
        val stagedId = spark.sql(s"SELECT snapshot_id FROM $table.snapshots WHERE summary['wap.id'] = 'wS'").collect()(0).getLong(0)
        spark.sql(s"CALL openhouse.system.cherrypick_snapshot('${catalogRelative(table)}', $stagedId)")
        assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == postRows,
          s"$label: publish did not reflect the staged write (expected $postRows)")
      }()

  val wapStagedOps: List[(String, TableTest[CoreTable.type])] = List(
    "wapStaged.insert"    -> wapStagedWrite("wapStaged.insert")(t => s"INSERT INTO $t VALUES ${coreRow(99, "staged")}")(3, 4),
    "wapStaged.overwrite" -> wapStagedWrite("wapStaged.overwrite")(t => s"INSERT OVERWRITE $t VALUES ${coreRow(7, "ow")}")(3, 1),
    // FINDING (WAP1): a staged DELETE is NOT honored by WAP — it commits to MAIN immediately and creates
    // NO staged snapshot (main 3→2, zero snapshots tagged wap.id), unlike staged INSERT/OVERWRITE/UPDATE/
    // MERGE which all stage. Observed on parquet+orc; whether this is stock Iceberg or OpenHouse-specific is
    // not determined here. A "staged" DELETE therefore silently publishes to main. Pins the observed behavior.
    "wapStaged.delete.bypassesWap" -> {
      TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
        .sql("wapStaged.delete.enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
        .step("wapStaged.delete.bypassesWap") { (spark, table) =>
          spark.conf.set("spark.wap.id", "wD")
          try spark.sql(s"DELETE FROM $table WHERE ${Core.long0.columnName} = 1") finally spark.conf.unset("spark.wap.id")
          val mainPre = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)
          val staged  = spark.sql(s"SELECT count(*) FROM $table.snapshots WHERE summary['wap.id'] = 'wD'").collect()(0).getLong(0)
          println(s"DIAG wapStaged.delete.bypassesWap: mainAfterStagedDelete=$mainPre stagedSnapshots=$staged")
          assert(mainPre == 2 && staged == 0,
            s"FINDING WAP1: expected staged DELETE to BYPASS WAP (commit to main=2, no staged snapshot); got main=$mainPre staged=$staged — behavior changed, re-audit AUDIT-FINDINGS WAP1")
        }()
    },
    "wapStaged.merge"     -> wapStagedWrite("wapStaged.merge")(t =>
      s"MERGE INTO $t USING (SELECT CAST(99 AS BIGINT) AS k) s ON $t.${Core.long0.columnName} = s.k " +
      s"WHEN NOT MATCHED THEN INSERT (${Core.columnNames.mkString(", ")}) VALUES (s.k, 9, 'm', 9.5, true, '2024-01-09-01')")(3, 4),
    // Staged UPDATE: main's value is unchanged pre-publish, changed after publish (count stays 3).
    "wapStaged.update.valueVisibleOnlyAfterPublish" -> {
      TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
        .sql("wapStaged.update.enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
        .step("wapStaged.update.valueVisibleOnlyAfterPublish") { (spark, table) =>
          spark.conf.set("spark.wap.id", "wU")
          try spark.sql(s"UPDATE $table SET ${Core.string0.columnName} = 'staged-upd' WHERE ${Core.long0.columnName} = 1")
          finally spark.conf.unset("spark.wap.id")
          val pre = spark.sql(s"SELECT ${Core.string0.columnName} FROM $table WHERE ${Core.long0.columnName} = 1").collect()(0).getString(0)
          assert(pre != "staged-upd", s"staged UPDATE leaked to main pre-publish: $pre")
          val stagedId = spark.sql(s"SELECT snapshot_id FROM $table.snapshots WHERE summary['wap.id'] = 'wU'").collect()(0).getLong(0)
          spark.sql(s"CALL openhouse.system.cherrypick_snapshot('${catalogRelative(table)}', $stagedId)")
          val post = spark.sql(s"SELECT ${Core.string0.columnName} FROM $table WHERE ${Core.long0.columnName} = 1").collect()(0).getString(0)
          assert(post == "staged-upd", s"publish did not reflect the staged UPDATE: $post")
        }()
    },
    // C3(a): two concurrent staged ids publish INDEPENDENTLY and in the chosen order.
    "wapStaged.twoIdsIndependent" -> {
      TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
        .sql("wapStaged.two.enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
        .step("wapStaged.twoIdsIndependent") { (spark, table) =>
          def staged(id: String, k: Int): Unit = {
            spark.conf.set("spark.wap.id", id)
            try spark.sql(s"INSERT INTO $table VALUES ${coreRow(k, s"s-$id")}") finally spark.conf.unset("spark.wap.id")
          }
          staged("wa", 101); staged("wb", 102)
          assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "either staged id leaked to main")
          def idOf(w: String): Long = spark.sql(s"SELECT snapshot_id FROM $table.snapshots WHERE summary['wap.id'] = '$w'").collect()(0).getLong(0)
          spark.sql(s"CALL openhouse.system.cherrypick_snapshot('${catalogRelative(table)}', ${idOf("wa")})")
          assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 4, "publishing wa did not advance main by 1")
          assert(spark.sql(s"SELECT count(*) FROM $table WHERE ${Core.long0.columnName} = 102").collect()(0).getLong(0) == 0, "wb published without being cherrypicked")
          spark.sql(s"CALL openhouse.system.cherrypick_snapshot('${catalogRelative(table)}', ${idOf("wb")})")
          assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 5, "publishing wb did not advance main to 5")
        }()
    },
    // C3(b): a staged (unpublished) snapshot is UNREFERENCED — assert expire_snapshots behaviour toward it
    // (G11(d): age-based expiration can delete staged WAP snapshots pre-publish). Characterize: after a
    // far-future expire, can the staged id still be cherrypicked, or is it stranded?
    "wapStaged.expireVsStaged" -> {
      TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
        .sql("wapStaged.exp.enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
        .step("wapStaged.expireVsStaged") { (spark, table) =>
          spark.conf.set("spark.wap.id", "wE")
          try spark.sql(s"INSERT INTO $table VALUES ${coreRow(200, "stg")}") finally spark.conf.unset("spark.wap.id")
          val stagedId = spark.sql(s"SELECT snapshot_id FROM $table.snapshots WHERE summary['wap.id'] = 'wE'").collect()(0).getLong(0)
          spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
          val survived = spark.sql(s"SELECT count(*) FROM $table.snapshots WHERE snapshot_id = $stagedId").collect()(0).getLong(0)
          val pub = try { spark.sql(s"CALL openhouse.system.cherrypick_snapshot('${catalogRelative(table)}', $stagedId)"); "published" }
            catch { case NonFatal(e) => s"stranded:${Exceptions.root(e).getClass.getSimpleName}" }
          println(s"DIAG wapStaged.expireVsStaged: stagedSurvivedExpire=$survived cherrypickAfterExpire=$pub")
          // Pin the audited hazard (G11 d): unreferenced staged snapshot is expirable -> stranded pre-publish.
          assert(survived == 0 && pub.startsWith("stranded"),
            s"G11(d): expected the unreferenced staged snapshot to be expired then un-cherrypickable; survived=$survived pub=$pub — re-audit")
        }()
    }
  )

  // B3 DDL-on-branch is NOT isolated — characterizes the leak (finding): schema/props/sortOrder are
  // table-global; ADD COLUMN while "on branch" mutates MAIN's schema, with no guard.
  val branchDdlLeakAddColumn: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("branch.leak.enable")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .sql("branch.leak.create")(t => s"ALTER TABLE $t CREATE BRANCH leakbr")()
      .step("branch.ddlLeak.addColumn") { (spark, table) =>
        spark.conf.set("spark.wap.branch", "leakbr")
        try spark.sql(s"ALTER TABLE $table ADD COLUMN leaked_col int")
        finally spark.conf.unset("spark.wap.branch")
        val mainCols = spark.table(table).schema.fields.map(_.name).toSeq
        assert(mainCols.contains("leaked_col"),
          s"characterizing the leak: ADD COLUMN on a branch mutated MAIN's schema — expected leaked_col in $mainCols")
      }()

  // B4 representative branch DML (update + delete on a branch), isolated from main.
  val branchDmlUpdateDelete: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("branch.dml.enable")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .sql("branch.dml.create")(t => s"ALTER TABLE $t CREATE BRANCH dmlbr")()
      .step("branch.dml.updateDelete") { (spark, table) =>
        spark.conf.set("spark.wap.branch", "dmlbr")
        try {
          spark.sql(s"UPDATE $table SET ${Core.string0.columnName} = 'br-upd' WHERE ${Core.long0.columnName} = 1")
          spark.sql(s"DELETE FROM $table WHERE ${Core.long0.columnName} = 2")
        } finally spark.conf.unset("spark.wap.branch")
        val onBranch = spark.sql(s"SELECT count(*) FROM $table VERSION AS OF 'dmlbr'").collect()(0).getLong(0)
        assert(onBranch == 2, s"branch should have 2 rows after delete, got $onBranch")
        assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "main unchanged by branch DML")
        val br1 = spark.sql(s"SELECT ${Core.string0.columnName} FROM $table VERSION AS OF 'dmlbr' WHERE ${Core.long0.columnName} = 1").collect()(0).getString(0)
        assert(br1 == "br-upd", s"branch update not applied: $br1")
      }()

  // B5 lifecycle (CREATE TAG / DROP BRANCH — both supported, verified) + WAP mixing negatives.
  val branchCreateTag: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("branch.lifecycle.tag") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table CREATE TAG mytag")
        assert(spark.sql(s"SELECT count(*) FROM $table.refs WHERE name = 'mytag' AND type = 'TAG'").collect()(0).getLong(0) == 1,
          "CREATE TAG did not create the tag ref")
      }()

  val branchDropBranch: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("branch.drop.create")(t => s"ALTER TABLE $t CREATE BRANCH tmpbr")()
      .step("branch.lifecycle.dropBranch") { (spark, table) =>
        assert(spark.sql(s"SELECT count(*) FROM $table.refs WHERE name = 'tmpbr'").collect()(0).getLong(0) == 1, "branch not created")
        spark.sql(s"ALTER TABLE $table DROP BRANCH tmpbr")
        assert(spark.sql(s"SELECT count(*) FROM $table.refs WHERE name = 'tmpbr'").collect()(0).getLong(0) == 0, "DROP BRANCH did not remove the ref")
      }()

  val branchNegWapIdAndBranch: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("branch.neg.enable")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .sql("branch.neg.create")(t => s"ALTER TABLE $t CREATE BRANCH nb")()
      .step("branch.neg.wapIdAndBranch") { (spark, table) =>
        spark.conf.set("spark.wap.id", "w1")
        spark.conf.set("spark.wap.branch", "nb")
        try {
          val e = Check.intercept[ValidationException](spark.sql(s"INSERT INTO $table VALUES ${coreRow(99, "x")}"))
          assert(e.getMessage.contains("Cannot set both WAP ID and branch"), s"msg: ${e.getMessage.take(140)}")
        } finally { spark.conf.unset("spark.wap.id"); spark.conf.unset("spark.wap.branch") }
      }()

  val branchNegInsertNonexistent: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("branch.neg.insertNonexistentBranch") { (spark, table) =>
        val e = Check.intercept[ValidationException](spark.sql(s"INSERT INTO $table.branch_nope VALUES ${coreRow(99, "x")}"))
        assert(e.getMessage.contains("does not exist"), s"msg: ${e.getMessage.take(140)}")
      }()

  // ── WAP mega-axis Stage B — systematic branch-DDL leak (G8) ──────────────────────────────────
  // Table-global DDL (schema / props / sortOrder / policy) run WHILE `spark.wap.branch` is set: per G8
  // these apply table-globally at every layer, so they LEAK to MAIN rather than staying branch-scoped.
  // Each pins the ACTUAL outcome on MAIN (wap.branch unset after the DDL) — leak / silent-no-op / rejected.
  // If OpenHouse later scopes branch DDL, these flip. Format-multiplexed by crossFmt (seedFmt-aware create).
  private def branchDdlOnBranch(label: String)(ddl: String => String)(assertMain: (SparkSession, String) => Unit): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql(s"$label.enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .sql(s"$label.createBranch")(t => s"ALTER TABLE $t CREATE BRANCH bddl")()
      .step(label) { (spark, table) =>
        spark.conf.set("spark.wap.branch", "bddl")
        val outcome = try { spark.sql(ddl(table)); "accepted" }
          catch { case NonFatal(e) => s"rejected:${Exceptions.root(e).getClass.getSimpleName}" }
        finally spark.conf.unset("spark.wap.branch")
        println(s"DIAG $label: branch-routed DDL $outcome")
        assertMain(spark, table)
      }()

  val branchDdlOps: List[(String, TableTest[CoreTable.type])] = List(
    // ADD COLUMN on a branch → main's schema gains the column (schema is table-global → leak).
    "branchDdl.addColumn.leaksToMain" -> branchDdlOnBranch("branchDdl.addColumn.leaksToMain")(
      t => s"ALTER TABLE $t ADD COLUMN br_added int") { (spark, table) =>
        val cols = spark.sql(s"DESCRIBE TABLE $table").collect().map(_.getString(0).trim).toSet
        assert(cols.contains("br_added"),
          "G8: ADD COLUMN on a branch should LEAK to main's schema (table-global); main did not gain the column — re-audit G8")
      },
    // SET TBLPROPERTIES on a branch → main gets the property (props are table-global → leak).
    "branchDdl.setTblProp.leaksToMain" -> branchDdlOnBranch("branchDdl.setTblProp.leaksToMain")(
      t => s"ALTER TABLE $t SET TBLPROPERTIES ('user.branchkey'='v1')") { (spark, table) =>
        val props = spark.sql(s"SHOW TBLPROPERTIES $table").collect().map(r => r.getString(0) -> r.getString(1)).toMap
        assert(props.get("user.branchkey").contains("v1"),
          s"G8: SET TBLPROPERTIES on a branch should LEAK to main; got ${props.get("user.branchkey")} — re-audit G8")
      },
    // ALTER COLUMN comment on a branch → main's schema metadata changes (leak).
    "branchDdl.alterColumnComment.leaksToMain" -> branchDdlOnBranch("branchDdl.alterColumnComment.leaksToMain")(
      t => s"ALTER TABLE $t ALTER COLUMN ${Core.string0.columnName} COMMENT 'br-comment'") { (spark, table) =>
        val c = spark.sql(s"DESCRIBE TABLE $table").collect()
          .find(_.getString(0).trim == Core.string0.columnName).map(_.getString(2)).getOrElse("")
        assert(Option(c).getOrElse("").contains("br-comment"),
          s"G8: ALTER COLUMN COMMENT on a branch should LEAK to main; main comment='$c' — re-audit G8")
      },
    // DROP COLUMN is rejected on main (unsupported) — assert it is ALSO rejected via a branch (the guard
    // is schema-global, not branch-aware): pin the rejection is unchanged under wap.branch.
    "branchDdl.dropColumn.rejected" -> branchDdlOnBranch("branchDdl.dropColumn.rejected")(
      t => s"ALTER TABLE $t DROP COLUMN ${Core.string0.columnName}") { (spark, table) =>
        val cols = spark.sql(s"DESCRIBE TABLE $table").collect().map(_.getString(0).trim).toSet
        assert(cols.contains(Core.string0.columnName),
          "DROP COLUMN must remain rejected (main keeps the column) whether or not spark.wap.branch is set")
      }
  )

  val branching: List[(String, TableTest[CoreTable.type])] = List(
    "branch.direct.isolation" -> branchDirectIsolation,
    "branch.wapConf.routing"  -> branchWapConfRouting,
    "wap.stagePublish"        -> wapStagePublish,
    "branch.ddlLeak.addColumn" -> branchDdlLeakAddColumn,
    "branch.dml.updateDelete" -> branchDmlUpdateDelete,
    "branch.lifecycle.tag"    -> branchCreateTag,
    "branch.lifecycle.dropBranch" -> branchDropBranch,
    "branch.neg.wapIdAndBranch" -> branchNegWapIdAndBranch,
    "branch.neg.insertNonexistentBranch" -> branchNegInsertNonexistent
  )


}

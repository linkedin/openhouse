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

trait InteractionScenarios extends ScenarioKit {
  import Rows._


  // ── DDL × history ──────────────────────────────────────────────────────────────────────────
  val interactTtAfterAddColumn: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("interact.ddl.ttAfterAddColumn") { (spark, table) =>
        val s0 = snapshotIds(spark, table).last
        spark.sql(s"ALTER TABLE $table ADD COLUMN extra_col INT")
        spark.sql(s"INSERT INTO $table VALUES $extraColInsert9")
        val current = spark.sql(s"SELECT * FROM $table LIMIT 1").columns.toSeq
        val travel  = spark.sql(s"SELECT * FROM $table VERSION AS OF $s0 LIMIT 1").columns.toSeq
        assert(current.contains("extra_col"), s"current read missing evolved column: $current")
        assert(!travel.contains("extra_col") && travel.size == Core.tableColumns.size,
          s"time travel must read with the SNAPSHOT's schema (no extra_col): $travel")
        assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF $s0").collect()(0).getLong(0) == 3,
          "pre-DDL snapshot row count wrong")
      }()

  val interactRestoreAfterAddColumn: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("interact.ddl.restoreAfterAddColumn") { (spark, table) =>
        val s0 = snapshotIds(spark, table).last
        spark.sql(s"ALTER TABLE $table ADD COLUMN extra_col INT")
        spark.sql(s"INSERT INTO $table VALUES $extraColInsert9")
        spark.sql(s"CALL openhouse.system.rollback_to_snapshot('${catalogRelative(table)}', $s0)")
        val cols = spark.sql(s"SELECT * FROM $table LIMIT 1").columns.toSeq
        assert(cols.contains("extra_col"), s"rollback rolls back DATA only — schema keeps the evolved column: $cols")
        assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "data not rolled back")
        assert(spark.sql(s"SELECT count(*) FROM $table WHERE extra_col IS NOT NULL").collect()(0).getLong(0) == 0,
          "rolled-back rows must read the evolved column as null")
        spark.sql(s"INSERT INTO $table VALUES $extraColInsert10") // table stays writable at the evolved arity
        assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 4, "post-rollback insert failed")
      }()

  // E1: data in the evolved column, then the (currently pinned-rejected) DROP — table stays intact.
  // Gating pin: if DROP COLUMN support ever lands this fails → extend to full post-drop coverage.
  val interactDropColAfterData: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("interact.ddl.dropColAfterData") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table ADD COLUMN extra_col INT")
        spark.sql(s"INSERT INTO $table VALUES $extraColInsert9")
        val e = Check.intercept[BadRequestException](spark.sql(s"ALTER TABLE $table DROP COLUMN extra_col"))
        assert(e.getMessage.contains("not found in newSchema"), s"drop rejection message changed: ${e.getMessage.take(200)}")
        assert(spark.sql(s"SELECT count(*) FROM $table WHERE extra_col = 42").collect()(0).getLong(0) == 1,
          "rejected drop must leave the column's data readable")
        spark.sql(s"INSERT INTO $table VALUES $extraColInsert10")
        assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 5,
          "rejected drop must leave the table writable")
      }()

  // ── RTAS × history / lineage ───────────────────────────────────────────────────────────────

  val interactRtasHistoryPreserved: TableTest[CoreTable.type] =
    rtasPrep.step("interact.rtas.historyPreserved") { (spark, table) =>
      val pre = snapshotIds(spark, table).last
      spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
      assert(spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0) == 2,
        "pre-RTAS snapshots must survive the replace")
      assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF $pre").collect()(0).getLong(0) == 3,
        "time travel to a pre-RTAS snapshot must work")
    }()

  val interactRtasRestoreRejected: TableTest[CoreTable.type] =
    rtasPrep.step("interact.rtas.restoreRejected") { (spark, table) =>
      val pre = snapshotIds(spark, table).last
      spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
      val e = Check.intercept[ValidationException](
        spark.sql(s"CALL openhouse.system.rollback_to_snapshot('${catalogRelative(table)}', $pre)"))
      assert(e.getMessage.contains("not an ancestor"),
        s"rollback across RTAS: expected the new-lineage/ancestry rejection, got: ${e.getMessage.take(200)}")
    }()

  // The recovery path rollback can't provide: set_current_snapshot has no ancestry requirement.
  val interactRtasSetCurrentRecovery: TableTest[CoreTable.type] =
    rtasPrep.step("interact.rtas.setCurrentRecovery") { (spark, table) =>
      val pre = snapshotIds(spark, table).last
      spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
      spark.sql(s"CALL openhouse.system.set_current_snapshot('${catalogRelative(table)}', $pre)")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3,
        "set_current_snapshot must recover the pre-RTAS state (no ancestry requirement)")
    }()

  val interactRtasWriteAfter: TableTest[CoreTable.type] =
    rtasPrep.step("interact.rtas.writeAfter") { (spark, table) =>
      spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
      spark.sql(s"INSERT INTO $table VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3,
        "replaced table must stay writable (DML-after-RTAS)")
    }()

  // G9 (partition half): the replace path skips checkPartitionSpecEvolution — RTAS CAN change the
  // spec where ALTER is pinned-rejected. Characterizes the bypass; if this ever fails, the guard
  // was extended to the replace path — update AUDIT-FINDINGS G9.
  val interactRtasPartitionSpecChange: TableTest[CoreTable.type] =
    rtasPrep.step("interact.rtas.partitionSpecChange") { (spark, table) =>
      spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource PARTITIONED BY (datepartition) AS SELECT * FROM $table")
      val desc = spark.sql(s"DESCRIBE TABLE $table").collect().toSeq
      // Confirmed live: the table gains a "# Partition Information" section (datepartition listed
      // both as a column and as a partition field) — the spec changed where ALTER is pinned-rejected.
      assert(desc.exists(_.getString(0) == "# Partition Information") &&
             desc.count(_.getString(0) == "datepartition") == 2,
        s"G9 appears FIXED — RTAS no longer changes the partition spec; update AUDIT-FINDINGS G9. DESCRIBE:\n" +
          desc.map(_.mkString(" | ")).mkString("\n"))
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "rows lost in re-spec RTAS")
    }()

  // G9 (schema half): column drop via RTAS projection, where ALTER DROP COLUMN is pinned-rejected.
  // Confirmed live (first run failed on the harness's own read-back because the column was GONE).
  // Runs on a side table so the pipeline's implicit full-schema read-back stays valid.
  val interactRtasDropsColumn: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("interact.rtas.dropsColumn") { (spark, table) =>
        val side = s"${table}_dropcol"
        spark.sql(s"DROP TABLE IF EXISTS $side")
        try {
          spark.sql(s"CREATE TABLE $side USING $dataSource TBLPROPERTIES ('replace.enabled'='true') AS SELECT * FROM $table")
          spark.sql(s"CREATE OR REPLACE TABLE $side USING $dataSource AS " +
            s"SELECT ${Core.long0.columnName}, ${Core.string0.columnName} FROM $side")
          val cols = spark.sql(s"SELECT * FROM $side LIMIT 1").columns.toSeq
          assert(cols == Seq(Core.long0.columnName, Core.string0.columnName),
            s"G9 appears FIXED — RTAS no longer drops columns (ALTER DROP stays rejected); update AUDIT-FINDINGS G9: $cols")
          assert(spark.sql(s"SELECT count(*) FROM $side").collect()(0).getLong(0) == 3, "rows lost in column-drop RTAS")
        } finally spark.sql(s"DROP TABLE IF EXISTS $side")
      }()

  // ── RTAS × table-property merge semantics (the THIRD property path beside CREATE and ALTER) ──
  val interactRtasPropsUserSurvival: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
        s"'write.format.default'='$seedFmt', 'replace.enabled'='true', 'user.key'='v1')")()
      .insert(3)()
      .step("interact.rtas.props.userSurvival") { (spark, table) =>
        spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
        val p = tableProps(spark, table)
        assert(p.get("user.key").contains("v1"), s"user prop lost across RTAS: user.key=${p.get("user.key")}")
        assert(p.get("replace.enabled").contains("true"), s"replace.enabled lost across RTAS: ${p.get("replace.enabled")}")
      }()

  val interactRtasPropsStatementWins: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
        s"'write.format.default'='$seedFmt', 'replace.enabled'='true', 'user.key'='v1')")()
      .insert(3)()
      .step("interact.rtas.props.statementWins") { (spark, table) =>
        spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource TBLPROPERTIES ('user.key'='v2') " +
          s"AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
        val p = tableProps(spark, table)
        assert(p.get("user.key").contains("v2"), s"statement TBLPROPERTIES must win over the old value: ${p.get("user.key")}")
        assert(p.get("replace.enabled").contains("true"),
          s"props NOT named in the statement must still survive (merge, not wholesale replace): ${p.get("replace.enabled")}")
      }()

  val interactRtasPropsCreateDefaulting: TableTest[CoreTable.type] =
    rtasPrep.step("interact.rtas.props.createDefaulting") { (spark, table) =>
      spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource TBLPROPERTIES ('write.format.default'='orc') " +
        s"AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
      val p = tableProps(spark, table)
      assert(p.get("write.format.default").contains("orc"),
        s"RTAS can change the storage format where ALTER can't rewrite: ${p.get("write.format.default")}")
      assert(p.get("format-version").forall(_ == "2"), s"forced format-version drifted: ${p.get("format-version")}")
      spark.sql(s"INSERT INTO $table VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "orc-format table not writable")
    }()

  val interactRtasPropsReservedPlane: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource PARTITIONED BY (datepartition) TBLPROPERTIES (" +
        s"'write.format.default'='$seedFmt', 'replace.enabled'='true')")()
      .insert(3)()
      .sql("setRetention")(t => s"ALTER TABLE $t SET POLICY (RETENTION = 30d ON COLUMN datepartition WHERE pattern = 'yyyy-MM-dd-HH')")()
      .step("interact.rtas.props.reservedPlane") { (spark, table) =>
        val uuidBefore = tableProps(spark, table).getOrElse("openhouse.tableUUID", "<absent>")
        spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource PARTITIONED BY (datepartition) " +
          s"AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
        val p = tableProps(spark, table)
        assert(p.getOrElse("openhouse.tableUUID", "<absent>") == uuidBefore,
          s"tableUUID must be preserved across RTAS: $uuidBefore -> ${p.get("openhouse.tableUUID")}")
        // G10 (confirmed live): RTAS silently WIPES the policies plane — the retention policy set
        // before the replace is gone after it (while tableUUID survives). Characterizes the bug;
        // if this fails, G10 was fixed — flip to a survival assertion and update AUDIT-FINDINGS.
        val policiesAfter = p.get("policies")
        assert(policiesAfter.forall(b => !b.toLowerCase.contains("retention")),
          s"G10 appears FIXED — retention policy survived RTAS; update AUDIT-FINDINGS G10 and flip this test: $policiesAfter")
      }()

  // RTAS on a table with an existing branch: refs travel in the replace payload — branch survives,
  // still readable at its (old-lineage) head.
  val interactRtasWithBranch: TableTest[CoreTable.type] =
    rtasPrep.step("interact.rtas.withBranch") { (spark, table) =>
      spark.sql(s"ALTER TABLE $table CREATE BRANCH keepbr")
      spark.sql(s"INSERT INTO $table.branch_keepbr VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
      val refs = spark.sql(s"SELECT name FROM $table.refs").collect().toSeq.map(_.getString(0)).toSet
      assert(refs.contains("keepbr"), s"branch ref lost across RTAS: $refs")
      assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF 'keepbr'").collect()(0).getLong(0) == 4,
        "branch head (old lineage) unreadable after RTAS")
    }()

  // ── branch × history / maintenance ─────────────────────────────────────────────────────────
  val interactBranchTtBeforeBranchPoint: TableTest[CoreTable.type] =
    coreTwoSnapshots.step("interact.branch.ttBeforeBranchPoint") { (spark, table) =>
      val snaps = snapshotIds(spark, table)
      val ts0 = spark.sql(s"SELECT committed_at FROM $table.snapshots ORDER BY committed_at LIMIT 1").collect()(0).getTimestamp(0)
      spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")
      spark.sql(s"ALTER TABLE $table CREATE BRANCH tb")
      spark.sql(s"INSERT INTO $table.branch_tb VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF 'tb'").collect()(0).getLong(0) == 6, "branch head")
      assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF ${snaps.head}").collect()(0).getLong(0) == 3,
        "snapshot-id travel to a pre-branch-point ancestor must work")
      spark.conf.set("spark.wap.branch", "tb")
      try {
        assert(spark.sql(s"SELECT count(*) FROM $table TIMESTAMP AS OF '$ts0'").collect()(0).getLong(0) == 3,
          "explicit TIMESTAMP AS OF must override spark.wap.branch and resolve against main history")
        assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF ${snaps.head}").collect()(0).getLong(0) == 3,
          "explicit VERSION AS OF must override spark.wap.branch")
      } finally spark.conf.unset("spark.wap.branch")
    }()

  // E5 characterization (mirror of G8): DDL on MAIN hits branches immediately — schema is
  // table-global, and an old-arity branch writer is broken mid-flight.
  val interactBranchMainDdlImmediate: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("interact.branch.mainDdlImmediate") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")
        spark.sql(s"ALTER TABLE $table CREATE BRANCH mb")
        spark.sql(s"INSERT INTO $table.branch_mb VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        spark.sql(s"ALTER TABLE $table ADD COLUMN extra_col INT") // DDL on MAIN
        val branchCols = spark.sql(s"SELECT * FROM $table VERSION AS OF 'mb' LIMIT 1").columns.toSeq
        assert(branchCols.contains("extra_col"), s"main DDL is table-global — branch reads see it immediately: $branchCols")
        val e = Check.intercept[AnalysisException](
          spark.sql(s"INSERT INTO $table.branch_mb VALUES (CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')"))
        assert(e.getMessage.toLowerCase.contains("not enough data columns"),
          s"old-arity branch writer must break after main DDL (characterizes the hazard): ${e.getMessage.take(200)}")
        spark.sql(s"INSERT INTO $table.branch_mb VALUES (CAST(8 AS BIGINT), 8, 'row-8', 8.5, true, '2024-01-08-07', 44)")
        assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF 'mb'").collect()(0).getLong(0) == 5,
          "new-arity branch write after main DDL")
      }()

  // E10: expiration is ref-aware — branch heads survive, shared ancestry prunes.
  val interactBranchExpireProtectsRefs: TableTest[CoreTable.type] =
    coreTwoSnapshots.step("interact.branch.expireProtectsRefs") { (spark, table) =>
      spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")
      spark.sql(s"ALTER TABLE $table CREATE BRANCH eb")
      spark.sql(s"INSERT INTO $table.branch_eb VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      spark.sql(s"INSERT INTO $table VALUES (CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
      assert(spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0) == 4, "expected 4 snapshots pre-expire")
      spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
      val refs = spark.sql(s"SELECT name FROM $table.refs").collect().toSeq.map(_.getString(0)).toSet
      assert(refs == Set("main", "eb"), s"branch/tag refs must survive expiration: $refs")
      assert(spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0) == 2,
        "shared ancestry prunes to the two ref heads")
      assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF 'eb'").collect()(0).getLong(0) == 6, "branch readable post-expire")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 6, "main readable post-expire")
    }()

  // C4: restore procedures target MAIN even while spark.wap.branch is set (procedures are not
  // branch-conf-routed) — the branch is untouched.
  val interactBranchRollbackWhileWapConf: TableTest[CoreTable.type] =
    coreTwoSnapshots.step("interact.branch.rollbackWhileWapConf") { (spark, table) =>
      val s0 = snapshotIds(spark, table).head
      spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")
      spark.sql(s"ALTER TABLE $table CREATE BRANCH rb")
      spark.sql(s"INSERT INTO $table.branch_rb VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      spark.conf.set("spark.wap.branch", "rb")
      try spark.sql(s"CALL openhouse.system.rollback_to_snapshot('${catalogRelative(table)}', $s0)")
      finally spark.conf.unset("spark.wap.branch")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3,
        "rollback under wap.branch conf still targets MAIN (procedures are not branch-routed)")
      assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF 'rb'").collect()(0).getLong(0) == 6,
        "branch untouched by the main rollback")
    }()

  // C1: rolled-past snapshots are unreferenced — expiration makes the rollback permanent.
  val interactRestoreExpireAfterRollback: TableTest[CoreTable.type] =
    coreTwoSnapshots.step("interact.restore.expireAfterRollback") { (spark, table) =>
      val snaps = snapshotIds(spark, table)
      spark.sql(s"CALL openhouse.system.rollback_to_snapshot('${catalogRelative(table)}', ${snaps.head})")
      spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
      assert(spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0) == 1,
        "the rolled-past snapshot must be expired (unreferenced)")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "current state intact")
      val e = Check.intercept[Exception](
        spark.sql(s"SELECT count(*) FROM $table VERSION AS OF ${snaps(1)}").collect())
      assert(Exceptions.causeChain(e).exists(t => Option(t.getMessage).exists(_.toLowerCase.contains("snapshot"))),
        s"travel to the expired snapshot must fail (rollback is now PERMANENT): ${e.getMessage.take(200)}")
    }()

  // ── THE COMPOSITE DEFECT: branch × expiration × merge (G11; INTERACTION-AUDIT §6) ───────────
  // Bytecode-confirmed mechanism: RemoveSnapshots retention is per-ref and head-anchored (no
  // protection for the ancestry BETWEEN live refs), and SnapshotUtil's ancestry walk SILENTLY
  // TRUNCATES at an expired hole and returns false. So policy-driven expiration between branch
  // work and the merge makes fast_forward spuriously reject with "not an ancestor" — even when
  // main never advanced — and, with no rebase in Iceberg, the branch is permanently stranded.
  // The pair test (branch × expire) PASSES because reads don't consume ancestry; only the merge does.
  val interactExpireMergeSpuriousReject: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("interact.branch.expireMerge.spuriousReject") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table CREATE BRANCH mb")
        spark.sql(s"INSERT INTO $table.branch_mb VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')") // B1
        spark.sql(s"INSERT INTO $table.branch_mb VALUES (CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')") // B2 (head)
        assert(countOf(spark, s"SELECT count(*) FROM $table.snapshots") == "3", "expected P, B1, B2")
        // main NEVER advances. This merge is valid right now (branch.fastForward.merge is the
        // no-expiration control proving it). Interpose the destroyer:
        spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
        // P2 VIOLATED: retention is per-ref head-anchored — the intermediate branch commit B1
        // (merge connectivity) is expired even though both refs are alive.
        assert(countOf(spark, s"SELECT count(*) FROM $table.snapshots") == "2",
          "retention keeps only the two ref heads; the intermediate branch snapshot is expired")
        // The pair-test ILLUSION: refs alive, branch fully readable — nothing looks broken.
        val refs = spark.sql(s"SELECT name FROM $table.refs").collect().toSeq.map(_.getString(0)).toSet
        assert(refs == Set("main", "mb"), s"both refs alive: $refs")
        assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'mb'") == "5", "branch readable")
        // P1 VIOLATED: the merge is now spuriously rejected — the ancestry walk from B2 hits the
        // B1 hole, silently truncates, and concludes main's head "is not an ancestor" of the branch.
        val e = Check.intercept[Exception](
          spark.sql(s"CALL openhouse.system.fast_forward('${catalogRelative(table)}', 'main', 'mb')"))
        assert(Option(e.getMessage).exists(_.contains("not an ancestor")),
          s"G11 appears FIXED — fast_forward survived expiration (or failed differently); update AUDIT-FINDINGS G11: " +
            s"${e.getClass.getName} ${Option(e.getMessage).getOrElse("").take(180)}")
        // P6 VIOLATED: no recovery path merges the branch. Characterize the cherry-pick fallback:
        val b2 = spark.sql(s"SELECT snapshot_id FROM $table.refs WHERE name = 'mb'").collect()(0).getLong(0)
        val cherry = try {
          spark.sql(s"CALL openhouse.system.cherrypick_snapshot('${catalogRelative(table)}', ${b2}L)")
          s"SUCCEEDED — main now ${countOf(spark, s"SELECT count(*) FROM $table")} rows (B1's commit silently LOST in the 'merge')"
        } catch { case t: Throwable => s"REJECTED ${t.getClass.getName} :: ${Option(t.getMessage).getOrElse("").take(160)}" }
        println(s"DIAG expireMerge.cherrypickFallback: $cherry")
        val mainCount = countOf(spark, s"SELECT count(*) FROM $table").toLong
        assert(mainCount == 3 || mainCount == 4, s"main must stay consistent (3, or 4 if cherry-pick half-merged): $mainCount")
        // Copy-out is the ONLY full recovery (data files survive: expiration ran cleanExpiredFiles(false)).
        assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'mb'") == "5",
          "branch data must remain readable for copy-out recovery")
      }()

  // P3 VIOLATED: WAP-staged snapshots are UNREFERENCED, so age-based expiration silently deletes
  // them before publish; the loss only becomes loud at publish time ("Cannot find snapshot").
  // OpenHouse's scheduled expiration job (default 3-day TTL) makes this automatic, not hypothetical.
  val interactExpireMergeStagedWapLoss: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .step("interact.branch.expireMerge.stagedWapLoss") { (spark, table) =>
        spark.conf.set("spark.wap.id", "w2")
        try spark.sql(s"INSERT INTO $table VALUES (CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01')")
        finally spark.conf.unset("spark.wap.id")
        assert(countOf(spark, s"SELECT count(*) FROM $table.snapshots WHERE summary['wap.id'] = 'w2'") == "1", "staged")
        spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
        // The SILENT loss: expiration reports nothing about the staged work it destroyed.
        assert(countOf(spark, s"SELECT count(*) FROM $table.snapshots WHERE summary['wap.id'] = 'w2'") == "0",
          "P3 appears FIXED — staged WAP snapshot survived expiration; update AUDIT-FINDINGS G11")
        // Loud only NOW, at publish — after the work is unrecoverable:
        val e = Check.intercept[Exception](
          spark.sql(s"CALL openhouse.system.publish_changes(table => '${catalogRelative(table)}', wap_id => 'w2')"))
        println(s"DIAG stagedWapLoss.publish: ${e.getClass.getName} :: ${Option(e.getMessage).getOrElse("").take(180)}")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "3", "main unchanged; the staged write is gone")
      }()

  // ── flags at CREATE + ALTER-to-MoR + compaction over evolved schema ────────────────────────
  val interactFlagsWapReplaceAtCreate: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
        s"'write.format.default'='$seedFmt', 'write.wap.enabled'='true', 'replace.enabled'='true')")()
      .insert(3)()
      .step("interact.flags.wapReplaceAtCreate") { (spark, table) =>
        val p = tableProps(spark, table)
        assert(p.get("write.wap.enabled").contains("true") && p.get("replace.enabled").contains("true"),
          s"flags set at CREATE must be honored: wap=${p.get("write.wap.enabled")} replace=${p.get("replace.enabled")}")
        spark.sql(s"ALTER TABLE $table CREATE BRANCH cb") // wap-at-create usable immediately
        val e = Check.intercept[BadRequestException](
          spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table"))
        assert(e.getMessage.contains("while WAP"),
          s"RTAS-while-WAP guard must fire from create-time flags too: ${e.getMessage.take(200)}")
      }()

  val interactMorAlterToMor: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)()
      .sql("seed(3, one-file)")(t =>
        s"INSERT INTO $t SELECT /*+ COALESCE(1) */ * FROM (${RowGenerator.valuesClause(Core, 3)}) AS seed")()
      .step("interact.mor.alterToMor") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')")
        spark.sql(s"DELETE FROM $table WHERE ${Core.long0.columnName} = 1")
        val deleteFiles = spark.sql(s"SELECT count(*) FROM $table.all_delete_files").collect()(0).getLong(0)
        assert(deleteFiles == 1,
          s"ALTER-to-MoR must govern subsequent deletes (expected 1 position-delete file, got $deleteFiles)")
        assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "row not deleted")
      }()

  val interactMaintCompactEvolved: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("interact.maint.compactEvolved") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table ADD COLUMN extra_col INT")
        spark.sql(s"INSERT INTO $table VALUES $extraColInsert9")
        spark.sql(s"INSERT INTO $table VALUES $extraColInsert10")
        spark.sql(s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}')")
        assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 5, "compaction changed row count")
        assert(spark.sql(s"SELECT count(*) FROM $table WHERE extra_col IN (42, 43)").collect()(0).getLong(0) == 2,
          "compaction over mixed-schema files must preserve evolved-column values")
        assert(spark.sql(s"SELECT count(*) FROM $table WHERE extra_col IS NULL").collect()(0).getLong(0) == 3,
          "pre-evolution rows must stay null in the evolved column")
      }()

  val interactions: List[(String, TableTest[CoreTable.type])] = List(
    "interact.ddl.ttAfterAddColumn"       -> interactTtAfterAddColumn,
    "interact.ddl.restoreAfterAddColumn"  -> interactRestoreAfterAddColumn,
    "interact.ddl.dropColAfterData"       -> interactDropColAfterData,
    "interact.rtas.historyPreserved"      -> interactRtasHistoryPreserved,
    "interact.rtas.restoreRejected"       -> interactRtasRestoreRejected,
    "interact.rtas.setCurrentRecovery"    -> interactRtasSetCurrentRecovery,
    "interact.rtas.writeAfter"            -> interactRtasWriteAfter,
    "interact.rtas.partitionSpecChange"   -> interactRtasPartitionSpecChange,
    "interact.rtas.dropsColumn"           -> interactRtasDropsColumn,
    "interact.rtas.props.userSurvival"    -> interactRtasPropsUserSurvival,
    "interact.rtas.props.statementWins"   -> interactRtasPropsStatementWins,
    "interact.rtas.props.createDefaulting" -> interactRtasPropsCreateDefaulting,
    "interact.rtas.props.reservedPlane"   -> interactRtasPropsReservedPlane,
    "interact.rtas.withBranch"            -> interactRtasWithBranch,
    "interact.branch.ttBeforeBranchPoint" -> interactBranchTtBeforeBranchPoint,
    "interact.branch.mainDdlImmediate"    -> interactBranchMainDdlImmediate,
    "interact.branch.expireProtectsRefs"  -> interactBranchExpireProtectsRefs,
    "interact.branch.rollbackWhileWapConf" -> interactBranchRollbackWhileWapConf,
    "interact.restore.expireAfterRollback" -> interactRestoreExpireAfterRollback,
    "interact.branch.expireMerge.spuriousReject" -> interactExpireMergeSpuriousReject,
    "interact.branch.expireMerge.stagedWapLoss"  -> interactExpireMergeStagedWapLoss,
    "interact.flags.wapReplaceAtCreate"   -> interactFlagsWapReplaceAtCreate,
    "interact.mor.alterToMor"             -> interactMorAlterToMor,
    "interact.maint.compactEvolved"       -> interactMaintCompactEvolved
  )

  // G2 characterization needs the REST lock (no SQL surface) → Ctx-based like controlPlane.
  // Sanity-checks the lock DOES block a normal write, then demonstrates RTAS sails through it.
  def interactRtasOnLockedTable(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_lockrtas"
    val Array(db, tbl) = table.stripPrefix("openhouse.").split("\\.", 2)
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(coreCreateParquet(table))
    spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, 3)}")
    spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('replace.enabled'='true')")
    try {
      val (lockStatus, lockBody) = Rest.post(ctx, s"/v1/databases/$db/tables/$tbl/lock", """{"locked":true}""")
      assert(lockStatus >= 200 && lockStatus < 300, s"lock POST failed: $lockStatus $lockBody")
      val blocked = Check.intercept[Exception](spark.sql(
        s"UPDATE $table SET ${Core.string0.columnName} = 'x' WHERE ${Core.long0.columnName} = 1"))
      assert(Exceptions.causeChain(blocked).exists(t => Option(t.getMessage).exists(_.toLowerCase.contains("locked"))),
        s"lock not enforced on UPDATE: ${blocked.getMessage.take(160)}")
      // G2: the replace branches never reach the isTableLocked check — RTAS replaces a LOCKED table.
      spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2,
        "G2 characterization: RTAS bypassed the lock (if a locked-table rejection landed here, G2 is FIXED — update AUDIT-FINDINGS)")
    } finally {
      Rest.delete(ctx, s"/v1/databases/$db/tables/$tbl/lock")
      spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  val interactionCtxOps: List[(String, Ctx => Unit)] = List(
    "interact.rtas.onLockedTable" -> interactRtasOnLockedTable
  )

  // ═══ Surface-completion axis: queued follow-ups + untested Iceberg surface ═══════════════════


}

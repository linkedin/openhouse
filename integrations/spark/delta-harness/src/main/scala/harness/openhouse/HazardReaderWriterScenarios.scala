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

trait HazardReaderWriterScenarios extends ScenarioKit {
  import Rows._

  val hazardStreamExpiredCheckpoint: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("hazard.stream.expiredCheckpoint") { (spark, table) =>
        // memory sink cannot recover from a checkpoint — stream into a second Iceberg table.
        val dst = s"${table}_sink"
        spark.sql(s"DROP TABLE IF EXISTS $dst")
        spark.sql(coreCreateParquet(dst))
        val ckpt = java.nio.file.Files.createTempDirectory("ck-hazard").toString
        def runStream(): Unit = {
          val q = spark.readStream.table(table)
            .writeStream.format("iceberg").outputMode("append")
            .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
            .option("checkpointLocation", ckpt).toTable(dst)
          assert(q.awaitTermination(120000), "stream did not finish"); q.stop()
        }
        try {
          runStream()                                                              // act 1: offset -> s1
          assert(countOf(spark, s"SELECT count(*) FROM $dst") == "3", "initial stream delivered the seed")
          spark.sql(s"INSERT INTO $table VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')") // s2
          runStream()                                                              // act 2: CONTROL restart
          assert(countOf(spark, s"SELECT count(*) FROM $dst") == "4",
            "control restart must deliver exactly the incremental row (restart mechanics work)")
          spark.sql(s"INSERT INTO $table VALUES (CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')") // s3
          spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
          // act 3: the checkpointed offset (s2) is expired -> restart bricked, typed.
          val e = Check.intercept[Exception](runStream())
          assert(Exceptions.causeChain(e).exists(t => Option(t.getMessage).exists(m =>
            m.contains("expired or removed") || m.contains("Cannot load current offset") || m.contains("Cannot find snapshot"))),
            s"H1 appears FIXED — stream restarted across the expired offset; update MODALITY-RECON H1: " +
              s"${e.getClass.getName} ${Option(e.getMessage).getOrElse("").take(200)}")
        } finally spark.sql(s"DROP TABLE IF EXISTS $dst")
      }()

  // H2 — CDC/changelog over expired lineage: expired explicit bound → hard typed error;
  // timestamp bound → SILENT under-report (the truth was 5 changes; the view shows fewer).
  val hazardCdcExpiredRange: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()                 // s1: 3 rows
      .step("hazard.cdc.expiredRange") { (spark, table) =>
        spark.sql(s"INSERT INTO $table VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')") // s2
        spark.sql(s"INSERT INTO $table VALUES (CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')") // s3
        val snaps = snapshotIds(spark, table)
        val ts0 = spark.sql(s"SELECT committed_at FROM $table.snapshots ORDER BY committed_at LIMIT 1").collect()(0).getTimestamp(0)
        val tsMid = spark.sql(s"SELECT committed_at FROM $table.snapshots WHERE snapshot_id = ${snaps(1)}").collect()(0).getTimestamp(0)
        spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
        // Characterize each bound placement over the punctured lineage. FULL truth would mean fixed.
        def changelog(optKey: String, optVal: String, truth: Long): String = try {
          val v = spark.sql(
            s"CALL openhouse.system.create_changelog_view(table => '${catalogRelative(table)}', " +
              s"options => map('$optKey', '$optVal'))").collect()(0).getString(0)
          val n = spark.sql(s"SELECT count(*) FROM $v").collect()(0).getLong(0)
          if (n < truth) s"SILENT under-report: $n of $truth true changes" else s"FULL: $n of $truth"
        } catch { case t: Throwable =>
          s"TYPED: ${t.getClass.getSimpleName} :: ${Option(t.getMessage).getOrElse("").take(140)}" }
        val a  = changelog("start-snapshot-id", snaps.head.toString, 5)   // explicit expired bound
        val b1 = changelog("start-timestamp", (ts0.getTime - 1000).toString, 5)   // before all history
        val b2 = changelog("start-timestamp", (tsMid.getTime - 1).toString, 2)    // mid-history, expired region
        println(s"DIAG cdc.explicitExpiredId: $a")
        println(s"DIAG cdc.tsBeforeHistory:  $b1")
        println(s"DIAG cdc.tsMidExpired:     $b2")
        Seq("explicitId" -> a, "tsBeforeHistory" -> b1, "tsMidExpired" -> b2).foreach { case (k, o) =>
          assert(!o.startsWith("FULL"),
            s"H2 appears FIXED for $k — changelog reported the full truth over expired lineage; update MODALITY-RECON H2: $o")
          assert(!o.toLowerCase.contains("expir"),
            s"H2 error now NAMES expiration for $k (readability improved) — update MODALITY-RECON H2/Audit B: $o")
        }
      }()

  // H3 — RTAS wipes column tags (same policies plane as G10) and column comments (new schema from SELECT).
  val hazardRtasWipesColumnTags: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("enableReplace")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('replace.enabled'='true')")()
      .sql("tagPii")(t => s"ALTER TABLE $t MODIFY COLUMN ${Core.string0.columnName} SET TAG = (PII)")()
      .step("hazard.rtas.wipesColumnTags") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table ALTER COLUMN ${Core.string0.columnName} COMMENT 'contains-pii'")
        val before = tableProps(spark, table).getOrElse("policies", "")
        assert(before.toLowerCase.contains("pii") || before.toLowerCase.contains("columntags"),
          s"PII tag not stored in policies before replace: '$before'")
        spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
        val after = tableProps(spark, table).getOrElse("policies", "")
        assert(!(after.toLowerCase.contains("pii")),
          s"H3 appears FIXED — PII column tag survived RTAS; update MODALITY-RECON H3 / AUDIT-FINDINGS: '$after'")
        val comment = spark.sql(s"DESCRIBE TABLE $table").collect().toSeq
          .find(_.getString(0) == Core.string0.columnName).map(_.getString(2)).getOrElse("")
        println(s"DIAG rtas.columnComment after replace: '${comment}' (was 'contains-pii')")
      }()

  // H5 — retention × branches: the DEFENDED path (positive invariant): main-side TTL delete +
  // expiration + orphan removal leave a live branch fully readable.
  val hazardRetentionBranchDefended: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource PARTITIONED BY (${Core.datePartition.columnName}) TBLPROPERTIES ('write.format.default'='$seedFmt')")()
      .insert(3)()
      .step("hazard.retentionBranch.defended") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table CREATE BRANCH rbb")
        spark.sql(s"DELETE FROM $table WHERE ${Core.long0.columnName} <= 2")     // retention-shaped main delete
        spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
        spark.sql(s"CALL openhouse.system.remove_orphan_files(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2020-01-01 00:00:00')")
        assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'rbb'") == "3",
          "H5 invariant: branch must remain fully readable after retention-delete + expire + orphan removal")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "1", "main reflects the TTL delete")
      }()

  // H6 — rename × consumers: metadata continuity (branch refs, history, writability survive rename).
  val hazardRenameConsumers: TableTest[CoreTable.type] =
    coreTwoSnapshots.step("hazard.rename.consumers") { (spark, table) =>
      val snaps = snapshotIds(spark, table)
      spark.sql(s"ALTER TABLE $table CREATE BRANCH rnb")
      spark.sql(s"INSERT INTO $table.branch_rnb VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      val renamed = s"${table}_rn"
      spark.sql(s"ALTER TABLE $table RENAME TO $renamed")
      try {
        assert(countOf(spark, s"SELECT count(*) FROM $renamed VERSION AS OF 'rnb'") == "6",
          "branch ref must survive rename (metadata is continuous)")
        assert(countOf(spark, s"SELECT count(*) FROM $renamed VERSION AS OF ${snaps.head}") == "3",
          "time travel must survive rename (same snapshot log)")
        spark.sql(s"INSERT INTO $renamed VALUES (CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
        assert(countOf(spark, s"SELECT count(*) FROM $renamed") == "6", "renamed table writable")
      } finally spark.sql(s"ALTER TABLE $renamed RENAME TO $table")               // restore for teardown
    }()

  // H7 — wap.enabled=false does NOT strand named branches (only staged wap.id snapshots — G4).
  val hazardWapToggleBranchesSurvive: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .step("hazard.wapToggle.branchesSurvive") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table CREATE BRANCH wtb")
        spark.sql(s"INSERT INTO $table.branch_wtb VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='false')")
        spark.sql(s"INSERT INTO $table.branch_wtb VALUES (CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
        assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'wtb'") == "5",
          "named branches must survive the WAP toggle (branch surface is not wap-gated)")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "3", "main untouched")
      }()

  // H8 — ADD COLUMN breaks every existing explicit-column writer (composition with the
  // partial-INSERT rejection): schema evolution is NOT writer-backward-compatible here,
  // contrary to ANSI SQL (omitted columns default to NULL).
  val hazardAddColumnBreaksWriters: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("hazard.addColumn.breaksWriters") { (spark, table) =>
        val allCols = Core.tableColumns.map(_.columnName).mkString(", ")
        val writerStatement = s"INSERT INTO $table ($allCols) VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')"
        spark.sql(writerStatement)                                                // the fleet's writer: green today
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "4", "writer works pre-evolution")
        spark.sql(s"ALTER TABLE $table ADD COLUMN extra_col INT")
        val e = Check.intercept[AnalysisException](spark.sql(writerStatement))    // IDENTICAL statement
        assert(e.getMessage.contains("extra_col") &&
               (e.getMessage.contains("CANNOT_FIND_DATA") || e.getMessage.toLowerCase.contains("cannot find data")),
          s"H8 appears FIXED — the pre-evolution writer survived ADD COLUMN (ANSI behavior!); update MODALITY-RECON H8 and BUGS.md: ${e.getMessage.take(200)}")
      }()

  // ── Reader × writer-class battery (BUILD-STATUS task #4) ─────────────────────────────────────
  // A reader (CDC changelog / incremental read / streaming) must correctly REPRESENT each writer
  // class (append / overwrite / delete / update / merge), and the physical mode (CoW vs MoR) must
  // not change what the reader reports. Bound each reader to the seed snapshot so only the writer's
  // change is under test. Non-vacuous core; the appraisal's 120 assumed every bound-shape crossed —
  // this builds the writer-class × reader core (~16), the part that actually varies by writer.
  // Format is a parameter (default parquet) so reader×writer blocks can multiplex across formats.
  private def cowCreate(t: String, fmt: String): String =
    s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$fmt')"
  private def cowCreate(t: String): String = cowCreate(t, "parquet")
  private def morCreate(t: String, fmt: String): String =
    s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES (${morPropsFmt(fmt)})"
  private def morCreate(t: String): String = morCreate(t, "parquet")

  private val writerClasses: List[(String, String => String)] = List(
    "append"    -> (t => s"INSERT INTO $t VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')"),
    "overwrite" -> (t => s"INSERT OVERWRITE $t SELECT * FROM $t WHERE ${Core.long0.columnName} <= 2"),
    "delete"    -> (t => s"DELETE FROM $t WHERE ${Core.long0.columnName} = 1"),
    "update"    -> (t => s"UPDATE $t SET ${Core.string0.columnName} = 'upd' WHERE ${Core.long0.columnName} = 2"),
    "merge"     -> (t => s"MERGE INTO $t t USING (SELECT CAST(2 AS BIGINT) k UNION ALL SELECT CAST(9 AS BIGINT)) s " +
      s"ON t.${Core.long0.columnName} = s.k WHEN MATCHED THEN UPDATE SET ${Core.string0.columnName} = 'm' " +
      s"WHEN NOT MATCHED THEN INSERT (${Core.long0.columnName}, ${Core.int0.columnName}, ${Core.string0.columnName}, " +
      s"${Core.double0.columnName}, ${Core.boolean0.columnName}, ${Core.datePartition.columnName}) " +
      s"VALUES (s.k, 9, 'row-9', 9.5, true, '2024-01-09-01')")
  )

  // CDC changelog must represent each writer class; assert the defining change-type + print the map.
  private def changelogWriterTest(cls: String, mor: Boolean, fmt: String): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(t => if (mor) morCreate(t, fmt) else cowCreate(t, fmt))().insert(3)()
      .step(s"readerWriter.changelog.$cls${if (mor) ".mor" else ""}") { (spark, table) =>
        val s0 = snapshotIds(spark, table).head
        spark.sql(writerClasses.toMap.apply(cls)(table))
        // FINDING (G13): a changelog scan REJECTS a MoR table whose update/merge wrote position-delete
        // files ("Delete files are currently not supported in changelog scans"). MoR delete-only and
        // all CoW writers work; MoR update/merge do NOT — CDC silently unavailable for that shape.
        val expectRejected = mor && (cls == "update" || cls == "merge")
        def buildView(): String = spark.sql(
          s"CALL openhouse.system.create_changelog_view(table => '${catalogRelative(table)}', " +
            s"options => map('start-snapshot-id', '$s0'))").collect()(0).getString(0)
        if (expectRejected) {
          val e = Check.intercept[Exception] { val v = buildView(); spark.sql(s"SELECT * FROM $v").collect() }
          assert(Exceptions.causeChain(e).exists(t => Option(t.getMessage).exists(_.contains("Delete files are currently not supported"))),
            s"G13 appears FIXED — changelog over MoR $cls no longer rejects delete files; update AUDIT-FINDINGS: ${e.getMessage.take(160)}")
          println(s"DIAG changelog.$cls.mor: REJECTED (G13 - delete files unsupported in changelog scans)")
        } else {
          val v = buildView()
          val types = spark.sql(s"SELECT _change_type, count(*) AS c FROM $v GROUP BY _change_type")
            .collect().toSeq.map(r => r.getString(0) -> r.getLong(1)).toMap
          println(s"DIAG changelog.$cls${if (mor) ".mor" else ""}: $types")
          cls match {
            case "append" => assert(types.getOrElse("INSERT", 0L) == 1 && !types.contains("DELETE"),
              s"append changelog must be a single INSERT, no DELETE: $types")
            case "delete" => assert(types.getOrElse("DELETE", 0L) == 1 && !types.contains("INSERT"),
              s"delete changelog must be a single DELETE, no INSERT: $types")
            case "update" => assert(types.getOrElse("DELETE", 0L) >= 1 && types.getOrElse("INSERT", 0L) >= 1,
              s"update changelog must decompose to DELETE(old)+INSERT(new): $types")
            case _        => assert(types.values.sum >= 1, s"$cls changelog must be non-empty: $types")
          }
        }
      }()

  // Incremental read (append scan) must reflect the writer: appends add rows; a delete/overwrite
  // changes the incremental row set. Bound start=seed.
  private def incrementalWriterTest(cls: String, fmt: String): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(t => cowCreate(t, fmt))().insert(3)()
      .step(s"readerWriter.incremental.$cls") { (spark, table) =>
        val s0 = snapshotIds(spark, table).head
        spark.sql(writerClasses.toMap.apply(cls)(table))
        val s1 = snapshotIds(spark, table).last
        val added = spark.read.format("iceberg").option("start-snapshot-id", s0).option("end-snapshot-id", s1)
          .load(table).count()
        println(s"DIAG incremental.$cls: added=$added")
        cls match {
          case "append" => assert(added == 1, s"append incremental must scan the 1 appended row: $added")
          case _        => assert(added >= 0, s"$cls incremental read must not error: $added")
        }
      }()

  // Streaming read must represent the writer: an append is delivered; a delete/overwrite snapshot is
  // rejected by the stream unless streaming-skip-* is set (characterize the two paths).
  def readerWriterStreamAppend(fmt: String): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(t => cowCreate(t, fmt))().insert(3)()
      .step("readerWriter.stream.append") { (spark, table) =>
        val dst = s"${table}_s"; spark.sql(s"DROP TABLE IF EXISTS $dst"); spark.sql(cowCreate(dst, fmt))
        val ckpt = java.nio.file.Files.createTempDirectory("ck-rw").toString
        def run(): Unit = { val q = spark.readStream.table(table).writeStream.format("iceberg")
          .outputMode("append").trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .option("checkpointLocation", ckpt).toTable(dst); assert(q.awaitTermination(120000)); q.stop() }
        try {
          run(); assert(countOf(spark, s"SELECT count(*) FROM $dst") == "3", "seed not streamed")
          spark.sql(writerClasses.toMap.apply("append")(table))
          run(); assert(countOf(spark, s"SELECT count(*) FROM $dst") == "4", "append not streamed incrementally")
        } finally spark.sql(s"DROP TABLE IF EXISTS $dst")
      }()

  def readerWriterStreamDelete(fmt: String): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(t => cowCreate(t, fmt))().insert(3)()
      .step("readerWriter.stream.deleteRejected") { (spark, table) =>
        val dst = s"${table}_sd"; spark.sql(s"DROP TABLE IF EXISTS $dst"); spark.sql(cowCreate(dst, fmt))
        val ckpt = java.nio.file.Files.createTempDirectory("ck-rwd").toString
        def run(): Unit = { val q = spark.readStream.table(table).writeStream.format("iceberg")
          .outputMode("append").trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .option("checkpointLocation", ckpt).toTable(dst); assert(q.awaitTermination(120000)); q.stop() }
        try {
          run()                                                       // consume the seed
          spark.sql(writerClasses.toMap.apply("delete")(table))       // a delete snapshot
          val e = Check.intercept[Exception](run())
          println(s"DIAG stream.afterDelete: ${e.getClass.getSimpleName} :: ${Option(e.getMessage).getOrElse("").take(140)}")
          assert(Exceptions.causeChain(e).exists(t => Option(t.getMessage).exists(m =>
            m.toLowerCase.contains("delete") || m.toLowerCase.contains("overwrite"))),
            s"append-only stream must reject a delete snapshot (streaming-skip-* needed): ${e.getMessage.take(140)}")
        } finally spark.sql(s"DROP TABLE IF EXISTS $dst")
      }()

  def readerWriterOps(fmt: String): List[(String, TableTest[CoreTable.type])] = {
    val changelog = for {
      (cls, _) <- writerClasses
      mor      <- List(false, true)
    } yield (s"readerWriter.changelog.$cls${if (mor) ".mor" else ""}", changelogWriterTest(cls, mor, fmt))
    val incremental = List("append", "delete", "overwrite", "update").map(c =>
      (s"readerWriter.incremental.$c", incrementalWriterTest(c, fmt)))
    changelog ++ incremental ++ List(
      "readerWriter.stream.append"         -> readerWriterStreamAppend(fmt),
      "readerWriter.stream.deleteRejected" -> readerWriterStreamDelete(fmt))
  }

  val hazardOps: List[(String, TableTest[CoreTable.type])] = List(
    "hazard.stream.expiredCheckpoint"   -> hazardStreamExpiredCheckpoint,
    "hazard.cdc.expiredRange"           -> hazardCdcExpiredRange,
    "hazard.rtas.wipesColumnTags"       -> hazardRtasWipesColumnTags,
    "hazard.retentionBranch.defended"   -> hazardRetentionBranchDefended,
    "hazard.rename.consumers"           -> hazardRenameConsumers,
    "hazard.wapToggle.branchesSurvive"  -> hazardWapToggleBranchesSurvive,
    "hazard.addColumn.breaksWriters"    -> hazardAddColumnBreaksWriters
  )

  // H4 — lock starves maintenance (needs the REST lock → Ctx-based). The same gate G2 shows the
  // replace path SKIPS is hit by every maintenance commit: upkeep is blocked, replacement is not.
  def hazardLockStarvesMaintenance(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_lockmaint"
    val Array(db, tbl) = table.stripPrefix("openhouse.").split("\\.", 2)
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(coreCreateParquet(table))
    spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, 3)}")
    spark.sql(s"INSERT INTO $table VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
    try {
      val (lockStatus, lockBody) = Rest.post(ctx, s"/v1/databases/$db/tables/$tbl/lock", """{"locked":true}""")
      assert(lockStatus >= 200 && lockStatus < 300, s"lock POST failed: $lockStatus $lockBody")
      val snapsBefore = spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0)
      val e = Check.intercept[Exception](spark.sql(
        s"CALL openhouse.system.expire_snapshots(table => '${table.stripPrefix("openhouse.")}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)"))
      assert(Exceptions.causeChain(e).exists(t => Option(t.getMessage).exists(_.toLowerCase.contains("locked"))),
        s"expected LOCKED rejection for the maintenance commit: ${e.getClass.getName} ${Option(e.getMessage).getOrElse("").take(180)}")
      spark.sql(s"REFRESH TABLE $table")
      val snapsAfter = spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0)
      assert(snapsAfter == snapsBefore, "locked table must accumulate snapshots (maintenance starved)")
      val (unlockStatus, _) = Rest.delete(ctx, s"/v1/databases/$db/tables/$tbl/lock")
      assert(unlockStatus >= 200 && unlockStatus < 300, "unlock failed")
      spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${table.stripPrefix("openhouse.")}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
      spark.sql(s"REFRESH TABLE $table")
      assert(spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0) < snapsBefore,
        "maintenance must proceed after unlock")
    } finally {
      Rest.delete(ctx, s"/v1/databases/$db/tables/$tbl/lock")
      spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  val hazardCtxOps: List[(String, Ctx => Unit)] = List(
    "hazard.lock.starvesMaintenance" -> hazardLockStarvesMaintenance
  )

}

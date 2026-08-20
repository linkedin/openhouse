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

trait SurfaceScenarios extends ScenarioKit {
  import Rows._


  // Audit-B regression guard: a rejection message shown to a SQL user must not be a raw stacktrace,
  // an [INTERNAL_ERROR], or a bare NPE. (It may still be MEH — jargony — that's tracked separately.)
  private def assertReadableMessage(context: String)(e: Throwable): Unit = {
    val m = Option(e.getMessage).getOrElse("")
    assert(m.nonEmpty, s"$context: empty error message (worst possible readability)")
    assert(!m.contains("[INTERNAL_ERROR]"), s"$context: internal error surfaced to the user: ${m.take(160)}")
    assert(!m.contains("\n\tat ") && !m.contains("\tat java."), s"$context: stacktrace frames in the user-facing message: ${m.take(160)}")
    assert(!m.startsWith("java.lang.NullPointerException"), s"$context: bare NPE surfaced: ${m.take(160)}")
  }

  val surfaceMsgReadabilityGuard: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.msg.readabilityGuard") { (spark, table) =>
        assertReadableMessage("dropColumn")(
          Check.intercept[Exception](spark.sql(s"ALTER TABLE $table DROP COLUMN ${Core.int0.columnName}")))
        assertReadableMessage("reservedProp")(
          Check.intercept[Exception](spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('openhouse.tableUUID'='x')")))
        assertReadableMessage("rtasDisabled")(
          Check.intercept[Exception](spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table")))
        assertReadableMessage("createNamespace")(
          Check.intercept[Exception](spark.sql("CREATE NAMESPACE openhouse.nope_ns")))
      }()

  // ── G8 legs: the other main-affecting DDLs leak from a branch to main ────────────────────────
  val surfaceBranchLeakSetProps: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("branch.leak.setProps") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")
        spark.sql(s"ALTER TABLE $table CREATE BRANCH lb2")
        spark.conf.set("spark.wap.branch", "lb2")
        try spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('user.leaked'='yes')")
        finally spark.conf.unset("spark.wap.branch")
        assert(tableProps(spark, table).get("user.leaked").contains("yes"),
          "G8 appears FIXED for SET TBLPROPERTIES — props no longer leak from branch to main; update AUDIT-FINDINGS G8")
      }()

  val surfaceBranchLeakWriteOrdered: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("branch.leak.writeOrderedBy") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")
        spark.sql(s"ALTER TABLE $table CREATE BRANCH lb3")
        spark.conf.set("spark.wap.branch", "lb3")
        try spark.sql(s"ALTER TABLE $table WRITE ORDERED BY ${Core.long0.columnName}")
        finally spark.conf.unset("spark.wap.branch")
        assert(tableProps(spark, table).get("write.distribution-mode").contains("range"),
          "G8 appears FIXED for WRITE ORDERED BY — sort order no longer leaks from branch to main; update AUDIT-FINDINGS G8")
      }()

  // ── G4 pin: toggling WAP off while staged snapshots exist is NOT guarded ─────────────────────
  val surfaceWapToggleNoGuard: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .step("branch.wapToggle.noGuard") { (spark, table) =>
        spark.conf.set("spark.wap.id", "w9")
        try spark.sql(s"INSERT INTO $table VALUES (CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01')")
        finally spark.conf.unset("spark.wap.id")
        val staged = countOf(spark, s"SELECT count(*) FROM $table.snapshots WHERE summary['wap.id'] = 'w9'")
        assert(staged == "1", s"staging failed: $staged staged snapshots")
        // G4 pin: the toggle is ACCEPTED with a staged snapshot outstanding (no guard exists).
        spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='false')")
        val stagedAfter = countOf(spark, s"SELECT count(*) FROM $table.snapshots WHERE summary['wap.id'] = 'w9'")
        println(s"DIAG wapToggle: stagedAfterToggle=$stagedAfter")
      }()

  // ── WAP negatives (B2 follow-ups) ────────────────────────────────────────────────────────────
  val surfaceWapDoubleCherrypick: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .step("wap.neg.doubleCherrypick") { (spark, table) =>
        spark.conf.set("spark.wap.id", "w1")
        try spark.sql(s"INSERT INTO $table VALUES (CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01')")
        finally spark.conf.unset("spark.wap.id")
        val sid = spark.sql(s"SELECT snapshot_id FROM $table.snapshots WHERE summary['wap.id'] = 'w1'").collect()(0).getLong(0)
        spark.sql(s"CALL openhouse.system.cherrypick_snapshot('${catalogRelative(table)}', ${sid}L)")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "4", "first publish failed")
        val e = Check.intercept[Exception](
          spark.sql(s"CALL openhouse.system.cherrypick_snapshot('${catalogRelative(table)}', ${sid}L)"))
        println(s"DIAG doubleCherrypick: ${e.getClass.getName} :: ${Option(e.getMessage).getOrElse("").take(180)}")
        assert(Option(e.getMessage).exists(m => m.toLowerCase.contains("duplicate") || m.toLowerCase.contains("already")),
          s"double cherry-pick should be rejected as a duplicate WAP commit: ${e.getMessage.take(180)}")
      }()

  val surfaceWapExpireRefTarget: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("wap.neg.expireRefTarget") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table CREATE BRANCH eb2")
        val headId = spark.sql(s"SELECT snapshot_id FROM $table.refs WHERE name = 'eb2'").collect()(0).getLong(0)
        val e = Check.intercept[Exception](spark.sql(
          s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', snapshot_ids => ARRAY(${headId}L))"))
        println(s"DIAG expireRefTarget: ${e.getClass.getName} :: ${Option(e.getMessage).getOrElse("").take(180)}")
      }()

  // ── Branch lifecycle tail: fast_forward IS the merge; replace branch ────────────────────────
  val surfaceBranchFastForwardMerge: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("branch.fastForward.merge") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table CREATE BRANCH fb")
        spark.sql(s"INSERT INTO $table.branch_fb VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        spark.sql(s"INSERT INTO $table.branch_fb VALUES (CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "3", "main advanced unexpectedly")
        spark.sql(s"CALL openhouse.system.fast_forward('${catalogRelative(table)}', 'main', 'fb')")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "5",
          "fast_forward must merge the branch into main (main == branch head)")
      }()

  val surfaceBranchFastForwardDivergent: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("branch.fastForward.divergent") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table CREATE BRANCH db")
        spark.sql(s"INSERT INTO $table.branch_db VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        spark.sql(s"INSERT INTO $table VALUES (CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')") // diverge main
        val e = Check.intercept[Exception](
          spark.sql(s"CALL openhouse.system.fast_forward('${catalogRelative(table)}', 'main', 'db')"))
        println(s"DIAG ffDivergent: ${e.getClass.getName} :: ${Option(e.getMessage).getOrElse("").take(180)}")
        assert(Option(e.getMessage).exists(m => m.toLowerCase.contains("ancestor") || m.toLowerCase.contains("fast-forward")),
          s"divergent fast_forward should be rejected with an ancestry error: ${e.getMessage.take(180)}")
      }()

  val surfaceBranchReplaceBranch: TableTest[CoreTable.type] =
    coreTwoSnapshots.step("branch.replaceBranch") { (spark, table) =>
      val snaps = snapshotIds(spark, table)
      spark.sql(s"ALTER TABLE $table CREATE BRANCH rb2")
      assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'rb2'") == "5", "branch at head")
      spark.sql(s"ALTER TABLE $table REPLACE BRANCH rb2 AS OF VERSION ${snaps.head}")
      assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'rb2'") == "3",
        "REPLACE BRANCH must retarget the ref to the older snapshot")
    }()

  // ── Streaming (structured streaming read + write) ────────────────────────────────────────────
  val surfaceStreamRead: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.stream.read") { (spark, table) =>
        val ckpt = java.nio.file.Files.createTempDirectory("ck-read").toString
        val sink = s"memsink_${System.nanoTime}"
        val q = spark.readStream.table(table)
          .writeStream.format("memory").queryName(sink)
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .option("checkpointLocation", ckpt)
          .start()
        assert(q.awaitTermination(120000), "streaming read did not finish in 120s")
        assert(countOf(spark, s"SELECT count(*) FROM $sink") == "3",
          "streaming read must deliver the seeded rows")
      }()

  val surfaceStreamWrite: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.stream.write") { (spark, table) =>
        import spark.implicits._
        implicit val sqlc: org.apache.spark.sql.SQLContext = spark.sqlContext
        val ms = org.apache.spark.sql.execution.streaming.MemoryStream[Long]
        ms.addData(100L, 101L)
        val df = ms.toDF().selectExpr(
          s"value AS ${Core.long0.columnName}",
          s"CAST(value AS INT) AS ${Core.int0.columnName}",
          s"concat('row-', value) AS ${Core.string0.columnName}",
          s"CAST(value AS DOUBLE) AS ${Core.double0.columnName}",
          s"true AS ${Core.boolean0.columnName}",
          s"'2024-01-01-00' AS ${Core.datePartition.columnName}")
        val ckpt = java.nio.file.Files.createTempDirectory("ck-write").toString
        val q = df.writeStream.format("iceberg").outputMode("append")
          .option("checkpointLocation", ckpt)
          .toTable(table)
        q.processAllAvailable()
        q.stop()
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "5",
          "streaming write must append the 2 streamed rows")
      }()

  // ── CDC: changelog view procedure ─────────────────────────────────────────────────────────────
  val surfaceCdcChangelogView: TableTest[CoreTable.type] =
    coreTwoSnapshots.step("surface.cdc.changelogView") { (spark, table) =>
      val viewName = spark.sql(
        s"CALL openhouse.system.create_changelog_view(table => '${catalogRelative(table)}')").collect()(0).getString(0)
      val changes = spark.sql(s"SELECT count(*) FROM $viewName").collect()(0).getLong(0)
      assert(changes == 5, s"changelog must contain one INSERT change per seeded row: $changes")
      val types = spark.sql(s"SELECT DISTINCT _change_type FROM $viewName").collect().toSeq.map(_.getString(0)).toSet
      assert(types == Set("INSERT"), s"append-only history must yield INSERT changes only: $types")
    }()

  // ── Procedures not yet exercised ─────────────────────────────────────────────────────────────
  // Manifest compaction must actually DO ITS JOB — reduce the manifest count — not merely preserve data.
  // Five separate appends produce ~5 manifests (one per commit); rewrite_manifests must coalesce them.
  val surfaceProcRewriteManifests: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)()
      .step("surface.proc.rewriteManifests") { (spark, table) =>
        (1 to 5).foreach(i => spark.sql(s"INSERT INTO $table VALUES ${coreRow(i, s"r$i")}"))
        val before = spark.sql(s"SELECT count(*) FROM $table.manifests").collect()(0).getLong(0)
        spark.sql(s"CALL openhouse.system.rewrite_manifests(table => '${catalogRelative(table)}', use_caching => false)")
        val after = spark.sql(s"SELECT count(*) FROM $table.manifests").collect()(0).getLong(0)
        println(s"DIAG surface.proc.rewriteManifests: manifests before=$before after=$after")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "5", "rewrite_manifests changed the live row set")
        assert(before >= 2 && after < before,
          s"rewrite_manifests did not COMPACT the manifests (before=$before after=$after) — it should coalesce them")
      }()

  val surfaceProcRewritePositionDeletes: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
        s"'write.format.default'='$seedFmt', 'write.delete.mode'='merge-on-read')")()
      .sql("seed(3, one-file)")(t =>
        s"INSERT INTO $t SELECT /*+ COALESCE(1) */ * FROM (${RowGenerator.valuesClause(Core, 3)}) AS seed")()
      .step("surface.proc.rewritePositionDeletes") { (spark, table) =>
        spark.sql(s"DELETE FROM $table WHERE ${Core.long0.columnName} = 1")
        assert(countOf(spark, s"SELECT count(*) FROM $table.all_delete_files") == "1", "MoR delete file missing")
        spark.sql(s"CALL openhouse.system.rewrite_position_delete_files(table => '${catalogRelative(table)}', options => map('rewrite-all', 'true'))")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "2", "rewrite_position_delete_files changed data")
      }()

  val surfaceProcPublishChanges: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .step("surface.proc.publishChanges") { (spark, table) =>
        spark.conf.set("spark.wap.id", "pw1")
        try spark.sql(s"INSERT INTO $table VALUES (CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01')")
        finally spark.conf.unset("spark.wap.id")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "3", "staged write must not be visible")
        spark.sql(s"CALL openhouse.system.publish_changes(table => '${catalogRelative(table)}', wap_id => 'pw1')")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "4",
          "publish_changes (the wap_id publish path beside cherrypick) must publish the staged write")
      }()

  val surfaceProcAncestorsOf: TableTest[CoreTable.type] =
    coreTwoSnapshots.step("surface.proc.ancestorsOf") { (spark, table) =>
      val n = spark.sql(s"CALL openhouse.system.ancestors_of(table => '${catalogRelative(table)}')").collect().length
      assert(n == 2, s"ancestors_of must list main's full ancestry (2 snapshots): $n")
    }()

  val surfaceProcRemoveOrphanReal: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.proc.removeOrphanReal") { (spark, table) =>
        val dataFile = spark.sql(s"SELECT file_path FROM $table.files LIMIT 1").collect()(0).getString(0).stripPrefix("file:")
        val orphan = java.nio.file.Paths.get(dataFile).getParent.resolve("zz_orphan_plant.parquet")
        java.nio.file.Files.write(orphan, "not-a-real-parquet".getBytes)
        java.nio.file.Files.setLastModifiedTime(orphan,
          java.nio.file.attribute.FileTime.fromMillis(1546300800000L)) // 2019-01-01
        spark.sql(s"CALL openhouse.system.remove_orphan_files(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2020-01-01 00:00:00')")
        assert(java.nio.file.Files.notExists(orphan), "planted orphan file must be removed")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "3", "live data must survive orphan removal")
      }()

  // ── Metadata surface: hidden columns + full metadata-table sweep ─────────────────────────────
  val surfaceMetaHiddenColumns: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.meta.hiddenColumns") { (spark, table) =>
        val rows = spark.sql(s"SELECT _file, _pos, _spec_id, _partition FROM $table").collect().toSeq
        assert(rows.size == 3, s"hidden metadata columns must be selectable per row: ${rows.size}")
        assert(rows.forall(r => r.getString(0) != null && r.getString(0).nonEmpty), "_file must be populated")
        assert(rows.forall(r => r.getLong(1) >= 0), "_pos must be populated")
      }()

  val surfaceMetaTableSweep: TableTest[CoreTable.type] =
    coreTwoSnapshots.step("surface.meta.tableSweep") { (spark, table) =>
      val metaTables = Seq("entries", "files", "manifests", "snapshots", "history", "refs", "partitions",
        "metadata_log_entries", "data_files", "all_data_files", "all_manifests", "all_entries", "all_files")
      metaTables.foreach { m =>
        val n = spark.sql(s"SELECT count(*) FROM $table.`$m`").collect()(0).getLong(0)
        assert(n >= 0, s"metadata table $m unreadable") // queryability is the assertion; count is a bonus
      }
      assert(countOf(spark, s"SELECT count(*) FROM $table.snapshots") == "2", "snapshots count sanity")
    }()

  val surfaceMetaPositionDeletes: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
        s"'write.format.default'='$seedFmt', 'write.delete.mode'='merge-on-read')")()
      .sql("seed(3, one-file)")(t =>
        s"INSERT INTO $t SELECT /*+ COALESCE(1) */ * FROM (${RowGenerator.valuesClause(Core, 3)}) AS seed")()
      .step("surface.meta.positionDeletes") { (spark, table) =>
        spark.sql(s"DELETE FROM $table WHERE ${Core.long0.columnName} = 1")
        assert(countOf(spark, s"SELECT count(*) FROM $table.position_deletes") == "1",
          "position_deletes metadata table must expose the position delete")
      }()

  // ── Concurrency: invariant-based (no torn state; failures must be typed) ─────────────────────
  private def runConcurrently(fs: Seq[() => Unit]): Seq[Throwable] = {
    val errors = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()
    val threads = fs.map(f => new Thread(() => try f() catch { case t: Throwable => errors.add(t) }))
    threads.foreach(_.start())
    threads.foreach(_.join(180000))
    errors.toArray(Array.empty[Throwable]).toSeq
  }

  private def isTypedCommitConflict(t: Throwable): Boolean =
    Exceptions.causeChain(t).exists { c =>
      val n = c.getClass.getName
      n.contains("CommitFailed") || n.contains("CommitStateUnknown") || n.contains("Validation") ||
        n.contains("BadRequest") || n.contains("WebClientResponse")
    }

  val surfaceConcAppendAppend: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.conc.appendAppend") { (spark, table) =>
        val failures = new java.util.concurrent.atomic.AtomicInteger(0)
        def writer(base: Int): () => Unit = () => (0 until 3).foreach { i =>
          try spark.sql(s"INSERT INTO $table VALUES (CAST(${base + i} AS BIGINT), ${base + i}, 'row-c', 1.5, true, '2024-01-09-01')")
          catch { case t: Throwable =>
            assert(isTypedCommitConflict(t), s"concurrent append failed with an UNTYPED error: ${t.getClass.getName} ${Option(t.getMessage).getOrElse("").take(160)}")
            failures.incrementAndGet()
          }
        }
        val errs = runConcurrently(Seq(writer(100), writer(200)))
        assert(errs.isEmpty, s"writer thread died outside the insert loop: ${errs.headOption.map(_.toString)}")
        val expected = 3 + 6 - failures.get
        assert(countOf(spark, s"SELECT count(*) FROM $table") == expected.toString,
          s"row count must equal successful appends (3 seed + ${6 - failures.get} landed)")
        println(s"DIAG conc.appendAppend: ${failures.get}/6 inserts hit a typed commit conflict")
      }()

  val surfaceConcUpdateUpdate: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.conc.updateUpdate") { (spark, table) =>
        val col = Core.string0.columnName
        def updater(v: String): () => Unit = () =>
          try spark.sql(s"UPDATE $table SET $col = '$v' WHERE ${Core.long0.columnName} = 2")
          catch { case t: Throwable =>
            assert(isTypedCommitConflict(t), s"concurrent update failed with an UNTYPED error: ${t.getClass.getName} ${Option(t.getMessage).getOrElse("").take(160)}") }
        val errs = runConcurrently(Seq(updater("AAA"), updater("BBB")))
        assert(errs.isEmpty, s"updater thread died with a non-conflict error: ${errs.headOption.map(_.toString)}")
        val v = spark.sql(s"SELECT $col FROM $table WHERE ${Core.long0.columnName} = 2").collect()(0).getString(0)
        assert(v == "AAA" || v == "BBB" || v == "row-2", s"row must hold one writer's value or the original, not torn state: $v")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "3", "row count must be unchanged")
      }()

  val surfaceConcRtasVsAppend: TableTest[CoreTable.type] =
    rtasPrep.step("surface.conc.rtasVsAppend") { (spark, table) =>
      def rtas(): Unit =
        try spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
        catch { case t: Throwable => assert(isTypedCommitConflict(t), s"RTAS race failed UNTYPED: ${t.getClass.getName}") }
      def append(): Unit =
        try spark.sql(s"INSERT INTO $table VALUES (CAST(30 AS BIGINT), 30, 'row-30', 30.5, true, '2024-01-09-01')")
        catch { case t: Throwable => assert(isTypedCommitConflict(t), s"append race failed UNTYPED: ${t.getClass.getName}") }
      val errs = runConcurrently(Seq(() => rtas(), () => append()))
      assert(errs.isEmpty, s"racing thread died with a non-conflict error: ${errs.headOption.map(_.toString)}")
      spark.sql(s"REFRESH TABLE $table")
      val n = countOf(spark, s"SELECT count(*) FROM $table").toLong
      assert(n == 2 || n == 3, s"RTAS-vs-append must settle to a consistent state (2 or 3 rows), got $n")
      println(s"DIAG conc.rtasVsAppend: settled at $n rows")
    }()

  // ── Schema-evolution edges ───────────────────────────────────────────────────────────────────
  val surfaceSchemaRelaxNotNull: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.schema.relaxNotNull") { (spark, table) =>
        val side = s"${table}_nn"
        spark.sql(s"DROP TABLE IF EXISTS $side")
        try {
          spark.sql(s"CREATE TABLE $side (id BIGINT, req INT NOT NULL) USING $dataSource")
          spark.sql(s"ALTER TABLE $side ALTER COLUMN req DROP NOT NULL")
          spark.sql(s"INSERT INTO $side VALUES (CAST(1 AS BIGINT), NULL)")
          assert(spark.sql(s"SELECT count(*) FROM $side WHERE req IS NULL").collect()(0).getLong(0) == 1,
            "relaxing NOT NULL must allow null writes (the inverse of the pinned-rejected tighten)")
        } finally spark.sql(s"DROP TABLE IF EXISTS $side")
      }()

  val surfaceSchemaDecimalWiden: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.schema.decimalWiden") { (spark, table) =>
        val side = s"${table}_dec"
        spark.sql(s"DROP TABLE IF EXISTS $side")
        try {
          spark.sql(s"CREATE TABLE $side (id BIGINT, dec DECIMAL(10,2)) USING $dataSource")
          spark.sql(s"INSERT INTO $side VALUES (CAST(1 AS BIGINT), CAST(12345678.99 AS DECIMAL(10,2)))")
          spark.sql(s"ALTER TABLE $side ALTER COLUMN dec TYPE DECIMAL(12,2)")
          spark.sql(s"INSERT INTO $side VALUES (CAST(2 AS BIGINT), CAST(1234567890.99 AS DECIMAL(12,2)))")
          assert(spark.sql(s"SELECT count(*) FROM $side").collect()(0).getLong(0) == 2,
            "decimal precision widen must keep old data readable and accept wider values")
        } finally spark.sql(s"DROP TABLE IF EXISTS $side")
      }()

  val surfaceSchemaNestedAddField: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.schema.nestedAddField") { (spark, table) =>
        val side = s"${table}_nst"
        spark.sql(s"DROP TABLE IF EXISTS $side")
        try {
          spark.sql(s"CREATE TABLE $side (id BIGINT, s STRUCT<x: INT, y: STRING>) USING $dataSource")
          spark.sql(s"INSERT INTO $side VALUES (CAST(1 AS BIGINT), named_struct('x', 1, 'y', 'a'))")
          spark.sql(s"ALTER TABLE $side ADD COLUMN s.w INT")
          assert(spark.sql(s"SELECT count(*) FROM $side WHERE s.w IS NULL").collect()(0).getLong(0) == 1,
            "adding a nested struct field must null-fill existing rows")
          spark.sql(s"INSERT INTO $side VALUES (CAST(2 AS BIGINT), named_struct('x', 2, 'y', 'b', 'w', 9))")
          assert(spark.sql(s"SELECT count(*) FROM $side WHERE s.w = 9").collect()(0).getLong(0) == 1,
            "the new nested field must be writable")
        } finally spark.sql(s"DROP TABLE IF EXISTS $side")
      }()

  val surfaceSchemaNestedDropField: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.schema.nestedDropField") { (spark, table) =>
        val side = s"${table}_nsd"
        spark.sql(s"DROP TABLE IF EXISTS $side")
        try {
          spark.sql(s"CREATE TABLE $side (id BIGINT, s STRUCT<x: INT, y: STRING>) USING $dataSource")
          spark.sql(s"INSERT INTO $side VALUES (CAST(1 AS BIGINT), named_struct('x', 1, 'y', 'a'))")
          val e = Check.intercept[Exception](spark.sql(s"ALTER TABLE $side DROP COLUMN s.x"))
          println(s"DIAG nestedDropField: ${e.getClass.getName} :: ${Option(e.getMessage).getOrElse("").take(180)}")
          assert(spark.sql(s"SELECT s.x FROM $side").collect()(0).getInt(0) == 1,
            "rejected nested drop must leave the field readable")
        } finally spark.sql(s"DROP TABLE IF EXISTS $side")
      }()

  val surfaceSchemaReorderExisting: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.schema.reorderExisting") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table ALTER COLUMN ${Core.string0.columnName} FIRST")
        val cols = spark.sql(s"SELECT * FROM $table LIMIT 1").columns.toSeq
        assert(cols.head == Core.string0.columnName, s"column reorder (FIRST) must change projection order: $cols")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "3", "reorder must not affect data")
      }()

  // ── Write-path configs ───────────────────────────────────────────────────────────────────────
  val surfaceWriteDistributionHash: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource PARTITIONED BY (${Core.datePartition.columnName}) " +
        s"TBLPROPERTIES ('write.format.default'='$seedFmt', 'write.distribution-mode'='hash')")()
      .insert(3)()
      .check("surface.write.distributionHash") { view =>
        assert(tableProps(view.spark, view.table).get("write.distribution-mode").contains("hash"), "hash mode not honored")
        assert(view.after.size == 3, "hash-distributed write failed")
      }

  val surfaceWriteTargetFileSize: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
        s"'write.format.default'='$seedFmt', 'write.target-file-size-bytes'='1048576')")()
      .insert(3)()
      .check("surface.write.targetFileSize") { view =>
        assert(tableProps(view.spark, view.table).get("write.target-file-size-bytes").contains("1048576"), "target size not honored")
        assert(view.after.size == 3, "write under custom target file size failed")
      }

  val surfaceWriteDfToBranch: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.write.dfToBranch") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table CREATE BRANCH wb")
        val df = spark.sql(s"SELECT CAST(50 AS BIGINT) AS ${Core.long0.columnName}, 50 AS ${Core.int0.columnName}, " +
          s"'row-50' AS ${Core.string0.columnName}, 50.5 AS ${Core.double0.columnName}, " +
          s"true AS ${Core.boolean0.columnName}, '2024-01-09-01' AS ${Core.datePartition.columnName}")
        df.writeTo(s"$table.branch_wb").append()
        assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'wb'") == "4",
          "DataFrame-API write must land on the branch")
        assert(countOf(spark, s"SELECT count(*) FROM $table") == "3", "main must be untouched by the branch DF write")
      }()

  // ── Pins: import/migration procedures, views, ANALYZE (expected-unsupported tripwires) ───────
  // The bogus-input probes showed these procedures fail on INPUT (NotFound/NoSuchTable), not on an
  // OpenHouse catalog block — so settle register_table with a REAL metadata file: is importing a
  // table into the managed catalog (bypassing normal creation) actually possible?
  val surfacePinImportProcs: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.pin.importProcs") { (spark, table) =>
        val metadataFile = spark.sql(
          s"SELECT file FROM $table.metadata_log_entries ORDER BY timestamp DESC LIMIT 1").collect()(0).getString(0)
        val regOutcome =
          try {
            spark.sql(s"CALL openhouse.system.register_table(table => 'dbMatrix.zz_reg', metadata_file => '$metadataFile')")
            val n = countOf(spark, "SELECT count(*) FROM openhouse.dbMatrix.zz_reg")
            spark.sql("DROP TABLE IF EXISTS openhouse.dbMatrix.zz_reg")
            s"REGISTERED (readable, $n rows) — import into the managed catalog is NOT blocked"
          } catch { case t: Throwable =>
            s"REJECTED ${t.getClass.getName} :: ${Option(t.getMessage).getOrElse("").take(160)}" }
        println(s"DIAG pin.register_table(real): $regOutcome")
        val snap = Check.intercept[Exception](spark.sql(
          s"CALL openhouse.system.snapshot(source_table => '${catalogRelative(table)}', table => 'dbMatrix.zz_snap')"))
        println(s"DIAG pin.snapshot: ${snap.getClass.getName} :: ${Option(snap.getMessage).getOrElse("").take(160)}")
        val add = Check.intercept[Exception](spark.sql(
          s"CALL openhouse.system.add_files(table => '${catalogRelative(table)}', source_table => '`parquet`.`/tmp/zz_nope_dir`')"))
        println(s"DIAG pin.add_files: ${add.getClass.getName} :: ${Option(add.getMessage).getOrElse("").take(160)}")
      }()

  val surfacePinViewsAnalyze: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.pin.viewsAnalyze") { (spark, table) =>
        val view = Check.intercept[Exception](spark.sql(s"CREATE VIEW openhouse.dbMatrix.zz_v1 AS SELECT 1 AS one"))
        println(s"DIAG pin.createView: ${view.getClass.getName} :: ${Option(view.getMessage).getOrElse("").take(160)}")
        val analyze = Check.intercept[Exception](spark.sql(s"ANALYZE TABLE $table COMPUTE STATISTICS"))
        println(s"DIAG pin.analyze: ${analyze.getClass.getName} :: ${Option(analyze.getMessage).getOrElse("").take(160)}")
      }()

  // Compaction × branch: does rewrite_data_files touch/break branch state, and where does it land
  // when spark.wap.branch is set? (Untested cell flagged in the surface appraisal.)
  val surfaceMaintCompactWithBranch: TableTest[CoreTable.type] =
    coreTwoSnapshots.step("surface.maint.compactWithBranch") { (spark, table) =>
      spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")
      spark.sql(s"ALTER TABLE $table CREATE BRANCH cb")
      spark.sql(s"INSERT INTO $table.branch_cb VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      spark.sql(s"INSERT INTO $table VALUES (CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
      val r = spark.sql(s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}', options => map('min-input-files', '2'))").collect()(0)
      println(s"DIAG compactWithBranch: mainCompaction rewritten=${r.get(0)} added=${r.get(1)}")
      assert(countOf(spark, s"SELECT count(*) FROM $table") == "6", "main data preserved by compaction")
      assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'cb'") == "6",
        "branch data preserved and readable after main compaction")
      spark.conf.set("spark.wap.branch", "cb")
      val confOutcome = try {
        val rc = spark.sql(s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}')").collect()(0)
        s"RAN (rewritten=${rc.get(0)}, added=${rc.get(1)})"
      } catch { case t: Throwable => s"THREW ${t.getClass.getSimpleName} :: ${Option(t.getMessage).getOrElse("").take(140)}" }
      finally spark.conf.unset("spark.wap.branch")
      println(s"DIAG compactUnderWapConf: $confOutcome")
      spark.sql(s"REFRESH TABLE $table")
      assert(countOf(spark, s"SELECT count(*) FROM $table") == "6", "main intact after conf-routed compaction attempt")
      assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'cb'") == "6", "branch intact after conf-routed compaction attempt")
    }()

  val surfaceOps: List[(String, TableTest[CoreTable.type])] = List(
    "surface.maint.compactWithBranch"     -> surfaceMaintCompactWithBranch,
    "surface.msg.readabilityGuard"        -> surfaceMsgReadabilityGuard,
    "branch.leak.setProps"                -> surfaceBranchLeakSetProps,
    "branch.leak.writeOrderedBy"          -> surfaceBranchLeakWriteOrdered,
    "branch.wapToggle.noGuard"            -> surfaceWapToggleNoGuard,
    "wap.neg.doubleCherrypick"            -> surfaceWapDoubleCherrypick,
    "wap.neg.expireRefTarget"             -> surfaceWapExpireRefTarget,
    "branch.fastForward.merge"            -> surfaceBranchFastForwardMerge,
    "branch.fastForward.divergent"        -> surfaceBranchFastForwardDivergent,
    "branch.replaceBranch"                -> surfaceBranchReplaceBranch,
    "surface.stream.read"                 -> surfaceStreamRead,
    "surface.stream.write"                -> surfaceStreamWrite,
    "surface.cdc.changelogView"           -> surfaceCdcChangelogView,
    "surface.proc.rewriteManifests"       -> surfaceProcRewriteManifests,
    "surface.proc.rewritePositionDeletes" -> surfaceProcRewritePositionDeletes,
    "surface.proc.publishChanges"         -> surfaceProcPublishChanges,
    "surface.proc.ancestorsOf"            -> surfaceProcAncestorsOf,
    "surface.proc.removeOrphanReal"       -> surfaceProcRemoveOrphanReal,
    "surface.meta.hiddenColumns"          -> surfaceMetaHiddenColumns,
    "surface.meta.tableSweep"             -> surfaceMetaTableSweep,
    "surface.meta.positionDeletes"        -> surfaceMetaPositionDeletes,
    "surface.conc.appendAppend"           -> surfaceConcAppendAppend,
    "surface.conc.updateUpdate"           -> surfaceConcUpdateUpdate,
    "surface.conc.rtasVsAppend"           -> surfaceConcRtasVsAppend,
    "surface.schema.relaxNotNull"         -> surfaceSchemaRelaxNotNull,
    "surface.schema.decimalWiden"         -> surfaceSchemaDecimalWiden,
    "surface.schema.nestedAddField"       -> surfaceSchemaNestedAddField,
    "surface.schema.nestedDropField"      -> surfaceSchemaNestedDropField,
    "surface.schema.reorderExisting"      -> surfaceSchemaReorderExisting,
    "surface.write.distributionHash"      -> surfaceWriteDistributionHash,
    "surface.write.targetFileSize"        -> surfaceWriteTargetFileSize,
    "surface.write.dfToBranch"            -> surfaceWriteDfToBranch,
    "surface.pin.importProcs"             -> surfacePinImportProcs,
    "surface.pin.viewsAnalyze"            -> surfacePinViewsAnalyze
  )

  // ═══ Hazard demonstrations H1-H8 (MODALITY-RECON.md; gates cleared per FEATURE-ANALYSIS-PLAN) ══
  // Each was PREDICTED by the state-flow model, verified in code/bytecode, and is demonstrated
  // live here. Characterizations flip loudly if the product fixes the hazard.

  // H1 — streaming checkpoint × expiration (G11's streaming twin). Three acts:
  // (1) stream + checkpoint; (2) CONTROL: plain restart picks up new rows (restart mechanics fine);
  // (3) expire past the checkpointed offset → restart is BRICKED with the typed error.

}

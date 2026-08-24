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

  private def runConcurrently(functions: Seq[() => Unit]): Seq[Throwable] = {
    val errors = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()
    val threads = functions.map(function =>
      new Thread(() =>
        try function()
        catch { case throwable: Throwable => errors.add(throwable) }))
    threads.foreach(_.start())
    threads.foreach(_.join(180000))
    errors.toArray(Array.empty[Throwable]).toSeq
  }

  private def isTypedCommitConflict(throwable: Throwable): Boolean =
    Exceptions.causeChain(throwable).exists { cause =>
      val className = cause.getClass.getName
      className.contains("CommitFailed") ||
        className.contains("CommitStateUnknown") ||
        className.contains("Validation") ||
        className.contains("BadRequest") ||
        className.contains("WebClientResponse")
    }

  private def surfaceBranchCases(format: String): List[Plan.Case] = {
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())
    val twoSnapshotPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)()
        .sql("insertMore")(table =>
          s"INSERT INTO $table VALUES " +
            "(CAST(4 AS BIGINT), 4, 'row-4', 4.5, true, '2024-01-04-03'), " +
            "(CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')")())
    val wapPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)()
        .sql("enableWap")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")())

    List(
      twoSnapshotPreparation.test(
        "surface.maint.compactWithBranch") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
            "('write.wap.enabled'='true')")
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH cb")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_cb VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
        val compactionResult = table.spark
          .sql(
            "CALL openhouse.system.rewrite_data_files(" +
              s"table => '${catalogRelative(table.name)}', " +
              "options => map('min-input-files', '2'))")
          .collect()(0)

        println(
          "DIAG compactWithBranch: " +
            s"mainCompaction rewritten=${compactionResult.get(0)} " +
            s"added=${compactionResult.get(1)}")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "6",
          "main compaction should preserve 6 rows")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'cb'") == "6",
          "main compaction should preserve the branch")

        table.spark.conf.set("spark.wap.branch", "cb")
        val branchRoutedOutcome =
          try {
            val result = table.spark
              .sql(
                "CALL openhouse.system.rewrite_data_files(" +
                  s"table => '${catalogRelative(table.name)}')")
              .collect()(0)
            s"RAN (rewritten=${result.get(0)}, added=${result.get(1)})"
          } catch {
            case exception: Throwable =>
              s"THREW ${exception.getClass.getSimpleName} :: " +
                Option(exception.getMessage).getOrElse("").take(140)
          } finally {
            table.spark.conf.unset("spark.wap.branch")
          }
        println(s"DIAG compactUnderWapConf: $branchRoutedOutcome")

        table.spark.sql(s"REFRESH TABLE ${table.name}")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "6",
          "branch-routed compaction attempt should preserve main")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'cb'") == "6",
          "branch-routed compaction attempt should preserve the branch")
      },
      basePreparation.test("surface.msg.readabilityGuard") { table =>
        assertReadableMessage("dropColumn")(
          Check.intercept[Exception](
            table.spark.sql(
              s"ALTER TABLE ${table.name} " +
                s"DROP COLUMN ${Core.int0.columnName}")))
        assertReadableMessage("reservedProp")(
          Check.intercept[Exception](
            table.spark.sql(
              s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
                "('openhouse.tableUUID'='x')")))
        assertReadableMessage("rtasDisabled")(
          Check.intercept[Exception](
            table.spark.sql(
              s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
                s"AS SELECT * FROM ${table.name}")))
        assertReadableMessage("createNamespace")(
          Check.intercept[Exception](
            table.spark.sql("CREATE NAMESPACE openhouse.nope_ns")))
      },
      basePreparation.test("branch.leak.setProps") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
            "('write.wap.enabled'='true')")
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH lb2")
        table.spark.conf.set("spark.wap.branch", "lb2")
        try {
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
              "('user.leaked'='yes')")
        } finally {
          table.spark.conf.unset("spark.wap.branch")
        }

        assert(
          tableProps(table.spark, table.name)
            .get("user.leaked")
            .contains("yes"),
          "branch-routed property update should change table-global metadata")
      },
      basePreparation.test("branch.leak.writeOrderedBy") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
            "('write.wap.enabled'='true')")
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH lb3")
        table.spark.conf.set("spark.wap.branch", "lb3")
        try {
          table.spark.sql(
            s"ALTER TABLE ${table.name} " +
              s"WRITE ORDERED BY ${Core.long0.columnName}")
        } finally {
          table.spark.conf.unset("spark.wap.branch")
        }

        assert(
          tableProps(table.spark, table.name)
            .get("write.distribution-mode")
            .contains("range"),
          "branch-routed ordering should change table-global metadata")
      },
      wapPreparation.test("branch.wapToggle.noGuard") { table =>
        table.spark.conf.set("spark.wap.id", "w9")
        try {
          table.spark.sql(
            s"INSERT INTO ${table.name} VALUES " +
              "(CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01')")
        } finally {
          table.spark.conf.unset("spark.wap.id")
        }
        val stagedSnapshotCount = countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name}.snapshots " +
            "WHERE summary['wap.id'] = 'w9'")
        assert(
          stagedSnapshotCount == "1",
          s"expected one staged snapshot, got $stagedSnapshotCount")

        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
            "('write.wap.enabled'='false')")
        val stagedAfterToggle = countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name}.snapshots " +
            "WHERE summary['wap.id'] = 'w9'")

        println(s"DIAG wapToggle: stagedAfterToggle=$stagedAfterToggle")
      },
      wapPreparation.test("wap.neg.doubleCherrypick") { table =>
        table.spark.conf.set("spark.wap.id", "w1")
        try {
          table.spark.sql(
            s"INSERT INTO ${table.name} VALUES " +
              "(CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01')")
        } finally {
          table.spark.conf.unset("spark.wap.id")
        }
        val stagedSnapshotId = table.spark
          .sql(
            s"SELECT snapshot_id FROM ${table.name}.snapshots " +
              "WHERE summary['wap.id'] = 'w1'")
          .collect()(0)
          .getLong(0)
        table.spark.sql(
          "CALL openhouse.system.cherrypick_snapshot(" +
            s"'${catalogRelative(table.name)}', ${stagedSnapshotId}L)")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "4",
          "first cherry-pick should publish the staged row")

        val exception = Check.intercept[Exception](
          table.spark.sql(
            "CALL openhouse.system.cherrypick_snapshot(" +
              s"'${catalogRelative(table.name)}', ${stagedSnapshotId}L)"))
        println(
          "DIAG doubleCherrypick: " +
            s"${exception.getClass.getName} :: " +
            Option(exception.getMessage).getOrElse("").take(180))
        assert(
          Option(exception.getMessage).exists(message =>
            message.toLowerCase.contains("duplicate") ||
              message.toLowerCase.contains("already")),
          "second cherry-pick should reject the duplicate WAP commit")
      },
      basePreparation.test("wap.neg.expireRefTarget") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH eb2")
        val branchHeadSnapshotId = table.spark
          .sql(
            s"SELECT snapshot_id FROM ${table.name}.refs " +
              "WHERE name = 'eb2'")
          .collect()(0)
          .getLong(0)
        val exception = Check.intercept[Exception](
          table.spark.sql(
            "CALL openhouse.system.expire_snapshots(" +
              s"table => '${catalogRelative(table.name)}', " +
              s"snapshot_ids => ARRAY(${branchHeadSnapshotId}L))"))

        println(
          "DIAG expireRefTarget: " +
            s"${exception.getClass.getName} :: " +
            Option(exception.getMessage).getOrElse("").take(180))
      },
      basePreparation.test("branch.fastForward.merge") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH fb")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_fb VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_fb VALUES " +
            "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "3",
          "branch writes should not advance main")

        table.spark.sql(
          "CALL openhouse.system.fast_forward(" +
            s"'${catalogRelative(table.name)}', 'main', 'fb')")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "5",
          "fast_forward should move main to the branch head")
      },
      basePreparation.test("branch.fastForward.divergent") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH db")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_db VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
        val exception = Check.intercept[Exception](
          table.spark.sql(
            "CALL openhouse.system.fast_forward(" +
              s"'${catalogRelative(table.name)}', 'main', 'db')"))

        println(
          "DIAG ffDivergent: " +
            s"${exception.getClass.getName} :: " +
            Option(exception.getMessage).getOrElse("").take(180))
        assert(
          Option(exception.getMessage).exists(message =>
            message.toLowerCase.contains("ancestor") ||
              message.toLowerCase.contains("fast-forward")),
          "divergent fast_forward should report an ancestry error")
      },
      twoSnapshotPreparation.test("branch.replaceBranch") { table =>
        val snapshots = snapshotIds(table.spark, table.name)
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH rb2")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'rb2'") == "5",
          "new branch should point at the current head")

        table.spark.sql(
          s"ALTER TABLE ${table.name} REPLACE BRANCH rb2 " +
            s"AS OF VERSION ${snapshots.head}")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'rb2'") == "3",
          "REPLACE BRANCH should retarget the branch to the older snapshot")
      })
  }

  private def surfaceReaderProcedureCases(
      format: String): List[Plan.Case] = {
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())
    val twoSnapshotPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)()
        .sql("insertMore")(table =>
          s"INSERT INTO $table VALUES " +
            "(CAST(4 AS BIGINT), 4, 'row-4', 4.5, true, '2024-01-04-03'), " +
            "(CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')")())
    val emptyPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")())
    val morPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', " +
            "'write.delete.mode'='merge-on-read')")()
        .sql("seed")(table =>
          s"INSERT INTO $table SELECT /*+ COALESCE(1) */ * FROM " +
            s"(${RowGenerator.valuesClause(Core, 3)}) AS seed")())
    val wapPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)()
        .sql("enableWap")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('write.wap.enabled'='true')")())

    List(
      basePreparation.test("surface.stream.read") { table =>
        val checkpoint =
          java.nio.file.Files.createTempDirectory("ck-read").toString
        val sink = s"memsink_${System.nanoTime}"
        val query = table.spark.readStream
          .table(table.name)
          .writeStream
          .format("memory")
          .queryName(sink)
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .option("checkpointLocation", checkpoint)
          .start()

        assert(
          query.awaitTermination(120000),
          "streaming read did not finish in 120 seconds")
        assert(
          countOf(table.spark, s"SELECT count(*) FROM $sink") == "3",
          "streaming read should deliver the three seed rows")
      },
      basePreparation.test("surface.stream.write") { table =>
        import table.spark.implicits._
        implicit val sqlContext: org.apache.spark.sql.SQLContext =
          table.spark.sqlContext
        val memoryStream =
          org.apache.spark.sql.execution.streaming.MemoryStream[Long]
        memoryStream.addData(100L, 101L)
        val rows = memoryStream.toDF().selectExpr(
          s"value AS ${Core.long0.columnName}",
          s"CAST(value AS INT) AS ${Core.int0.columnName}",
          s"concat('row-', value) AS ${Core.string0.columnName}",
          s"CAST(value AS DOUBLE) AS ${Core.double0.columnName}",
          s"true AS ${Core.boolean0.columnName}",
          s"'2024-01-01-00' AS ${Core.datePartition.columnName}")
        val checkpoint =
          java.nio.file.Files.createTempDirectory("ck-write").toString
        val query = rows.writeStream
          .format("iceberg")
          .outputMode("append")
          .option("checkpointLocation", checkpoint)
          .toTable(table.name)

        query.processAllAvailable()
        query.stop()
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "5",
          "streaming write should append two rows")
      },
      twoSnapshotPreparation.test("surface.cdc.changelogView") { table =>
        val view = table.spark
          .sql(
            "CALL openhouse.system.create_changelog_view(" +
              s"table => '${catalogRelative(table.name)}')")
          .collect()(0)
          .getString(0)
        val changeCount = table.spark
          .sql(s"SELECT count(*) FROM $view")
          .collect()(0)
          .getLong(0)
        val changeTypes = table.spark
          .sql(s"SELECT DISTINCT _change_type FROM $view")
          .collect()
          .map(_.getString(0))
          .toSet

        assert(
          changeCount == 5,
          s"append-only changelog should contain 5 changes, got $changeCount")
        assert(
          changeTypes == Set("INSERT"),
          s"append-only changelog should contain only INSERT: $changeTypes")
      },
      emptyPreparation.test("surface.proc.rewriteManifests") { table =>
        (1 to 5).foreach(index =>
          table.spark.sql(
            s"INSERT INTO ${table.name} VALUES " +
              coreRow(index, s"r$index")))
        val manifestCountBefore = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}.manifests")
          .collect()(0)
          .getLong(0)
        table.spark.sql(
          "CALL openhouse.system.rewrite_manifests(" +
            s"table => '${catalogRelative(table.name)}', " +
            "use_caching => false)")
        val manifestCountAfter = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}.manifests")
          .collect()(0)
          .getLong(0)

        println(
          "DIAG surface.proc.rewriteManifests: " +
            s"manifests before=$manifestCountBefore after=$manifestCountAfter")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "5",
          "rewrite_manifests should preserve the five rows")
        assert(
          manifestCountBefore >= 2 &&
            manifestCountAfter < manifestCountBefore,
          "rewrite_manifests should compact the manifest set")
      },
      morPreparation.test(
        "surface.proc.rewritePositionDeletes") { table =>
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}.all_delete_files") == "1",
          "MoR delete should create one position-delete file")

        table.spark.sql(
          "CALL openhouse.system.rewrite_position_delete_files(" +
            s"table => '${catalogRelative(table.name)}', " +
            "options => map('rewrite-all', 'true'))")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "2",
          "rewrite_position_delete_files should preserve live rows")
      },
      wapPreparation.test("surface.proc.publishChanges") { table =>
        table.spark.conf.set("spark.wap.id", "pw1")
        try {
          table.spark.sql(
            s"INSERT INTO ${table.name} VALUES " +
              "(CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01')")
        } finally {
          table.spark.conf.unset("spark.wap.id")
        }
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "3",
          "staged write should not be visible before publish")

        table.spark.sql(
          "CALL openhouse.system.publish_changes(" +
            s"table => '${catalogRelative(table.name)}', wap_id => 'pw1')")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "4",
          "publish_changes should publish the staged row")
      },
      twoSnapshotPreparation.test("surface.proc.ancestorsOf") { table =>
        val ancestorCount = table.spark
          .sql(
            "CALL openhouse.system.ancestors_of(" +
              s"table => '${catalogRelative(table.name)}')")
          .collect()
          .length

        assert(
          ancestorCount == 2,
          s"ancestors_of should list two snapshots, got $ancestorCount")
      },
      basePreparation.test("surface.proc.removeOrphanReal") { table =>
        val dataFile = table.spark
          .sql(s"SELECT file_path FROM ${table.name}.files LIMIT 1")
          .collect()(0)
          .getString(0)
          .stripPrefix("file:")
        val orphanFile = java.nio.file.Paths
          .get(dataFile)
          .getParent
          .resolve("zz_orphan_plant.parquet")
        java.nio.file.Files.write(
          orphanFile,
          "not-a-real-parquet".getBytes)
        java.nio.file.Files.setLastModifiedTime(
          orphanFile,
          java.nio.file.attribute.FileTime.fromMillis(1546300800000L))

        table.spark.sql(
          "CALL openhouse.system.remove_orphan_files(" +
            s"table => '${catalogRelative(table.name)}', " +
            "older_than => TIMESTAMP '2020-01-01 00:00:00')")
        assert(
          java.nio.file.Files.notExists(orphanFile),
          "remove_orphan_files should delete the planted orphan")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "3",
          "remove_orphan_files should preserve live data")
      },
      basePreparation.test("surface.meta.hiddenColumns") { table =>
        val rows = table.spark
          .sql(
            s"SELECT _file, _pos, _spec_id, _partition FROM ${table.name}")
          .collect()
          .toSeq

        assert(
          rows.size == 3,
          s"hidden metadata columns should return 3 rows, got ${rows.size}")
        assert(
          rows.forall(row =>
            Option(row.getString(0)).exists(_.nonEmpty)),
          "_file should be populated for every row")
        assert(
          rows.forall(_.getLong(1) >= 0),
          "_pos should be non-negative for every row")
      },
      twoSnapshotPreparation.test("surface.meta.tableSweep") { table =>
        val metadataTables = Seq(
          "entries",
          "files",
          "manifests",
          "snapshots",
          "history",
          "refs",
          "partitions",
          "metadata_log_entries",
          "data_files",
          "all_data_files",
          "all_manifests",
          "all_entries",
          "all_files")
        metadataTables.foreach { metadataTable =>
          val rowCount = table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name}.`$metadataTable`")
            .collect()(0)
            .getLong(0)
          assert(
            rowCount >= 0,
            s"metadata table $metadataTable should be queryable")
        }
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}.snapshots") == "2",
          "snapshot metadata should contain two snapshots")
      },
      morPreparation.test("surface.meta.positionDeletes") { table =>
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}.position_deletes") == "1",
          "position_deletes should expose the MoR position delete")
      })
  }

  private def surfaceRemainingCases(format: String): List[Plan.Case] = {
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())
    val replacePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)()
        .sql("enableReplace")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('replace.enabled'='true')")())
    val hashPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"PARTITIONED BY (${Core.datePartition.columnName}) " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', " +
            "'write.distribution-mode'='hash')")()
        .insert(3)())
    val targetSizePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', " +
            "'write.target-file-size-bytes'='1048576')")()
        .insert(3)())

    List(
      basePreparation.test("surface.conc.appendAppend") { table =>
        val failureCount =
          new java.util.concurrent.atomic.AtomicInteger(0)
        def writer(base: Int): () => Unit = () =>
          (0 until 3).foreach { offset =>
            val value = base + offset
            try {
              table.spark.sql(
                s"INSERT INTO ${table.name} VALUES " +
                  s"(CAST($value AS BIGINT), $value, 'row-c', 1.5, " +
                  "true, '2024-01-09-01')")
            } catch {
              case exception: Throwable =>
                assert(
                  isTypedCommitConflict(exception),
                  "concurrent append failed with an untyped error: " +
                    s"${exception.getClass.getName}")
                failureCount.incrementAndGet()
            }
          }
        val threadErrors =
          runConcurrently(Seq(writer(100), writer(200)))
        val expectedRowCount = 3 + 6 - failureCount.get
        val actualRowCount = countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name}")

        assert(
          threadErrors.isEmpty,
          s"writer thread failed outside the insert loop: $threadErrors")
        assert(
          actualRowCount == expectedRowCount.toString,
          s"expected $expectedRowCount rows, got $actualRowCount")
        println(
          s"DIAG conc.appendAppend: ${failureCount.get}/6 inserts " +
            "hit a typed commit conflict")
      },
      basePreparation.test("surface.conc.updateUpdate") { table =>
        val column = Core.string0.columnName
        def updater(value: String): () => Unit = () =>
          try {
            table.spark.sql(
              s"UPDATE ${table.name} SET $column = '$value' " +
                s"WHERE ${Core.long0.columnName} = 2")
          } catch {
            case exception: Throwable =>
              assert(
                isTypedCommitConflict(exception),
                "concurrent update failed with an untyped error: " +
                  s"${exception.getClass.getName}")
          }
        val threadErrors =
          runConcurrently(Seq(updater("AAA"), updater("BBB")))
        val finalValue = table.spark
          .sql(
            s"SELECT $column FROM ${table.name} " +
              s"WHERE ${Core.long0.columnName} = 2")
          .collect()(0)
          .getString(0)

        assert(
          threadErrors.isEmpty,
          s"updater thread failed with a non-conflict error: $threadErrors")
        assert(
          finalValue == "AAA" ||
            finalValue == "BBB" ||
            finalValue == "row-2",
          s"concurrent updates produced a torn value: $finalValue")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "3",
          "concurrent updates should not change row count")
      },
      replacePreparation.test("surface.conc.rtasVsAppend") { table =>
        def replaceTable(): Unit =
          try {
            table.spark.sql(
              s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
                s"AS SELECT * FROM ${table.name} " +
                s"WHERE ${Core.long0.columnName} <= 2")
          } catch {
            case exception: Throwable =>
              assert(
                isTypedCommitConflict(exception),
                s"RTAS race failed with ${exception.getClass.getName}")
          }
        def appendRow(): Unit =
          try {
            table.spark.sql(
              s"INSERT INTO ${table.name} VALUES " +
                "(CAST(30 AS BIGINT), 30, 'row-30', 30.5, " +
                "true, '2024-01-09-01')")
          } catch {
            case exception: Throwable =>
              assert(
                isTypedCommitConflict(exception),
                s"append race failed with ${exception.getClass.getName}")
          }
        val threadErrors =
          runConcurrently(Seq(() => replaceTable(), () => appendRow()))

        assert(
          threadErrors.isEmpty,
          s"racing thread failed with a non-conflict error: $threadErrors")
        table.spark.sql(s"REFRESH TABLE ${table.name}")
        val rowCount = countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name}").toLong
        assert(
          rowCount == 2 || rowCount == 3,
          s"RTAS and append race settled at $rowCount rows")
        println(s"DIAG conc.rtasVsAppend: settled at $rowCount rows")
      },
      basePreparation.test("surface.schema.relaxNotNull") { table =>
        val sideTable = s"${table.name}_nn"
        table.spark.sql(s"DROP TABLE IF EXISTS $sideTable")
        try {
          table.spark.sql(
            s"CREATE TABLE $sideTable " +
              s"(id BIGINT, req INT NOT NULL) USING $dataSource")
          table.spark.sql(
            s"ALTER TABLE $sideTable ALTER COLUMN req DROP NOT NULL")
          table.spark.sql(
            s"INSERT INTO $sideTable VALUES (CAST(1 AS BIGINT), NULL)")
          assert(
            table.spark
              .sql(s"SELECT count(*) FROM $sideTable WHERE req IS NULL")
              .collect()(0)
              .getLong(0) == 1,
            "relaxing NOT NULL should allow a null write")
        } finally {
          table.spark.sql(s"DROP TABLE IF EXISTS $sideTable")
        }
      },
      basePreparation.test("surface.schema.decimalWiden") { table =>
        val sideTable = s"${table.name}_dec"
        table.spark.sql(s"DROP TABLE IF EXISTS $sideTable")
        try {
          table.spark.sql(
            s"CREATE TABLE $sideTable " +
              s"(id BIGINT, dec DECIMAL(10,2)) USING $dataSource")
          table.spark.sql(
            s"INSERT INTO $sideTable VALUES " +
              "(CAST(1 AS BIGINT), CAST(12345678.99 AS DECIMAL(10,2)))")
          table.spark.sql(
            s"ALTER TABLE $sideTable ALTER COLUMN dec TYPE DECIMAL(12,2)")
          table.spark.sql(
            s"INSERT INTO $sideTable VALUES " +
              "(CAST(2 AS BIGINT), CAST(1234567890.99 AS DECIMAL(12,2)))")
          assert(
            table.spark
              .sql(s"SELECT count(*) FROM $sideTable")
              .collect()(0)
              .getLong(0) == 2,
            "decimal widening should preserve old and new values")
        } finally {
          table.spark.sql(s"DROP TABLE IF EXISTS $sideTable")
        }
      },
      basePreparation.test("surface.schema.nestedAddField") { table =>
        val sideTable = s"${table.name}_nst"
        table.spark.sql(s"DROP TABLE IF EXISTS $sideTable")
        try {
          table.spark.sql(
            s"CREATE TABLE $sideTable " +
              s"(id BIGINT, s STRUCT<x: INT, y: STRING>) USING $dataSource")
          table.spark.sql(
            s"INSERT INTO $sideTable VALUES " +
              "(CAST(1 AS BIGINT), named_struct('x', 1, 'y', 'a'))")
          table.spark.sql(
            s"ALTER TABLE $sideTable ADD COLUMN s.w INT")
          assert(
            table.spark
              .sql(s"SELECT count(*) FROM $sideTable WHERE s.w IS NULL")
              .collect()(0)
              .getLong(0) == 1,
            "new nested field should null-fill the existing row")

          table.spark.sql(
            s"INSERT INTO $sideTable VALUES " +
              "(CAST(2 AS BIGINT), " +
              "named_struct('x', 2, 'y', 'b', 'w', 9))")
          assert(
            table.spark
              .sql(s"SELECT count(*) FROM $sideTable WHERE s.w = 9")
              .collect()(0)
              .getLong(0) == 1,
            "new nested field should be writable")
        } finally {
          table.spark.sql(s"DROP TABLE IF EXISTS $sideTable")
        }
      },
      basePreparation.test("surface.schema.nestedDropField") { table =>
        val sideTable = s"${table.name}_nsd"
        table.spark.sql(s"DROP TABLE IF EXISTS $sideTable")
        try {
          table.spark.sql(
            s"CREATE TABLE $sideTable " +
              s"(id BIGINT, s STRUCT<x: INT, y: STRING>) USING $dataSource")
          table.spark.sql(
            s"INSERT INTO $sideTable VALUES " +
              "(CAST(1 AS BIGINT), named_struct('x', 1, 'y', 'a'))")
          val exception = Check.intercept[Exception](
            table.spark.sql(
              s"ALTER TABLE $sideTable DROP COLUMN s.x"))

          println(
            "DIAG nestedDropField: " +
              s"${exception.getClass.getName} :: " +
              Option(exception.getMessage).getOrElse("").take(180))
          assert(
            table.spark
              .sql(s"SELECT s.x FROM $sideTable")
              .collect()(0)
              .getInt(0) == 1,
            "rejected nested drop should leave the field readable")
        } finally {
          table.spark.sql(s"DROP TABLE IF EXISTS $sideTable")
        }
      },
      basePreparation.test("surface.schema.reorderExisting") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} " +
            s"ALTER COLUMN ${Core.string0.columnName} FIRST")
        val columns = table.spark
          .sql(s"SELECT * FROM ${table.name} LIMIT 1")
          .columns
          .toSeq

        assert(
          columns.head == Core.string0.columnName,
          s"FIRST should move the column to the front: $columns")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "3",
          "column reorder should preserve the rows")
      },
      hashPreparation.test("surface.write.distributionHash") { table =>
        val properties = tableProps(table.spark, table.name)
        val rowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)

        assert(
          properties.get("write.distribution-mode").contains("hash"),
          "hash distribution mode should be retained")
        assert(
          rowCount == 3,
          s"hash-distributed seed should contain 3 rows, got $rowCount")
      },
      targetSizePreparation.test("surface.write.targetFileSize") { table =>
        val properties = tableProps(table.spark, table.name)
        val rowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)

        assert(
          properties
            .get("write.target-file-size-bytes")
            .contains("1048576"),
          "target file size should be retained")
        assert(
          rowCount == 3,
          s"custom target-size seed should contain 3 rows, got $rowCount")
      },
      basePreparation.test("surface.write.dfToBranch") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH wb")
        val row = table.spark.sql(
          s"SELECT CAST(50 AS BIGINT) AS ${Core.long0.columnName}, " +
            s"50 AS ${Core.int0.columnName}, " +
            s"'row-50' AS ${Core.string0.columnName}, " +
            s"50.5 AS ${Core.double0.columnName}, " +
            s"true AS ${Core.boolean0.columnName}, " +
            s"'2024-01-09-01' AS ${Core.datePartition.columnName}")
        row.writeTo(s"${table.name}.branch_wb").append()

        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name} VERSION AS OF 'wb'") == "4",
          "DataFrame writer should append to the branch")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "3",
          "DataFrame branch write should leave main unchanged")
      },
      basePreparation.test("surface.pin.importProcs") { table =>
        val metadataFile = table.spark
          .sql(
            s"SELECT file FROM ${table.name}.metadata_log_entries " +
              "ORDER BY timestamp DESC LIMIT 1")
          .collect()(0)
          .getString(0)
        val registerOutcome =
          try {
            table.spark.sql(
              "CALL openhouse.system.register_table(" +
                "table => 'dbMatrix.zz_reg', " +
                s"metadata_file => '$metadataFile')")
            val rowCount = countOf(
              table.spark,
              "SELECT count(*) FROM openhouse.dbMatrix.zz_reg")
            table.spark.sql(
              "DROP TABLE IF EXISTS openhouse.dbMatrix.zz_reg")
            s"REGISTERED (readable, $rowCount rows)"
          } catch {
            case exception: Throwable =>
              s"REJECTED ${exception.getClass.getName} :: " +
                Option(exception.getMessage).getOrElse("").take(160)
          }
        println(s"DIAG pin.register_table(real): $registerOutcome")

        val snapshotException = Check.intercept[Exception](
          table.spark.sql(
            "CALL openhouse.system.snapshot(" +
              s"source_table => '${catalogRelative(table.name)}', " +
              "table => 'dbMatrix.zz_snap')"))
        println(
          "DIAG pin.snapshot: " +
            s"${snapshotException.getClass.getName} :: " +
            Option(snapshotException.getMessage).getOrElse("").take(160))

        val addFilesException = Check.intercept[Exception](
          table.spark.sql(
            "CALL openhouse.system.add_files(" +
              s"table => '${catalogRelative(table.name)}', " +
              "source_table => '`parquet`.`/tmp/zz_nope_dir`')"))
        println(
          "DIAG pin.add_files: " +
            s"${addFilesException.getClass.getName} :: " +
            Option(addFilesException.getMessage).getOrElse("").take(160))
      },
      basePreparation.test("surface.pin.viewsAnalyze") { table =>
        val viewException = Check.intercept[Exception](
          table.spark.sql(
            "CREATE VIEW openhouse.dbMatrix.zz_v1 AS SELECT 1 AS one"))
        println(
          "DIAG pin.createView: " +
            s"${viewException.getClass.getName} :: " +
            Option(viewException.getMessage).getOrElse("").take(160))

        val analyzeException = Check.intercept[Exception](
          table.spark.sql(
            s"ANALYZE TABLE ${table.name} COMPUTE STATISTICS"))
        println(
          "DIAG pin.analyze: " +
            s"${analyzeException.getClass.getName} :: " +
            Option(analyzeException.getMessage).getOrElse("").take(160))
      })
  }

  val surfaceCases: List[Plan.Case] =
    List("parquet", "orc").flatMap { format =>
      surfaceBranchCases(format) ++
        surfaceReaderProcedureCases(format) ++
        surfaceRemainingCases(format)
    }

  // ═══ Hazard demonstrations H1-H8 (MODALITY-RECON.md; gates cleared per FEATURE-ANALYSIS-PLAN) ══
  // Each was PREDICTED by the state-flow model, verified in code/bytecode, and is demonstrated
  // live here. Characterizations flip loudly if the product fixes the hazard.

  // H1 — streaming checkpoint × expiration (G11's streaming twin). Three acts:
  // (1) stream + checkpoint; (2) CONTROL: plain restart picks up new rows (restart mechanics fine);
  // (3) expire past the checkpointed offset → restart is BRICKED with the typed error.

}

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

// The standard surface families. A surface case pins one edge of what the catalog exposes on a
// plain copy-on-write table: a reader, a procedure, a metadata table, a concurrency outcome, a
// schema change, or a write property. The concurrency helpers below are feature neutral, so a
// feature layer reuses them through a self-type on this trait. The cases run on parquet and orc.
trait SurfaceScenarios extends ScenarioKit {
  import Rows._

  protected def runConcurrently(functions: Seq[() => Unit]): Seq[Throwable] = {
    val errors = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()
    val start = new java.util.concurrent.CountDownLatch(1)
    val threads = functions.zipWithIndex.map { case (function, index) =>
      val thread = new Thread(
        () =>
          try {
            start.await()
            function()
          } catch {
            case interrupted: InterruptedException =>
              Thread.currentThread().interrupt()
              errors.add(interrupted)
            case throwable: Throwable =>
              errors.add(throwable)
          },
        s"delta-harness-concurrent-$index")
      thread.setDaemon(true)
      thread
    }
    threads.foreach(_.start())
    start.countDown()

    val deadline =
      System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(3)
    threads.foreach { thread =>
      val remainingNanos = deadline - System.nanoTime()
      if (remainingNanos > 0) {
        java.util.concurrent.TimeUnit.NANOSECONDS.timedJoin(thread, remainingNanos)
      }
    }

    threads.filter(_.isAlive).foreach { thread =>
      errors.add(
        new AssertionError(
          s"${thread.getName} did not complete within 3 minutes"))
      thread.interrupt()
    }
    errors.toArray(Array.empty[Throwable]).toSeq
  }

  protected def isTypedCommitConflict(throwable: Throwable): Boolean =
    Exceptions.causeChain(throwable).exists { cause =>
      val className = cause.getClass.getName
      className.contains("CommitFailed") ||
        className.contains("CommitStateUnknown") ||
        className.contains("Validation") ||
        className.contains("BadRequest") ||
        className.contains("WebClientResponse")
    }

  // Each surface family builds the starting states it needs, so a family reads on its own. The
  // seeded table is the plainest of them, so the feature layers build their cases on it too.
  protected def surfaceBasePreparation(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)(),
      description = s"Three seed rows with keys 1, 2 and 3 in an unpartitioned $format table.")

  private def surfaceTwoSnapshotPreparation(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)()
        .sql("insertMore")(table =>
          s"INSERT INTO $table VALUES " +
            "(CAST(4 AS BIGINT), 4, 'row-4', 4.5, true, '2024-01-04-03'), " +
            "(CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')")(),
      description = s"Five rows across two snapshots (a 3-row seed then a 2-row insert) in an " +
        s"unpartitioned $format table.")

  private def surfaceEmptyPreparation(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")(),
      description = s"An unseeded, empty unpartitioned $format table.")

  private def surfaceHashPreparation(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"PARTITIONED BY (${Core.datePartition.columnName}) " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', " +
            "'write.distribution-mode'='hash')")()
        .insert(3)(),
      description = s"Three seed rows in a $format table partitioned by datepartition with " +
        "write.distribution-mode=hash.")

  private def surfaceTargetFileSizePreparation(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', " +
            "'write.target-file-size-bytes'='1048576')")()
        .insert(3)(),
      description = s"Three seed rows in an unpartitioned $format table with " +
        "write.target-file-size-bytes=1048576.")

  // The structured-streaming reader and writer, and the changelog view.
  def surfaceReaderCases(format: String): List[Plan.Case] =
    List(
      surfaceBasePreparation(format).test(
        "surface.stream.read",
        "A Spark structured streaming read of the table, run in AvailableNow batch mode, " +
          "delivers all 3 seed rows to a memory sink within 120 seconds.") { table =>
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
      surfaceBasePreparation(format).test(
        "surface.stream.write",
        "A Spark structured streaming append of two rows through the iceberg write-stream " +
          "format lands both rows, growing the table from 3 to 5 rows.") { table =>
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
      surfaceTwoSnapshotPreparation(format).test(
        "surface.cdc.changelogView",
        "create_changelog_view over an append-only history reports 5 changes, all of change " +
          "type INSERT.") { table =>
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
      })

  // The rewrite procedure that compacts the manifest set.
  def surfaceRewriteProcedureCases(format: String): List[Plan.Case] =
    List(
      surfaceEmptyPreparation(format).test(
        "surface.proc.rewriteManifests",
        "After 5 single-row inserts fragment the manifest list, rewrite_manifests compacts it " +
          "to fewer manifests while preserving all 5 rows.") { table =>
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
      })

  // The procedures that read snapshot ancestry and remove orphan files.
  def surfaceSnapshotProcedureCases(format: String): List[Plan.Case] =
    List(
      surfaceTwoSnapshotPreparation(format).test(
        "surface.proc.ancestorsOf",
        "ancestors_of lists both snapshots of the table's two-snapshot history.") { table =>
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
      surfaceBasePreparation(format).test(
        "surface.proc.removeOrphanReal",
        "remove_orphan_files deletes a planted, backdated stray file next to a real data file " +
          "while the table's 3 live rows remain intact.") { table =>
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
      })

  // The hidden metadata columns and the Iceberg metadata tables.
  def surfaceMetadataCases(format: String): List[Plan.Case] =
    List(
      surfaceBasePreparation(format).test(
        "surface.meta.hiddenColumns",
        "Selecting the hidden metadata columns _file, _pos, _spec_id and _partition returns " +
          "one row per seed row, each with a populated file path and a non-negative position.") {
        table =>
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
      surfaceTwoSnapshotPreparation(format).test(
        "surface.meta.tableSweep",
        "Every Iceberg metadata table (entries, files, manifests, snapshots, history, refs, " +
          "partitions, and their all_* variants) is queryable without error, and the snapshots " +
          "metadata table reports the table's 2 snapshots.") { table =>
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
          table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name}.`$metadataTable`")
            .collect()
        }
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}.snapshots") == "2",
          "snapshot metadata should contain two snapshots")
      })

  // Two writers racing on one table. Every outcome is either a commit or a typed commit conflict.
  def surfaceConcurrencyCases(format: String): List[Plan.Case] =
    List(
      surfaceBasePreparation(format).test(
        "surface.conc.appendAppend",
        "Two threads concurrently insert 3 rows each; every insert either commits or fails " +
          "with a typed commit-conflict exception, and the final row count matches 3 plus the " +
          "number of inserts that actually committed.") { table =>
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
      surfaceBasePreparation(format).test(
        "surface.conc.updateUpdate",
        "Two threads concurrently UPDATE the same row to different values; the row count stays " +
          "at 3, and the final value is one of the two competing updates or the original seed " +
          "value, with any failure being a typed commit conflict.") { table =>
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
      })

  // Schema changes that Iceberg allows and the ones the catalog rejects.
  def surfaceSchemaCases(format: String): List[Plan.Case] =
    List(
      surfaceBasePreparation(format).test(
        "surface.schema.relaxNotNull",
        "On a side table, dropping NOT NULL from a column allows a subsequent insert of a null " +
          "value for that column.") { table =>
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
      surfaceBasePreparation(format).test(
        "surface.schema.decimalWiden",
        "On a side table, widening a decimal column's precision preserves the original row and " +
          "accepts a new row whose value only fits the wider precision.") { table =>
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
      surfaceBasePreparation(format).test(
        "surface.schema.nestedAddField",
        "On a side table, ADD COLUMN of a new nested struct field null-fills it for the " +
          "existing row and accepts a new row that sets the field.") { table =>
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
      surfaceBasePreparation(format).test(
        "surface.schema.nestedDropField",
        "On a side table, ALTER TABLE DROP COLUMN of a nested struct field is rejected with an " +
          "exception, and the field remains readable afterward.") { table =>
        val sideTable = s"${table.name}_nsd"
        table.spark.sql(s"DROP TABLE IF EXISTS $sideTable")
        try {
          table.spark.sql(
            s"CREATE TABLE $sideTable " +
              s"(id BIGINT, s STRUCT<x: INT, y: STRING>) USING $dataSource")
          table.spark.sql(
            s"INSERT INTO $sideTable VALUES " +
              "(CAST(1 AS BIGINT), named_struct('x', 1, 'y', 'a'))")
          Check.intercept[Exception](
            table.spark.sql(
              s"ALTER TABLE $sideTable DROP COLUMN s.x"))

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
      surfaceBasePreparation(format).test(
        "surface.schema.reorderExisting",
        "ALTER TABLE ALTER COLUMN ... FIRST moves that column to the front of the schema while " +
          "preserving all 3 rows.") { table =>
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
      })

  // The write-planning properties: distribution mode and target file size.
  def surfaceWriteCases(format: String): List[Plan.Case] =
    List(
      surfaceHashPreparation(format).test(
        "surface.write.distributionHash",
        "The write.distribution-mode=hash property requested at creation is retained and the " +
          "table holds its 3 seed rows.") { table =>
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
      surfaceTargetFileSizePreparation(format).test(
        "surface.write.targetFileSize",
        "The write.target-file-size-bytes=1048576 property requested at creation is retained " +
          "and the table holds its 3 seed rows.") { table =>
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
      })

  // Pins on the surfaces the catalog rejects: the import procedures, views and ANALYZE TABLE.
  def surfacePinCases(format: String): List[Plan.Case] =
    List(
      surfaceBasePreparation(format).test(
        "surface.pin.importProcs",
        "register_table onto a new name makes the source table's snapshot readable there " +
          "(3 rows) without affecting the source, and dropping the registered table leaves " +
          "the source untouched; the system.snapshot and system.add_files procedures are " +
          "each confirmed to reject their unsupported inputs with an exception.") { table =>
        val registeredTable = s"${table.name}_registered"
        val metadataFile = table.spark
          .sql(
            s"SELECT file FROM ${table.name}.metadata_log_entries " +
              "ORDER BY timestamp DESC LIMIT 1")
          .collect()(0)
          .getString(0)

        try {
          table.spark.sql(
            "CALL openhouse.system.register_table(" +
              s"table => '${catalogRelative(registeredTable)}', " +
              s"metadata_file => '$metadataFile')")
          assert(
            countOf(
              table.spark,
              s"SELECT count(*) FROM $registeredTable") == "3",
            "register_table should make all source rows readable")
        } finally {
          try {
            table.spark.sql(
              s"DROP TABLE IF EXISTS $registeredTable")
          } catch {
            case NonFatal(_) => ()
          }
        }
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "3",
          "dropping the registered table should not remove source rows")

        Check.intercept[Exception](
          table.spark.sql(
            "CALL openhouse.system.snapshot(" +
              s"source_table => '${catalogRelative(table.name)}', " +
              "table => 'dbMatrix.zz_snap')"))

        Check.intercept[Exception](
          table.spark.sql(
            "CALL openhouse.system.add_files(" +
              s"table => '${catalogRelative(table.name)}', " +
              "source_table => '`parquet`.`/tmp/zz_nope_dir`')"))
      },
      surfaceBasePreparation(format).test(
        "surface.pin.viewsAnalyze",
        "CREATE VIEW and ANALYZE TABLE COMPUTE STATISTICS are each rejected with an " +
          "exception.") { table =>
        Check.intercept[Exception](
          table.spark.sql(
            "CREATE VIEW openhouse.dbMatrix.zz_v1 AS SELECT 1 AS one"))

        Check.intercept[Exception](
          table.spark.sql(
            s"ANALYZE TABLE ${table.name} COMPUTE STATISTICS"))
      })
}

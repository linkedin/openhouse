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

// Shared foundation for every Scenario trait: the table/layout/prep "kit". All domain traits
// (DmlScenarios, ForkScenarios, ...) extend this, so mixing them into `object Scenarios` puts
// ScenarioKit first in the linearization → its vals initialize before any domain's, exactly as
// in the original single object. `protected` members are the shared kit; `public` ones are also
// consumed by `object Plan`.
trait ScenarioKit {
  import Rows._

  protected val Core = CoreTable            // brevity in the typed column references below
  protected val cols = Core.columnNames.mkString(", ") // source column list, so renames propagate

  // Short typed views of the current rows, keyed by the long column, for incremental assertions.
  protected def keyed(rows: Seq[Row]): Seq[Long] = rows.map(_.get(Core.long0)).sorted
  protected def longToString(rows: Seq[Row]): Map[Long, String] =
    rows.map(row => row.get(Core.long0) -> row.get(Core.string0)).toMap

  // ── the layout axis: file format x partitioning, crossed with every operation ──────────
  // Each layout is a plain literal CREATE statement (no dynamic assembly): the column list is one
  // shared literal `columnDefinitions`, and format/partition are literal fragments. createSchema
  // cross-checks the literal against CoreTable's declared columns, so the two can't silently drift.
  protected val columnDefinitions =
    "foo_col_long bigint, foo_col_int int, foo_col_string string, foo_col_double double, foo_col_boolean boolean, datepartition string"

  final case class Layout(label: String, create: String => String)

  protected val partitionVariants = List("unpartitioned" -> "", "partitioned" -> "PARTITIONED BY (datepartition)")

  val layouts: List[Layout] =
    for {
      format                        <- List("parquet", "orc", "avro")
      (partitionLabel, partitionClause) <- partitionVariants
    } yield Layout(s"$partitionLabel/$format", table =>
      s"CREATE TABLE $table ($columnDefinitions) USING $dataSource $partitionClause " +
        s"TBLPROPERTIES ('write.format.default'='$format')")

  // Merge-on-read layouts: same shapes, but DELETE/UPDATE/MERGE write position-delete files
  // (format v2) instead of rewriting data files. Crossed with the mutation operations only.
  val morLayouts: List[Layout] =
    for {
      format                        <- List("parquet", "orc", "avro")
      (partitionLabel, partitionClause) <- partitionVariants
    } yield Layout(s"mor-$partitionLabel/$format", table =>
      s"CREATE TABLE $table ($columnDefinitions) USING $dataSource $partitionClause " +
        s"TBLPROPERTIES ('write.format.default'='$format', 'format-version'='2', " +
        s"'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')")

  // Dedicated layouts for the CoW/MoR *physical* discriminator (below). Both pin
  // `write.distribution-mode=none` and are unpartitioned so a single seed INSERT lands all rows in
  // ONE data file; deleting a strict subset is then necessarily a PARTIAL-file match, which Iceberg
  // cannot satisfy by whole-file elimination. That makes the physical outcome deterministic: MoR
  // must add a position-delete file, CoW must rewrite the data file and add none. (The general
  // `morLayouts` seed splits across files, so a boundary-aligned delete can legitimately drop a
  // whole file with no position delete — correct Iceberg behaviour, but not what we want to pin.)
  val morVerifyLayouts: List[Layout] =
    List("parquet", "orc", "avro").map(format => Layout(s"mor-verify/$format", table =>
      s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
        s"'write.format.default'='$format', 'format-version'='2', 'write.distribution-mode'='none', " +
        s"'write.delete.mode'='merge-on-read')"))

  val cowVerifyLayouts: List[Layout] =
    List("parquet", "orc", "avro").map(format => Layout(s"cow-verify/$format", table =>
      s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
        s"'write.format.default'='$format', 'format-version'='2', 'write.distribution-mode'='none', " +
        s"'write.delete.mode'='copy-on-write')"))

  // Preparation: create under `layout` and seed `numberOfRows` deterministic rows. Interchangeable
  // with RTAS / drop+undrop preparations later — same resulting state.
  def createAndSeed(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(layout.create)().insert(numberOfRows)()

  // Preparation for the physical CoW/MoR discriminator: seed all rows into ONE data file. A plain
  // seed INSERT fans the rows across a couple of files (writer-dependent), so a strict-subset delete
  // can land on a whole file and be satisfied by file elimination rather than a position delete. The
  // `COALESCE(1)` hint forces a single write task → a single data file, so deleting a strict subset
  // is deterministically a PARTIAL-file match: MoR must add a position-delete file, CoW must rewrite.
  def createAndSeedSingleFile(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(layout.create)()
      .sql(s"seed($numberOfRows, one-file)")(table =>
        s"INSERT INTO $table SELECT /*+ COALESCE(1) */ * FROM (${RowGenerator.valuesClause(Core, numberOfRows)}) AS seed")(
        view => assert(view.after.size == numberOfRows,
          s"single-file seed expected $numberOfRows rows, got ${view.after.size}"))

  // Phase 24 preparation multipliers: a DDL evolves the starting state, then a DML op runs on it.
  // Ordered prep (sort order) is arity-neutral → crosses ALL operations. Evolved prep adds a column
  // → INSERT arity changes, so it crosses only ops that don't re-insert all columns (delete/update/read).
  def createAndSeedOrdered(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    createAndSeed(layout, numberOfRows).sql("prep.ordered")(t => s"ALTER TABLE $t WRITE ORDERED BY ${CoreTable.long0.columnName}")()

  def createAndSeedEvolved(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    createAndSeed(layout, numberOfRows).sql("prep.evolved")(t => s"ALTER TABLE $t ADD COLUMN prep_extra int")()

  // Branch-routing prep (the T axis, wap-conf mechanism): seed on main, fork a branch, then set
  // spark.wap.branch so the ENTIRE downstream operation (writes AND reads) routes to the branch —
  // no per-op rewrite needed. The op's delta assertions are relative to view.before (also the
  // branch), so they hold unchanged. Each case runs in its own spark.newSession() (parallel runner),
  // so the conf never leaks across cases. This crosses the whole DML catalog onto a branch.
  def createAndSeedOnBranch(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    createAndSeed(layout, numberOfRows)
      .sql("prep.enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .step("prep.routeToBranch") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table CREATE BRANCH b")
        spark.conf.set("spark.wap.branch", "b")
      }()

  // RTAS prep prefix (the P axis, replace-lineage leg — SURFACE-APPRAISAL step 2): create + seed,
  // then CREATE OR REPLACE ... AS SELECT * re-specifying the SAME shape, so the table is
  // functionally identical but reached via the replace path (the path G9/G10 showed misbehaves).
  // Every downstream DML op then runs on a replace-lineage table. FULL CROSS (Phase 28): all 6 layouts
  // ({unpartitioned,partitioned} × {parquet,orc,avro}) — mirrors the core `dml` block's layout coverage so
  // the RTAS/replace-lineage substrate carries the same DML surface as the plain CREATE substrate.
  // (label, partitionClause, format).
  val rtasPrepShapes: List[(String, String, String)] =
    for { (pl, pc) <- partitionVariants; fmt <- List("parquet", "orc", "avro") } yield (s"$pl/$fmt", pc, fmt)

  // MoR-read prep (closes the review's "reads on MoR with deletes is a distinct scan path" gap —
  // SURFACE-APPRAISAL step 1). The current MoR bucket runs mutation ops (each reads back once), but
  // never crosses the READ variants against a table carrying a LIVE position delete. Seed a single
  // data file (COALESCE(1)) on a MoR layout, delete a strict subset → a position-delete file the
  // reader must APPLY at scan time (not a whole-file elimination). Downstream read ops then assert
  // the deleted row is excluded under each read shape (projection, filter-pushdown, ...).
  def createAndSeedMorDeleted(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    createAndSeedSingleFile(layout, numberOfRows)
      .step("prep.morDelete") { (spark, table) =>
        spark.sql(s"DELETE FROM $table WHERE ${Core.long0.columnName} = 1")   // strict subset → position delete
      } { view =>
        assert(view.after.size == numberOfRows - 1, s"MoR prep delete failed: ${view.after.size}")
        val deleteFiles = view.spark.sql(s"SELECT count(*) FROM ${view.table}.all_delete_files").collect()(0).getLong(0)
        assert(deleteFiles == 1, s"MoR prep must leave a live position-delete file, got $deleteFiles")
      }

  // Undrop prep (the P axis, drop→undrop leg — SURFACE-APPRAISAL, requires embedded real HTS). Seed a
  // plain table, then take it through the FULL soft-delete → restore round-trip on the real HTS, and
  // hand the RESTORED table to the downstream op. The point is a modality audit: every feature's state
  // (rows, snapshot lineage, refs, spec, sort order, properties, MoR delete files, schema) must survive
  // the round-trip, so the whole DML/DDL catalog is crossed onto the restored table. Soft-delete is
  // driven directly on HTS (customer DROP hard-deletes); restore uses the customer Tables API.
  def createAndSeedUndropped(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    createAndSeed(layout, numberOfRows)
      .step("prep.undrop") { (spark, table) =>
        val Array(db, tbl) = table.stripPrefix("openhouse.").split("\\.", 2)
        val (sdCode, sdBody) = HtsAdmin.softDelete(db, tbl)
        assert(sdCode >= 200 && sdCode < 300, s"HTS soft-delete failed ($sdCode): $sdBody")
        val deletedAtMs = HtsAdmin.softDeletedAtMs(db, tbl)
          .getOrElse(throw new AssertionError(s"soft-deleted table $db.$tbl not found in querySoftDeleted"))
        val (rCode, rBody) = HtsAdmin.restore(db, tbl, deletedAtMs)
        assert(rCode >= 200 && rCode < 300, s"restore failed ($rCode): $rBody")
      } { view =>
        assert(view.after.size == numberOfRows,
          s"restored table must keep its $numberOfRows rows, got ${view.after.size}")
      }

  def createAndSeedRtas(partitionClause: String, numberOfRows: Int, format: String = "parquet"): TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource $partitionClause " +
        s"TBLPROPERTIES ('write.format.default'='$format', 'replace.enabled'='true')")()
      .insert(numberOfRows)()
      .sql("prep.rtas")(t => s"CREATE OR REPLACE TABLE $t USING $dataSource $partitionClause " +
        s"TBLPROPERTIES ('write.format.default'='$format') AS SELECT * FROM $t")()
      // Iceberg documents CREATE OR REPLACE ... AS SELECT as ATOMIC on a SparkCatalog, so the client
      // should observe a consistent table afterward with no manual refresh. On the embedded catalog it
      // does; on the OpenHouse REST-backed catalog the client can retain a stale metadata pointer across
      // the replace (surfacing downstream as a 400 "incorrect version" or "table not found after
      // refresh"). REFRESH re-reads the committed pointer so the suite is robust to that catalog
      // divergence; the divergence itself is filed as a product bug (see Remote Test Findings).
      .sql("prep.rtas.refresh")(t => s"REFRESH TABLE $t")()

  // RTAS prep on a MERGE-ON-READ table (over-prune miss #1): the replace re-specifies the MoR delete/
  // update/merge modes, so downstream mutation ops exercise the MoR write path on a replace-lineage
  // table. Non-vacuous per the appraisal — replace + MoR is a distinct combination.
  protected def morPropsFmt(format: String) = s"'write.format.default'='$format', 'format-version'='2', " +
    "'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read'"
  protected val morProps = morPropsFmt("parquet")

  def createAndSeedRtasMor(partitionClause: String, numberOfRows: Int, format: String = "parquet"): TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource $partitionClause " +
        s"TBLPROPERTIES (${morPropsFmt(format)}, 'replace.enabled'='true')")()
      .insert(numberOfRows)()
      .sql("prep.rtasMor")(t => s"CREATE OR REPLACE TABLE $t USING $dataSource $partitionClause " +
        s"TBLPROPERTIES (${morPropsFmt(format)}) AS SELECT * FROM $t")()
      // See createAndSeedRtas: REFRESH after the atomic replace guards the shared suite against the
      // OpenHouse catalog's stale-pointer divergence (filed as a product bug).
      .sql("prep.rtasMor.refresh")(t => s"REFRESH TABLE $t")()


  // ── hoisted shared helpers (used across domain traits) ──
  protected def coreTwoSnapshots(fmt: String): TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(table => s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$fmt')")()
      .insert(3)()
      .sql("insertMore")(table => s"INSERT INTO $table VALUES " +
        s"(CAST(4 AS BIGINT), 4, 'row-4', 4.5, true, '2024-01-04-03'), (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')")()
  // No-arg overload (parquet) keeps the many existing single-format call sites unchanged.
  protected def coreTwoSnapshots: TableTest[CoreTable.type] = coreTwoSnapshots("parquet")

  // Snapshots in ancestry order (root first), following the parent_id chain — deterministic even
  // if two commits happen to share a committed_at millisecond (which `ORDER BY committed_at` is not).
  protected def snapshotIds(spark: SparkSession, table: String): Seq[Long] = {
    val rows = spark.sql(s"SELECT snapshot_id, parent_id FROM $table.snapshots").collect().toSeq
    val ids = rows.map(_.getLong(0)).toSet
    val childByParent = rows.collect { case r if !r.isNullAt(1) => r.getLong(1) -> r.getLong(0) }.toMap
    val root = rows.collectFirst { case r if r.isNullAt(1) || !ids.contains(r.getLong(1)) => r.getLong(0) }.get
    val order = scala.collection.mutable.ListBuffer(root)
    var cur = root
    while (childByParent.contains(cur)) { cur = childByParent(cur); order += cur }
    order.toList
  }

  protected def catalogRelative(table: String): String = table.stripPrefix("openhouse.")

  protected def coreRow(long: Long, tag: String): String =
    s"(CAST($long AS BIGINT), ${long.toInt}, '$tag', ${long}.5, false, '2024-01-01-00')"

  protected val L = CoreTable.long0.columnName

  // The Spark datasource short-name for `CREATE TABLE ... USING <name>`. Defaults to "iceberg" — the
  // Apache Iceberg DataSourceRegister short-name that OSS OpenHouse registers and that every OpenHouse
  // itest uses. A downstream environment whose shaded runtime relocates the Iceberg datasource to a
  // different short-name (for example to let multiple Iceberg libraries coexist on one classpath) would
  // find that `USING iceberg` does not resolve there. This is a plain `var` — not a runtime knob — that an
  // environment adapter overrides purely in code (for example `Scenarios.dataSource = "openhouse"`) once,
  // before it builds `Plan.cases`. The emitted SQL is otherwise byte-identical across environments.
  var dataSource: String = "iceberg"

  // "should be format-independent" is a hypothesis this harness must verify, not assume (see G8/G10, and
  // the fork carries patched ORC paths). Only table-LESS ops (no CREATE) have no format axis.
  protected val seedFmtTL = new ThreadLocal[String]()
  def seedFmt: String = Option(seedFmtTL.get).getOrElse("parquet")
  def withSeedFmt[A](fmt: String)(body: => A): A = {
    seedFmtTL.set(fmt); try body finally seedFmtTL.remove()
  }
  protected def coreCreateParquet(table: String): String =
    s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$seedFmt')"

  protected def undropSeed(ctx: Ctx, name: String): (String, String, String) = {
    val table = s"${ctx.namespace}.$name"
    val Array(db, tbl) = table.stripPrefix("openhouse.").split("\\.", 2)
    ctx.spark.sql(s"DROP TABLE IF EXISTS $table")
    ctx.spark.sql(coreCreateParquet(table))
    ctx.spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, 3)}")
    (table, db, tbl)
  }

  protected def softDeleteRestore(ctx: Ctx, db: String, tbl: String): Unit = {
    assert(HtsAdmin.softDelete(db, tbl)._1 / 100 == 2, s"soft-delete $db.$tbl failed")
    val ms = HtsAdmin.softDeletedAtMs(db, tbl).getOrElse(throw new AssertionError(s"no deletedAtMs for $db.$tbl"))
    assert(HtsAdmin.restore(db, tbl, ms)._1 / 100 == 2, s"restore $db.$tbl failed")
  }

  protected def tableProps(spark: SparkSession, table: String): Map[String, String] =
    spark.sql(s"SHOW TBLPROPERTIES $table").collect().toSeq.map(r => r.getString(0) -> r.getString(1)).toMap

  protected val extraColInsert9  = "(CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01', 42)"
  protected val extraColInsert10 = "(CAST(10 AS BIGINT), 10, 'row-10', 10.5, true, '2024-01-10-01', 43)"

  protected def rtasPrep: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("enableReplace")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('replace.enabled'='true')")()

  protected def countOf(spark: SparkSession, sql: String): String =
    spark.sql(sql).collect()(0).getLong(0).toString

}

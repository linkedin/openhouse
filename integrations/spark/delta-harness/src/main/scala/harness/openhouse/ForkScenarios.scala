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

trait ForkScenarios extends ScenarioKit {
  import Rows._

  // ── Column-default (fork #251) — OSS Spark DDL path ──────────────────────────────────────────
  // Column defaults are TABLED (see ICEBERG-FORK-AUDIT.md). This test characterizes what the OSS Spark 3.5
  // DDL path does with `ALTER TABLE t ADD COLUMN c int DEFAULT 5`; the behavior is identical on the
  // published 1.5.2.15 and the branch build (#251 is api/core only, with no Spark write wiring). Measured:
  //   • accepted at Spark parse time (Spark 3.5 owns the DEFAULT grammar);
  //   • the default is not written into the Iceberg schema (DESCRIBE shows `c|int|null`, no default);
  //   • pre-existing rows read NULL;
  //   • an INSERT that omits the column is rejected INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA
  //     (same root as bug1 — no column-default write wiring in the connector).
  // These are behavior pins: if a future build changes any of the above, the asserts flip and it is re-audited.
  private def forkColDefaultAddColumn(fmt: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_coldef_$fmt"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(s"CREATE TABLE $table (id bigint, s string) USING $dataSource TBLPROPERTIES ('write.format.default'='$fmt')")
    spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")

    // (1) The customer path is ACCEPTED at parse time (Spark owns the grammar) — pin no-throw.
    spark.sql(s"ALTER TABLE $table ADD COLUMN c int DEFAULT 5")

    // (2) The default is not written into the persisted schema — column c has no default metadata.
    val cDesc = spark.sql(s"DESCRIBE TABLE EXTENDED $table").collect()
                  .map(_.mkString("|")).filter(_.matches("(?i)^c\\|.*")).mkString(" ;; ")
    assert(!cDesc.toLowerCase.contains("default") && !cDesc.contains("5"),
      s"[$fmt] expected no default persisted for c, but DESCRIBE shows: $cDesc — a #251-containing build may now be wired; re-audit")

    // (3) The default is NOT backfilled on read — pre-existing rows read NULL, not 5.
    val nulls = spark.sql(s"SELECT count(*) FROM $table WHERE c IS NULL").collect()(0).getLong(0)
    assert(nulls == 2,
      s"[$fmt] expected the default NOT applied on read (2 NULLs), got $nulls — a #251-containing build may now apply defaults; re-audit")

    // (4) The default is NOT applied on write — an insert that omits c is rejected (no write wiring).
    val omit = Check.intercept[org.apache.spark.sql.AnalysisException] {
      spark.sql(s"INSERT INTO $table (id, s) VALUES (3, 'c')")
    }
    val omitMsg = Exceptions.causeChain(omit).flatMap(e => Option(e.getMessage)).mkString(" | ")
    assert(omitMsg.contains("CANNOT_FIND_DATA"),
      s"[$fmt] expected omit-insert rejected with CANNOT_FIND_DATA (no column-default write wiring), got: $omitMsg")

    println(s"DIAG fork.colDefault[$fmt]: accepted=yes persistedDefault=no readBackfill=no writeApply=no(CANNOT_FIND_DATA)")
    spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // ── Column-default (fork #251) — SchemaParser serialization ──────────────────────────────────────
  // Characterizes the api/core surface of #251: NestedField carries `initial-default`/`write-default` and
  // SchemaParser serializes them into the schema JSON. `toJson` takes no format-version parameter, so the
  // key serializes regardless of the table's format version. Exercised directly via reflection so the SAME
  // source compiles and runs in BOTH artifacts:
  //   • published 1.5.2.15  → NestedField.builder() is absent → records "API unsupported";
  //   • branch HEAD (#251)  → builds a defaulted field, checks SchemaParser emits `initial-default` and
  //                           that it round-trips (fromJson→toJson).
  // Reflection (not direct calls) is required because the builder API does not exist in the release jar;
  // a direct reference would not COMPILE in default (release) mode.
  private def forkColDefaultApiSerialization(ctx: Ctx): Unit = {
    val nestedFieldCls = Class.forName("org.apache.iceberg.types.Types$NestedField")
    val builderM = scala.util.Try(nestedFieldCls.getMethod("builder"))
    if (builderM.isFailure) {
      // Published release: the #251 column-default API is absent. Pin that absence (feature not present).
      println("DIAG fork.colDefault.api: NestedField.builder ABSENT — #251 column-default API unsupported (published release artifact)")
      val ms = nestedFieldCls.getMethods.map(_.getName).toSet
      assert(!ms.contains("initialDefault") && !ms.contains("writeDefault"),
        "NestedField exposes initial/write-default accessors but no builder() — unexpected partial #251; re-audit")
      return
    }
    // Branch HEAD: #251 present. Build `optional int c` carrying initial-default=5 via the builder.
    val builder0 = builderM.get.invoke(null)
    def chain(b: AnyRef, m: String, argT: Class[_], arg: AnyRef): AnyRef =
      b.getClass.getMethod(m, argT).invoke(b, arg)
    def chain0(b: AnyRef, m: String): AnyRef = b.getClass.getMethod(m).invoke(b)
    val intType = Class.forName("org.apache.iceberg.types.Types$IntegerType")
      .getMethod("get").invoke(null)
    var b = chain(builder0, "withId", java.lang.Integer.TYPE, java.lang.Integer.valueOf(3))
    b = chain(b, "withName", classOf[String], "c")
    b = chain(b, "ofType", Class.forName("org.apache.iceberg.types.Type"), intType)
    b = chain0(b, "asOptional")
    b = chain(b, "withInitialDefault", classOf[Object], java.lang.Integer.valueOf(5))
    val field = b.getClass.getMethod("build").invoke(b)
      .asInstanceOf[org.apache.iceberg.types.Types.NestedField]

    // Assemble a schema [id, c(default=5)] and serialize it — no format version is even passed.
    val idField = org.apache.iceberg.types.Types.NestedField.required(
      1, "id", org.apache.iceberg.types.Types.LongType.get())
    val schema = new org.apache.iceberg.Schema(java.util.Arrays.asList(idField, field))
    val json = org.apache.iceberg.SchemaParser.toJson(schema)
    println(s"DIAG fork.colDefault.api: #251 PRESENT; serialized schema JSON = $json")

    // (a) The default is serialized into the schema JSON.
    assert(json.contains("initial-default"),
      s"expected #251 SchemaParser to serialize 'initial-default' into the schema JSON, got: $json")
    // (b) toJson takes no format-version argument — the key serializes the same regardless of format version.
    // (c) Round-trips through fromJson→toJson.
    val reparsed = org.apache.iceberg.SchemaParser.fromJson(json)
    val json2 = org.apache.iceberg.SchemaParser.toJson(reparsed)
    assert(json2.contains("initial-default"),
      s"expected 'initial-default' to survive fromJson->toJson round-trip, got: $json2")
    println("DIAG fork.colDefault.api: initial-default serialized (no format-version argument) + round-trips")
  }

  // Reflectively build an `optional int` NestedField carrying initial-default=`dflt` (the #251 builder).
  // Returns None when the API is absent (published release) so callers can pin that cleanly.
  private def buildDefaultedIntField(id: Int, name: String, dflt: Int): Option[org.apache.iceberg.types.Types.NestedField] = {
    val nfCls = Class.forName("org.apache.iceberg.types.Types$NestedField")
    val bm = scala.util.Try(nfCls.getMethod("builder"))
    if (bm.isFailure) return None
    def chain(b: AnyRef, m: String, at: Class[_], a: AnyRef): AnyRef = b.getClass.getMethod(m, at).invoke(b, a)
    def chain0(b: AnyRef, m: String): AnyRef = b.getClass.getMethod(m).invoke(b)
    val intType = Class.forName("org.apache.iceberg.types.Types$IntegerType").getMethod("get").invoke(null)
    var b = chain(bm.get.invoke(null), "withId", java.lang.Integer.TYPE, java.lang.Integer.valueOf(id))
    b = chain(b, "withName", classOf[String], name)
    b = chain(b, "ofType", Class.forName("org.apache.iceberg.types.Type"), intType)
    b = chain0(b, "asOptional")
    b = chain(b, "withInitialDefault", classOf[Object], java.lang.Integer.valueOf(dflt))
    Some(b.getClass.getMethod("build").invoke(b).asInstanceOf[org.apache.iceberg.types.Types.NestedField])
  }

  // ── Column-default (fork #251) — READ-APPLY characterization PROBE (TABLED / not a bug claim) ─────
  // TABLED per repo owner: "it is not fundamentally broken … if there is a gap, it's implemented somewhere."
  // This probe records, but does NOT assert a verdict on, what THIS harness config does — i.e. the OSS
  // Spark 3.5 read path over branch iceberg-core. It does NOT exercise LinkedIn's PRIVATE Spark fork, which
  // is the likely home of the missing-column read-application. So a NULL here is a property of this harness,
  // NOT proof the feature is broken. Left as a DIAG-only probe (asserts only the undisputed half: the
  // default persists into the committed schema). Revisit when default values are un-tabled AND the private
  // Spark reader is available to test against.
  private def forkColDefaultReadApplyProbe(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val nfCls = Class.forName("org.apache.iceberg.types.Types$NestedField")
    val apiPresent = scala.util.Try(nfCls.getMethod("builder")).isSuccess
    if (!apiPresent) {
      // Published release: no way to set a default, so there is nothing to read back. Assert the API is
      // genuinely absent (so this is not a silent green) and return.
      println("DIAG fork.colDefault.readApplyProbe: #251 API absent (published release) — nothing to probe")
      assert(!nfCls.getMethods.map(_.getName).toSet.contains("initialDefault"),
        "NestedField exposes initialDefault but builder() is absent — unexpected partial #251; re-audit")
      return
    }
    val cat = "coldefroapply"
    val wh  = s"/tmp/coldef-readapply-${System.nanoTime()}"
    spark.conf.set(s"spark.sql.catalog.$cat", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"spark.sql.catalog.$cat.type", "hadoop")
    spark.conf.set(s"spark.sql.catalog.$cat.warehouse", wh)
    val t = s"$cat.d.t_readapply"
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(s"CREATE TABLE $t (id bigint) USING $dataSource")
    spark.sql(s"INSERT INTO $t VALUES (1),(2)") // data files physically contain ONLY `id`

    // Set a column default the way a private engine would: evolve the schema to [id, c int DEFAULT 5] via
    // the low-level TableMetadata API (public UpdateSchema has no set-default op on the branch).
    val table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, t)
    val cur   = table.schema()
    val nextId = cur.highestFieldId() + 1
    val cField = buildDefaultedIntField(nextId, "c", 5).getOrElse(
      throw new AssertionError("#251 builder present but field build failed"))
    val cols = new java.util.ArrayList[org.apache.iceberg.types.Types.NestedField](cur.columns())
    cols.add(cField)
    val s2 = new org.apache.iceberg.Schema(cols)
    val ops = table.asInstanceOf[org.apache.iceberg.HasTableOperations].operations()
    val base = ops.current()
    val updated = org.apache.iceberg.TableMetadata.buildFrom(base).setCurrentSchema(s2, s2.highestFieldId()).build()
    ops.commit(base, updated)

    // ASSERT only the undisputed half: the default persists into the committed schema (ungated).
    val persisted = org.apache.iceberg.SchemaParser.toJson(
      org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, t).schema())
    assert(persisted.contains("initial-default"),
      s"expected initial-default to persist into the committed schema, got: $persisted")

    // DIAG only — record what the OSS-Spark read path returns here; NO verdict (read-apply may live in the
    // private Spark reader not exercised by this harness).
    spark.sql(s"REFRESH TABLE $t")
    val vals = spark.sql(s"SELECT c FROM $t ORDER BY id").collect()
                 .map(r => if (r.isNullAt(0)) "NULL" else r.getInt(0).toString)
    println(s"DIAG fork.colDefault.readApplyProbe: OSS-Spark read of defaulted col over old files = " +
            s"[${vals.mkString(",")}] (harness-config observation only; private Spark reader NOT tested; TABLED)")
    spark.sql(s"DROP TABLE IF EXISTS $t")
  }

  // ── #249 (d69c1fd91) — partitioned write distribution default ─────────────────────────────────────
  // The fork changes the DEFAULT write.distribution-mode for PARTITIONED writes from Apache's HASH to
  // NONE (Spark 3.5). With HASH, the writer shuffles rows so each partition is written by one task ->
  // ~(#partitions) data files. With NONE, no shuffle -> each input task writes every partition it holds
  // -> up to (#tasks × #partitions) files. This test appends the SAME multi-task DataFrame into a
  // 4-partition table twice — once with the default, once with an explicit HASH — and compares the data-
  // file counts. It pins that (a) explicit HASH clusters to ~#partitions, and (b) the default does not
  // cluster more than HASH. Run under both runtimes via ICEBERG_RUNTIME_JAR: the DIAG file counts show
  // the branch-vs-release difference (fork NONE default -> more files than a HASH-default build).
  private def forkPartitionDistDefault(fmt: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val nParts = 4
    val nTasks = 8
    def buildAndCountFiles(tbl: String, extraProps: String): Long = {
      spark.sql(s"DROP TABLE IF EXISTS $tbl")
      spark.sql(s"CREATE TABLE $tbl (id bigint, p int) USING $dataSource PARTITIONED BY (p) " +
        s"TBLPROPERTIES ('format-version'='2', 'write.format.default'='$fmt'$extraProps)")
      // nTasks input partitions, each holding rows for all nParts table partitions.
      val df = spark.range(0, 400)
        .selectExpr("id", s"cast(id % $nParts as int) as p")
        .repartition(nTasks)
      df.writeTo(tbl).append()
      val n = spark.sql(s"SELECT count(*) FROM $tbl.data_files").collect()(0).getLong(0)
      spark.sql(s"DROP TABLE IF EXISTS $tbl")
      n
    }
    val nDefault = buildAndCountFiles(s"${ctx.namespace}.t_dist_def_$fmt", "")
    val nHash    = buildAndCountFiles(s"${ctx.namespace}.t_dist_hash_$fmt", ", 'write.distribution-mode'='hash'")
    println(s"DIAG fork.partitionDist[$fmt]: defaultFiles=$nDefault hashFiles=$nHash " +
            s"(parts=$nParts tasks=$nTasks; default==hash => HASH-default build, default>hash => NONE-default #249)")
    // (a) Explicit HASH clusters by partition -> roughly one file per partition (allow slack for spill).
    assert(nHash <= nParts * 2,
      s"[$fmt] write.distribution-mode=hash should cluster to ~$nParts files, got $nHash")
    // (b) The default never clusters MORE than HASH (fork default is NONE => >=; never <).
    assert(nDefault >= nHash,
      s"[$fmt] default partitioned distribution produced FEWER files than HASH (default=$nDefault hash=$nHash) — unexpected; re-audit #249")
  }

  // (count, sumBytes) of the CURRENT data files — used by the compaction fork probes below.
  private def dataFileStats(spark: SparkSession, table: String): (Long, Long) = {
    val r = spark.sql(s"SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM $table.data_files").collect()(0)
    (r.getLong(0), r.getLong(1))
  }

  private def showProps(spark: SparkSession, table: String): Map[String, String] =
    spark.sql(s"SHOW TBLPROPERTIES $table").collect().toSeq.map(r => r.getString(0) -> r.getString(1)).toMap

  // ── #229 (write.delete-file-replication) — MoR delete-file HDFS replication factor ───────────────────
  // TableProperties.DELETE_FILE_REPLICATION = "write.delete-file-replication". SparkWriteConf resolves it
  // (sessionConf spark.sql.iceberg.delete-file-replication > tableProperty write.delete-file-replication >
  // option > default 3) into a `short` that SparkPositionDeltaWrite / SparkPositionDeletesRewrite feed to
  // OutputFileFactory.replicationFactor(short); the factory stamps it onto the delete file's FileIO output
  // properties so HDFS sets that block-replication on the position-delete file. The HDFS replication itself
  // is NOT observable on the local FS this harness runs on — so this is an accepted LOW-observability pin:
  //   • the property round-trips through the OpenHouse catalog metadata (SHOW TBLPROPERTIES);
  //   • a MoR DELETE physically writes a position-delete file (the path that consumes the factor);
  //   • the DML result is correct and the property survives the mutation.
  private def forkDeleteFileReplication(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_delrepl"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    // MoR + unpartitioned + distribution=none so one seed INSERT lands ONE data file; a partial DELETE is
    // then necessarily a position delete (not whole-file elimination) — the delete-file write path.
    spark.sql(s"CREATE TABLE $table (id bigint, s string) USING $dataSource TBLPROPERTIES (" +
      s"'format-version'='2', 'write.distribution-mode'='none', 'write.delete.mode'='merge-on-read', " +
      s"'write.update.mode'='merge-on-read', 'write.delete-file-replication'='2')")
    // COALESCE(1) => a single data file, so deleting a strict subset is a PARTIAL-file match that MoR
    // must satisfy with a position-delete file (not whole-file elimination).
    spark.sql(s"INSERT INTO $table SELECT /*+ COALESCE(1) */ * FROM (VALUES (1L,'a'),(2L,'b'),(3L,'c')) AS s(id, s)")

    // (1) The property round-trips through the OpenHouse catalog metadata.
    val p1 = showProps(spark, table)
    assert(p1.get("write.delete-file-replication").contains("2"),
      s"expected write.delete-file-replication=2 to round-trip, got ${p1.get("write.delete-file-replication")}")

    // (2) A MoR DELETE writes a position-delete file (the write path that consumes the replication factor).
    spark.sql(s"DELETE FROM $table WHERE id = 1")
    val delFiles = spark.sql(s"SELECT count(*) FROM $table.delete_files").collect()(0).getLong(0)
    assert(delFiles >= 1, s"MoR DELETE should write a position-delete file, got $delFiles")

    // (3) DML result is correct (the replication factor never alters the logical row set).
    val rows = spark.sql(s"SELECT id FROM $table ORDER BY id").collect().toSeq.map(_.getLong(0))
    assert(rows == Seq(2L, 3L), s"expected [2,3] after MoR delete, got $rows")

    // (4) The property survives the mutation (still honored in metadata after the delete-file write).
    val p2 = showProps(spark, table)
    assert(p2.get("write.delete-file-replication").contains("2"), "write.delete-file-replication lost after DELETE")

    println(s"DIAG fork.deleteFileReplication: prop=2 roundtrips=yes deleteFiles=$delFiles rows=${rows.mkString(",")} " +
      s"(HDFS block-replication not observable on local FS; property honored in metadata + MoR DML unaffected)")
    spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // ── #219 (OutputFileFactory.FILE_REPLICATION_FACTOR) — output-file replication factor ─────────────────
  // KEY CORRECTION: the constant is FILE_REPLICATION_FACTOR = "file-replication-factor" — NOT the guessed
  // "write.file-replication-factor", and it is NOT a settable table property at all. It is the per-output-
  // file property KEY that OutputFileFactory stamps into the FileIO property map when a replicationFactor
  // is present (getProperties()), consumed by HDFS to set the file's block replication. The ONLY caller
  // that feeds a replicationFactor is the DELETE-file path (SparkPositionDeltaWrite/SparkPositionDeletesRewrite,
  // via SparkWriteConf.deleteFileReplication()) — data-file factories never set it. So #219 is the low-level
  // OutputFileFactory API manifestation of the same mechanism as #229, pinned at the API surface where it IS
  // observable: build the factory with a factor and assert it stamps FILE_REPLICATION_FACTOR into the output-
  // file property map. Reflection is used for the fork-only builder method + the private getProperties() so
  // this source compiles against a stock artifact too.
  private def forkFileReplicationFactor(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val offCls = Class.forName("org.apache.iceberg.io.OutputFileFactory")

    // (1) Pin the EXACT key string (corrects the common mis-guess "write.file-replication-factor").
    val keyFieldT = scala.util.Try(offCls.getField("FILE_REPLICATION_FACTOR"))
    assert(keyFieldT.isSuccess, "OutputFileFactory.FILE_REPLICATION_FACTOR absent — replication-factor fork feature missing")
    val key = keyFieldT.get.get(null).asInstanceOf[String]
    assert(key == "file-replication-factor",
      s"""expected FILE_REPLICATION_FACTOR == "file-replication-factor" (an output-file property key, NOT a "write." table prop), got "$key"""")

    // Need a real Iceberg Table to build a factory.
    val table = s"${ctx.namespace}.t_filerepl"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(s"CREATE TABLE $table (id bigint, s string) USING $dataSource TBLPROPERTIES ('format-version'='2')")
    spark.sql(s"INSERT INTO $table VALUES (1,'a'),(2,'b')")
    val icebergTable = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, table)

    // (2) Build an OutputFileFactory carrying replicationFactor=2 via the fork builder (reflected — the
    //     .replicationFactor(short) method is a fork addition).
    val builder = offCls.getMethod("builderFor", classOf[org.apache.iceberg.Table], java.lang.Integer.TYPE, java.lang.Long.TYPE)
      .invoke(null, icebergTable, java.lang.Integer.valueOf(1), java.lang.Long.valueOf(1L))
    val replMT = scala.util.Try(builder.getClass.getMethod("replicationFactor", java.lang.Short.TYPE))
    assert(replMT.isSuccess, "OutputFileFactory.Builder.replicationFactor(short) absent — replication fork missing")
    replMT.get.invoke(builder, java.lang.Short.valueOf(2.toShort))
    val factory = builder.getClass.getMethod("build").invoke(builder)
    assert(factory != null, "OutputFileFactory build returned null")

    // (3) OBSERVABLE: the factory stamps FILE_REPLICATION_FACTOR -> "2" into the per-output-file property
    //     map it hands the FileIO. getProperties() is private -> reflect it.
    val gp = offCls.getDeclaredMethod("getProperties"); gp.setAccessible(true)
    val props = gp.invoke(factory).asInstanceOf[java.util.Map[String, String]]
    assert(props.get(key) == "2",
      s"expected output-file property $key=2 stamped by the factory, got ${props.get(key)}")

    // (4) Writes still succeed and rows are correct (the factor never corrupts the data path).
    spark.sql(s"INSERT INTO $table VALUES (3,'c')")
    val rows = spark.sql(s"SELECT id FROM $table ORDER BY id").collect().toSeq.map(_.getLong(0))
    assert(rows == Seq(1L, 2L, 3L), s"rows wrong after write: $rows")

    println(s"DIAG fork.fileReplicationFactor: key='$key' (corrected from guessed 'write.file-replication-factor'); " +
      s"factory stamps $key=${props.get(key)} into output-file props; writes ok rows=${rows.mkString(",")}")
    spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // ── #228 (spark.sql.iceberg.split-size) — Spark read split size ───────────────────────────────────────
  // SparkSQLProperties.SPLIT_SIZE = "spark.sql.iceberg.split-size". Set via spark.conf.set; SparkReadConf
  // uses it to combine/split data files into read tasks. This one IS observable: with several small files,
  // a large split-size combines them into FEWER read tasks and a tiny split-size splits into MORE — visible
  // via rdd.getNumPartitions — while the row set is invariant. × parquet+orc (planning is over both).
  private def forkSplitSize(fmt: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_splitsize_$fmt"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    // distribution=none + several separate INSERTs => several distinct data files. open-file-cost=1 so
    // per-file planning weight is the file's byte LENGTH (not the 4MB default that would swamp small
    // files) — that makes split-size the governing knob, so the task-count effect is actually visible.
    spark.sql(s"CREATE TABLE $table (id bigint, s string) USING $dataSource " +
      s"TBLPROPERTIES ('write.format.default'='$fmt', 'write.distribution-mode'='none', 'read.split.open-file-cost'='1')")
    val nFiles = 6
    for (i <- 0 until nFiles) spark.sql(s"INSERT INTO $table SELECT ${i}L, repeat('r$i', 4000)")
    val fileCount = spark.sql(s"SELECT count(*) FROM $table.data_files").collect()(0).getLong(0)
    assert(fileCount >= 2, s"[$fmt] expected multiple data files for a split test, got $fileCount")

    val key = org.apache.iceberg.spark.SparkSQLProperties.SPLIT_SIZE // "spark.sql.iceberg.split-size"
    val saved = spark.conf.getOption(key)
    def keys(): Seq[Long] = spark.sql(s"SELECT id FROM $table ORDER BY id").collect().toSeq.map(_.getLong(0))
    def rddParts(): Int = spark.sql(s"SELECT * FROM $table").rdd.getNumPartitions
    val expected = (0 until nFiles).map(_.toLong)
    try {
      // (a) The prompt's core path: set spark.sql.iceberg.split-size via spark.conf.set and read the
      //     multi-file table under a large and a tiny split-size — the row set must be invariant.
      spark.conf.set(key, (512L * 1024 * 1024).toString)
      val bigRows = keys(); val bigRdd = rddParts()
      spark.conf.set(key, "1")
      val smallRows = keys(); val smallRdd = rddParts()
      assert(bigRows == expected && smallRows == expected,
        s"[$fmt] split-size must not change the row set: big=$bigRows small=$smallRows expected=$expected")
      assert(smallRdd >= bigRdd,
        s"[$fmt] a smaller split-size must not DECREASE the read RDD partition count: small=$smallRdd big=$bigRdd")

      // (b) DETERMINISTIC observability of the same knob at the planner: with open-file-cost=1 the per-
      //     file planning weight is its byte length, so a split-size below one file combines nothing
      //     (nFiles task groups) while a split-size above the whole table combines everything (1 group).
      val ice = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, table)
      val szKey = org.apache.iceberg.TableProperties.SPLIT_SIZE // "read.split.target-size"
      def planGroups(splitBytes: Long): Int = {
        val it = ice.newScan().option(szKey, splitBytes.toString).planTasks().iterator()
        var n = 0; while (it.hasNext) { it.next(); n += 1 }
        n
      }
      val bigGroups   = planGroups(512L * 1024 * 1024) // one combined group
      val smallGroups = planGroups(1L)                 // one group per file
      assert(bigGroups == 1, s"[$fmt] a split-size above the whole table should plan 1 task group, got $bigGroups")
      assert(smallGroups == fileCount,
        s"[$fmt] a split-size below one file should plan one task group per file ($fileCount), got $smallGroups")

      println(s"DIAG fork.splitSize[$fmt]: key='$key' files=$fileCount rows-correct(big+small)=yes " +
        s"rddParts(big=$bigRdd,small=$smallRdd) plannedTaskGroups(bigSplit=$bigGroups,smallSplit=$smallGroups) " +
        s"(split-size governs read task-group count: 1 vs $fileCount)")
    } finally {
      saved match { case Some(v) => spark.conf.set(key, v); case None => spark.conf.unset(key) }
      spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  // ── #233 (bin-pack by data-file length) — rewrite_data_files compaction ──────────────────────────────
  // The fork's bin-pack rewrite weights data files by their LENGTH (file_size_in_bytes) when packing them
  // into rewrite groups. That weighting is an internal planner detail — not locally observable via SQL — so
  // this is a CHARACTERIZATION: create several UNEVENLY-sized data files, run rewrite_data_files(rewrite-all),
  // assert the row set is preserved, and DIAG the before/after file count + total bytes. × parquet+orc (the
  // compaction decodes + re-encodes file bytes, so the format is not vacuous).
  private def forkBinPackByLength(fmt: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_binpack_$fmt"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(s"CREATE TABLE $table (id bigint, s string) USING $dataSource " +
      s"TBLPROPERTIES ('write.format.default'='$fmt', 'write.distribution-mode'='none')")
    // Unevenly-sized data files: a tiny one, a small one, and a big one.
    spark.sql(s"INSERT INTO $table VALUES (1,'a')")
    spark.sql(s"INSERT INTO $table VALUES (2,'b'),(3,'c')")
    spark.sql(s"INSERT INTO $table SELECT id, repeat('x', 200) FROM range(100, 400)")
    val before = dataFileStats(spark, table)
    assert(before._1 >= 3, s"[$fmt] expected >=3 uneven data files, got ${before._1}")
    val totalRows = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)

    spark.sql(s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}', options => map('rewrite-all', 'true'))")

    val after = dataFileStats(spark, table)
    val totalRows2 = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)
    assert(totalRows2 == totalRows, s"[$fmt] rewrite_data_files changed the row count: $totalRows -> $totalRows2")
    val probe = spark.sql(s"SELECT s FROM $table WHERE id = 1").collect()(0).getString(0)
    assert(probe == "a", s"[$fmt] rewrite altered a row: id=1 s=$probe")

    println(s"DIAG fork.binPackByLength[$fmt]: beforeFiles=${before._1} beforeBytes=${before._2} " +
      s"afterFiles=${after._1} afterBytes=${after._2} rows=$totalRows " +
      s"(bin-pack weights by data-file length; characterization only — rows preserved)")
    spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // ── #189 (budgeted rewrite ordering by file-sequence-number) — rewrite_data_files ─────────────────────
  // The fork's budgeted rewrite ORDERS candidate files by their file-sequence-number when spending a rewrite
  // budget. The ordering decision is metadata-level and NOT locally observable via SQL, and it shares the
  // rewrite_data_files execution path with #233 (fork.binPackByLength) — so rather than duplicate that, this
  // pins the DISTINCT, observable half: the ordering KEY (file_sequence_number, on the .entries metadata
  // table) is exposed and monotonic across commits, and rewrite-all preserves the row set. Ordering is over
  // sequence numbers (not file bytes) => format-vacuous => single format (parquet).
  private def forkCompactionOrder(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_compord"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(s"CREATE TABLE $table (id bigint, s string) USING $dataSource " +
      "TBLPROPERTIES ('write.format.default'='parquet', 'write.distribution-mode'='none')")
    // Several commits => several data files with DISTINCT, increasing file-sequence-numbers (the ordering key).
    val nCommits = 4
    for (i <- 0 until nCommits) spark.sql(s"INSERT INTO $table VALUES (${i}L, 'c$i')")
    val seqs = spark.sql(
      s"SELECT file_sequence_number FROM $table.entries WHERE status != 2 AND data_file.content = 0 " +
      s"ORDER BY file_sequence_number").collect().toSeq.map(_.getLong(0))
    assert(seqs.size >= nCommits, s"expected >= $nCommits live data-file entries with sequence numbers, got ${seqs.size}: $seqs")
    assert(seqs == seqs.sorted, s"file sequence numbers not monotonic: $seqs")
    assert(seqs.distinct.size >= 2, s"expected multiple distinct file sequence numbers (the ordering key), got ${seqs.distinct}")
    val totalRows = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)

    spark.sql(s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}', options => map('rewrite-all', 'true'))")

    val totalRows2 = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)
    assert(totalRows2 == totalRows, s"rewrite changed the row count: $totalRows -> $totalRows2")
    val filesAfter = spark.sql(s"SELECT count(*) FROM $table.data_files").collect()(0).getLong(0)
    val keys = spark.sql(s"SELECT id FROM $table ORDER BY id").collect().toSeq.map(_.getLong(0))
    assert(keys == (0 until nCommits).map(_.toLong), s"rewrite altered the row set: $keys")

    println(s"DIAG fork.compactionOrder: fileSeqNumbers=${seqs.mkString(",")} (ordering key for budgeted rewrite) " +
      s"filesBefore=${seqs.size} filesAfter=$filesAfter rows=$totalRows " +
      s"(ordering is metadata-level/not locally observable; pin: seq-numbers exposed+monotonic, rewrite preserves rows; " +
      s"shares the rewrite path with fork.binPackByLength #233)")
    spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  val forkCases: List[Plan.Case] =
    List(
      Plan.Case(
        "fork.colDefault.addColumnInert @ parquet",
        forkColDefaultAddColumn("parquet")),
      Plan.Case(
        "fork.colDefault.addColumnInert @ orc",
        forkColDefaultAddColumn("orc")),
      Plan.Case(
        "fork.colDefault.apiSerialization @ core",
        forkColDefaultApiSerialization),
      Plan.Case(
        "fork.colDefault.readApplyProbe @ core",
        forkColDefaultReadApplyProbe),
      Plan.Case(
        "fork.partitionDist.default @ parquet",
        forkPartitionDistDefault("parquet")),
      Plan.Case(
        "fork.partitionDist.default @ orc",
        forkPartitionDistDefault("orc")),
      Plan.Case(
        "fork.deleteFileReplication @ mor",
        forkDeleteFileReplication),
      Plan.Case(
        "fork.fileReplicationFactor @ core",
        forkFileReplicationFactor),
      Plan.Case(
        "fork.splitSize @ parquet",
        forkSplitSize("parquet")),
      Plan.Case(
        "fork.splitSize @ orc",
        forkSplitSize("orc")),
      Plan.Case(
        "fork.binPackByLength @ parquet",
        forkBinPackByLength("parquet")),
      Plan.Case(
        "fork.binPackByLength @ orc",
        forkBinPackByLength("orc")),
      Plan.Case(
        "fork.compactionOrder @ parquet",
        forkCompactionOrder))

}

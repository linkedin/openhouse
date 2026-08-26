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

  // Column-default DDL path, format-parameterized.
  // ALTER TABLE ... ADD COLUMN c int DEFAULT 5 is accepted at Spark parse time, but the connector does
  // not wire the default into the write path: the default value is not written into the Iceberg schema,
  // pre-existing rows read null for the new column, and an INSERT that omits the column is rejected
  // with INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA because there is no default to fill it in with.
  private def forkColDefaultAddColumn(fmt: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_coldef_$fmt"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(s"CREATE TABLE $table (id bigint, s string) USING $dataSource TBLPROPERTIES ('write.format.default'='$fmt')")
    spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")

    // (1) The DDL is accepted at parse time; Spark owns the DEFAULT grammar.
    spark.sql(s"ALTER TABLE $table ADD COLUMN c int DEFAULT 5")

    // (2) The default is not written into the persisted schema; column c has no default metadata.
    val cDesc = spark.sql(s"DESCRIBE TABLE EXTENDED $table").collect()
                  .map(_.mkString("|")).filter(_.matches("(?i)^c\\|.*")).mkString(" ;; ")
    assert(!cDesc.toLowerCase.contains("default") && !cDesc.contains("5"),
      s"[$fmt] expected no default persisted for c, but DESCRIBE shows: $cDesc")

    // (3) The default is not backfilled on read; pre-existing rows read null, not 5.
    val nulls = spark.sql(s"SELECT count(*) FROM $table WHERE c IS NULL").collect()(0).getLong(0)
    assert(nulls == 2,
      s"[$fmt] expected the default not applied on read (2 nulls), got $nulls")

    // (4) The default is not applied on write; an insert that omits c is rejected.
    val omit = Check.intercept[org.apache.spark.sql.AnalysisException] {
      spark.sql(s"INSERT INTO $table (id, s) VALUES (3, 'c')")
    }
    val omitMsg = Exceptions.causeChain(omit).flatMap(e => Option(e.getMessage)).mkString(" | ")
    assert(omitMsg.contains("CANNOT_FIND_DATA"),
      s"[$fmt] expected omit-insert rejected with CANNOT_FIND_DATA, got: $omitMsg")

    println(s"fork.colDefault[$fmt]: accepted=yes persistedDefault=no readBackfill=no writeApply=no(CANNOT_FIND_DATA)")
    spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // Column-default API serialization at the schema level.
  // NestedField carries initial-default and write-default, and SchemaParser serializes them into the
  // schema JSON. toJson takes no format-version parameter, so the key serializes the same regardless of
  // the table's format version. This runs against either artifact through reflection, since the builder
  // API does not exist in every Iceberg release jar and a direct reference would fail to compile there:
  // when NestedField.builder() is absent, the test records that the column-default API is unsupported;
  // when it is present, the test builds a defaulted field, confirms SchemaParser emits initial-default,
  // and confirms the value survives a fromJson then toJson round trip.
  private def forkColDefaultApiSerialization(ctx: Ctx): Unit = {
    val nestedFieldCls = Class.forName("org.apache.iceberg.types.Types$NestedField")
    val builderM = scala.util.Try(nestedFieldCls.getMethod("builder"))
    if (builderM.isFailure) {
      // The column-default API is absent on this artifact; assert that absence is total.
      println("fork.colDefault.api: NestedField.builder absent, column-default API unsupported on this artifact")
      val ms = nestedFieldCls.getMethods.map(_.getName).toSet
      assert(!ms.contains("initialDefault") && !ms.contains("writeDefault"),
        "NestedField exposes initial/write-default accessors but no builder()")
      return
    }
    // The column-default API is present; build `optional int c` carrying initial-default=5.
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

    // Assemble a schema [id, c(default=5)] and serialize it; no format version is passed to toJson.
    val idField = org.apache.iceberg.types.Types.NestedField.required(
      1, "id", org.apache.iceberg.types.Types.LongType.get())
    val schema = new org.apache.iceberg.Schema(java.util.Arrays.asList(idField, field))
    val json = org.apache.iceberg.SchemaParser.toJson(schema)
    println(s"fork.colDefault.api: column-default API present, serialized schema JSON = $json")

    // (a) The default is serialized into the schema JSON.
    assert(json.contains("initial-default"),
      s"expected SchemaParser to serialize 'initial-default' into the schema JSON, got: $json")
    // (b) toJson takes no format-version argument, so the key serializes the same regardless of format version.
    // (c) The value round-trips through fromJson then toJson.
    val reparsed = org.apache.iceberg.SchemaParser.fromJson(json)
    val json2 = org.apache.iceberg.SchemaParser.toJson(reparsed)
    assert(json2.contains("initial-default"),
      s"expected 'initial-default' to survive the fromJson/toJson round trip, got: $json2")
    println("fork.colDefault.api: initial-default serialized with no format-version argument and round-trips")
  }

  // Reflectively builds an optional int NestedField carrying initial-default=dflt.
  // Returns None when the builder API is absent so callers can assert that absence directly.
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

  // Column-default persistence versus read-apply, over data files written before the default existed.
  // A schema evolution that adds a defaulted column is committed directly through the low-level
  // TableMetadata API, since the public UpdateSchema surface has no set-default operation. The test
  // asserts the one deterministic half of this behavior: the default value persists into the committed
  // schema. What the OSS Spark read path returns for pre-existing rows over that defaulted column is not
  // part of this connector's documented read contract, so that value is recorded for reference rather
  // than asserted.
  private def forkColDefaultReadApplyProbe(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val nfCls = Class.forName("org.apache.iceberg.types.Types$NestedField")
    val apiPresent = scala.util.Try(nfCls.getMethod("builder")).isSuccess
    if (!apiPresent) {
      // No builder API means there is no way to set a default, so assert that absence directly.
      println("fork.colDefault.readApplyProbe: column-default builder API is absent, nothing to probe")
      assert(!nfCls.getMethods.map(_.getName).toSet.contains("initialDefault"),
        "NestedField exposes initialDefault but builder() is absent")
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
    spark.sql(s"INSERT INTO $t VALUES (1),(2)") // data files physically contain only id

    // Evolve the schema to [id, c int DEFAULT 5] directly through TableMetadata.
    val table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, t)
    val cur   = table.schema()
    val nextId = cur.highestFieldId() + 1
    val cField = buildDefaultedIntField(nextId, "c", 5).getOrElse(
      throw new AssertionError("builder API present but field build failed"))
    val cols = new java.util.ArrayList[org.apache.iceberg.types.Types.NestedField](cur.columns())
    cols.add(cField)
    val s2 = new org.apache.iceberg.Schema(cols)
    val ops = table.asInstanceOf[org.apache.iceberg.HasTableOperations].operations()
    val base = ops.current()
    val updated = org.apache.iceberg.TableMetadata.buildFrom(base).setCurrentSchema(s2, s2.highestFieldId()).build()
    ops.commit(base, updated)

    // The default persists into the committed schema.
    val persisted = org.apache.iceberg.SchemaParser.toJson(
      org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, t).schema())
    assert(persisted.contains("initial-default"),
      s"expected initial-default to persist into the committed schema, got: $persisted")

    // Recorded for reference only: the read path's treatment of the defaulted column over old files is
    // not part of this connector's documented contract.
    spark.sql(s"REFRESH TABLE $t")
    val vals = spark.sql(s"SELECT c FROM $t ORDER BY id").collect()
                 .map(r => if (r.isNullAt(0)) "NULL" else r.getInt(0).toString)
    println(s"fork.colDefault.readApplyProbe: read of defaulted column over pre-existing rows = " +
            s"[${vals.mkString(",")}] (recorded for reference, not asserted)")
    spark.sql(s"DROP TABLE IF EXISTS $t")
  }

  // Partitioned write distribution default, format-parameterized.
  // The connector defaults write.distribution-mode to NONE for partitioned writes. With HASH, the
  // writer shuffles rows so each partition is written by a
  // single task, producing roughly one data file per partition. With NONE, no shuffle happens, so every
  // input task writes every partition it holds, producing up to (input tasks times partitions) files.
  // This test appends the same multi-task DataFrame into a 4-partition table twice, once under the
  // default and once under an explicit hash distribution, and compares the resulting data file counts.
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
    println(s"fork.partitionDist[$fmt]: defaultFiles=$nDefault hashFiles=$nHash (parts=$nParts tasks=$nTasks)")
    // Explicit hash clusters by partition, with slack for spill.
    assert(nHash <= nParts * 2,
      s"[$fmt] write.distribution-mode=hash should cluster to about $nParts files, got $nHash")
    assert(nDefault > nHash,
      s"[$fmt] expected the default distribution mode to produce more files than hash " +
        s"(default=$nDefault hash=$nHash)")
  }

  // (count, sumBytes) of the current data files, used by the compaction tests below.
  private def dataFileStats(spark: SparkSession, table: String): (Long, Long) = {
    val r = spark.sql(s"SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM $table.data_files").collect()(0)
    (r.getLong(0), r.getLong(1))
  }

  // Output-file replication factor at the OutputFileFactory level.
  // The property key that OutputFileFactory stamps into the per-output-file property map is
  // FILE_REPLICATION_FACTOR, "file-replication-factor". It is not a settable table property; it is the
  // key HDFS reads to set block replication on an output file when a replication factor is supplied to
  // the factory. Only the delete-file write path feeds a replication factor to the factory; data-file
  // factories never set it. This test builds a factory with an explicit replication factor and asserts
  // the exact key it stamps into the output-file property map, then confirms writes still succeed and
  // return correct rows afterward. Reflection is used because the builder method and getProperties are
  // not part of the public compiled API on every Iceberg artifact this test runs against.
  private def forkFileReplicationFactor(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val offCls = Class.forName("org.apache.iceberg.io.OutputFileFactory")

    // (1) Assert the exact key string.
    val keyFieldT = scala.util.Try(offCls.getField("FILE_REPLICATION_FACTOR"))
    assert(keyFieldT.isSuccess, "OutputFileFactory.FILE_REPLICATION_FACTOR is absent")
    val key = keyFieldT.get.get(null).asInstanceOf[String]
    assert(key == "file-replication-factor",
      s"""expected FILE_REPLICATION_FACTOR to equal "file-replication-factor", got "$key"""")

    // Need a real Iceberg Table to build a factory.
    val table = s"${ctx.namespace}.t_filerepl"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(s"CREATE TABLE $table (id bigint, s string) USING $dataSource TBLPROPERTIES ('format-version'='2')")
    spark.sql(s"INSERT INTO $table VALUES (1,'a'),(2,'b')")
    val icebergTable = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, table)

    // (2) Build an OutputFileFactory carrying replicationFactor=2.
    val builder = offCls.getMethod("builderFor", classOf[org.apache.iceberg.Table], java.lang.Integer.TYPE, java.lang.Long.TYPE)
      .invoke(null, icebergTable, java.lang.Integer.valueOf(1), java.lang.Long.valueOf(1L))
    val replMT = scala.util.Try(builder.getClass.getMethod("replicationFactor", java.lang.Short.TYPE))
    assert(replMT.isSuccess, "OutputFileFactory.Builder.replicationFactor(short) is absent")
    replMT.get.invoke(builder, java.lang.Short.valueOf(2.toShort))
    val factory = Option(builder.getClass.getMethod("build").invoke(builder))
      .getOrElse(throw new AssertionError("OutputFileFactory build returned null"))

    // (3) The factory stamps FILE_REPLICATION_FACTOR -> "2" into the per-output-file property map.
    val gp = offCls.getDeclaredMethod("getProperties"); gp.setAccessible(true)
    val props = gp.invoke(factory).asInstanceOf[java.util.Map[String, String]]
    assert(props.get(key) == "2",
      s"expected output-file property $key=2 stamped by the factory, got ${props.get(key)}")

    // (4) Writes still succeed and rows are correct.
    spark.sql(s"INSERT INTO $table VALUES (3,'c')")
    val rows = spark.sql(s"SELECT id FROM $table ORDER BY id").collect().toSeq.map(_.getLong(0))
    assert(rows == Seq(1L, 2L, 3L), s"rows wrong after write: $rows")

    println(s"fork.fileReplicationFactor: key='$key'; factory stamps $key=${props.get(key)} into output-file props; " +
      s"writes ok rows=${rows.mkString(",")}")
    spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // Spark read split size, format-parameterized.
  // spark.sql.iceberg.split-size controls how the read path combines or splits data files into read
  // tasks. With several small files, a large split size combines them into fewer read tasks and a tiny
  // split size splits them into more, visible through rdd.getNumPartitions, while the row set stays
  // invariant. This test also checks the same knob at the planner level directly.
  private def forkSplitSize(fmt: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_splitsize_$fmt"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    // distribution=none plus several separate inserts produces several distinct data files. An
    // open-file-cost of 1 sets each file's planning weight to its byte length, making split-size the
    // knob that governs task-group count.
    spark.sql(s"CREATE TABLE $table (id bigint, s string) USING $dataSource " +
      s"TBLPROPERTIES ('write.format.default'='$fmt', 'write.distribution-mode'='none', 'read.split.open-file-cost'='1')")
    val numberOfFiles = 6
    (0 until numberOfFiles).foreach { fileIndex =>
      spark.sql(s"INSERT INTO $table SELECT ${fileIndex}L, repeat('r$fileIndex', 4000)")
    }
    val fileCount = spark.sql(s"SELECT count(*) FROM $table.data_files").collect()(0).getLong(0)
    assert(fileCount >= 2, s"[$fmt] expected multiple data files for a split test, got $fileCount")

    val key = org.apache.iceberg.spark.SparkSQLProperties.SPLIT_SIZE // "spark.sql.iceberg.split-size"
    val saved = spark.conf.getOption(key)
    def keys(): Seq[Long] = spark.sql(s"SELECT id FROM $table ORDER BY id").collect().toSeq.map(_.getLong(0))
    def rddParts(): Int = spark.sql(s"SELECT * FROM $table").rdd.getNumPartitions
    val expected = (0 until numberOfFiles).map(_.toLong)
    try {
      // (a) Set spark.sql.iceberg.split-size directly and read the multi-file table under a large and a
      //     tiny split size; the row set must be invariant either way.
      spark.conf.set(key, (512L * 1024 * 1024).toString)
      val bigRows = keys(); val bigRdd = rddParts()
      spark.conf.set(key, "1")
      val smallRows = keys(); val smallRdd = rddParts()
      assert(bigRows == expected && smallRows == expected,
        s"[$fmt] split-size must not change the row set: big=$bigRows small=$smallRows expected=$expected")
      assert(smallRdd >= bigRdd,
        s"[$fmt] a smaller split-size must not decrease the read RDD partition count: small=$smallRdd big=$bigRdd")

      // (b) The same knob checked directly at the planner: with open-file-cost=1, each file's planning
      //     weight is its byte length, so a split-size below one file combines nothing (one task group
      //     per file) while a split-size above the whole table combines everything into one group.
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

      println(s"fork.splitSize[$fmt]: key='$key' files=$fileCount " +
        s"rddParts(big=$bigRdd,small=$smallRdd) plannedTaskGroups(bigSplit=$bigGroups,smallSplit=$smallGroups)")
    } finally {
      saved match { case Some(v) => spark.conf.set(key, v); case None => spark.conf.unset(key) }
      spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  // Bin-pack compaction weighted by data-file length.
  // rewrite_data_files packs data files into rewrite groups weighted by file length. The weighting
  // decision itself is an internal planner detail with no local SQL surface, so this test observes what
  // is externally checkable: compacting a table with unevenly sized data files through
  // rewrite_data_files preserves both the row count and every row's value.
  private def forkBinPackByLength(fmt: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_binpack_$fmt"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(s"CREATE TABLE $table (id bigint, s string) USING $dataSource " +
      s"TBLPROPERTIES ('write.format.default'='$fmt', 'write.distribution-mode'='none')")
    // Unevenly sized data files: a tiny one, a small one, and a big one.
    spark.sql(s"INSERT INTO $table VALUES (1,'a')")
    spark.sql(s"INSERT INTO $table VALUES (2,'b'),(3,'c')")
    spark.sql(s"INSERT INTO $table SELECT id, repeat('x', 200) FROM range(100, 400)")
    val before = dataFileStats(spark, table)
    assert(before._1 >= 3, s"[$fmt] expected at least 3 uneven data files, got ${before._1}")
    val totalRows = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)

    spark.sql(s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}', options => map('rewrite-all', 'true'))")

    val after = dataFileStats(spark, table)
    val totalRows2 = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)
    assert(totalRows2 == totalRows, s"[$fmt] rewrite_data_files changed the row count: $totalRows -> $totalRows2")
    val probe = spark.sql(s"SELECT s FROM $table WHERE id = 1").collect()(0).getString(0)
    assert(probe == "a", s"[$fmt] rewrite altered a row: id=1 s=$probe")

    println(s"fork.binPackByLength[$fmt]: beforeFiles=${before._1} beforeBytes=${before._2} " +
      s"afterFiles=${after._1} afterBytes=${after._2} rows=$totalRows")
    spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  // Budgeted rewrite ordering by file-sequence-number.
  // A budgeted rewrite orders candidate files by file-sequence-number when spending its rewrite budget.
  // That ordering decision is metadata-level with no local SQL surface, and it shares its execution path
  // with the bin-pack compaction test above, so this test checks the distinct, externally observable
  // half: the ordering key, file_sequence_number on the entries metadata table, is exposed and increases
  // monotonically across commits, and rewrite_data_files with rewrite-all preserves the row set. The
  // Sequence numbers define the ordering, so a single format is sufficient here.
  private def forkCompactionOrder(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_compord"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(s"CREATE TABLE $table (id bigint, s string) USING $dataSource " +
      "TBLPROPERTIES ('write.format.default'='parquet', 'write.distribution-mode'='none')")
    // Several commits produce several data files with distinct, increasing file-sequence-numbers.
    val numberOfCommits = 4
    (0 until numberOfCommits).foreach { commitIndex =>
      spark.sql(s"INSERT INTO $table VALUES (${commitIndex}L, 'c$commitIndex')")
    }
    val seqs = spark.sql(
      s"SELECT file_sequence_number FROM $table.entries WHERE status != 2 AND data_file.content = 0 " +
      s"ORDER BY file_sequence_number").collect().toSeq.map(_.getLong(0))
    assert(
      seqs.size >= numberOfCommits,
      s"expected at least $numberOfCommits live data-file entries with sequence numbers, got ${seqs.size}: $seqs")
    assert(seqs == seqs.sorted, s"file sequence numbers not monotonic: $seqs")
    assert(seqs.distinct.size >= 2, s"expected multiple distinct file sequence numbers, got ${seqs.distinct}")
    val totalRows = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)

    spark.sql(s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}', options => map('rewrite-all', 'true'))")

    val totalRowsAfter = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)
    assert(totalRowsAfter == totalRows, s"rewrite changed the row count: $totalRows -> $totalRowsAfter")
    val filesAfter = spark.sql(s"SELECT count(*) FROM $table.data_files").collect()(0).getLong(0)
    val keys = spark.sql(s"SELECT id FROM $table ORDER BY id").collect().toSeq.map(_.getLong(0))
    assert(keys == (0 until numberOfCommits).map(_.toLong), s"rewrite altered the row set: $keys")

    println(s"fork.compactionOrder: fileSeqNumbers=${seqs.mkString(",")} filesBefore=${seqs.size} " +
      s"filesAfter=$filesAfter rows=$totalRows")
    spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  val forkColumnDefaultAndDistributionCases: List[Plan.Case] =
    List(
      Plan.Case(
        "fork.colDefault.addColumnInert @ parquet",
        forkColDefaultAddColumn("parquet"),
        description = "ALTER TABLE ADD COLUMN ... DEFAULT is accepted on a parquet table, but the " +
          "default is not written into the schema, pre-existing rows read null for it, and an insert " +
          "that omits the column is rejected."),
      Plan.Case(
        "fork.colDefault.addColumnInert @ orc",
        forkColDefaultAddColumn("orc"),
        description = "ALTER TABLE ADD COLUMN ... DEFAULT is accepted on an orc table, but the " +
          "default is not written into the schema, pre-existing rows read null for it, and an insert " +
          "that omits the column is rejected."),
      Plan.Case(
        "fork.colDefault.apiSerialization @ core",
        forkColDefaultApiSerialization,
        description = "A NestedField built with an initial default serializes 'initial-default' into " +
          "the schema JSON and the value survives a fromJson/toJson round trip, on a build that carries " +
          "the column-default API."),
      Plan.Case(
        "fork.colDefault.readApplyProbe @ core",
        forkColDefaultReadApplyProbe,
        description = "A column default added after existing data files persists into the committed " +
          "schema. The read path's returned value for pre-existing rows over that column is recorded " +
          "for reference, since it is not part of this connector's documented read contract."),
      Plan.Case(
        "fork.partitionDist.default @ parquet",
        forkPartitionDistDefault("parquet"),
        description = "Appending the same multi-task write to a 4-way partitioned parquet table " +
          "produces at least as many data files under the default write distribution mode as under an " +
          "explicit hash distribution, and hash distribution clusters to about one file per partition."),
      Plan.Case(
        "fork.partitionDist.default @ orc",
        forkPartitionDistDefault("orc"),
        description = "Appending the same multi-task write to a 4-way partitioned orc table produces " +
          "at least as many data files under the default write distribution mode as under an explicit " +
          "hash distribution, and hash distribution clusters to about one file per partition."))

  // The fork cases are two contribution lists. One more fork entry sits between them in the
  // catalog; the layer that owns that entry supplies it and Plan keeps the order.
  val forkFileAndCompactionCases: List[Plan.Case] =
    List(
      Plan.Case(
        "fork.fileReplicationFactor @ core",
        forkFileReplicationFactor,
        description = "OutputFileFactory exposes the key 'file-replication-factor', a factory built " +
          "with replication factor 2 stamps that key into its output-file properties, and writes made " +
          "through the table afterward still produce the correct rows."),
      Plan.Case(
        "fork.splitSize @ parquet",
        forkSplitSize("parquet"),
        description = "Reading a multi-file parquet table under a large spark.sql.iceberg.split-size " +
          "and a tiny one returns the same rows both times, and the tiny split size does not decrease " +
          "the read task count relative to the large one."),
      Plan.Case(
        "fork.splitSize @ orc",
        forkSplitSize("orc"),
        description = "Reading a multi-file orc table under a large spark.sql.iceberg.split-size and " +
          "a tiny one returns the same rows both times, and the tiny split size does not decrease the " +
          "read task count relative to the large one."),
      Plan.Case(
        "fork.binPackByLength @ parquet",
        forkBinPackByLength("parquet"),
        description = "Compacting a parquet table with unevenly sized data files through " +
          "rewrite_data_files preserves the row count and every row's value."),
      Plan.Case(
        "fork.binPackByLength @ orc",
        forkBinPackByLength("orc"),
        description = "Compacting an orc table with unevenly sized data files through " +
          "rewrite_data_files preserves the row count and every row's value."),
      Plan.Case(
        "fork.compactionOrder @ parquet",
        forkCompactionOrder,
        description = "File sequence numbers on live data-file entries are exposed and increase " +
          "monotonically across commits, and rewrite_data_files with rewrite-all preserves the row " +
          "count and the row set."))

}

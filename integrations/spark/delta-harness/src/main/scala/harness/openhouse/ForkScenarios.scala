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

// The fork cases pin behavior decided by LinkedIn's fork of Apache Iceberg, the com.linkedin.iceberg artifacts this
// module depends on: the column-default path, the write distribution default for a partitioned write, the output-file
// replication key, the read split size, and the compaction plan. These behaviors have no catalog SQL surface of their
// own, so a case reaches them through the Iceberg API or a Spark configuration and asserts the result a caller can
// observe.
trait ForkScenarios extends ScenarioKit {
  import Rows._

  /**
   * ALTER TABLE ADD COLUMN c int DEFAULT 5 parses, and the default value stops at the parser: the committed schema
   * records no default for c, pre-existing rows read null for it, and an INSERT that omits c is rejected with
   * INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA. The file format is the parameter.
   */
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

  /**
   * A NestedField built with an initial default serializes initial-default into the schema JSON, and that value
   * survives a fromJson then toJson round trip. SchemaParser.toJson takes no format-version parameter, so the key
   * serializes the same at every format version. On an artifact whose NestedField exposes no builder, the
   * column-default API is absent entirely, down to the initialDefault and writeDefault accessors, and the case pins
   * that absence. Reflection reaches the builder because some Iceberg release jars leave it out, which a direct
   * reference would fail to compile against.
   */
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
    // (b) toJson takes no format-version argument, so the key serializes the same regardless of format version. (c) The
    // value round-trips through fromJson then toJson.
    val reparsed = org.apache.iceberg.SchemaParser.fromJson(json)
    val json2 = org.apache.iceberg.SchemaParser.toJson(reparsed)
    assert(json2.contains("initial-default"),
      s"expected 'initial-default' to survive the fromJson/toJson round trip, got: $json2")
    println("fork.colDefault.api: initial-default serialized with no format-version argument and round-trips")
  }

  /**
   * Reflectively builds an optional int NestedField carrying the given initial default. Returns None when the builder
   * API is absent, so a caller can assert that absence directly.
   */
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

  /**
   * A column default added after data files exist persists into the committed schema. The schema evolution goes through
   * the low-level TableMetadata API because the public UpdateSchema surface has no set-default operation. The
   * documented read contract covers schema persistence only. The case prints the OSS Spark read result for pre-existing
   * rows as diagnostic output, while its assertions stop at the persisted schema.
   */
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

    // Recorded for reference only: the read path's treatment of the defaulted column over old files is not part of this
    // connector's documented contract.
    spark.sql(s"REFRESH TABLE $t")
    val vals = spark.sql(s"SELECT c FROM $t ORDER BY id").collect()
                 .map(r => if (r.isNullAt(0)) "NULL" else r.getInt(0).toString)
    println(s"fork.colDefault.readApplyProbe: read of defaulted column over pre-existing rows = " +
            s"[${vals.mkString(",")}] (recorded for reference, not asserted)")
    spark.sql(s"DROP TABLE IF EXISTS $t")
  }

  /**
   * A partitioned write defaults write.distribution-mode to NONE, so every input task writes every partition it holds
   * and one append produces up to (input tasks times partitions) data files. Under an explicit HASH distribution the
   * writer shuffles rows so one task owns each partition, clustering the append to roughly one file per partition.
   * Appending the same multi-task DataFrame into a 4-partition table under each mode therefore yields at least as many
   * files under the default as under HASH. The file format is the parameter.
   */
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

  /** Returns the count and the total byte size of the table's current data files. */
  private def dataFileStats(spark: SparkSession, table: String): (Long, Long) = {
    val r = spark.sql(s"SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM $table.data_files").collect()(0)
    (r.getLong(0), r.getLong(1))
  }

  /**
   * OutputFileFactory exposes FILE_REPLICATION_FACTOR as "file-replication-factor", and a factory built with a
   * replication factor stamps that key into the property map of the output files it creates. Writes made through the
   * table afterward still return the correct rows. It is not a settable table property; it is the key HDFS reads to set
   * block replication on an output file when a replication factor is supplied to the factory, and the delete-file write
   * path is the one path that supplies one. Reflection reaches the builder and getProperties because some Iceberg
   * artifacts leave them out of the public compiled API.
   */
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

  /**
   * spark.sql.iceberg.split-size decides how the read path combines data files into read tasks. Over several small
   * files, a large split size combines them into fewer read tasks and a tiny split size splits them into more, visible
   * through rdd.getNumPartitions, and both reads return the same rows. The planner shows the same effect directly: a
   * split size above the whole table plans one task group, and a split size below one file plans one group per file.
   * The file format is the parameter.
   */
  private def forkSplitSize(fmt: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = s"${ctx.namespace}.t_splitsize_$fmt"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    // distribution=none plus several separate inserts produces several distinct data files. An open-file-cost of 1 sets
    // each file's planning weight to its byte length, making split-size the knob that governs task-group count.
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
      // (a) Set spark.sql.iceberg.split-size directly and read the multi-file table under a large and a tiny split
      // size; the row set must be invariant either way.
      spark.conf.set(key, (512L * 1024 * 1024).toString)
      val bigRows = keys(); val bigRdd = rddParts()
      spark.conf.set(key, "1")
      val smallRows = keys(); val smallRdd = rddParts()
      assert(bigRows == expected && smallRows == expected,
        s"[$fmt] split-size must not change the row set: big=$bigRows small=$smallRows expected=$expected")
      assert(smallRdd >= bigRdd,
        s"[$fmt] a smaller split-size must not decrease the read RDD partition count: small=$smallRdd big=$bigRdd")

      // (b) The same knob checked directly at the planner: with open-file-cost=1, each file's planning weight is its
      // byte length, so a split-size below one file combines nothing (one task group per file) while a split-size above
      // the whole table combines everything into one group.
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

  /**
   * rewrite_data_files packs data files into rewrite groups weighted by file length. Compacting a table whose data
   * files are unevenly sized preserves the row count and every row's value, which is the observable result of that
   * packing; the weighting itself is a planner decision that no SQL surface exposes. The file format is the parameter.
   */
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

  /**
   * file_sequence_number is exposed on the live data-file entries of the entries metadata table and increases
   * monotonically across commits, and rewrite_data_files with rewrite-all preserves the row count and the row set. A
   * budgeted rewrite spends its budget in file-sequence-number order, so that column is the observable half of the
   * ordering decision. Sequence numbers order commits the same way in every file format, so parquet alone covers this
   * behavior.
   */
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

  /** The column-default and write-distribution fork cases. */
  val forkColumnDefaultAndDistributionCases: List[Plan.Case] =
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
        forkPartitionDistDefault("orc")))

  /**
   * The output-file, split-size and compaction fork cases. They are the second of two fork contribution lists: one more
   * fork entry sits between the two in the catalog, supplied by the layer that owns it, and Plan keeps that order.
   */
  val forkFileAndCompactionCases: List[Plan.Case] =
    List(
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

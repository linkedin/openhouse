// Replication PoC — runs in spark-shell against the local OpenHouse catalog (`openhouse`).
//   docker exec local.spark-master /var/config/run_replicate.sh </dev/null
//
// P0/P1: eager FULL-HISTORY physical replication with rename.
//  - floor (first replayed snapshot onto empty dest) = full materialization
//  - subsequent snapshots = append delta (addedDataFiles)  [P0 = append-only]
//  - each dest snapshot stamps `source-snapshot-id` in its summary (per-snapshot recovery + reconcile)
//  - reconcile/idempotency: skip source snapshots already present in dest summaries
//  - oracle: for every source snapshot i, dest AS OF (mapped) == source AS OF i  (multiset)

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.iceberg.{DataFile, DataFiles, Snapshot, Table}
import org.apache.iceberg.spark.Spark3Util
import scala.collection.JavaConverters._

val DB  = "db"
val SRC = s"openhouse.$DB.p0_append"
val DST = s"openhouse.$DB.p0_replica"

def loadTbl(n: String): Table = Spark3Util.loadIcebergTable(spark, n)
def tableExists(n: String): Boolean = try { loadTbl(n); true } catch { case _: Throwable => false }

// committed history, oldest -> newest (walk parentId from current)
def chainOf(t: Table): List[Snapshot] = {
  var out = List.empty[Snapshot]; var cur = t.currentSnapshot()
  while (cur != null) { out = cur :: out; val p = cur.parentId(); cur = if (p != null) t.snapshot(p) else null }
  out
}
def rowsAsOf(tbl: String, snapId: Long): List[String] =
  spark.read.format("iceberg").option("snapshot-id", snapId).load(tbl).collect().map(_.toString).sorted.toList

// copy one source data file to a remapped path under dest's location; relocate the DataFile (metrics carried)
def relocate(dst: Table, df: DataFile, hconf: org.apache.hadoop.conf.Configuration): DataFile = {
  val srcPath = new Path(df.path().toString)
  val dstPath = new Path(s"${dst.location()}/data/${srcPath.getName}")
  val dstFs = dstPath.getFileSystem(hconf); dstFs.mkdirs(dstPath.getParent)
  FileUtil.copy(srcPath.getFileSystem(hconf), srcPath, dstFs, dstPath, false, true, hconf)
  DataFiles.builder(dst.spec()).copy(df).withPath(dstPath.toString).build()
}

def replicateTable(source: String, dest: String): Unit = {
  val src = loadTbl(source)
  if (!tableExists(dest)) {
    spark.sql(s"CREATE TABLE $dest (${spark.table(source).schema.toDDL}) USING iceberg")
    println(s"[repl] created dest $dest")
  }
  val already = loadTbl(dest).snapshots().asScala
    .flatMap(s => Option(s.summary().get("source-snapshot-id"))).map(_.toLong).toSet
  val chain = chainOf(src)
  val toReplay = chain.filter(s => !already.contains(s.snapshotId))
  println(s"[repl] chain=${chain.map(_.snapshotId).mkString(",")} already=${already.size} toReplay=${toReplay.size}")
  var bootstrapped = already.nonEmpty
  val hconf = spark.sessionState.newHadoopConf()
  for (s <- toReplay) {
    val dst = loadTbl(dest)
    val files: List[DataFile] =
      if (!bootstrapped) src.newScan().useSnapshot(s.snapshotId).planFiles().iterator().asScala.map(_.file()).toList
      else s.addedDataFiles(src.io()).asScala.toList
    bootstrapped = true
    val relocated = files.map(df => relocate(dst, df, hconf))
    val ap = dst.newAppend(); relocated.foreach(ap.appendFile)
    ap.set("source-snapshot-id", s.snapshotId.toString); ap.set("source-table", source)
    ap.commit()
    println(s"[repl]   replayed src snap ${s.snapshotId}: +${relocated.size} files")
  }
}

def oracle(source: String, dest: String): Boolean = {
  val src = loadTbl(source)
  val map = loadTbl(dest).snapshots().asScala
    .flatMap(s => Option(s.summary().get("source-snapshot-id")).map(_.toLong -> s.snapshotId)).toMap
  var allOk = true
  for (s <- chainOf(src)) {
    val srcRows = rowsAsOf(source, s.snapshotId)
    val ok = map.get(s.snapshotId).exists(d => rowsAsOf(dest, d) == srcRows)
    allOk &= ok
    println(f"[oracle] src ${s.snapshotId} -> dest ${map.get(s.snapshotId).getOrElse("MISSING")}: ${if (ok) "PASS" else "FAIL"} (${srcRows.size} rows)")
  }
  println(s"[oracle] OVERALL: ${if (allOk) "PASS" else "FAIL"}")
  allOk
}

// ---- scenario: P0 full history + P1 idempotency + P1 incremental ---------------------------------
spark.sql(s"DROP TABLE IF EXISTS $DST")
spark.sql(s"DROP TABLE IF EXISTS $SRC")
spark.sql(s"CREATE TABLE $SRC (id INT, data STRING) USING iceberg")
spark.sql(s"INSERT INTO $SRC VALUES (1,'a'),(2,'b')")   // snap 1 (genesis)
spark.sql(s"INSERT INTO $SRC VALUES (3,'c')")            // snap 2
spark.sql(s"INSERT INTO $SRC VALUES (4,'d'),(5,'e')")    // snap 3

println("\n[P0] bootstrap full-history replicate")
replicateTable(SRC, DST)
val p0 = oracle(SRC, DST)

println("\n[P1] idempotent re-run (expect toReplay=0)")
replicateTable(SRC, DST)
val idem = oracle(SRC, DST)

println("\n[P1] incremental: add snap 4, replicate")
spark.sql(s"INSERT INTO $SRC VALUES (6,'f')")            // snap 4
replicateTable(SRC, DST)
val inc = oracle(SRC, DST)

println(s"\n[SUMMARY] P0=$p0  idempotent=$idem  incremental=$inc")

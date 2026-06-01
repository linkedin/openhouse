// Replication PoC — runs in spark-shell against the local OpenHouse catalog (`openhouse`).
// Loaded via: docker exec local.spark-master /var/config/run_replicate.sh
//
// STATUS: SPIKE first (gate). Full `replicateTable` is outlined at the bottom as the next work.
// The spike answers the one load-bearing unknown: does OpenHouse accept a committed snapshot whose
// DataFiles point at paths WE placed (relocated, metrics carried), via the normal Iceberg commit path?

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.iceberg.{DataFile, DataFiles, Table}
import org.apache.iceberg.spark.Spark3Util
import scala.collection.JavaConverters._

val DB = "db"
val SRC = s"openhouse.$DB.src_spike"
val DST = s"openhouse.$DB.dst_spike"

def loadTbl(name: String): Table = Spark3Util.loadIcebergTable(spark, name)

// ---- spike ---------------------------------------------------------------------------------------
def spike(): Unit = {
  // fresh state each run (reset without tearing down the stack)
  spark.sql(s"DROP TABLE IF EXISTS $DST")
  spark.sql(s"DROP TABLE IF EXISTS $SRC")

  spark.sql(s"CREATE TABLE $SRC (id INT, data STRING) USING iceberg")
  spark.sql(s"INSERT INTO $SRC VALUES (1,'a'),(2,'b'),(3,'c')")

  val src = loadTbl(SRC)
  val srcSnap = src.currentSnapshot()
  val srcFiles: List[DataFile] = src.newScan().planFiles().iterator().asScala.map(_.file()).toList
  println(s"[spike] source snapshot=${srcSnap.snapshotId} dataFiles=${srcFiles.size}")

  // empty dest with the same schema (rename: different table id)
  spark.sql(s"CREATE TABLE $DST (id INT, data STRING) USING iceberg")
  val dst = loadTbl(DST)
  val hconf = spark.sessionState.newHadoopConf()

  // copy each source data file to a remapped path under the dest table location; relocate the DataFile
  val relocated: List[DataFile] = srcFiles.map { df =>
    val srcPath = new Path(df.path().toString)
    val fileName = srcPath.getName
    val dstPath = new Path(s"${dst.location()}/data/$fileName")
    val srcFs = srcPath.getFileSystem(hconf)
    val dstFs = dstPath.getFileSystem(hconf)
    dstFs.mkdirs(dstPath.getParent)
    FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, true, hconf)
    println(s"[spike] copied $srcPath -> $dstPath (${df.fileSizeInBytes} bytes, ${df.recordCount} recs)")
    // carry ALL metrics from the source DataFile; only the path changes
    DataFiles.builder(dst.spec()).copy(df).withPath(dstPath.toString).build()
  }

  // commit through the OpenHouse catalog, stamping the source snapshot id in the dest snapshot summary
  val append = dst.newAppend()
  relocated.foreach(append.appendFile)
  append.set("source-snapshot-id", srcSnap.snapshotId.toString)
  append.set("source-table", SRC)
  append.commit()
  println(s"[spike] committed dest snapshot referencing relocated files")

  // ORACLE: dest must equal source
  val srcRows = spark.table(SRC).collect().map(_.toString).sorted.toList
  val dstRows = spark.table(DST).collect().map(_.toString).sorted.toList
  val ok = srcRows == dstRows
  println(s"[spike] RESULT: ${if (ok) "PASS" else "FAIL"}  src=$srcRows dst=$dstRows")
  val summ = loadTbl(DST).currentSnapshot().summary()
  println(s"[spike] dest snapshot summary source-snapshot-id=${summ.get("source-snapshot-id")}")
}

spike()

// ---- full function (NEXT — P0..P2) ---------------------------------------------------------------
// def replicateTable(source: String, dest: String, opts: Opts): Result = {
//   // 1. load source; compute in-window snapshots (retention-bounded); reconcile vs dest summaries
//   // 2. floor = useSnapshot(floorId).planFiles() (full); newer = per-snapshot delta (added/removed)
//   // 3. per snapshot in order: anti-join vs dest files (manifest-vs-manifest), copy missing bytes to
//   //    remapped paths (executors, carry metrics), author matching dest snapshot
//   //    (newAppend/newRowDelta/newOverwrite with relocated DataFiles), .set("source-snapshot-id", id)
//   // 4. P2: positional delete files -> rewrite internal file_path column to remapped paths
//   // oracle: for every in-window i, dest AS OF mapped_i == src AS OF i (multiset)
// }

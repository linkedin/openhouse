// Replication PoC — runs in spark-shell against the local OpenHouse catalog (`openhouse`).
//   docker exec local.spark-master /var/config/run_replicate.sh </dev/null
//
// Eager FULL-HISTORY physical replication with rename.
//  - each source snapshot maps to one dest snapshot, stamped `source-snapshot-id` (recovery + reconcile)
//  - reconcile/idempotency: skip source snapshots already present in dest summaries
//  - oracle: for every source snapshot i, dest AS OF (mapped) == source AS OF i  (multiset)

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.iceberg.{DataFile, DataFiles, Snapshot, SnapshotUpdate, Table}
import org.apache.iceberg.spark.Spark3Util
import scala.collection.JavaConverters._
import scala.util.Try

val DB  = "db"
val SRC = s"openhouse.$DB.p0_append"
val DST = s"openhouse.$DB.p0_replica"

def loadTbl(n: String): Table = Spark3Util.loadIcebergTable(spark, n)

// committed history, oldest -> newest (walk parentId from current)
def chainOf(t: Table): List[Snapshot] = {
  def walk(s: Snapshot, acc: List[Snapshot]): List[Snapshot] =
    if (s == null) acc else walk(Option(s.parentId()).map(t.snapshot(_)).orNull, s :: acc)
  walk(t.currentSnapshot(), Nil)
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

// What a source snapshot does to the dest file set. `add` is in SOURCE space (relocated at apply time);
// `remove` is in SOURCE space (mapped to the mirrored dest files at apply time).
case class Replay(add: List[DataFile], remove: List[DataFile])

def planReplay(src: Table, s: Snapshot, isFloor: Boolean): Replay =
  if (isFloor) Replay(src.newScan().useSnapshot(s.snapshotId).planFiles().iterator().asScala.map(_.file()).toList, Nil)
  else         Replay(s.addedDataFiles(src.io()).asScala.toList, s.removedDataFiles(src.io()).asScala.toList)

// dest files that mirror the given source files (by deterministic remap)
def mirrorOnDest(dst: Table, srcFiles: List[DataFile]): List[DataFile] = {
  val live = dst.newScan().planFiles().iterator().asScala.map(_.file()).map(f => f.path().toString -> f).toMap
  srcFiles.flatMap(rf => live.get(s"${dst.location()}/data/${new Path(rf.path().toString).getName}"))
}

def stampCommit(op: SnapshotUpdate[_], s: Snapshot, source: String): Unit = {
  op.set("source-snapshot-id", s.snapshotId.toString); op.set("source-table", source); op.commit()
}

def applyReplay(dst: Table, s: Snapshot, plan: Replay, source: String,
                hconf: org.apache.hadoop.conf.Configuration): String = {
  val adds = plan.add.map(df => relocate(dst, df, hconf))
  val dels = mirrorOnDest(dst, plan.remove)
  val op: SnapshotUpdate[_] = dels match {
    case Nil => val a = dst.newAppend();    adds.foreach(a.appendFile);                      a
    case ds  => val o = dst.newOverwrite(); adds.foreach(o.addFile); ds.foreach(o.deleteFile); o
  }
  stampCommit(op, s, source)
  val kind = if (dels.isEmpty) "append" else "overwrite"
  s"$kind +${adds.size} -${dels.size}"
}

def ensureDest(source: String, dest: String): Table =
  Try(loadTbl(dest)).getOrElse {
    spark.sql(s"CREATE TABLE $dest (${spark.table(source).schema.toDDL}) USING iceberg")
    println(s"[repl] created dest $dest"); loadTbl(dest)
  }

def replicateTable(source: String, dest: String): Unit = {
  val src = loadTbl(source)
  ensureDest(source, dest)
  val already = loadTbl(dest).snapshots().asScala
    .flatMap(s => Option(s.summary().get("source-snapshot-id"))).map(_.toLong).toSet
  val chain = chainOf(src)
  val toReplay = chain.filter(s => !already.contains(s.snapshotId))
  println(s"[repl] chain=${chain.size} already=${already.size} toReplay=${toReplay.size}")
  val hconf = spark.sessionState.newHadoopConf()
  val freshDest = already.isEmpty
  toReplay.zipWithIndex.foreach { case (s, i) =>
    Option(s.addedDeleteFiles(src.io()).asScala.toList).filter(_.nonEmpty).foreach(d =>
      println(s"[repl]   NOTE snap ${s.snapshotId} has ${d.size} delete file(s) (MOR) — not yet handled"))
    val isFloor = freshDest && i == 0
    val summary = applyReplay(loadTbl(dest), s, planReplay(src, s, isFloor), source, hconf)
    println(s"[repl]   replayed src snap ${s.snapshotId} ${if (isFloor) "(floor) " else ""}$summary")
  }
}

def oracle(source: String, dest: String): Boolean = {
  val src = loadTbl(source)
  val map = loadTbl(dest).snapshots().asScala
    .flatMap(s => Option(s.summary().get("source-snapshot-id")).map(_.toLong -> s.snapshotId)).toMap
  val results = chainOf(src).map { s =>
    val srcRows = rowsAsOf(source, s.snapshotId)
    val ok = map.get(s.snapshotId).exists(d => rowsAsOf(dest, d) == srcRows)
    println(s"[oracle] src ${s.snapshotId} -> dest ${map.getOrElse(s.snapshotId, "MISSING")}: ${if (ok) "PASS" else "FAIL"} (${srcRows.size} rows)")
    ok
  }
  val allOk = results.forall(identity)
  println(s"[oracle] OVERALL: ${if (allOk) "PASS" else "FAIL"}")
  allOk
}

// ---- scenario: P0 full history + P1 idempotency/incremental + P2 CoW mutations -------------------
spark.sql(s"DROP TABLE IF EXISTS $DST")
spark.sql(s"DROP TABLE IF EXISTS $SRC")
spark.sql(s"CREATE TABLE $SRC (id INT, data STRING) USING iceberg")
spark.sql(s"INSERT INTO $SRC VALUES (1,'a'),(2,'b')")   // snap 1 (genesis)
spark.sql(s"INSERT INTO $SRC VALUES (3,'c')")            // snap 2
spark.sql(s"INSERT INTO $SRC VALUES (4,'d'),(5,'e')")    // snap 3

println("\n[P0] bootstrap full-history replicate"); replicateTable(SRC, DST); val p0 = oracle(SRC, DST)
println("\n[P1] idempotent re-run (expect toReplay=0)"); replicateTable(SRC, DST); val idem = oracle(SRC, DST)
println("\n[P1] incremental: add snap 4"); spark.sql(s"INSERT INTO $SRC VALUES (6,'f')")
replicateTable(SRC, DST); val inc = oracle(SRC, DST)

println("\n[P2] CoW delete (id=3)"); spark.sql(s"DELETE FROM $SRC WHERE id = 3")
replicateTable(SRC, DST); val p2del = oracle(SRC, DST)
println("\n[P2] overwrite (replace whole table)"); spark.sql(s"INSERT OVERWRITE $SRC VALUES (1,'a'),(2,'b'),(99,'z')")
replicateTable(SRC, DST); val p2ow = oracle(SRC, DST)
println("\n[P2] compaction (rewrite_data_files; may no-op on tiny table)")
Try(spark.sql(s"CALL openhouse.system.rewrite_data_files(table => '$DB.p0_append')"))
  .failed.foreach(e => println(s"[P2] compaction CALL skipped: ${e.getMessage.take(120)}"))
replicateTable(SRC, DST); val p2cmp = oracle(SRC, DST)

println(s"\n[SUMMARY] P0=$p0 idempotent=$idem incremental=$inc p2_delete=$p2del p2_overwrite=$p2ow p2_compaction=$p2cmp")

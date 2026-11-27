// Seeds source tables (the shape matrix) once into the local OpenHouse catalog. Run via:
//   docker exec -e SCALA=seed_sources.scala local.spark-master /var/config/run_replicate.sh
// Sources persist in HDFS/HTS; re-run replicate.scala against them without re-seeding.

val DB = "db"
spark.sql(s"CREATE DATABASE IF NOT EXISTS openhouse.$DB")

// P0: append-only, multiple snapshots (each INSERT = one snapshot = one+ data files).
val p0 = s"openhouse.$DB.p0_append"
spark.sql(s"DROP TABLE IF EXISTS $p0")
spark.sql(s"CREATE TABLE $p0 (id INT, data STRING) USING iceberg")
spark.sql(s"INSERT INTO $p0 VALUES (1,'a'),(2,'b')")   // snapshot 1
spark.sql(s"INSERT INTO $p0 VALUES (3,'c')")            // snapshot 2
spark.sql(s"INSERT INTO $p0 VALUES (4,'d'),(5,'e')")    // snapshot 3
println(s"[seed] $p0 snapshots: " +
  org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, p0).snapshots().iterator().asScala.size)

// TODO P0b: partitioned append-only source.
// TODO P2: MOR (row-level deletes via MERGE/DELETE), overwrite, then a compaction (rewrite_data_files),
//          then a schema-evolution step (ADD/RENAME column). Build these as the phases need them.

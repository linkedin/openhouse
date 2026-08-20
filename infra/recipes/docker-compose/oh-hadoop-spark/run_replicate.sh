#!/bin/sh
# Runs the replication PoC scala inside the warm spark-master container against the local OpenHouse catalog.
# Host usage (stack already up via `docker compose up`):
#   docker exec local.spark-master /var/config/run_replicate.sh                # runs replicate.scala
#   docker exec -e SCALA=seed_sources.scala local.spark-master /var/config/run_replicate.sh
#
# Iterate by editing /var/config/<scala> on the host (mounted) and re-running this exec. No teardown.
set -e

SCALA="${SCALA:-replicate.scala}"
cd /opt/spark

bin/spark-shell --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.2.0 \
  --jars /opt/spark/openhouse-spark-runtime_2.12-latest-all.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions \
  --conf spark.sql.catalog.openhouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.openhouse.catalog-impl=com.linkedin.openhouse.spark.OpenHouseCatalog \
  --conf spark.sql.catalog.openhouse.metrics-reporter-impl=com.linkedin.openhouse.javaclient.OpenHouseMetricsReporter \
  --conf spark.sql.catalog.openhouse.uri=http://openhouse-tables:8080 \
  --conf spark.sql.catalog.openhouse.auth-token="$(cat /var/config/openhouse.token)" \
  --conf spark.sql.catalog.openhouse.cluster=LocalHadoopCluster \
  -i "/var/config/${SCALA}"

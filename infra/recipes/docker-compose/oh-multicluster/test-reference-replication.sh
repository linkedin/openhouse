#!/usr/bin/env bash
set -euo pipefail

export COMPOSE_PARALLEL_LIMIT=1

recipe_dir="$(cd "$(dirname "$0")" && pwd)"
compose_file="$recipe_dir/docker-compose.yml"
project_name="oh-multicluster-reference-replication-e2e-$$"
database="replication_e2e_$$"
source_table="source_$$"
renamed_source_table="${source_table}_renamed"
destination_table="destination_$$"
output_file="$(mktemp)"
stack_started_by_script=false
source_created=false
destination_created=false
admin_token=""

compose() {
  docker compose --project-name "$project_name" -f "$compose_file" "$@"
}

port_base="${OH_MULTICLUSTER_TEST_PORT_BASE:-28000}"
export OH_MULTICLUSTER_TABLES_A_PORT="${OH_MULTICLUSTER_TABLES_A_PORT:-$((port_base + 0))}"
export OH_MULTICLUSTER_HOUSETABLES_A_PORT="${OH_MULTICLUSTER_HOUSETABLES_A_PORT:-$((port_base + 1))}"
export OH_MULTICLUSTER_JOBS_A_PORT="${OH_MULTICLUSTER_JOBS_A_PORT:-$((port_base + 2))}"
export OH_MULTICLUSTER_TABLES_B_PORT="${OH_MULTICLUSTER_TABLES_B_PORT:-$((port_base + 10))}"
export OH_MULTICLUSTER_HOUSETABLES_B_PORT="${OH_MULTICLUSTER_HOUSETABLES_B_PORT:-$((port_base + 11))}"
export OH_MULTICLUSTER_JOBS_B_PORT="${OH_MULTICLUSTER_JOBS_B_PORT:-$((port_base + 12))}"
export OH_MULTICLUSTER_NAMENODE_A_HTTP_PORT="${OH_MULTICLUSTER_NAMENODE_A_HTTP_PORT:-$((port_base + 70))}"
export OH_MULTICLUSTER_NAMENODE_A_RPC_PORT="${OH_MULTICLUSTER_NAMENODE_A_RPC_PORT:-$((port_base + 71))}"
export OH_MULTICLUSTER_DATANODE_A_HTTP_PORT="${OH_MULTICLUSTER_DATANODE_A_HTTP_PORT:-$((port_base + 72))}"
export OH_MULTICLUSTER_DATANODE_A_IPC_PORT="${OH_MULTICLUSTER_DATANODE_A_IPC_PORT:-$((port_base + 73))}"
export OH_MULTICLUSTER_NAMENODE_B_HTTP_PORT="${OH_MULTICLUSTER_NAMENODE_B_HTTP_PORT:-$((port_base + 74))}"
export OH_MULTICLUSTER_NAMENODE_B_RPC_PORT="${OH_MULTICLUSTER_NAMENODE_B_RPC_PORT:-$((port_base + 75))}"
export OH_MULTICLUSTER_DATANODE_B_HTTP_PORT="${OH_MULTICLUSTER_DATANODE_B_HTTP_PORT:-$((port_base + 76))}"
export OH_MULTICLUSTER_DATANODE_B_IPC_PORT="${OH_MULTICLUSTER_DATANODE_B_IPC_PORT:-$((port_base + 77))}"
export OH_MULTICLUSTER_MYSQL_A_PORT="${OH_MULTICLUSTER_MYSQL_A_PORT:-$((port_base + 80))}"
export OH_MULTICLUSTER_MYSQL_B_PORT="${OH_MULTICLUSTER_MYSQL_B_PORT:-$((port_base + 81))}"
export OH_MULTICLUSTER_SPARK_MASTER_UI_PORT="${OH_MULTICLUSTER_SPARK_MASTER_UI_PORT:-$((port_base + 90))}"
export OH_MULTICLUSTER_SPARK_MASTER_RPC_PORT="${OH_MULTICLUSTER_SPARK_MASTER_RPC_PORT:-$((port_base + 91))}"
export OH_MULTICLUSTER_SPARK_WORKER_UI_PORT="${OH_MULTICLUSTER_SPARK_WORKER_UI_PORT:-$((port_base + 92))}"
export OH_MULTICLUSTER_SPARK_WORKER_RPC_PORT="${OH_MULTICLUSTER_SPARK_WORKER_RPC_PORT:-$((port_base + 93))}"
export OH_MULTICLUSTER_LIVY_PORT="${OH_MULTICLUSTER_LIVY_PORT:-$((port_base + 94))}"
export OH_MULTICLUSTER_PROMETHEUS_PORT="${OH_MULTICLUSTER_PROMETHEUS_PORT:-$((port_base + 95))}"
export OH_MULTICLUSTER_JAEGER_UI_PORT="${OH_MULTICLUSTER_JAEGER_UI_PORT:-$((port_base + 96))}"
export OH_MULTICLUSTER_JAEGER_GRPC_PORT="${OH_MULTICLUSTER_JAEGER_GRPC_PORT:-$((port_base + 97))}"
export OH_MULTICLUSTER_JAEGER_HTTP_PORT="${OH_MULTICLUSTER_JAEGER_HTTP_PORT:-$((port_base + 98))}"
export OH_MULTICLUSTER_OPA_PORT="${OH_MULTICLUSTER_OPA_PORT:-$((port_base + 99))}"

spark_sql() {
  local token="$1"
  local statement="$2"

  compose exec -T spark-livy /opt/spark/bin/spark-sql \
    --master spark://spark-master:7077 \
    --jars /opt/spark/openhouse-spark-runtime_2.12-latest-all.jar \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.2.0 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions \
    --conf spark.sql.catalog.openhouse_a=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.openhouse_a.catalog-impl=com.linkedin.openhouse.spark.OpenHouseCatalog \
    --conf spark.sql.catalog.openhouse_a.uri=http://tables-a:8080 \
    --conf spark.sql.catalog.openhouse_a.cluster=LocalHadoopClusterA \
    --conf "spark.sql.catalog.openhouse_a.auth-token=$token" \
    --conf spark.sql.catalog.openhouse_b=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.openhouse_b.catalog-impl=com.linkedin.openhouse.spark.OpenHouseCatalog \
    --conf spark.sql.catalog.openhouse_b.uri=http://tables-b:8080 \
    --conf spark.sql.catalog.openhouse_b.cluster=LocalHadoopClusterB \
    --conf "spark.sql.catalog.openhouse_b.auth-token=$token" \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode-a:9000 \
    --conf spark.sql.warehouse.dir=file:///tmp/spark-reference-replication-warehouse \
    --conf spark.driver.memory=1g \
    --conf spark.executor.memory=512m \
    --silent \
    -e "$statement"
}

run_replicator() {
  compose exec -T spark-livy /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --class com.linkedin.openhouse.jobs.spark.replication.ReferenceReplicatorSparkApp \
    --jars /opt/spark/openhouse-spark-runtime_2.12-latest-all.jar \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.2.0 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions \
    --conf spark.sql.catalog.openhouse_a=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.openhouse_a.catalog-impl=com.linkedin.openhouse.spark.OpenHouseCatalog \
    --conf spark.sql.catalog.openhouse_a.uri=http://tables-a:8080 \
    --conf spark.sql.catalog.openhouse_a.cluster=LocalHadoopClusterA \
    --conf "spark.sql.catalog.openhouse_a.auth-token=$admin_token" \
    --conf spark.sql.catalog.openhouse_b=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.openhouse_b.catalog-impl=com.linkedin.openhouse.spark.OpenHouseCatalog \
    --conf spark.sql.catalog.openhouse_b.uri=http://tables-b:8080 \
    --conf spark.sql.catalog.openhouse_b.cluster=LocalHadoopClusterB \
    --conf "spark.sql.catalog.openhouse_b.auth-token=$admin_token" \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode-a:9000 \
    --conf spark.sql.warehouse.dir=file:///tmp/spark-reference-replication-warehouse \
    --conf spark.driver.memory=1g \
    --conf spark.executor.memory=512m \
    /opt/spark/openhouse-spark-apps_2.12-latest-all.jar \
    --sourceClusterId "$source_cluster_id" \
    --sourceTableUUID "$source_table_uuid" \
    --sourceCreationTime "$source_creation_time" \
    --sourceCatalog openhouse_a \
    --sourceTablesApiUrl "http://tables-a:8080" \
    --destinationClusterId "$destination_cluster_id" \
    --destinationCatalog openhouse_b \
    --destinationTablesApiUrl "http://tables-b:8080" \
    --token "$admin_token"
}

get_table() {
  local port="$1"
  local table="$2"
  curl --silent --show-error --fail \
    -H "Authorization: $admin_token" \
    "http://localhost:${port}/v1/databases/${database}/tables/${table}"
}

prepare_hdfs() {
  local namenode="$1"
  for _ in $(seq 1 60); do
    if compose exec -T "$namenode" hdfs dfs -ls / >/dev/null 2>&1; then
      compose exec -T "$namenode" hdfs dfs -mkdir -p \
        /tmp/hive /data/openhouse /user/hive/warehouse /user/openhouse
      compose exec -T "$namenode" hdfs dfs -chmod 1777 /tmp
      compose exec -T "$namenode" hdfs dfs -chmod 777 \
        /tmp/hive /data/openhouse /user/hive /user/hive/warehouse /user/openhouse
      return
    fi
    sleep 2
  done
  echo "NameNode $namenode did not become ready." >&2
  return 1
}

wait_for_health() {
  local service="$1"
  local port="$2"
  for _ in $(seq 1 90); do
    if curl --silent --show-error --fail \
      "http://localhost:${port}/actuator/health" >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done
  echo "$service did not become healthy." >&2
  return 1
}

cleanup() {
  local exit_status="$?"
  if [[ "$exit_status" -ne 0 && "$stack_started_by_script" == true ]]; then
    echo "End-to-end test failed; recent isolated container logs follow." >&2
    compose logs --no-color --tail=100 spark-livy spark-worker-a spark-master tables-a tables-b >&2 || true
  fi
  if [[ "$source_created" == true && -n "$admin_token" ]]; then
    spark_sql "$admin_token" "DROP TABLE IF EXISTS openhouse_a.$database.$source_table" \
      >/dev/null 2>&1 || true
    spark_sql "$admin_token" \
      "DROP TABLE IF EXISTS openhouse_a.$database.$renamed_source_table" >/dev/null 2>&1 || true
  fi
  if [[ "$destination_created" == true && -n "$admin_token" ]]; then
    spark_sql "$admin_token" "DROP TABLE IF EXISTS openhouse_b.$database.$destination_table" \
      >/dev/null 2>&1 || true
    spark_sql "$admin_token" \
      "DROP TABLE IF EXISTS openhouse_b.$database.$renamed_source_table" >/dev/null 2>&1 || true
  fi
  if [[ "$stack_started_by_script" == true ]]; then
    compose down --volumes >/dev/null 2>&1 || true
  fi
  rm -f "$output_file"
  return "$exit_status"
}
trap cleanup EXIT

stack_started_by_script=true
compose up -d --build tables-a tables-b spark-livy spark-worker-a
wait_for_health "House Tables service A" "$OH_MULTICLUSTER_HOUSETABLES_A_PORT"
wait_for_health "Tables service A" "$OH_MULTICLUSTER_TABLES_A_PORT"
wait_for_health "House Tables service B" "$OH_MULTICLUSTER_HOUSETABLES_B_PORT"
wait_for_health "Tables service B" "$OH_MULTICLUSTER_TABLES_B_PORT"
prepare_hdfs namenode-a
prepare_hdfs namenode-b

admin_token="$(compose exec -T spark-livy cat /var/config/openhouse.token | tr -d '\r\n')"
spark_sql "$admin_token" \
  "CREATE TABLE openhouse_a.$database.$source_table (id BIGINT, value STRING) USING iceberg"
source_created=true
spark_sql "$admin_token" \
  "CREATE TABLE openhouse_b.$database.$destination_table (id BIGINT, value STRING) USING iceberg TBLPROPERTIES ('openhouse.tableType'='REPLICA_TABLE')"
destination_created=true

source_metadata="$(get_table "$OH_MULTICLUSTER_TABLES_A_PORT" "$source_table")"
destination_metadata="$(get_table "$OH_MULTICLUSTER_TABLES_B_PORT" "$destination_table")"
source_cluster_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["clusterId"])' <<<"$source_metadata")"
source_table_uuid="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["tableUUID"])' <<<"$source_metadata")"
source_creation_time="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["creationTime"])' <<<"$source_metadata")"
destination_cluster_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["clusterId"])' <<<"$destination_metadata")"
destination_table_uuid="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["tableUUID"])' <<<"$destination_metadata")"
destination_creation_time="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["creationTime"])' <<<"$destination_metadata")"
python3 -c 'import json,sys; assert json.load(sys.stdin)["tableType"] == "REPLICA_TABLE"' <<<"$destination_metadata"

destination_request="$(
  python3 - "$source_cluster_id" "$source_table_uuid" "$source_creation_time" \
    "$destination_cluster_id" "$destination_table_uuid" "$destination_creation_time" \
    "$database" "$source_table" "$destination_table" <<'PY'
import json
import sys
source_cluster, source_uuid, source_created, destination_cluster, destination_uuid, destination_created, database, source_table, destination_table = sys.argv[1:]
print(json.dumps({
    "sourceClusterId": source_cluster,
    "sourceTableUUID": source_uuid,
    "sourceCreationTime": int(source_created),
    "sourceDatabaseId": database,
    "sourceTableId": source_table,
    "destinationClusterId": destination_cluster,
    "destinationTableUUID": destination_uuid,
    "destinationCreationTime": int(destination_created),
    "destinationDatabaseId": database,
    "destinationTableId": destination_table,
    "expectedVersion": 0,
}))
PY
)"
curl --silent --show-error --fail \
  -X PUT \
  -H "Authorization: $admin_token" \
  -H "Content-Type: application/json" \
  --data "$destination_request" \
  "http://localhost:${OH_MULTICLUSTER_TABLES_B_PORT}/v1/replication/destinations" >/dev/null

spark_sql "$admin_token" "INSERT INTO openhouse_a.$database.$source_table VALUES (1, 'one')"
run_replicator
spark_sql "$admin_token" "INSERT INTO openhouse_a.$database.$source_table VALUES (2, 'two')"
run_replicator
spark_sql "$admin_token" "INSERT INTO openhouse_a.$database.$source_table VALUES (3, 'three')"
run_replicator
spark_sql "$admin_token" \
  "ALTER TABLE openhouse_a.$database.$source_table RENAME TO openhouse_a.$database.$renamed_source_table"
spark_sql "$admin_token" \
  "INSERT INTO openhouse_a.$database.$renamed_source_table VALUES (4, 'four')"
run_replicator

spark_sql "$admin_token" \
  "SELECT id FROM openhouse_b.$database.$renamed_source_table ORDER BY id" >"$output_file"
actual_rows="$(awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ {gsub(/[[:space:]]/, ""); print}' "$output_file")"
if [[ "$actual_rows" != $'1\n2\n3\n4' ]]; then
  echo "Destination data differs from source: $actual_rows" >&2
  cat "$output_file" >&2
  exit 1
fi

old_destination_status="$(
  curl --silent --show-error \
    -H "Authorization: $admin_token" \
    -o /dev/null -w '%{http_code}' \
    "http://localhost:${OH_MULTICLUSTER_TABLES_B_PORT}/v1/databases/${database}/tables/${destination_table}"
)"
if [[ "$old_destination_status" != "404" ]]; then
  echo "Expected the old destination locator to be absent, got HTTP $old_destination_status." >&2
  exit 1
fi

final_source_metadata="$(get_table "$OH_MULTICLUSTER_TABLES_A_PORT" "$renamed_source_table")"
final_destination_metadata="$(get_table "$OH_MULTICLUSTER_TABLES_B_PORT" "$renamed_source_table")"
python3 - "$source_table_uuid" "$source_creation_time" "$destination_table_uuid" \
  "$destination_creation_time" "$final_source_metadata" "$final_destination_metadata" <<'PY'
import json
import sys
source_uuid, source_created, destination_uuid, destination_created, source_json, destination_json = sys.argv[1:]
source = json.loads(source_json)
destination = json.loads(destination_json)
assert source["tableUUID"] == source_uuid and int(source["creationTime"]) == int(source_created), source
assert destination["tableUUID"] == destination_uuid and int(destination["creationTime"]) == int(destination_created), destination
assert destination["tableType"] == "REPLICA_TABLE", destination
PY

source_snapshot_id="$(
  spark_sql "$admin_token" \
    "SELECT snapshot_id FROM openhouse_a.$database.$renamed_source_table.snapshots ORDER BY committed_at DESC LIMIT 1" |
    awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ {gsub(/[[:space:]]/, ""); value=$0} END {print value}'
)"
destination_snapshot_id="$(
  spark_sql "$admin_token" \
    "SELECT snapshot_id FROM openhouse_b.$database.$renamed_source_table.snapshots ORDER BY committed_at DESC LIMIT 1" |
    awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ {gsub(/[[:space:]]/, ""); value=$0} END {print value}'
)"
checkpoint="$(
  curl --silent --show-error --fail --get \
    -H "Authorization: $admin_token" \
    --data-urlencode "sourceClusterId=$source_cluster_id" \
    --data-urlencode "sourceTableUUID=$source_table_uuid" \
    --data-urlencode "sourceCreationTime=$source_creation_time" \
    --data-urlencode "destinationClusterId=$destination_cluster_id" \
    --data-urlencode "destinationTableUUID=$destination_table_uuid" \
    --data-urlencode "destinationCreationTime=$destination_creation_time" \
    "http://localhost:${OH_MULTICLUSTER_TABLES_B_PORT}/v1/replication/checkpoints"
)"
python3 - "$source_table_uuid" "$destination_table_uuid" "$source_snapshot_id" \
  "$destination_snapshot_id" "$final_source_metadata" "$final_destination_metadata" "$checkpoint" <<'PY'
import json
import sys
source_uuid, destination_uuid, source_snapshot, destination_snapshot, source_json, destination_json, checkpoint_json = sys.argv[1:]
source = json.loads(source_json)
destination = json.loads(destination_json)
checkpoint = json.loads(checkpoint_json)
assert checkpoint["sourceTableUUID"] == source_uuid, checkpoint
assert checkpoint["destinationTableUUID"] == destination_uuid, checkpoint
assert int(checkpoint["sourceSnapshotId"]) == int(source_snapshot), checkpoint
assert int(checkpoint["destinationSnapshotId"]) == int(destination_snapshot), checkpoint
assert checkpoint["sourceTableVersion"] == source["tableVersion"], checkpoint
assert checkpoint["destinationTableVersion"] == destination["tableVersion"], checkpoint
assert int(checkpoint["revision"]) > 0, checkpoint
PY

echo "Reference replication e2e passed: interleaved commits, source rename, replica identity, data, and checkpoint progress verified."

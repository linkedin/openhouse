#!/usr/bin/env bash
set -euo pipefail

export COMPOSE_PARALLEL_LIMIT=1

recipe_dir="$(cd "$(dirname "$0")" && pwd)"
compose_file="$recipe_dir/docker-compose.yml"
project_name="oh-multicluster-reference-replication"
database="spark_reference_auth_$$"
table="replica_write_guard"
output_file="$(mktemp)"
table_created=false
stack_started_by_script=false

compose() {
  docker compose --project-name "$project_name" -f "$compose_file" "$@"
}

spark_sql() {
  local token="$1"
  local statement="$2"

  compose exec -T spark-livy /opt/spark/bin/spark-sql \
    --master spark://spark-master:7077 \
    --jars /opt/spark/openhouse-spark-runtime_2.12-latest-all.jar \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.2.0 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions \
    --conf spark.sql.catalog.openhouse_b=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.openhouse_b.catalog-impl=com.linkedin.openhouse.spark.OpenHouseCatalog \
    --conf spark.sql.catalog.openhouse_b.uri=http://tables-b:8080 \
    --conf spark.sql.catalog.openhouse_b.cluster=LocalHadoopClusterB \
    --conf "spark.sql.catalog.openhouse_b.auth-token=$token" \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode-b:9000 \
    --conf spark.sql.warehouse.dir=file:///tmp/spark-reference-replication-warehouse \
    --conf spark.driver.memory=2g \
    --silent \
    -e "$statement"
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

cleanup() {
  if [[ "$table_created" == true && -n "${admin_token:-}" ]]; then
    spark_sql "$admin_token" "DROP TABLE IF EXISTS openhouse_b.$database.$table" >/dev/null 2>&1 || true
  fi
  if [[ "$stack_started_by_script" == true ]]; then
    compose down --volumes >/dev/null 2>&1 || true
  fi
  rm -f "$output_file"
}
trap cleanup EXIT

export OH_MULTICLUSTER_TABLES_A_PORT="${OH_MULTICLUSTER_TABLES_A_PORT:-18000}"
export OH_MULTICLUSTER_HOUSETABLES_A_PORT="${OH_MULTICLUSTER_HOUSETABLES_A_PORT:-18001}"
export OH_MULTICLUSTER_JOBS_A_PORT="${OH_MULTICLUSTER_JOBS_A_PORT:-18002}"
export OH_MULTICLUSTER_TABLES_B_PORT="${OH_MULTICLUSTER_TABLES_B_PORT:-18010}"
export OH_MULTICLUSTER_HOUSETABLES_B_PORT="${OH_MULTICLUSTER_HOUSETABLES_B_PORT:-18011}"
export OH_MULTICLUSTER_JOBS_B_PORT="${OH_MULTICLUSTER_JOBS_B_PORT:-18012}"
export OH_MULTICLUSTER_NAMENODE_A_HTTP_PORT="${OH_MULTICLUSTER_NAMENODE_A_HTTP_PORT:-19870}"
export OH_MULTICLUSTER_NAMENODE_A_RPC_PORT="${OH_MULTICLUSTER_NAMENODE_A_RPC_PORT:-19000}"
export OH_MULTICLUSTER_DATANODE_A_HTTP_PORT="${OH_MULTICLUSTER_DATANODE_A_HTTP_PORT:-19864}"
export OH_MULTICLUSTER_DATANODE_A_IPC_PORT="${OH_MULTICLUSTER_DATANODE_A_IPC_PORT:-19866}"
export OH_MULTICLUSTER_NAMENODE_B_HTTP_PORT="${OH_MULTICLUSTER_NAMENODE_B_HTTP_PORT:-19871}"
export OH_MULTICLUSTER_NAMENODE_B_RPC_PORT="${OH_MULTICLUSTER_NAMENODE_B_RPC_PORT:-19004}"
export OH_MULTICLUSTER_DATANODE_B_HTTP_PORT="${OH_MULTICLUSTER_DATANODE_B_HTTP_PORT:-19865}"
export OH_MULTICLUSTER_DATANODE_B_IPC_PORT="${OH_MULTICLUSTER_DATANODE_B_IPC_PORT:-19867}"
export OH_MULTICLUSTER_MYSQL_A_PORT="${OH_MULTICLUSTER_MYSQL_A_PORT:-13306}"
export OH_MULTICLUSTER_MYSQL_B_PORT="${OH_MULTICLUSTER_MYSQL_B_PORT:-13307}"
export OH_MULTICLUSTER_SPARK_MASTER_UI_PORT="${OH_MULTICLUSTER_SPARK_MASTER_UI_PORT:-19001}"
export OH_MULTICLUSTER_SPARK_MASTER_RPC_PORT="${OH_MULTICLUSTER_SPARK_MASTER_RPC_PORT:-17077}"
export OH_MULTICLUSTER_SPARK_WORKER_UI_PORT="${OH_MULTICLUSTER_SPARK_WORKER_UI_PORT:-19002}"
export OH_MULTICLUSTER_SPARK_WORKER_RPC_PORT="${OH_MULTICLUSTER_SPARK_WORKER_RPC_PORT:-17000}"
export OH_MULTICLUSTER_LIVY_PORT="${OH_MULTICLUSTER_LIVY_PORT:-19003}"
export OH_MULTICLUSTER_PROMETHEUS_PORT="${OH_MULTICLUSTER_PROMETHEUS_PORT:-19090}"
export OH_MULTICLUSTER_JAEGER_UI_PORT="${OH_MULTICLUSTER_JAEGER_UI_PORT:-19686}"
export OH_MULTICLUSTER_JAEGER_GRPC_PORT="${OH_MULTICLUSTER_JAEGER_GRPC_PORT:-14317}"
export OH_MULTICLUSTER_JAEGER_HTTP_PORT="${OH_MULTICLUSTER_JAEGER_HTTP_PORT:-14318}"
export OH_MULTICLUSTER_OPA_PORT="${OH_MULTICLUSTER_OPA_PORT:-18181}"

if ! compose ps --status running -q tables-b | grep -q .; then
  stack_started_by_script=true
  compose up -d --build tables-b spark-livy spark-worker-a
fi

wait_for_health() {
  local service="$1"
  local port="$2"
  for _ in $(seq 1 60); do
    if curl --silent --show-error --fail \
      "http://localhost:${port}/actuator/health" >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done
  echo "$service did not become healthy." >&2
  return 1
}

wait_for_health "House Tables service B" "$OH_MULTICLUSTER_HOUSETABLES_B_PORT"
wait_for_health "Tables service B" "$OH_MULTICLUSTER_TABLES_B_PORT"

prepare_hdfs namenode-b

admin_token="$(compose exec -T spark-livy cat /var/config/openhouse.token | tr -d '\r\n')"
user_token="$(compose exec -T spark-livy cat /var/config/u_tableowner.token | tr -d '\r\n')"

spark_sql "$admin_token" \
  "CREATE TABLE openhouse_b.$database.$table (id BIGINT, value STRING) USING iceberg TBLPROPERTIES ('openhouse.tableType'='REPLICA_TABLE')"
table_created=true

curl --silent --show-error --fail \
  -H "Authorization: Bearer $admin_token" \
  "http://localhost:${OH_MULTICLUSTER_TABLES_B_PORT}/v1/databases/$database/tables/$table" |
  python3 -c 'import json,sys; body=json.load(sys.stdin); assert body.get("tableType") == "REPLICA_TABLE", body'

if spark_sql "$user_token" \
  "INSERT INTO openhouse_b.$database.$table VALUES (1, 'ordinary-write')" >"$output_file" 2>&1; then
  echo "Expected an ordinary u_tableowner write to REPLICA_TABLE to fail." >&2
  cat "$output_file" >&2
  exit 1
fi

if ! grep -Eiq '403|forbidden|unauthorized|access.?denied|SYSTEM_ADMIN' "$output_file"; then
  echo "The ordinary write failed for an unexpected reason:" >&2
  cat "$output_file" >&2
  exit 1
fi

spark_sql "$admin_token" \
  "SELECT COUNT(*) FROM openhouse_b.$database.$table" >"$output_file" 2>&1
if ! grep -Eq '^[[:space:]]*0[[:space:]]*$' "$output_file"; then
  echo "The denied write changed the replica table:" >&2
  cat "$output_file" >&2
  exit 1
fi

echo "Replica write authorization check passed: u_tableowner was denied and the table stayed empty."

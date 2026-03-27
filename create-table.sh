#!/usr/bin/env bash
set -euo pipefail

TOKEN=$(cat /Users/mkuchenb/code/openhouse/services/common/src/main/resources/dummy.token)
LIVY_URL="http://localhost:9003"
TABLES_URL="http://localhost:8000"

SID=""
trap '[[ -n "$SID" ]] && curl -s -X DELETE "$LIVY_URL/sessions/$SID" > /dev/null 2>&1 || true' EXIT

echo "Creating Livy session..."
body=$(cat <<'ENDJSON'
{"kind":"spark","conf":{"spark.jars":"local:/opt/spark/openhouse-spark-runtime_2.12-latest-all.jar","spark.jars.packages":"org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.2.0","spark.sql.extensions":"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions","spark.sql.catalog.openhouse":"org.apache.iceberg.spark.SparkCatalog","spark.sql.catalog.openhouse.catalog-impl":"com.linkedin.openhouse.spark.OpenHouseCatalog","spark.sql.catalog.openhouse.uri":"http://openhouse-tables:8080","spark.sql.catalog.openhouse.cluster":"LocalHadoopCluster"}}
ENDJSON
)
body=$(echo "$body" | jq --arg t "$TOKEN" '.conf["spark.sql.catalog.openhouse.auth-token"] = $t')
resp=$(curl -s -X POST "$LIVY_URL/sessions" -H 'Content-Type: application/json' -d "$body")
SID=$(echo "$resp" | jq -r '.id')
while true; do
  sleep 2
  state=$(curl -s "$LIVY_URL/sessions/$SID" | jq -r '.state')
  [[ "$state" == "idle" ]] && break
done
echo "Session $SID ready"

run() {
  local code="$1"
  echo "Running: ${code:0:100}..."
  local resp
  resp=$(curl -s -X POST "$LIVY_URL/sessions/$SID/statements" -H 'Content-Type: application/json' -d "$(jq -n --arg c "$code" '{code:$c}')")
  local stmt_id
  stmt_id=$(echo "$resp" | jq -r '.id')
  while true; do
    sleep 2
    local state
    state=$(curl -s "$LIVY_URL/sessions/$SID/statements/$stmt_id" | jq -r '.state')
    [[ "$state" == "available" || "$state" == "error" ]] && break
  done
}

run 'spark.sql("CREATE TABLE openhouse.demo_db.sl_demo (id INT, name STRING) USING iceberg")'
run 'spark.sql("INSERT INTO openhouse.demo_db.sl_demo VALUES (1, '\''alice'\''), (2, '\''bob'\'')").show(false)'

echo ""
echo "=== Table created. Checking API response ==="
curl -s -H "Authorization: Bearer $TOKEN" "$TABLES_URL/v1/databases/demo_db/tables/sl_demo" | python3 -m json.tool

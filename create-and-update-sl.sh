#!/usr/bin/env bash
set -euo pipefail

TOKEN=$(cat /Users/mkuchenb/code/openhouse/services/common/src/main/resources/dummy.token)
LIVY_URL="http://localhost:9003"
TABLES_URL="http://localhost:8000"
HTS_URL="http://localhost:8001"
DB="demo_db"
TABLE="sl_demo2"

SID=""
trap '[[ -n "$SID" ]] && curl -s -X DELETE "$LIVY_URL/sessions/$SID" > /dev/null 2>&1 || true' EXIT

echo "=== Creating Livy session ==="
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

echo ""
echo "=== Step 1: Create table and insert data ==="
run "spark.sql(\"DROP TABLE IF EXISTS openhouse.${DB}.${TABLE}\")"
run "spark.sql(\"CREATE TABLE openhouse.${DB}.${TABLE} (id INT, name STRING) USING iceberg\")"
run "spark.sql(\"INSERT INTO openhouse.${DB}.${TABLE} VALUES (1, 'alice'), (2, 'bob')\")"

echo ""
echo "=== Table created ==="
curl -s -H "Authorization: Bearer $TOKEN" "$TABLES_URL/v1/databases/$DB/tables/$TABLE" | jq '{tableLocation, storageLocations}'

echo ""
echo "=== Step 2: Register new storage location and update ==="
NEW_BASE="hdfs://namenode:9000/data/openhouse/${DB}/${TABLE}_v2"
sl_resp=$(curl -s -X POST "$HTS_URL/hts/storageLocations" \
  -H 'Content-Type: application/json' \
  -d "{\"uri\":\"${NEW_BASE}\"}")
SL_ID=$(echo "$sl_resp" | jq -r '.storageLocationId')
echo "New storageLocationId: $SL_ID"

curl -s -X POST "$HTS_URL/hts/storageLocations/link?databaseId=$DB&tableId=$TABLE&storageLocationId=$SL_ID"

curl -s -X PATCH \
  "$TABLES_URL/v1/databases/$DB/tables/$TABLE/storageLocation" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"storageLocationId\":\"$SL_ID\"}" > /dev/null

echo ""
echo "=== Updated table ==="
curl -s -H "Authorization: Bearer $TOKEN" "$TABLES_URL/v1/databases/$DB/tables/$TABLE" | jq '{tableLocation, storageLocations}'

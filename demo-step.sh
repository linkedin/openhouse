#!/usr/bin/env bash
set -euo pipefail

TOKEN=$(cat /Users/mkuchenb/code/openhouse/services/common/src/main/resources/dummy.token)
LIVY_URL="http://localhost:9003"
DB="demo_db"
TABLE="sl_branch"

# Reuse existing idle session
SID=$(curl -s "$LIVY_URL/sessions" | jq -r '[.sessions[] | select(.state=="idle")][0].id')
if [[ "$SID" == "null" || -z "$SID" ]]; then
  echo "No idle Livy session. Creating one..."
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
    [[ "$state" == "dead" || "$state" == "error" ]] && { echo "FAILED: $state"; exit 1; }
  done
fi
echo "Using session $SID"

run() {
  local code="$1"
  echo "Running: ${code:0:120}..."
  local resp
  resp=$(curl -s -X POST "$LIVY_URL/sessions/$SID/statements" -H 'Content-Type: application/json' -d "$(jq -n --arg c "$code" '{code:$c}')")
  local stmt_id
  stmt_id=$(echo "$resp" | jq -r '.id')
  while true; do
    sleep 2
    local st
    st=$(curl -s "$LIVY_URL/sessions/$SID/statements/$stmt_id")
    local state
    state=$(echo "$st" | jq -r '.state')
    if [[ "$state" == "available" || "$state" == "error" ]]; then
      echo "$st" | jq -r '.output.data["text/plain"] // .output.evalue // empty'
      break
    fi
  done
}

echo ""
echo "=== Step 2: Insert row ==="
run "spark.sql(\"INSERT INTO openhouse.${DB}.${TABLE} VALUES (1, 'inserted before migration')\")"
echo ""
echo "=== Verify ==="
run "spark.sql(\"SELECT * FROM openhouse.${DB}.${TABLE}\").show(false)"

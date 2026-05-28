#!/usr/bin/env bash
# local-spark-sql.sh — Execute a SQL statement against the local OpenHouse docker cluster.
#
# Reuses an existing idle Livy session if one exists. First run pays the cold start (~20s),
# subsequent runs execute immediately.
#
# Usage: ./local-spark-sql.sh "INSERT INTO openhouse.db1.smoke_tbl VALUES ('3', 'c')"
#        ./local-spark-sql.sh --kill   # tear down the reusable session
set -e

LIVY="http://localhost:9003"
TOKEN=$(cat /Users/mkuchenb/code/openhouse/services/common/src/main/resources/dummy.token)

if [ "$1" = "--kill" ]; then
  SESSIONS=$(curl -sf "$LIVY/sessions" | jq -r '.sessions[].id')
  for sid in $SESSIONS; do
    curl -sf -X DELETE "$LIVY/sessions/$sid" > /dev/null 2>&1
    echo "Killed session $sid"
  done
  exit 0
fi

if [ -z "$1" ]; then
  echo "Usage: local-spark-sql.sh \"<SQL statement>\""
  echo "       local-spark-sql.sh --kill"
  exit 1
fi

SQL="$1"

SESSION_ID=$(curl -sf "$LIVY/sessions" | jq -r '[.sessions[] | select(.state == "idle")] | first | .id // empty')

if [ -z "$SESSION_ID" ]; then
  SPARK_CONF=$(jq -n --arg token "$TOKEN" '{
    kind: "sql",
    conf: {
      "spark.jars": "local:/opt/spark/openhouse-spark-runtime_2.12-latest-all.jar",
      "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.2.0",
      "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions",
      "spark.sql.catalog.openhouse": "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.openhouse.catalog-impl": "com.linkedin.openhouse.spark.OpenHouseCatalog",
      "spark.sql.catalog.openhouse.uri": "http://openhouse-tables:8080",
      "spark.sql.catalog.openhouse.auth-token": $token,
      "spark.sql.catalog.openhouse.cluster": "LocalHadoopCluster"
    }
  }')

  echo "No idle session found, creating one..."
  SESSION_ID=$(curl -sf -X POST "$LIVY/sessions" \
    -H "Content-Type: application/json" \
    -d "$SPARK_CONF" | jq -r '.id')

  for i in $(seq 1 60); do
    STATE=$(curl -sf "$LIVY/sessions/$SESSION_ID/state" | jq -r '.state')
    [ "$STATE" = "idle" ] && break
    if [ "$STATE" = "error" ] || [ "$STATE" = "dead" ]; then
      echo "FAIL: session state=$STATE"
      exit 1
    fi
    sleep 2
  done
  [ "$STATE" = "idle" ] || { echo "FAIL: session never reached idle (state=$STATE)"; exit 1; }
fi

STMT_ID=$(curl -sf -X POST "$LIVY/sessions/$SESSION_ID/statements" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg code "$SQL" '{code: $code}')" | jq -r '.id')

for i in $(seq 1 60); do
  STMT_STATE=$(curl -sf "$LIVY/sessions/$SESSION_ID/statements/$STMT_ID" | jq -r '.state')
  [ "$STMT_STATE" = "available" ] && break
  [ "$STMT_STATE" = "error" ] && break
  sleep 1
done

RESULT=$(curl -sf "$LIVY/sessions/$SESSION_ID/statements/$STMT_ID")
STATUS=$(echo "$RESULT" | jq -r '.output.status')

if [ "$STATUS" = "error" ]; then
  echo "FAIL: SQL execution error:"
  echo "$RESULT" | jq -r '.output.evalue, .output.traceback[]?'
  exit 1
fi

echo "$RESULT" | jq -r '.output.data["text/plain"] // ""'

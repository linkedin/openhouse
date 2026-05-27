#!/usr/bin/env bash
# demo-setup.sh — Populate tables, expire snapshots, wait for the continuous
# analyzer + scheduler to schedule OFD jobs.
#
# Prerequisites: ./gradlew dockerUp  (stack must be healthy)
#
# After this script completes, batched-OFD Spark jobs have been submitted via
# the continuous scheduler. Use demo-check.sh to watch for completion.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOKEN=$(cat "$SCRIPT_DIR/services/common/src/main/resources/dummy.token")
TABLES_API="http://localhost:8000"
JOBS_API="http://localhost:8002/jobs"
OPT_API="http://localhost:8005"
LIVY_API="http://localhost:9003"

TABLES="demo_ofd_a:5 demo_ofd_b:7 demo_ofd_c:4"
TABLE_COUNT=3
ANALYZER_WAIT_SECS=90
SCHEDULER_WAIT_SECS=90

wait_for_job() {
  local JOB_ID="$1" LABEL="$2" MAX_SECS="${3:-180}"
  local i=0
  while [ $i -lt "$MAX_SECS" ]; do
    STATE=$(curl -sf "$JOBS_API/$JOB_ID" | jq -r '.state // empty' 2>/dev/null || echo "")
    [ "$STATE" = "SUCCEEDED" ] && return 0
    [ "$STATE" = "FAILED" ] && { echo "FAIL: $LABEL job $JOB_ID FAILED"; exit 1; }
    sleep 5
    i=$((i + 5))
  done
  echo "FAIL: $LABEL job $JOB_ID timed out after ${MAX_SECS}s (last state: $STATE)"
  exit 1
}

kill_idle_session() {
  IDLE=$(curl -sf "$LIVY_API/sessions" \
    | jq -r '[.sessions[] | select(.state=="idle")] | first | .id // empty')
  if [ -n "$IDLE" ]; then
    curl -sf -X DELETE "$LIVY_API/sessions/$IDLE" > /dev/null
    echo "  Freed idle Livy session $IDLE"
  fi
}

wait_for_count() {
  local URL="$1" EXPECTED="$2" LABEL="$3" MAX_SECS="$4"
  local i=0
  while [ $i -lt "$MAX_SECS" ]; do
    COUNT=$(curl -sf "$URL" | jq 'length' 2>/dev/null || echo "0")
    [ "$COUNT" -ge "$EXPECTED" ] && return 0
    printf "  %s: %d/%d (waited %ds)\r" "$LABEL" "$COUNT" "$EXPECTED" "$i"
    sleep 5
    i=$((i + 5))
  done
  echo ""
  echo "FAIL: $LABEL expected $EXPECTED, got $COUNT after ${MAX_SECS}s"
  exit 1
}

echo "=== [1/4] Create tables and populate with data ==="
rm -f /tmp/demo_ofd_locs.txt /tmp/demo_ofd_uuids.txt

for entry in $TABLES; do
  TABLE="${entry%%:*}"
  WRITES="${entry##*:}"
  ORPHANS=$((WRITES - 2))

  "$SCRIPT_DIR/local-spark-sql.sh" "DROP TABLE IF EXISTS openhouse.db1.$TABLE" > /dev/null
  "$SCRIPT_DIR/local-spark-sql.sh" "CREATE TABLE openhouse.db1.$TABLE (
    id STRING, val STRING
  ) TBLPROPERTIES ('maintenance.optimizer.ofd.enabled'='true')" > /dev/null

  for i in $(seq 1 "$WRITES"); do
    "$SCRIPT_DIR/local-spark-sql.sh" \
      "INSERT OVERWRITE openhouse.db1.$TABLE VALUES ('$i', 'row$i')" > /dev/null
    printf "  $TABLE: insert %d/%d\r" "$i" "$WRITES"
  done
  echo ""

  TBL_JSON=$(curl -sf -H "Authorization: Bearer $TOKEN" \
    "$TABLES_API/v1/databases/db1/tables/$TABLE")
  TABLE_LOC=$(dirname "$(echo "$TBL_JSON" | jq -r '.tableLocation')")
  TABLE_UUID=$(echo "$TBL_JSON" | jq -r '.tableUUID')
  echo "  $TABLE -> $TABLE_LOC ($WRITES snapshots, $ORPHANS will become orphans)"
  echo "  $TABLE uuid=$TABLE_UUID"
  echo "$TABLE=$TABLE_LOC" >> /tmp/demo_ofd_locs.txt
  echo "$TABLE=$TABLE_UUID" >> /tmp/demo_ofd_uuids.txt
done

STATS_COUNT=$(curl -sf "$OPT_API/v1/optimizer/stats?limit=100" | jq 'length')
[ "$STATS_COUNT" -ge "$TABLE_COUNT" ] \
  || { echo "FAIL: expected $TABLE_COUNT stats rows, got $STATS_COUNT"; exit 1; }
echo "PASS: $STATS_COUNT stats rows posted by Tables Service on-commit hook"

echo ""
echo "=== [2/4] Expire old snapshots (creates orphan data files) ==="
kill_idle_session

EXPIRE_JOBS=""
for entry in $TABLES; do
  TABLE="${entry%%:*}"
  BODY=$(jq -n --arg n "demo-expire-$TABLE" --arg t "db1.$TABLE" \
    '{jobName:$n, clusterId:"LocalHadoopCluster",
      jobConf:{jobType:"SNAPSHOTS_EXPIRATION",
        args:["--tableName",$t,"--maxAge","1","--granularity","days","--versions","1"]}}')
  JOB_ID=$(curl -sf -X POST "$JOBS_API" -H "Content-Type: application/json" -d "$BODY" \
    | jq -r '.jobId')
  [ -n "$JOB_ID" ] || { echo "FAIL: could not submit expiration job for $TABLE"; exit 1; }
  echo "  $TABLE: submitted $JOB_ID"
  EXPIRE_JOBS="$EXPIRE_JOBS $JOB_ID"
done

for JOB_ID in $EXPIRE_JOBS; do
  wait_for_job "$JOB_ID" "snapshot-expiration" 180
  echo "  $JOB_ID: SUCCEEDED"
done

while IFS='=' read -r TABLE TABLE_LOC; do
  COUNT=$(docker exec local.namenode \
    hdfs dfs -ls -R "$TABLE_LOC/data/" 2>/dev/null | grep -c "\.orc" || echo "0")
  [ "$COUNT" -ge 2 ] \
    || { echo "FAIL: $TABLE has $COUNT files after expiration, expected >= 2"; exit 1; }
  echo "  $TABLE: $COUNT files on HDFS ($((COUNT-1)) orphans + 1 live)"
done < /tmp/demo_ofd_locs.txt

echo ""
echo "=== [3/4] Wait for analyzer → scheduler → SCHEDULED ==="
echo "  (analyzer and scheduler both run every ~30s; PENDING is transient)"
wait_for_count "$OPT_API/v1/optimizer/operations?status=SCHEDULED&limit=100" \
  "$TABLE_COUNT" "SCHEDULED ops" "$(($ANALYZER_WAIT_SECS + $SCHEDULER_WAIT_SECS))"
echo ""
echo "PASS: $TABLE_COUNT operations SCHEDULED"

echo ""
echo "=== [4/4] (skip) — SCHEDULED is the SUCCESS-readiness signal ==="

echo ""
echo "Setup complete. Batched-OFD Spark jobs have been launched."
echo "Run ./demo-check.sh to verify SUCCESS in history + orphan files deleted."

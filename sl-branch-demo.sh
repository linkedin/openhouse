#!/usr/bin/env bash
# =============================================================================
# StorageLocation Branch + Rollback Demo
# =============================================================================
# Flow:
#   1. Create table, insert row, verify
#   2. Record pre-migration snapshot ("create branch")
#   3. Migrate storage location to v2
#   4. Verify select
#   5. Insert row, verify
#   6. Roll back with data movement (rollback + cherrypick)
#   7. Verify all data present
#   8. Insert row, verify (lands at original location)
# =============================================================================
set -euo pipefail

TOKEN=$(cat /Users/mkuchenb/code/openhouse/services/common/src/main/resources/dummy.token)
LIVY_URL="http://localhost:9003"
TABLES_URL="http://localhost:8000"
HTS_URL="http://localhost:8001"
DB="demo_db"
TABLE="sl_branch"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
pass() { echo -e "${GREEN}[PASS]${NC}  $*"; }
fail() { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }
step() { echo -e "\n${YELLOW}===== $* =====${NC}"; }

# ---------------------------------------------------------------------------
# Livy helpers
# ---------------------------------------------------------------------------
SID=""
trap '[[ -n "$SID" ]] && curl -s -X DELETE "$LIVY_URL/sessions/$SID" > /dev/null 2>&1 || true' EXIT

livy_start() {
  log "Creating Livy session..."
  local body
  body=$(cat <<'ENDJSON'
{"kind":"spark","conf":{"spark.jars":"local:/opt/spark/openhouse-spark-runtime_2.12-latest-all.jar","spark.jars.packages":"org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.2.0","spark.sql.extensions":"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions","spark.sql.catalog.openhouse":"org.apache.iceberg.spark.SparkCatalog","spark.sql.catalog.openhouse.catalog-impl":"com.linkedin.openhouse.spark.OpenHouseCatalog","spark.sql.catalog.openhouse.uri":"http://openhouse-tables:8080","spark.sql.catalog.openhouse.cluster":"LocalHadoopCluster"}}
ENDJSON
  )
  body=$(echo "$body" | jq --arg t "$TOKEN" '.conf["spark.sql.catalog.openhouse.auth-token"] = $t')
  local resp
  resp=$(curl -s -X POST "$LIVY_URL/sessions" -H 'Content-Type: application/json' -d "$body")
  SID=$(echo "$resp" | jq -r '.id')
  while true; do
    sleep 2
    local state
    state=$(curl -s "$LIVY_URL/sessions/$SID" | jq -r '.state')
    [[ "$state" == "idle" ]] && break
    [[ "$state" == "dead" || "$state" == "error" ]] && fail "Livy session failed ($state)"
  done
  pass "Session $SID ready"
}

livy_run() {
  local code="$1"
  log "Spark: ${code:0:120}..."
  local resp
  resp=$(curl -s -X POST "$LIVY_URL/sessions/$SID/statements" -H 'Content-Type: application/json' \
    -d "$(jq -n --arg c "$code" '{code:$c}')")
  local stmt_id
  stmt_id=$(echo "$resp" | jq -r '.id')
  while true; do
    sleep 2
    local st
    st=$(curl -s "$LIVY_URL/sessions/$SID/statements/$stmt_id")
    local state
    state=$(echo "$st" | jq -r '.state')
    if [[ "$state" == "available" || "$state" == "error" ]]; then
      local status
      status=$(echo "$st" | jq -r '.output.status')
      if [[ "$status" == "error" ]]; then
        echo -e "${RED}  Error: $(echo "$st" | jq -r '.output.evalue')${NC}"
        return 1
      fi
      echo "$st" | jq -r '.output.data["text/plain"] // empty'
      return 0
    fi
  done
}

livy_sql() { livy_run "spark.sql(\"$1\").show(false)"; }

get_latest_snapshot() {
  livy_run "println(spark.sql(\"SELECT snapshot_id FROM openhouse.${DB}.${TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1\").collect()(0).getLong(0))" \
    | grep -oE '^[0-9]+$' | tail -1
}

# ---------------------------------------------------------------------------
# Step 1: Create table + insert
# ---------------------------------------------------------------------------
step "Step 1: Create table"

livy_start

livy_sql "DROP TABLE IF EXISTS openhouse.${DB}.${TABLE}"
livy_sql "CREATE TABLE openhouse.${DB}.${TABLE} (id INT, step STRING) USING iceberg"

step "Step 2: Insert row"
livy_sql "INSERT INTO openhouse.${DB}.${TABLE} VALUES (1, 'inserted before migration')"

step "Step 3: Verify"
livy_sql "SELECT * FROM openhouse.${DB}.${TABLE}"

# ---------------------------------------------------------------------------
# Step 4: Create branch (record snapshot)
# ---------------------------------------------------------------------------
step "Step 4: Create branch (record pre-migration snapshot)"

PRE_SNAP=$(get_latest_snapshot)
pass "Pre-migration snapshot: $PRE_SNAP"

table_json=$(curl -s -H "Authorization: Bearer $TOKEN" "$TABLES_URL/v1/databases/$DB/tables/$TABLE")
ORIG_SL_ID=$(echo "$table_json" | jq -r '.storageLocations[0].storageLocationId')
TABLE_UUID=$(echo "$table_json" | jq -r '.tableUUID')
log "Original storageLocationId: $ORIG_SL_ID"
log "Original tableLocation: $(echo "$table_json" | jq -r '.tableLocation')"

# ---------------------------------------------------------------------------
# Step 5: Migrate
# ---------------------------------------------------------------------------
step "Step 5: Migrate to new storage location"

NEW_URI="hdfs://namenode:9000/data/openhouse/${DB}/${TABLE}_v2"
log "Registering: $NEW_URI"
sl_resp=$(curl -s -X POST "$HTS_URL/hts/storageLocations" \
  -H 'Content-Type: application/json' \
  -d "{\"uri\":\"${NEW_URI}\"}")
NEW_SL_ID=$(echo "$sl_resp" | jq -r '.storageLocationId')
pass "New storageLocationId: $NEW_SL_ID"

curl -s -X POST "$HTS_URL/hts/storageLocations/link?tableUuid=$TABLE_UUID&storageLocationId=$NEW_SL_ID" > /dev/null
curl -s -X PATCH "$TABLES_URL/v1/databases/$DB/tables/$TABLE/storageLocation" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"storageLocationId\":\"$NEW_SL_ID\"}" > /dev/null
pass "Table migrated to v2"

# ---------------------------------------------------------------------------
# Step 6: Verify after migration
# ---------------------------------------------------------------------------
step "Step 6: Verify after migration"
livy_run "spark.sql(\"REFRESH TABLE openhouse.${DB}.${TABLE}\")" > /dev/null 2>&1 || true
livy_sql "SELECT * FROM openhouse.${DB}.${TABLE}"

# ---------------------------------------------------------------------------
# Step 7: Insert after migration + verify
# ---------------------------------------------------------------------------
step "Step 7: Insert after migration"
livy_sql "INSERT INTO openhouse.${DB}.${TABLE} VALUES (2, 'inserted after migration')"
livy_sql "SELECT * FROM openhouse.${DB}.${TABLE}"

POST_SNAP=$(get_latest_snapshot)
pass "Post-migration snapshot: $POST_SNAP"

# ---------------------------------------------------------------------------
# Step 8: Roll back with data movement
# ---------------------------------------------------------------------------
step "Step 8: Roll back with data movement"

log "Patching storage location back to original..."
curl -s -X PATCH "$TABLES_URL/v1/databases/$DB/tables/$TABLE/storageLocation" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"storageLocationId\":\"$ORIG_SL_ID\"}" > /dev/null
pass "Storage location restored to original"

log "Rolling back to pre-migration snapshot..."
livy_run "spark.sql(\"REFRESH TABLE openhouse.${DB}.${TABLE}\")" > /dev/null 2>&1 || true
livy_run "spark.sql(\"CALL openhouse.system.rollback_to_snapshot('${DB}.${TABLE}', ${PRE_SNAP})\")"
pass "Rolled back to snapshot $PRE_SNAP"

log "Cherry-picking post-migration data..."
livy_run "spark.sql(\"CALL openhouse.system.cherrypick_snapshot('${DB}.${TABLE}', ${POST_SNAP})\")"
pass "Cherry-picked snapshot $POST_SNAP"

# ---------------------------------------------------------------------------
# Step 9: Verify after rollback
# ---------------------------------------------------------------------------
step "Step 9: Verify after rollback (should have both rows)"
livy_sql "SELECT * FROM openhouse.${DB}.${TABLE}"

# ---------------------------------------------------------------------------
# Step 10: Insert after rollback + verify
# ---------------------------------------------------------------------------
step "Step 10: Insert after rollback"
livy_sql "INSERT INTO openhouse.${DB}.${TABLE} VALUES (3, 'inserted after rollback')"
livy_sql "SELECT * FROM openhouse.${DB}.${TABLE}"

# Show final state
step "Final state"
table_json=$(curl -s -H "Authorization: Bearer $TOKEN" "$TABLES_URL/v1/databases/$DB/tables/$TABLE")
echo "$table_json" | jq '{tableLocation, storageLocations}'

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Demo complete!${NC}"
echo -e "${GREEN}========================================${NC}"

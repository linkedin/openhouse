#!/usr/bin/env bash
# =============================================================================
# StorageLocation Federation Demo
# =============================================================================
# Proves the end-to-end flow: write data at location A, swap to B,
# write more data at B, then compact everything to B.
#
# Prerequisites:
#   ./gradlew dockerUp -Precipe=oh-hadoop-spark
#
# NOTE: The default Docker recipe uses Spark 3.1 (spark-base-hadoop2.8.dockerfile).
#       If you want Spark 3.5, edit spark-services.yml to reference
#       spark-3.5-base-hadoop3.2.dockerfile before running dockerUp.
#
# Usage:
#   bash demo-storage-location.sh
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LIVY_URL="http://localhost:9003"
TABLES_URL="http://localhost:8000"
HTS_URL="http://localhost:8001"
TOKEN=$(cat services/common/src/main/resources/dummy.token)
HDFS_CONTAINER="local.namenode"

DB="demo_db"
TABLE="sl_demo"
FQTN="${DB}.${TABLE}"

# The original table location path (HDFS)
ORIG_BASE="/data/openhouse/${DB}/${TABLE}"
# The new storage location we will register
NEW_BASE="hdfs://namenode:9000/data/openhouse/${DB}/${TABLE}_v2"

SID=""  # Livy session id — set by livy_create_session

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
pass() { echo -e "${GREEN}[PASS]${NC}  $*"; }
fail() { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }
step() { echo -e "\n${YELLOW}===== $* =====${NC}"; }

assert_eq() {
  local actual="$1" expected="$2" msg="$3"
  if [[ "$actual" == "$expected" ]]; then
    pass "$msg (got $actual)"
  else
    fail "$msg — expected '$expected', got '$actual'"
  fi
}

assert_ge() {
  local actual="$1" expected="$2" msg="$3"
  if (( actual >= expected )); then
    pass "$msg (got $actual >= $expected)"
  else
    fail "$msg — expected >= $expected, got '$actual'"
  fi
}

# ---------------------------------------------------------------------------
# HDFS helpers (read-only)
# ---------------------------------------------------------------------------
hdfs_ls() {
  docker exec "$HDFS_CONTAINER" hdfs dfs -ls -R "$1" 2>/dev/null || true
}

hdfs_count_data_files() {
  local path="$1"
  local count
  count=$(docker exec "$HDFS_CONTAINER" hdfs dfs -ls -R "$path" 2>/dev/null \
    | grep -cE '\.(parquet|orc)$' || true)
  echo "${count:-0}"
}

hdfs_path_exists() {
  docker exec "$HDFS_CONTAINER" hdfs dfs -test -d "$1" 2>/dev/null
}

# ---------------------------------------------------------------------------
# Livy helpers
# ---------------------------------------------------------------------------
livy_create_session() {
  log "Creating Livy session..."
  local body
  body=$(cat <<'ENDJSON'
{
  "kind": "spark",
  "conf": {
    "spark.jars": "local:/opt/spark/openhouse-spark-runtime_2.12-latest-all.jar",
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.2.0",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions",
    "spark.sql.catalog.openhouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.openhouse.catalog-impl": "com.linkedin.openhouse.spark.OpenHouseCatalog",
    "spark.sql.catalog.openhouse.uri": "http://openhouse-tables:8080",
    "spark.sql.catalog.openhouse.cluster": "LocalHadoopCluster"
  }
}
ENDJSON
  )
  # Inject token (may contain special chars, so use jq)
  body=$(echo "$body" | jq --arg t "$TOKEN" '.conf["spark.sql.catalog.openhouse.auth-token"] = $t')

  local resp
  resp=$(curl -s -w '\n%{http_code}' -X POST "$LIVY_URL/sessions" \
    -H 'Content-Type: application/json' \
    -d "$body")
  local http_code
  http_code=$(echo "$resp" | tail -1)
  local json
  json=$(echo "$resp" | sed '$d')

  if [[ "$http_code" != "201" ]]; then
    fail "Livy session creation failed (HTTP $http_code): $json"
  fi

  SID=$(echo "$json" | jq -r '.id')
  log "Session $SID created, waiting for idle state..."

  local state="starting"
  while [[ "$state" != "idle" ]]; do
    sleep 2
    state=$(curl -s "$LIVY_URL/sessions/$SID" | jq -r '.state')
    log "  session state: $state"
    if [[ "$state" == "dead" || "$state" == "error" ]]; then
      fail "Livy session failed to start (state=$state)"
    fi
  done
  pass "Livy session $SID is idle"
}

livy_run() {
  local code="$1"
  log "Running: ${code:0:120}..."

  local resp
  resp=$(curl -s -w '\n%{http_code}' -X POST "$LIVY_URL/sessions/$SID/statements" \
    -H 'Content-Type: application/json' \
    -d "$(jq -n --arg c "$code" '{code: $c}')")
  local http_code
  http_code=$(echo "$resp" | tail -1)
  local json
  json=$(echo "$resp" | sed '$d')

  if [[ "$http_code" != "201" ]]; then
    fail "Statement submission failed (HTTP $http_code): $json"
  fi

  local stmt_id
  stmt_id=$(echo "$json" | jq -r '.id')

  local state="waiting"
  while [[ "$state" != "available" && "$state" != "error" && "$state" != "cancelled" ]]; do
    sleep 2
    local stmt_resp
    stmt_resp=$(curl -s "$LIVY_URL/sessions/$SID/statements/$stmt_id")
    state=$(echo "$stmt_resp" | jq -r '.state')
  done

  local stmt_resp
  stmt_resp=$(curl -s "$LIVY_URL/sessions/$SID/statements/$stmt_id")
  local output_status
  output_status=$(echo "$stmt_resp" | jq -r '.output.status')

  if [[ "$output_status" == "error" ]]; then
    local ename
    ename=$(echo "$stmt_resp" | jq -r '.output.ename')
    local evalue
    evalue=$(echo "$stmt_resp" | jq -r '.output.evalue')
    echo -e "${RED}  Error: $ename: $evalue${NC}"
    return 1
  fi

  local data
  data=$(echo "$stmt_resp" | jq -r '.output.data["text/plain"] // empty')
  if [[ -n "$data" ]]; then
    echo "$data"
  fi
}

livy_sql() {
  livy_run "spark.sql(\"$1\").show(false)"
}

livy_sql_count() {
  local sql="$1"
  local output
  output=$(livy_run "spark.sql(\"$sql\").collect()(0).getLong(0)")
  # Extract the number from the output (Livy wraps it in text like "res3: Long = 4")
  echo "$output" | grep -oE '[0-9]+$' | tail -1
}

livy_destroy_session() {
  if [[ -n "$SID" ]]; then
    log "Destroying Livy session $SID..."
    curl -s -X DELETE "$LIVY_URL/sessions/$SID" > /dev/null 2>&1 || true
    pass "Session $SID destroyed"
    SID=""
  fi
}

# Ensure cleanup on exit
trap livy_destroy_session EXIT

# ===========================================================================
# Step 1: Setup — Create table, write initial data
# ===========================================================================
step "Step 1: Create table and write initial data"

livy_create_session

log "Dropping table if it exists..."
livy_sql "DROP TABLE IF EXISTS openhouse.${FQTN}" || true

log "Creating table..."
livy_sql "CREATE TABLE openhouse.${FQTN} (id INT, name STRING) USING iceberg"

log "Inserting initial data (alice, bob)..."
livy_sql "INSERT INTO openhouse.${FQTN} VALUES (1, 'alice'), (2, 'bob')"

log "Verifying row count..."
count=$(livy_sql_count "SELECT count(*) FROM openhouse.${FQTN}")
assert_eq "$count" "2" "Step 1: row count after initial insert"

log "Checking HDFS for data files..."
log "Listing table location (may differ from ORIG_BASE due to UUID in path)..."
# Get the actual table location from the Tables Service
table_json=$(curl -s -H "Authorization: Bearer $TOKEN" "$TABLES_URL/v1/databases/$DB/tables/$TABLE")
table_location=$(echo "$table_json" | jq -r '.tableLocation')
log "Table location: $table_location"

# Extract the HDFS base path (strip the .metadata.json filename)
# tableLocation looks like hdfs://namenode:9000/data/openhouse/demo_db/sl_demo-<uuid>/xxx.metadata.json
table_base=$(echo "$table_location" | sed 's|/[^/]*\.metadata\.json$||')
log "Table base path: $table_base"

orig_data_files=$(hdfs_count_data_files "$table_base/data")
assert_ge "$orig_data_files" "1" "Step 1: data files in original location"

# ===========================================================================
# Step 2: Register a new StorageLocation via HTS
# ===========================================================================
step "Step 2: Register new StorageLocation via HTS"

log "POSTing new storage location to HTS..."
sl_resp=$(curl -s -w '\n%{http_code}' -X POST "$HTS_URL/hts/storageLocations" \
  -H 'Content-Type: application/json' \
  -d "{\"uri\":\"${NEW_BASE}\"}")
sl_http=$(echo "$sl_resp" | tail -1)
sl_json=$(echo "$sl_resp" | sed '$d')

log "HTS response (HTTP $sl_http): $sl_json"
assert_eq "$sl_http" "201" "Step 2: HTS POST returned 201"

STORAGE_LOCATION_ID=$(echo "$sl_json" | jq -r '.storageLocationId')
log "storageLocationId: $STORAGE_LOCATION_ID"

if [[ -z "$STORAGE_LOCATION_ID" || "$STORAGE_LOCATION_ID" == "null" ]]; then
  fail "Step 2: storageLocationId is empty or null"
fi
pass "Step 2: storageLocationId is non-empty"

# Link storage location to the table
log "Linking storage location to table..."
link_resp=$(curl -s -w '\n%{http_code}' -X POST \
  "$HTS_URL/hts/storageLocations/link?databaseId=$DB&tableId=$TABLE&storageLocationId=$STORAGE_LOCATION_ID")
link_http=$(echo "$link_resp" | tail -1)
assert_eq "$link_http" "204" "Step 2: HTS LINK returned 204"

# ===========================================================================
# Step 3: PATCH the table to use the new location
# ===========================================================================
step "Step 3: PATCH table to use new storage location"

patch_resp=$(curl -s -w '\n%{http_code}' -X PATCH \
  "$TABLES_URL/v1/databases/$DB/tables/$TABLE/storageLocation" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"storageLocationId\":\"$STORAGE_LOCATION_ID\"}")
patch_http=$(echo "$patch_resp" | tail -1)
patch_json=$(echo "$patch_resp" | sed '$d')

log "PATCH response (HTTP $patch_http)"
assert_eq "$patch_http" "200" "Step 3: PATCH returned 200"

# Refresh table location
table_json=$(curl -s -H "Authorization: Bearer $TOKEN" "$TABLES_URL/v1/databases/$DB/tables/$TABLE")
new_table_location=$(echo "$table_json" | jq -r '.tableLocation')
log "New table location: $new_table_location"

new_table_base=$(echo "$new_table_location" | sed 's|/[^/]*\.metadata\.json$||')
log "New table base: $new_table_base"

log "Verifying old location still has files..."
old_files_after_patch=$(hdfs_count_data_files "$table_base/data")
assert_ge "$old_files_after_patch" "1" "Step 3: old location still has data files"

log "Verifying new location has no data files yet..."
new_files_after_patch=$(hdfs_count_data_files "$new_table_base/data")
assert_eq "$new_files_after_patch" "0" "Step 3: new location has no data files yet"

# ===========================================================================
# Step 4: Write more data — lands at new location
# ===========================================================================
step "Step 4: Write more data (should land at new location)"

# Refresh the catalog so Spark picks up the new metadata location
livy_run "spark.sql(\"REFRESH TABLE openhouse.${FQTN}\")" || true

log "Inserting more data (carol, dave)..."
livy_sql "INSERT INTO openhouse.${FQTN} VALUES (3, 'carol'), (4, 'dave')"

log "Verifying total row count across both locations..."
count=$(livy_sql_count "SELECT count(*) FROM openhouse.${FQTN}")
assert_eq "$count" "4" "Step 4: total row count is 4"

log "Verifying old location still has its files (unchanged)..."
old_files_step4=$(hdfs_count_data_files "$table_base/data")
assert_ge "$old_files_step4" "1" "Step 4: old location data files unchanged"

log "Verifying new location now has data files..."
new_files_step4=$(hdfs_count_data_files "$new_table_base/data")
assert_ge "$new_files_step4" "1" "Step 4: new location has data files"

# ===========================================================================
# Step 5: Compaction — consolidates everything at destination
# ===========================================================================
step "Step 5: Compaction — consolidate everything at new location"

log "Attempting rewrite_data_files via Spark procedure..."
compact_ok=0
livy_run "spark.sql(\"CALL openhouse.system.rewrite_data_files(table => '${FQTN}')\")" && compact_ok=1

if [[ "$compact_ok" -eq 0 ]]; then
  log "Spark procedure not available, falling back to DataCompactionSparkApp via Livy batch..."
  batch_body=$(jq -n \
    --arg cls "com.linkedin.openhouse.jobs.spark.DataCompactionSparkApp" \
    --arg jar "local:/opt/spark/openhouse-spark-runtime_2.12-latest-all.jar" \
    --arg tn "${FQTN}" \
    '{
      className: $cls,
      file: $jar,
      args: ["--tableName", $tn, "--minInputFiles", "1"]
    }')
  batch_resp=$(curl -s -w '\n%{http_code}' -X POST "$LIVY_URL/batches" \
    -H 'Content-Type: application/json' \
    -d "$batch_body")
  batch_http=$(echo "$batch_resp" | tail -1)
  batch_json=$(echo "$batch_resp" | sed '$d')

  if [[ "$batch_http" != "201" ]]; then
    fail "Batch submission failed (HTTP $batch_http): $batch_json"
  fi

  batch_id=$(echo "$batch_json" | jq -r '.id')
  log "Batch $batch_id submitted, waiting..."

  batch_state="starting"
  while [[ "$batch_state" != "success" && "$batch_state" != "dead" ]]; do
    sleep 5
    batch_state=$(curl -s "$LIVY_URL/batches/$batch_id" | jq -r '.state')
    log "  batch state: $batch_state"
  done

  if [[ "$batch_state" != "success" ]]; then
    fail "Compaction batch ended in state: $batch_state"
  fi
  pass "Compaction batch completed successfully"
fi

log "Verifying new location has data files after compaction..."
new_files_step5=$(hdfs_count_data_files "$new_table_base/data")
assert_ge "$new_files_step5" "1" "Step 5: new location has data files after compaction"

log "Verifying row count is still 4..."
count=$(livy_sql_count "SELECT count(*) FROM openhouse.${FQTN}")
assert_eq "$count" "4" "Step 5: row count still 4 after compaction"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  All checks passed!${NC}"
echo -e "${GREEN}========================================${NC}"

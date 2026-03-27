#!/usr/bin/env bash
# =============================================================================
# alter_table_update_storagelocation.sh
#
# Migrates an OpenHouse table to a new storage location.
# Allocates a new SL, derives the URI from the table's existing path,
# and swaps the table to the new location.
#
# Usage:
#   ./alter_table_update_storagelocation.sh <database.table>
#
# Example:
#   ./alter_table_update_storagelocation.sh demo_db.sl_test
# =============================================================================
set -euo pipefail

FULL_NAME="${1:?Usage: $0 <database.table>}"

DB="${FULL_NAME%%.*}"
TABLE="${FULL_NAME#*.}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOKEN="${TOKEN:-$(cat "$SCRIPT_DIR/services/common/src/main/resources/dummy.token")}"
TABLES_URL="${TABLES_URL:-http://localhost:8000}"
HTS_URL="${HTS_URL:-http://localhost:8001}"

# 1. Get current table info to derive base path
TABLE_JSON=$(curl -s -H "Authorization: Bearer $TOKEN" \
  "$TABLES_URL/v1/databases/$DB/tables/$TABLE")

ORIG_SL_ID=$(echo "$TABLE_JSON" | jq -r '.storageLocations[0].storageLocationId')
TABLE_LOCATION=$(echo "$TABLE_JSON" | jq -r '.tableLocation')

# Derive base prefix: strip the table-dir and metadata filename
# e.g. hdfs://namenode:9000/data/openhouse/demo_db/sl_test-UUID/meta.json
#   -> hdfs://namenode:9000/data/openhouse/demo_db
BASE_PREFIX=$(echo "$TABLE_LOCATION" | sed 's|/[^/]*/[^/]*$||')

echo "Original SL:  $ORIG_SL_ID"

# 2. Allocate a new storage location
NEW_SL_ID=$(curl -s -X POST "$HTS_URL/hts/storageLocations/allocate" \
  -H 'Content-Type: application/json' \
  | jq -r '.storageLocationId')

NEW_URI="$BASE_PREFIX/$TABLE-$NEW_SL_ID"
echo "New SL:       $NEW_SL_ID"
echo "New URI:      $NEW_URI"

# 3. Set the new SL's URI
curl -s -X PATCH "$HTS_URL/hts/storageLocations/$NEW_SL_ID" \
  -H 'Content-Type: application/json' \
  -d "{\"uri\":\"$NEW_URI\"}" > /dev/null

# 4. Link the new storage location to the table
curl -s -X POST \
  "$HTS_URL/hts/storageLocations/link?databaseId=$DB&tableId=$TABLE&storageLocationId=$NEW_SL_ID" > /dev/null

# 5. Swap the table's active storage location
curl -s -X PATCH "$TABLES_URL/v1/databases/$DB/tables/$TABLE/storageLocation" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"storageLocationId\":\"$NEW_SL_ID\"}" > /dev/null

echo ""
echo "Migrated $DB.$TABLE: $ORIG_SL_ID -> $NEW_SL_ID"

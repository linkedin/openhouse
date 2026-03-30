#!/usr/bin/env bash
set -euo pipefail

TOKEN=$(cat /Users/mkuchenb/code/openhouse/services/common/src/main/resources/dummy.token)
TABLES_URL="http://localhost:8000"
HTS_URL="http://localhost:8001"
DB="demo_db"
TABLE="sl_demo"

# Get current table info
table_json=$(curl -s -H "Authorization: Bearer $TOKEN" "$TABLES_URL/v1/databases/$DB/tables/$TABLE")
echo "Current tableLocation: $(echo "$table_json" | jq -r '.tableLocation')"
echo "Current storageLocations: $(echo "$table_json" | jq '.storageLocations')"

# Register a new storage location
NEW_BASE="hdfs://namenode:9000/data/openhouse/${DB}/${TABLE}_v2"
echo ""
echo "Registering new storage location: $NEW_BASE"
sl_resp=$(curl -s -X POST "$HTS_URL/hts/storageLocations" \
  -H 'Content-Type: application/json' \
  -d "{\"uri\":\"${NEW_BASE}\"}")
SL_ID=$(echo "$sl_resp" | jq -r '.storageLocationId')
echo "New storageLocationId: $SL_ID"

# Link it to the table
echo "Linking to table..."
curl -s -X POST "$HTS_URL/hts/storageLocations/link?databaseId=$DB&tableId=$TABLE&storageLocationId=$SL_ID"

# Patch the table to use the new location
echo "Patching table to use new location..."
patch_resp=$(curl -s -X PATCH \
  "$TABLES_URL/v1/databases/$DB/tables/$TABLE/storageLocation" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"storageLocationId\":\"$SL_ID\"}")
echo ""
echo "Updated table:"
echo "$patch_resp" | jq '{tableLocation, storageLocations}'

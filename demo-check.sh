#!/usr/bin/env bash
# demo-check.sh — Run after demo-setup.sh. Verifies SUCCESS history rows and
# shows HDFS file counts per table.
set -e

OPT_API="http://localhost:8005"

if [ ! -f /tmp/demo_ofd_locs.txt ] || [ ! -f /tmp/demo_ofd_uuids.txt ]; then
  echo "ERROR: /tmp/demo_ofd_locs.txt or /tmp/demo_ofd_uuids.txt not found — run demo-setup.sh first"
  exit 1
fi

echo "=== HDFS data files after OFD ==="
while IFS='=' read -r TABLE TABLE_LOC; do
  FILE_COUNT=$(docker exec local.namenode \
    hdfs dfs -ls -R "$TABLE_LOC/data/" 2>/dev/null \
    | grep -c "\.orc" || echo "0")
  echo "  $TABLE: $FILE_COUNT files remaining"
done < /tmp/demo_ofd_locs.txt

echo ""
echo "=== Operation history per table ==="
while IFS='=' read -r TABLE TABLE_UUID; do
  echo "  --- $TABLE ($TABLE_UUID) ---"
  curl -sf "$OPT_API/v1/optimizer/operations-history/$TABLE_UUID?limit=10" \
    | jq '.[] | {status, orphanFilesDeleted, orphanBytesDeleted, errorMessage, completedAt}'
done < /tmp/demo_ofd_uuids.txt

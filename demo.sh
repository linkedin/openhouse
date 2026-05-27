#!/usr/bin/env bash
# demo.sh — Full end-to-end optimizer demo from a clean docker stack.
#
# Prerequisites: ./gradlew dockerUp  (stack must be healthy)
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OPT_API="http://localhost:8005"

TABLE_COUNT=3
SUCCESS_WAIT_SECS=300

bash "$SCRIPT_DIR/demo-setup.sh"

echo ""
echo "=== [Wait] OFD Spark jobs to complete (up to 5 min) ==="
for i in $(seq 1 30); do
  TOTAL_HISTORY=0
  SUCCESS_HISTORY=0
  if [ -f /tmp/demo_ofd_uuids.txt ]; then
    while IFS='=' read -r _ UUID; do
      LATEST=$(curl -sf "$OPT_API/v1/optimizer/operations-history/$UUID?limit=1" \
        | jq -r '.[0].status // empty' 2>/dev/null || echo "")
      [ -n "$LATEST" ] && TOTAL_HISTORY=$((TOTAL_HISTORY + 1))
      [ "$LATEST" = "SUCCESS" ] && SUCCESS_HISTORY=$((SUCCESS_HISTORY + 1))
    done < /tmp/demo_ofd_uuids.txt
  fi
  [ "$SUCCESS_HISTORY" -ge "$TABLE_COUNT" ] && break
  echo "  $i/30: $SUCCESS_HISTORY SUCCESS / $TOTAL_HISTORY total history rows..."
  sleep 10
done

[ "$SUCCESS_HISTORY" -ge "$TABLE_COUNT" ] \
  || { echo "FAIL: only $SUCCESS_HISTORY/$TABLE_COUNT operations reached SUCCESS"; bash "$SCRIPT_DIR/demo-check.sh"; exit 1; }

echo ""
echo "PASS: all $TABLE_COUNT operations completed with SUCCESS"
echo ""
bash "$SCRIPT_DIR/demo-check.sh"

#!/usr/bin/env bash
#
# enable-hdfs-debug.sh — Enable HDFS/Iceberg debug logging on a local OpenHouse cluster.
#
# Targets the tables-service running via:
#   ./gradlew dockerUp -Precipe=oh-hadoop
#
# Sets DEBUG on the key Hadoop, HDFS, and Iceberg loggers via the Spring Boot
# Actuator /actuator/loggers endpoint at runtime. No restart required.
#
# Usage:
#   ./scripts/enable-hdfs-debug.sh           # Enable DEBUG logging
#   ./scripts/enable-hdfs-debug.sh --undo    # Reset to INFO
#
# To target a non-default host/port:
#   ./scripts/enable-hdfs-debug.sh --host localhost --port 8000
#
# To set a specific logger manually:
#   curl -X POST http://localhost:8000/actuator/loggers/org.apache.hadoop.hdfs.DFSClient \
#     -H 'Content-Type: application/json' \
#     -d '{"configuredLevel": "DEBUG"}'
#
# To check the current effective level of a logger:
#   curl -s http://localhost:8000/actuator/loggers/org.apache.hadoop.hdfs.DFSClient \
#     | python3 -m json.tool
#
# To reset a logger to its inherited level:
#   curl -X POST http://localhost:8000/actuator/loggers/org.apache.hadoop.hdfs.DFSClient \
#     -H 'Content-Type: application/json' \
#     -d '{"configuredLevel": null}'
#
set -euo pipefail

HOST="localhost"
PORT="8000"
UNDO=false

LOGGERS=(
  # Filesystem layer — logs on FileSystem init and mount table refresh
  "org.apache.hadoop.fs"
  # NameNode HA proxy provider — covers all variants (IPFailover, ObserverRead, RequestHedging)
  "org.apache.hadoop.hdfs.server.namenode.ha"
  # IPC/RPC layer — connection establishment and retry events
  "org.apache.hadoop.ipc.Client"
  "org.apache.hadoop.io.retry.RetryInvocationHandler"
  # HDFS client data path — silent on healthy ops; verbose on block errors/retries
  "org.apache.hadoop.hdfs.DFSClient"
  "org.apache.hadoop.hdfs.DFSInputStream"
  "org.apache.hadoop.hdfs.DFSOutputStream"
  "org.apache.hadoop.hdfs.DataStreamer"
  # Iceberg metadata operations — table refresh, commit, CAS
  "org.apache.iceberg"
)

usage() {
  cat <<'EOF'
Usage: enable-hdfs-debug.sh [OPTIONS]

Enable or disable HDFS/Iceberg debug logging on a local OpenHouse cluster.

Options:
  --host HOST   tables-service host (default: localhost)
  --port PORT   tables-service port (default: 8000)
  --undo        Reset loggers to inherited level
  -h, --help    Show this help
EOF
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)    HOST="$2"; shift 2 ;;
    --port)    PORT="$2"; shift 2 ;;
    --undo)    UNDO=true; shift ;;
    -h|--help) usage ;;
    *)         echo "Unknown option: $1"; usage ;;
  esac
done

BASE_URL="http://${HOST}:${PORT}"

if ! curl -sf "${BASE_URL}/actuator/health" &>/dev/null; then
  echo "ERROR: tables-service not responding at ${BASE_URL}."
  echo "  Start the cluster: ./gradlew dockerUp -Precipe=oh-hadoop"
  exit 1
fi

if $UNDO; then
  echo "Resetting HDFS debug loggers at ${BASE_URL} ..."
  for logger in "${LOGGERS[@]}"; do
    rc=$(curl -s -o /dev/null -w '%{http_code}' \
      -X POST "${BASE_URL}/actuator/loggers/${logger}" \
      -H 'Content-Type: application/json' \
      -d '{"configuredLevel": null}')
    [[ "$rc" == "204" || "$rc" == "200" ]] && echo "  RESET  $logger" || echo "  FAILED $logger (HTTP $rc)"
  done
  echo ""
  echo "Done. Logging restored to defaults."
else
  echo "Enabling HDFS debug logging at ${BASE_URL} ..."
  echo ""
  FAILED=0
  for logger in "${LOGGERS[@]}"; do
    rc=$(curl -s -o /dev/null -w '%{http_code}' \
      -X POST "${BASE_URL}/actuator/loggers/${logger}" \
      -H 'Content-Type: application/json' \
      -d '{"configuredLevel": "DEBUG"}')
    if [[ "$rc" == "204" || "$rc" == "200" ]]; then
      echo "  DEBUG  $logger"
    else
      echo "  FAILED $logger (HTTP $rc)"
      FAILED=$((FAILED + 1))
    fi
  done
  echo ""
  [[ $FAILED -gt 0 ]] && echo "WARNING: $FAILED logger(s) failed to set."
  echo "HDFS debug logging active. Run table operations to generate log output."
  echo ""
  echo "When done: $0 --undo"
fi

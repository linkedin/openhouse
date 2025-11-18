#!/bin/bash
# Quick start command for Spark Shell with OpenHouse catalog
# Run this AFTER: ssh ltx1-holdemgw03.grid.linkedin.com && ksudo -e openhouse

# Usage: ./spark-shell-command.sh [your-name]
# Example: ./spark-shell-command.sh abhishek

ASSIGNEE=${1:-"test"}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="logs/${ASSIGNEE}"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/session_${TIMESTAMP}.log"

echo "Starting Spark Shell for OpenHouse Bug Bash..."
echo "Logs will be saved to: ${LOG_FILE}"
echo ""

spark-shell \
  --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse \
  --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse \
  2>/dev/null | tee "${LOG_FILE}"


#!/bin/bash
# Bug Bash Quick Start Script for LinkedIn OpenHouse Testing
# Run this ON THE GATEWAY after SSH and ksudo

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Check if spark-shell is available (i.e., we're on the gateway)
if ! command -v spark-shell &> /dev/null; then
  echo -e "${RED}════════════════════════════════════════${NC}"
  echo -e "${RED}ERROR: spark-shell not found!${NC}"
  echo -e "${RED}════════════════════════════════════════${NC}"
  echo ""
  echo -e "${YELLOW}This script must be run ON THE GATEWAY, not locally.${NC}"
  echo ""
  echo -e "${BOLD}Follow these steps:${NC}"
  echo ""
  echo -e "${YELLOW}1. SSH to the gateway:${NC}"
  echo "   ssh ltx1-holdemgw03.grid.linkedin.com"
  echo ""
  echo -e "${YELLOW}2. Clone/pull the bug bash branch:${NC}"
  echo "   git clone https://github.com/linkedin/openhouse.git"
  echo "   cd openhouse"
  echo "   git checkout bug-bash-wap-2024-11"
  echo "   cd bug-bash-wap"
  echo ""
  echo -e "${YELLOW}3. Authenticate:${NC}"
  echo "   ksudo -e openhouse"
  echo ""
  echo -e "${YELLOW}4. Run this script again:${NC}"
  echo "   ./start-testing.sh"
  echo ""
  exit 1
fi

echo -e "${BLUE}════════════════════════════════════════${NC}"
echo -e "${BOLD}Bug Bash: Quick Start${NC}"
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo ""

# Get assignee name
echo -e "${YELLOW}Enter your name (e.g., abhishek):${NC} "
read -r ASSIGNEE

if [ -z "$ASSIGNEE" ]; then
  echo -e "${RED}Error: Name cannot be empty${NC}"
  exit 1
fi

# Create log directory
LOG_DIR="logs/${ASSIGNEE}"
mkdir -p "$LOG_DIR"

# Generate timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/session_${TIMESTAMP}.log"

echo ""
echo -e "${GREEN}✓ Setup complete for: ${ASSIGNEE}${NC}"
echo -e "${GREEN}✓ Log directory: ${LOG_DIR}${NC}"
echo ""

echo -e "${BLUE}════════════════════════════════════════${NC}"
echo -e "${BOLD}Starting Spark Shell...${NC}"
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}Connecting to: ltx1-holdem-openhouse${NC}"
echo -e "${YELLOW}Log file: ${LOG_FILE}${NC}"
echo ""
echo -e "${GREEN}Press Ctrl+D or type :quit to exit spark-shell${NC}"
echo ""

# Small delay so user can read the message
sleep 2

# Launch spark-shell with logging
exec spark-shell \
  --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse \
  --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse \
  2>&1 | tee "$LOG_FILE"
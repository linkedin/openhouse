#!/bin/bash
# Bug Bash Quick Start Script for LinkedIn OpenHouse Testing
# Run this locally to get the commands you need

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m' # No Color

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

# Create log directory locally
LOG_DIR="logs/${ASSIGNEE}"
mkdir -p "$LOG_DIR"

# Generate timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo ""
echo -e "${GREEN}✓ Setup complete for: ${ASSIGNEE}${NC}"
echo ""

# Show their assignments
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo -e "${BOLD}Your Test Assignments${NC}"
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo ""
SQL_FILE=$(find results -name "sql-*-${ASSIGNEE}.md" 2>/dev/null | head -1)
JAVA_FILE=$(find results -name "java-*-${ASSIGNEE}.md" 2>/dev/null | head -1)

if [ -n "$SQL_FILE" ]; then
  echo -e "${GREEN}  ✓ ${SQL_FILE}${NC}"
fi
if [ -n "$JAVA_FILE" ]; then
  echo -e "${GREEN}  ✓ ${JAVA_FILE}${NC}"
fi
echo ""

# Show the commands to run
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo -e "${BOLD}Copy and Run These Commands:${NC}"
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}# Step 1: SSH to the gateway${NC}"
echo "ssh ltx1-holdemgw03.grid.linkedin.com"
echo ""
echo -e "${YELLOW}# Step 2: Authenticate${NC}"
echo "ksudo -e openhouse"
echo ""
echo -e "${YELLOW}# Step 3: Start spark-shell${NC}"
echo "spark-shell \\"
echo "  --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse \\"
echo "  --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse"
echo ""
echo -e "${GREEN}💡 Tip: Use Ctrl+D or type :quit to exit spark-shell${NC}"
echo ""
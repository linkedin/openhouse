#!/bin/bash
# Bug Bash Quick Start Script for LinkedIn OpenHouse Testing
# Generates all commands you need with proper paths

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Bug Bash: Quick Start Setup${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# Get assignee name
echo -e "${YELLOW}Enter your name (e.g., abhishek):${NC} "
read -r ASSIGNEE

if [ -z "$ASSIGNEE" ]; then
  echo -e "${RED}Error: Name cannot be empty${NC}"
  exit 1
fi

# Get absolute path to bug-bash-wap directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs/${ASSIGNEE}"
mkdir -p "$LOG_DIR"

# Generate timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/session_${TIMESTAMP}.log"

echo ""
echo -e "${GREEN}✓ Setup complete for: ${ASSIGNEE}${NC}"
echo -e "${GREEN}✓ Log directory: ${LOG_DIR}${NC}"
echo ""

# Create the gateway script
GATEWAY_SCRIPT="${LOG_DIR}/run-on-gateway.sh"
cat > "$GATEWAY_SCRIPT" << EOF
#!/bin/bash
# Run this script on the gateway after: ksudo -e openhouse

cd ${SCRIPT_DIR}

echo "Starting Spark Shell for ${ASSIGNEE}..."
echo "Logs will be saved to: ${LOG_FILE}"
echo ""

spark-shell \\
  --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse \\
  --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse \\
  2>/dev/null | tee "${LOG_FILE}"
EOF
chmod +x "$GATEWAY_SCRIPT"

echo -e "${BLUE}=========================================${NC}"
echo -e "${BOLD}Copy and Run These Commands:${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo -e "${YELLOW}# Step 1: SSH to gateway${NC}"
echo "ssh ltx1-holdemgw03.grid.linkedin.com"
echo ""
echo -e "${YELLOW}# Step 2: Authenticate${NC}"
echo "ksudo -e openhouse"
echo ""
echo -e "${YELLOW}# Step 3: Run the spark shell${NC}"
echo "cd ${SCRIPT_DIR}"
echo "./logs/${ASSIGNEE}/run-on-gateway.sh"
echo ""
echo -e "${GREEN}✓ Complete script saved to: ${GATEWAY_SCRIPT}${NC}"
echo ""

# Display test information
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Your Test Assignments${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo -e "Check your assignments in:"
echo -e "${YELLOW}assignments.md${NC}"
echo ""
echo -e "Your result files are located at:"
SQL_FILE=$(find results -name "sql-*-${ASSIGNEE}.md" 2>/dev/null | head -1)
JAVA_FILE=$(find results -name "java-*-${ASSIGNEE}.md" 2>/dev/null | head -1)

if [ -n "$SQL_FILE" ]; then
  echo -e "${YELLOW}  - ${SQL_FILE}${NC}"
fi
if [ -n "$JAVA_FILE" ]; then
  echo -e "${YELLOW}  - ${JAVA_FILE}${NC}"
fi
echo ""

# Quick reference
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Quick Reference${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo -e "${YELLOW}Enable WAP on a table:${NC}"
echo -e "  ALTER TABLE openhouse.d1.test_xxx SET TBLPROPERTIES ('write.wap.enabled'='true');"
echo ""
echo -e "${YELLOW}Create a branch:${NC}"
echo -e "  ALTER TABLE openhouse.d1.test_xxx CREATE BRANCH myBranch;"
echo ""
echo -e "${YELLOW}Insert to a branch:${NC}"
echo -e "  INSERT INTO openhouse.d1.test_xxx.branch_myBranch VALUES ('data');"
echo ""
echo -e "${YELLOW}View snapshots:${NC}"
echo -e "  SELECT * FROM openhouse.d1.test_xxx.snapshots;"
echo ""
echo -e "${YELLOW}View refs:${NC}"
echo -e "  SELECT * FROM openhouse.d1.test_xxx.refs;"
echo ""

# Tips
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Tips${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo -e "1. Use unique table names: ${YELLOW}test_sql01_${TIMESTAMP}${NC}"
echo -e "2. Copy commands to your result file as you execute them"
echo -e "3. Remember to clean up: ${YELLOW}DROP TABLE openhouse.d1.test_xxx${NC}"
echo -e "4. Update status in your result file: 🔲 → 🔄 → ✅/❌"
echo ""
echo -e "${GREEN}Good luck with your bug bash testing! 🐛🔨${NC}"
echo ""


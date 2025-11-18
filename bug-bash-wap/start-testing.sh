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
cat > "$GATEWAY_SCRIPT" << 'SCRIPT_EOF'
#!/bin/bash
# Run this script on the gateway after: ksudo -e openhouse

echo "========================================="
echo "Starting Spark Shell"
echo "========================================="
echo ""

# Start spark-shell (don't suppress stderr so you can see errors)
spark-shell \
  --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse \
  --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse

echo ""
echo "Spark shell exited."
SCRIPT_EOF
chmod +x "$GATEWAY_SCRIPT"

echo -e "${BLUE}=========================================${NC}"
echo -e "${BOLD}How to Start Testing:${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo -e "${YELLOW}1. SSH to the gateway:${NC}"
echo "   ssh ltx1-holdemgw03.grid.linkedin.com"
echo ""
echo -e "${YELLOW}2. Authenticate:${NC}"
echo "   ksudo -e openhouse"
echo ""
echo -e "${YELLOW}3. Run the script:${NC}"
echo "   ${SCRIPT_DIR}/logs/${ASSIGNEE}/run-on-gateway.sh"
echo ""
echo -e "${GREEN}Your personalized script: ${GATEWAY_SCRIPT}${NC}"
echo ""
echo -e "${YELLOW}Optional - Capture logs:${NC}"
echo "   ${SCRIPT_DIR}/logs/${ASSIGNEE}/run-on-gateway.sh | tee ${LOG_DIR}/session_${TIMESTAMP}.log"
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
echo -e "${BOLD}Operation              Spark SQL                                    Java API${NC}"
echo -e "─────────────────────────────────────────────────────────────────────────────────────────────"
echo -e "Write data            INSERT INTO table VALUES (...)               table.newAppend().appendFile(...).commit()"
echo -e "Create branch         ALTER TABLE table CREATE BRANCH name         builder.setRef(name, ref)"
echo -e "Cherry-pick           CALL openhouse.system.cherrypick_snapshot()  Manual copy + setBranchSnapshot()"
echo -e "Fast-forward          CALL openhouse.system.fast_forward()         builder.setRef(name, targetRef)"
echo -e "Expire snapshots      CALL openhouse.system.expire_snapshots()     builder.removeSnapshots(ids)"
echo -e "Stage WAP             spark.conf().set(\"spark.wap.id\", \"...\")     .set(\"wap.id\", \"...\").stageOnly()"
echo -e "Target branch         spark.conf().set(\"spark.wap.branch\", \"...\") Direct branch in append"
echo ""
echo -e "${YELLOW}Enable WAP:${NC}"
echo -e "  ALTER TABLE openhouse.d1.test_xxx SET TBLPROPERTIES ('write.wap.enabled'='true');"
echo ""
echo -e "${YELLOW}View metadata:${NC}"
echo -e "  SELECT * FROM openhouse.d1.test_xxx.snapshots;"
echo -e "  SELECT * FROM openhouse.d1.test_xxx.refs;"
echo ""
echo -e "${YELLOW}Create test files (Java API):${NC}"
echo -e "  DataFile file = DataFiles.builder(spec)"
echo -e "    .withPath(\"/path/to/data-a.parquet\")"
echo -e "    .withFileSizeInBytes(10)"
echo -e "    .withPartitionPath(\"data_bucket=0\")"
echo -e "    .withRecordCount(1)"
echo -e "    .build();"
echo ""

# Tips
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Tips${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo -e "1. ${BOLD}Use unique table names:${NC} ${YELLOW}test_sql01_${TIMESTAMP}${NC}"
echo ""
echo -e "2. ${BOLD}Copy commands as you go:${NC} Paste each command into your result file"
echo -e "   under \"Steps Executed\" section"
echo ""
echo -e "3. ${BOLD}Update your status:${NC} Edit the ${YELLOW}**Status:**${NC} line in your result file:"
echo -e "   - Start:        ${YELLOW}**Status:** 🔲 NOT STARTED${NC}"
echo -e "   - Working:      ${YELLOW}**Status:** 🔄 IN PROGRESS${NC}"
echo -e "   - Test passed:  ${YELLOW}**Status:** ✅ PASS${NC}"
echo -e "   - Found bug:    ${YELLOW}**Status:** ❌ FAIL${NC}"
echo ""
echo -e "4. ${BOLD}Document results:${NC} Fill in verification queries and their output"
echo ""
echo -e "5. ${BOLD}Clean up:${NC} ${YELLOW}DROP TABLE openhouse.d1.test_xxx${NC}"
echo ""
echo -e "${GREEN}Good luck with your bug bash testing! 🐛🔨${NC}"
echo ""


#!/bin/bash
# Bug Bash Quick Start Script for LinkedIn OpenHouse Testing
# This script helps you quickly set up your testing environment

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Bug Bash: Quick Start Testing Setup${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# Get assignee name
echo -e "${YELLOW}Enter your name (e.g., abhishek):${NC}"
read -r ASSIGNEE

# Create personalized log directory
LOG_DIR="logs/${ASSIGNEE}"
mkdir -p "$LOG_DIR"

# Generate timestamp for this session
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/session_${TIMESTAMP}.log"

echo ""
echo -e "${GREEN}✓ Created log directory: ${LOG_DIR}${NC}"
echo -e "${GREEN}✓ Session logs will be saved to: ${LOG_FILE}${NC}"
echo ""

# Display connection instructions
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Step 1: SSH to LinkedIn Gateway${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo -e "Run this command in your terminal:"
echo -e "${YELLOW}ssh ltx1-holdemgw03.grid.linkedin.com${NC}"
echo ""
echo -e "Press Enter when connected..."
read -r

# Display ksudo instructions
echo ""
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Step 2: Authenticate with ksudo${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo -e "Run this command on the gateway:"
echo -e "${YELLOW}ksudo -e openhouse${NC}"
echo ""
echo -e "Press Enter when authenticated..."
read -r

# Generate spark-shell command
echo ""
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Step 3: Start Spark Shell${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo -e "Copy and run this command:"
echo ""
echo -e "${GREEN}spark-shell \\"
echo -e "  --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse \\"
echo -e "  --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse \\"
echo -e "  2>/dev/null | tee ${LOG_FILE}${NC}"
echo ""

# Save command to a file for easy copy-paste
COMMAND_FILE="${LOG_DIR}/spark-shell-command.sh"
cat > "$COMMAND_FILE" << 'EOF'
#!/bin/bash
# Spark Shell Command for OpenHouse Bug Bash
spark-shell \
  --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse \
  --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse \
  2>/dev/null
EOF

echo -e "${GREEN}✓ Command saved to: ${COMMAND_FILE}${NC}"
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


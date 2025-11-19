#!/bin/bash
# Bug Bash Quick Start Script for LinkedIn OpenHouse Testing
# Run this locally to set up and get instructions

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "${BLUE}════════════════════════════════════════${NC}"
echo -e "${BOLD}Bug Bash: Quick Start Setup${NC}"
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
echo -e "${GREEN}✓ Log directory: ${LOG_DIR}${NC}"
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

# Quick reference
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo -e "${BOLD}Quick Reference Commands${NC}"
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo ""

echo -e "${YELLOW}1. Create Table (Spark SQL):${NC}"
echo -e "  CREATE TABLE openhouse.d1.test_${ASSIGNEE}_${TIMESTAMP} (id INT, name STRING)"
echo -e "  USING iceberg PARTITIONED BY (id);"
echo ""

echo -e "${YELLOW}2. Java API Imports (copy into spark-shell):${NC}"
echo -e "  import liopenhouse.relocated.org.apache.iceberg._"
echo -e "  import liopenhouse.relocated.org.apache.iceberg.catalog._"
echo -e "  import liopenhouse.relocated.org.apache.iceberg.types.Types._"
echo -e "  import liopenhouse.relocated.org.apache.iceberg.data._"
echo -e "  import liopenhouse.relocated.org.apache.iceberg.spark._"
echo -e "  import com.linkedin.openhouse.spark.OpenHouseSparkUtils"
echo ""

echo -e "${YELLOW}3. Common Types & Accessors:${NC}"
echo -e "  val catalog = spark.sessionState.catalogManager.catalog(\"openhouse\")"
echo -e "    .asInstanceOf[liopenhouse.relocated.org.apache.iceberg.spark.SparkCatalog]"
echo -e "  val table: Table = catalog.loadTable(Identifier.of(\"d1\", \"table_name\"))"
echo -e "  val snapshot: Snapshot = table.currentSnapshot()"
echo -e "  val metadata: TableMetadata = table.ops().current()"
echo ""

echo -e "${BOLD}Operation              Spark SQL                                    Java API${NC}"
echo -e "─────────────────────────────────────────────────────────────────────────────────────────────"
echo -e "Write data            INSERT INTO table VALUES (1, 'test')         table.newAppend().appendFile(...).commit()"
echo -e "Create branch         ALTER TABLE table CREATE BRANCH name         builder.setRef(name, ref)"
echo -e "Cherry-pick           CALL openhouse.system.cherrypick_snapshot()  Manual copy + setBranchSnapshot()"
echo -e "Fast-forward          CALL openhouse.system.fast_forward()         builder.setRef(name, targetRef)"
echo -e "Expire snapshots      CALL openhouse.system.expire_snapshots()     builder.removeSnapshots(ids)"
echo -e "Stage WAP             spark.conf().set(\"spark.wap.id\", \"wap1\")    .set(\"wap.id\", \"wap1\").stageOnly()"
echo -e "Target branch         spark.conf().set(\"spark.wap.branch\", \"b1\")  Direct branch in append"
echo ""

echo -e "${YELLOW}Enable WAP:${NC}"
echo -e "  ALTER TABLE openhouse.d1.test_xxx SET TBLPROPERTIES ('write.wap.enabled'='true');"
echo ""

echo -e "${YELLOW}View metadata:${NC}"
echo -e "  SELECT * FROM openhouse.d1.test_xxx.snapshots;"
echo -e "  SELECT * FROM openhouse.d1.test_xxx.refs;"
echo -e "  SELECT * FROM openhouse.d1.test_xxx.branch_myBranch;"
echo ""

echo -e "${YELLOW}Query current snapshot ID:${NC}"
echo -e "  val snapshotId = table.currentSnapshot().snapshotId()"
echo -e "  val parentId = table.currentSnapshot().parentId()"
echo ""

# Tips
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo -e "${BOLD}Testing Tips${NC}"
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo ""
echo -e "1. ${BOLD}Use unique table names:${NC} ${YELLOW}test_${ASSIGNEE}_${TIMESTAMP}${NC}"
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

# Create a spark-shell command file
SPARK_CMD="${LOG_DIR}/spark-shell-cmd.txt"
cat > "$SPARK_CMD" << 'EOF'
spark-shell --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse
EOF

# Show how to start testing
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo -e "${BOLD}Ready to Start Testing!${NC}"
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}Step 1: SSH to gateway and authenticate${NC}"
echo ""
echo -e "  ssh ltx1-holdemgw03.grid.linkedin.com"
echo ""
echo -e "${YELLOW}Step 2: Run ksudo (creates authenticated subshell)${NC}"
echo ""
echo -e "  ksudo -s OPENHOUSE,HDFS,WEBHDFS,SWEBHDFS,HCAT,RM -e openhouse"
echo ""
echo -e "${YELLOW}Step 3: In the ksudo subshell, start spark-shell${NC}"
echo ""
echo -e "  ${GREEN}spark-shell --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse${NC}"
echo ""
echo -e "${BLUE}💾 The spark-shell command is saved to: ${SPARK_CMD}${NC}"
echo ""
echo -e "${GREEN}💡 Tip: Use Ctrl+D or type :quit to exit spark-shell, then Ctrl+D again to exit ksudo${NC}"
echo ""
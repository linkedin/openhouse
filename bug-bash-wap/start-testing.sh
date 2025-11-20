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
echo -e "  spark.sql(s\"CREATE TABLE openhouse.u_openhouse.test_${ASSIGNEE}_${TIMESTAMP} (id INT, name STRING)\")"
echo ""

echo -e "${YELLOW}2. Java API Imports (copy into spark-shell):${NC}"
echo -e "  // Import only what you need so Spark's built-in 'spark' stays unambiguous"
echo -e "  import liopenhouse.relocated.org.apache.iceberg.Table"
echo -e "  import liopenhouse.relocated.org.apache.iceberg.Snapshot"
echo -e "  import liopenhouse.relocated.org.apache.iceberg.SnapshotRef"
echo -e "  import liopenhouse.relocated.org.apache.iceberg.TableMetadata"
echo -e "  import liopenhouse.relocated.org.apache.iceberg.catalog.TableIdentifier"
echo -e "  import liopenhouse.relocated.org.apache.iceberg.types.Types"
echo ""

echo -e "${YELLOW}Commit snapshot S1 on main (pattern from WapIdJavaTest):${NC}"
echo -e "  val dataFile: org.apache.iceberg.DataFile = /* build via DataFiles */"
echo -e "  table.newAppend().appendFile(dataFile).commit()"
echo -e "  val snapshotIdMain = table.currentSnapshot().snapshotId()"
echo -e "  // See apps/spark/src/test/java/com/linkedin/openhouse/catalog/e2e/WapIdJavaTest.java"
echo ""

echo -e "${YELLOW}Commit snapshot S3 on main in the same transaction:${NC}"
echo -e "  val txn = table.newTransaction()"
echo -e "  val appendS3 = txn.newAppend(); appendS3.appendFile(dataFileC); appendS3.commit()"
echo -e "  val snapshotIdS3 = txn.table().currentSnapshot().snapshotId()"
echo -e "  val refsUpdate = txn.manageSnapshots()"
echo -e "  refsUpdate.setRef(\"test\", SnapshotRef.branchBuilder(snapshotIdS2).build())"
echo -e "  refsUpdate.setBranchSnapshot(SnapshotRef.MAIN_BRANCH, snapshotIdS3)"
echo -e "  refsUpdate.commit(); txn.commitTransaction()"
echo -e "  // Transaction pattern also used in services/tables/.../SnapshotsControllerTest.java"
echo ""

echo -e "${YELLOW}3. Common Types & Accessors:${NC}"
echo -e "  val catalog = spark.sessionState.catalogManager.catalog(\"openhouse\")"
echo -e "    .asInstanceOf[liopenhouse.relocated.org.apache.iceberg.spark.SparkCatalog]"
echo -e "  val table: Table = catalog.loadTable(TableIdentifier.of(\"u_openhouse\", \"table_name\"))"
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
echo -e "  ALTER TABLE openhouse.u_openhouse.test_xxx SET TBLPROPERTIES ('write.wap.enabled'='true');"
echo ""

echo -e "${YELLOW}View metadata:${NC}"
echo -e "  SELECT * FROM openhouse.u_openhouse.test_xxx.snapshots;"
echo -e "  SELECT * FROM openhouse.u_openhouse.test_xxx.refs;"
echo -e "  SELECT * FROM openhouse.u_openhouse.test_xxx.branch_myBranch;"
echo ""

echo -e "${YELLOW}Query current snapshot ID:${NC}"
echo -e "  val snapshotId = table.currentSnapshot().snapshotId()"
echo -e "  val parentId = table.currentSnapshot().parentId()"
echo ""

echo -e "${YELLOW}Reference Test Examples:${NC}"
echo -e "  Spark SQL: integrations/spark/spark-3.5/openhouse-spark-itest/src/test/java/"
echo -e "             com/linkedin/openhouse/spark/catalogtest/BranchTestSpark3_5.java"
echo -e "  Java API:  apps/spark/src/test/java/com/linkedin/openhouse/catalog/e2e/"
echo -e "             WapIdJavaTest.java"
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
echo -e "5. ${BOLD}Clean up:${NC} ${YELLOW}DROP TABLE openhouse.u_openhouse.test_xxx${NC}"
echo ""

# Create a spark-shell command file
SPARK_CMD="${LOG_DIR}/spark-shell-cmd.txt"
cat > "$SPARK_CMD" << 'EOF'
spark-shell --spark-version 3.5.2 --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse
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
echo -e "  ${GREEN}spark-shell --spark-version 3.5.2 --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse${NC}"
echo ""
echo -e "${BLUE}💾 The spark-shell command is saved to: ${SPARK_CMD}${NC}"
echo ""
echo -e "${GREEN}💡 Tip: Use Ctrl+D or type :quit to exit spark-shell, then Ctrl+D again to exit ksudo${NC}"
echo ""
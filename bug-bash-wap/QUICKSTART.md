# Quick Start Guide for Bug Bash Testing

## 🚀 Fast Setup

### Step 0: Clone and Checkout Branch

First, get the code and checkout the bug bash branch:

```bash
# Clone the repository (if you haven't already)
git clone https://github.com/linkedin/openhouse.git
cd openhouse

# Checkout the bug bash branch
git checkout bug-bash-wap-2024-11

# Navigate to the bug bash directory
cd bug-bash-wap
```

### Option 1: Two-Step Setup (Recommended)

**Step 1: Run setup to see assignments and tips**

```bash
# You should already be in bug-bash-wap/ from Step 0
./start-testing.sh
# Enter your name when prompted
```

**This shows you:**
- ✅ Your test assignments (SQL + Java)
- ✅ Quick reference commands table
- ✅ Testing tips (table names, status updates, etc.)
- ✅ The command to run next

**Step 2: Connect and start testing**

```bash
# Run the generated connect script
logs/[your-name]/connect.sh

# Or copy-paste the full command shown at the end of step 1
```

**The connect script:**
- ✅ SSHs to gateway (handles 2FA if needed)
- ✅ Runs `ksudo -s OPENHOUSE,HDFS,WEBHDFS,SWEBHDFS,HCAT,RM -e openhouse`
- ✅ Starts spark-shell with correct OpenHouse configuration
- ✅ All in one command!

**Example flow:**
```
$ ./start-testing.sh
════════════════════════════════════════
Bug Bash: Quick Start Setup
════════════════════════════════════════

Enter your name (e.g., abhishek): abhishek

✓ Setup complete for: abhishek
✓ Log directory: logs/abhishek

════════════════════════════════════════
Your Test Assignments
════════════════════════════════════════

  ✓ results/sql-1-abhishek.md
  ✓ results/java-1-abhishek.md

════════════════════════════════════════
Quick Reference Commands
════════════════════════════════════════
[... command table ...]

════════════════════════════════════════
Testing Tips
════════════════════════════════════════
[... tips ...]

════════════════════════════════════════
Ready to Start Testing!
════════════════════════════════════════

Run this command to connect and start spark-shell:

  logs/abhishek/connect.sh

$ logs/abhishek/connect.sh
[Connects to gateway, authenticates, starts spark-shell...]
```

### Option 2: Manual Setup (Direct Commands)

If you already know what you need:

```bash
# 1. SSH to Gateway
ssh ltx1-holdemgw03.grid.linkedin.com

# 2. Authenticate
ksudo -e openhouse

# 3. Start Spark Shell
spark-shell \
  --spark-version 3.5.2 \
  --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse \
  --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse
```

---

## 📋 After Spark Shell Starts

### 1. Create Your Test Table
```scala
val timestamp = System.currentTimeMillis()
val tableName = s"test_sql01_${timestamp}"
spark.sql(s"CREATE TABLE openhouse.u_openhouse.${tableName} (name string)")
```

### Java API Imports (Spark Shell)
Avoid `import liopenhouse.relocated.org.apache.iceberg._`. That wildcard also brings an Iceberg `spark` package into scope, which collides with Spark shell’s built-in `spark` session and leads to `reference to spark is ambiguous`. Instead, import only the Iceberg types you need:
```scala
import liopenhouse.relocated.org.apache.iceberg.Table
import liopenhouse.relocated.org.apache.iceberg.Snapshot
import liopenhouse.relocated.org.apache.iceberg.SnapshotRef
import liopenhouse.relocated.org.apache.iceberg.TableMetadata
import liopenhouse.relocated.org.apache.iceberg.catalog.TableIdentifier
import liopenhouse.relocated.org.apache.iceberg.types.Types
```
Use `TableIdentifier.of("u_openhouse", tableName)` (instead of `Identifier.of`) whenever you load tables via the catalog.

### Commit Snapshot S1 to `main`
Before branching, follow the same pattern that `apps/spark/src/test/java/com/linkedin/openhouse/catalog/e2e/WapIdJavaTest.java` uses to create the first committed snapshot on `main`:
```scala
// Build a DataFile (Iceberg helper APIs shown in WapIdJavaTest)
val dataFile: org.apache.iceberg.DataFile = ...

// Append to main and commit => creates snapshot S1
table.newAppend().appendFile(dataFile).commit()
val snapshotIdMain = table.currentSnapshot().snapshotId()
```
That committed snapshot becomes `S1` and establishes the lineage that later WAP/branch operations rely on.

### Commit Snapshot S3 to `main` in the same transaction
Use Iceberg’s `Transaction` API (see `services/tables/src/test/java/com/linkedin/openhouse/tables/e2e/h2/SnapshotsControllerTest.java`) to stage your S3 data and repoint refs atomically:
```scala
val txn = table.newTransaction()

// Stage the data that should become S3 (no branch updated yet)
val appendS3 = txn.newAppend()
appendS3.appendFile(dataFileC) // your FILE_C equivalent
appendS3.commit()
val snapshotIdS3 = txn.table().currentSnapshot().snapshotId()

// Update refs within the same transaction
val refsUpdate = txn.manageSnapshots()
refsUpdate.setRef("test", SnapshotRef.branchBuilder(snapshotIdS2).build()) // repoint branch
refsUpdate.setBranchSnapshot(SnapshotRef.MAIN_BRANCH, snapshotIdS3)        // publish S3 to main
refsUpdate.commit()

// Atomically publish branch + main updates
txn.commitTransaction()
```
Add other specific classes (e.g., `DataFile`) as required by your test scenario.

### 2. Enable WAP (if needed)
```scala
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} SET TBLPROPERTIES ('write.wap.enabled'='true')")
```

### 3. Execute Your Test
Follow the test prompt in your assigned result file:
- `results/sql-01-your-name.md` (for SQL tests)
- `results/java-01-your-name.md` (for Java tests)

### 4. Copy Commands to Result File
As you execute each command, copy-paste it into your result file under "Steps Executed"

### 5. Document Results
Fill in:
- Verification queries and their output
- Expected vs Actual results table
- Any issues found

### 6. Clean Up
```scala
spark.sql(s"DROP TABLE openhouse.u_openhouse.${tableName}")
spark.conf().unset("spark.wap.id")
spark.conf().unset("spark.wap.branch")
```

---

## 🔍 Common Commands Reference

```scala
// Setup (run once per test)
val timestamp = System.currentTimeMillis()
val tableName = s"test_xxx_${timestamp}"
```

### Table Operations
```scala
spark.sql(s"CREATE TABLE openhouse.u_openhouse.${tableName} (name string)")
spark.sql(s"DROP TABLE openhouse.u_openhouse.${tableName}")
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} SET TBLPROPERTIES ('write.wap.enabled'='true')")
```

### Branch Operations
```scala
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} CREATE BRANCH myBranch")
spark.sql(s"INSERT INTO openhouse.u_openhouse.${tableName}.branch_myBranch VALUES ('data')")
spark.sql(s"SELECT * FROM openhouse.u_openhouse.${tableName} VERSION AS OF 'myBranch'").show()
```

### WAP Operations
```scala
// WAP staging
spark.conf.set("spark.wap.id", "wap1")
spark.sql(s"INSERT INTO openhouse.u_openhouse.${tableName} VALUES ('wap_data')")
spark.conf.unset("spark.wap.id")

// WAP branch
spark.conf.set("spark.wap.branch", "myBranch")
spark.sql(s"INSERT INTO openhouse.u_openhouse.${tableName} VALUES ('data')")
spark.conf.unset("spark.wap.branch")
```

### Metadata Queries
```scala
spark.sql(s"SELECT * FROM openhouse.u_openhouse.${tableName}.snapshots").show(false)
spark.sql(s"SELECT * FROM openhouse.u_openhouse.${tableName}.refs").show(false)
spark.sql(s"SELECT * FROM openhouse.u_openhouse.${tableName}.history").show(false)
```

### System Procedures
```scala
// Fast-forward
spark.sql(s"CALL openhouse.system.fast_forward('openhouse.u_openhouse.${tableName}', 'target', 'source')")

// Cherry-pick (note: no 'openhouse.' prefix in table identifier)
spark.sql(s"CALL openhouse.system.cherrypick_snapshot('u_openhouse.${tableName}', snapshotId)")

// Expire snapshots
spark.sql(s"CALL openhouse.system.expire_snapshots(table => 'u_openhouse.${tableName}', snapshot_ids => Array(id1, id2))")
```

---

## 📝 Submit Your Results

### Update Status
In your result file, change:
```markdown
**Status:** 🔲 NOT STARTED
```
to:
```markdown
**Status:** ✅ PASS
# OR
**Status:** ❌ FAIL
```

### Commit and Push
```bash
cd /path/to/openhouse
git add bug-bash-wap/results/sql-01-your-name.md
git commit -m "Results: SQL-01 Diamond Branch Merge - PASS"
git push origin bug-bash-wap-2024-11
```

---

## 🆘 Troubleshooting

### Can't connect to gateway
```bash
# Try alternate gateway
ssh ltx1-holdemgw04.grid.linkedin.com
```

### ksudo fails
```bash
# Refresh kerberos ticket
kinit
ksudo -e openhouse
```

### Spark shell won't start
```bash
# Check if openhouse service is running
curl https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse
```

### Table already exists
```bash
# Use a unique timestamp
CREATE TABLE openhouse.d1.test_sql01_$(date +%s) (name string);
```

---

## 💡 Pro Tips

1. **Use unique table names** with timestamps to avoid conflicts
2. **Save commands as you go** - don't wait until the end
3. **Check metadata frequently** - query `.snapshots` and `.refs` after each operation
4. **Document everything** - even unexpected behavior is valuable
5. **Don't rush** - thorough testing finds more bugs!

---

## 📊 Check Your Progress

```bash
cd bug-bash-wap
./collect-results.sh
```

---

**Need help?** Ask in the team Slack channel or check the main README.md


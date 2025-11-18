# Quick Start Guide for Bug Bash Testing

## 🚀 Fast Setup

### Option 1: Two-Step Setup (Recommended)

**Step 1: Run setup to see assignments and tips**

```bash
cd bug-bash-wap
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
  --conf spark.sql.catalog.openhouse.cluster=ltx1-holdem-openhouse \
  --conf spark.sql.catalog.openhouse.uri=https://openhouse.grid1-k8s-0.grid.linkedin.com:31189/clusters/openhouse
```

---

## 📋 After Spark Shell Starts

### 1. Create Your Test Table
```scala
spark.sql("CREATE TABLE openhouse.d1.test_sql01_12345 (name string)")
```

### 2. Enable WAP (if needed)
```scala
spark.sql("ALTER TABLE openhouse.d1.test_sql01_12345 SET TBLPROPERTIES ('write.wap.enabled'='true')")
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
spark.sql("DROP TABLE openhouse.d1.test_sql01_12345")
spark.conf().unset("spark.wap.id")
spark.conf().unset("spark.wap.branch")
```

---

## 🔍 Common Commands Reference

### Table Operations
```sql
CREATE TABLE openhouse.d1.test_xxx (name string);
DROP TABLE openhouse.d1.test_xxx;
ALTER TABLE openhouse.d1.test_xxx SET TBLPROPERTIES ('write.wap.enabled'='true');
```

### Branch Operations
```sql
ALTER TABLE openhouse.d1.test_xxx CREATE BRANCH myBranch;
INSERT INTO openhouse.d1.test_xxx.branch_myBranch VALUES ('data');
SELECT * FROM openhouse.d1.test_xxx VERSION AS OF 'myBranch';
```

### WAP Operations
```scala
// In Spark shell (Scala)
spark.conf().set("spark.wap.id", "wap1")
spark.sql("INSERT INTO openhouse.d1.test_xxx VALUES ('wap_data')")
spark.conf().unset("spark.wap.id")
```

### Metadata Queries
```sql
SELECT * FROM openhouse.d1.test_xxx.snapshots;
SELECT * FROM openhouse.d1.test_xxx.refs;
SELECT * FROM openhouse.d1.test_xxx.history;
```

### System Procedures
```sql
CALL openhouse.system.fast_forward('openhouse.d1.test_xxx', 'target', 'source');
CALL openhouse.system.cherrypick_snapshot('d1.test_xxx', snapshot_id);
CALL openhouse.system.expire_snapshots(table => 'd1.test_xxx', snapshot_ids => Array(id1));
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


# Bug Bash: SnapshotDiffApplier Multi-Branch Testing

## Overview
This bug bash tests the complex multi-branch Git-like behavior in `SnapshotDiffApplier.java`. The goal is to stress-test edge cases involving branches, WAP (Write-Audit-Publish), cherry-picks, fast-forwards, and snapshot management.

## Quick Start for Team Members

**🚀 New:** See [QUICKSTART.md](QUICKSTART.md) for fast setup with helper scripts!

### Step 1: Clone and Checkout (One-time setup)
```bash
# Clone the LinkedIn OpenHouse repository
git clone https://github.com/linkedin/openhouse.git
cd openhouse

# Checkout the bug bash branch
git fetch origin
git checkout bug-bash-wap-2024-11

# Navigate to bug bash folder
cd bug-bash-wap
```

### Step 1.5: Quick Setup (LinkedIn Internal)

**🎯 Simplest way to start testing:**

```bash
# 1. SSH to the gateway
ssh ltx1-holdemgw03.grid.linkedin.com

# 2. Clone the repo (if not already done)
git clone https://github.com/linkedin/openhouse.git
cd openhouse
git checkout bug-bash-wap-2024-11
cd bug-bash-wap

# 3. Authenticate
ksudo -e openhouse

# 4. Run the interactive start script
./start-testing.sh
# This will automatically launch spark-shell with the correct configuration
```

See [QUICKSTART.md](QUICKSTART.md) for alternative methods and detailed commands.

### Step 2: View Your Assignment
```bash
# Check which tests you're assigned
cat assignments.md
# You have 2 tests: 1 Spark SQL + 1 Java API
```

### Step 3: Read Test Details
Review your test prompts in `test-details.md` for full descriptions and expected behavior.

### Step 4: Execute Your Tests

**For Spark SQL tests:**
```bash
# Start spark-shell with OpenHouse catalog
spark-shell --conf spark.sql.catalog.openhouse=...

# Execute test steps and copy commands to your result file
# Update status from 🔲 to 🔄 while working
```

**For Java tests:**
```bash
# Add your test to the appropriate test file
# e.g., apps/spark/src/test/java/com/linkedin/openhouse/catalog/e2e/BugBashTest.java

# Run the test
./gradlew test --tests "BugBashTest.testXXX"
```

### Step 5: Fill in Results Template
Open your assigned file (e.g., `results/sql-01-abhishek.md`) and:
- Update **Status:** from 🔲 to ✅ (PASS) or ❌ (FAIL)
- Paste your actual commands in "Steps Executed"
- Document verification queries and results
- Mark issues found (if any)

### Step 6: Commit and Push Your Results
```bash
# Before starting: Always pull latest
git pull origin bug-bash-wap-2024-11

# After completing a test
git add bug-bash-wap/results/[your-test].md
git commit -m "Results: [Test Name] - PASS"
# OR: git commit -m "Results: [Test Name] - FAIL (found bug in parent rewriting)"
git push origin bug-bash-wap-2024-11
```

### Step 7: Repeat for Your Second Test
Each person has 2 tests assigned - repeat steps 4-6 for your second test.

---

## Collaboration Workflow

### Status Emoji Guide
Update the `**Status:**` line in your result file:
- 🔲 `NOT STARTED` - Haven't begun yet
- 🔄 `IN PROGRESS` - Currently working on it
- ✅ `PASS` - Test passed all assertions
- ❌ `FAIL` - Test failed or found bugs (this is good!)
- ⚠️ `PARTIAL` - Some assertions passed, some failed

### Git Workflow
```bash
# BEFORE starting work
git pull origin bug-bash-wap-2024-11  # Get latest updates

# DURING work
# Update your result file as you go

# AFTER completing a test
git add bug-bash-wap/results/[your-test].md
git commit -m "Results: SQL-01 Diamond Branch Merge - PASS"
git push origin bug-bash-wap-2024-11
```

### Check Overall Progress
```bash
cd bug-bash-wap
./collect-results.sh  # Shows completion status and bugs found
```

### Tips for Smooth Collaboration
1. **Commit frequently** - Push after each test completion
2. **Pull before starting** - Always `git pull` to get latest
3. **Use descriptive commit messages** - Include test name and result
4. **Clean up test tables** - Drop tables after testing
5. **Document bugs clearly** - Include steps to reproduce
6. **Ask questions early** - Don't get blocked!

---

## File Structure

```
bug-bash-wap/
├── README.md                    # Main documentation
├── QUICKSTART.md                # Fast setup guide ⭐
├── assignments.md               # Test assignments by person
├── test-details.md             # All test prompts with full details
├── TEMPLATE.md                 # Blank template for reference
├── start-testing.sh            # Interactive setup script ⭐
├── spark-shell-command.sh      # Quick spark-shell launcher ⭐
├── collect-results.sh          # Results collection script
├── results/                    # Individual test result files
│   ├── sql-01-abhishek.md
│   ├── sql-02-daniel.md
│   ├── ...
│   ├── java-01-abhishek.md
│   └── java-10-shanthoosh.md
└── logs/                       # Session logs (auto-generated)
    ├── abhishek/
    │   └── session_20241118_123456.log
    └── daniel/
        └── session_20241118_234567.log
```

⭐ = New helper scripts for fast setup!

## Test Execution Guidelines

### Prerequisites

**Spark SQL Tests:**
- Access to spark-shell with OpenHouse catalog configured
- Database `openhouse.d1` available for testing
- Permissions to create/drop tables

**Java Tests:**
- Familiarity with `WapIdJavaTest.java` as a reference
- Access to run integration tests
- Understanding of Iceberg Table API

### Best Practices

1. **Use unique table names** to avoid conflicts
   - SQL: `test_sql01_${timestamp}`
   - Java: `test_java01_${your_initials}`

2. **Save your commands** as you execute them
   - Copy-paste into the markdown template
   - Optionally save full logs to `logs/` directory

3. **Verify each step** before proceeding
   - Query `.snapshots` table
   - Query `.refs` table
   - Verify data isolation between branches

4. **Clean up after yourself**
   - Drop test tables
   - Unset spark configuration (wap.id, wap.branch)

5. **Document issues clearly**
   - Include steps to reproduce
   - Note expected vs actual behavior
   - Save error messages

### Common Pitfalls

❌ **Forgetting to enable WAP**
```sql
-- Don't forget this for WAP tests!
ALTER TABLE table_name SET TBLPROPERTIES ('write.wap.enabled'='true');
```

❌ **Not unsetting spark.wap.id**
```scala
// Clean up between tests
spark.conf().unset("spark.wap.id")
spark.conf().unset("spark.wap.branch")
```

❌ **Using the wrong table reference**
```sql
-- Wrong: queries main branch when wap.branch is set
SELECT * FROM openhouse.d1.table_name;

-- Right: explicit branch reference
SELECT * FROM openhouse.d1.table_name.branch_feature;
```

## Reference Commands

### Spark SQL Quick Reference

```sql
-- Table operations
CREATE TABLE openhouse.d1.test_xxx (name string);
DROP TABLE openhouse.d1.test_xxx;
ALTER TABLE openhouse.d1.test_xxx SET TBLPROPERTIES ('write.wap.enabled'='true');

-- Data operations
INSERT INTO openhouse.d1.test_xxx VALUES ('data');
INSERT INTO openhouse.d1.test_xxx.branch_myBranch VALUES ('branch_data');
SELECT * FROM openhouse.d1.test_xxx VERSION AS OF 'branch_name';

-- Branch operations
ALTER TABLE openhouse.d1.test_xxx CREATE BRANCH myBranch;
ALTER TABLE openhouse.d1.test_xxx DROP BRANCH myBranch;

-- System procedures
CALL openhouse.system.fast_forward('openhouse.d1.test_xxx', 'target', 'source');
CALL openhouse.system.cherrypick_snapshot('d1.test_xxx', snapshot_id);
CALL openhouse.system.expire_snapshots(table => 'd1.test_xxx', snapshot_ids => Array(id1, id2));

-- Metadata queries
SELECT * FROM openhouse.d1.test_xxx.snapshots;
SELECT * FROM openhouse.d1.test_xxx.refs;
SELECT * FROM openhouse.d1.test_xxx.history;
```

### Java API Quick Reference

```java
// Get table
Operations operations = Operations.withCatalog(spark, null);
Table table = operations.getTable("d1.test_xxx");

// Append data
table.newAppend().appendFile(FILE_A).commit();

// WAP staging
table.newAppend()
  .appendFile(FILE_B)
  .set("wap.id", "wap1")
  .stageOnly()
  .commit();

// Branch management
SnapshotRef ref = SnapshotRef.branchBuilder(snapshotId).build();
table.manageSnapshots()
  .setRef("branch_name", ref)
  .commit();

table.manageSnapshots()
  .setBranchSnapshot(snapshot, "branch_name")
  .commit();

// Snapshot management
table.manageSnapshots()
  .removeSnapshots(snapshotId1, snapshotId2)
  .commit();

// Query metadata
table.currentSnapshot();
table.refs();
table.snapshots();
snapshot.parentId();
```

## Submission Checklist

Before marking your test as complete:

- [ ] All test steps executed successfully
- [ ] Expected vs Actual results table filled out
- [ ] Verification queries documented with results
- [ ] Issues clearly documented (if any)
- [ ] Test tables cleaned up
- [ ] Spark configuration reset
- [ ] Markdown file committed and pushed
- [ ] Status updated in `assignments.md` (optional)

## For Organizers: Monitoring Progress

### Pull Latest Results
```bash
cd openhouse
git checkout bug-bash-wap-2024-11
git pull origin bug-bash-wap-2024-11
```

### Check Overall Status
```bash
cd bug-bash-wap
./collect-results.sh
```

**Example output:**
```
Completion Status:
  Total Tests:    20
  Completed:      5
  In Progress:    3
  Not Started:    12

Test Results:
  ✅ Passed:      4
  ❌ Failed:      1
  
Issues Found:
🐛 [sql-01-abhishek] by abhishek
   Cherry-pick doesn't update parent correctly
```

### Review Individual Results
```bash
# View specific test results
cat results/sql-01-abhishek.md

# Check git log for recent submissions
git log --oneline --graph
```

---

## Getting Help

- **Questions about test prompts:** Review `test-details.md` or ask in Slack
- **Technical blockers:** Tag the organizer
- **Reference implementations:** Check `BranchTestSpark3_5.java`, `WapIdTest.java`
- **API documentation:** Iceberg docs at https://iceberg.apache.org/docs/latest/

## Timeline

**Start Date:** [Set your date]  
**End Date:** [Set your deadline]  
**Review Meeting:** [Schedule a review session]

## Success Criteria

A test is considered **PASS** if:
- All assertions in the test prompt are verified
- No unexpected errors occurred
- Snapshot and ref management behaves as expected
- Data isolation is maintained across branches

A test is considered **FAIL** if:
- Unexpected errors occur
- Assertions don't hold
- SnapshotDiffApplier behaves incorrectly
- **Note:** This is a good thing! Finding bugs is the goal!

## After Bug Bash

1. **Review meeting:** Discuss findings as a team
2. **Create GitHub issues:** For each bug found
3. **Prioritize fixes:** Based on severity and impact
4. **Document learnings:** Update docs and test suite

---

**Questions?** Contact [your-name] or ask in [your-slack-channel]

Happy bug hunting! 🐛🔨


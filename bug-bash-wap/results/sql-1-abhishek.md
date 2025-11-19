# Test: SQL-1 - Diamond Branch Merge with WAP Publishing
**Assignee:** abhishek  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, enable WAP (write.wap.enabled=true), insert base data on main, create branch A from main, create branch B from main, insert data on branch A, insert different data on branch B, stage WAP snapshot on main, fast-forward main to branch A, cherry-pick WAP snapshot to branch B, verify: main has branch A data, branch B has its own data plus WAP data, all three branches point to different snapshots.

## Quick Reference
```scala
// Create table
spark.sql(s"CREATE TABLE openhouse.u_openhouse.test_xxx (id INT, data STRING)")

// Enable WAP
spark.sql(s"ALTER TABLE openhouse.u_openhouse.test_xxx SET TBLPROPERTIES ('write.wap.enabled'='true')")

// Insert data
spark.sql(s"INSERT INTO openhouse.u_openhouse.test_xxx VALUES (1, 'data')")

// Create branch
spark.sql(s"ALTER TABLE openhouse.u_openhouse.test_xxx CREATE BRANCH myBranch")

// Set WAP config
spark.conf.set("spark.wap.id", "wap1")
spark.conf.set("spark.wap.branch", "myBranch")

// Cherry-pick: CALL openhouse.system.cherrypick_snapshot('openhouse.u_openhouse.test_xxx', 'main', snapshotId)
// Fast-forward: CALL openhouse.system.fast_forward('openhouse.u_openhouse.test_xxx', 'branch1', 'branch2')

// View snapshots
spark.sql(s"SELECT snapshot_id, operation, summary FROM openhouse.u_openhouse.test_xxx.snapshots").show(false)

// View refs
spark.sql(s"SELECT name, snapshot_id FROM openhouse.u_openhouse.test_xxx.refs").show(false)

// Query branch data
spark.sql(s"SELECT * FROM openhouse.u_openhouse.test_xxx.branch_myBranch").show()

// Drop table
spark.sql(s"DROP TABLE openhouse.u_openhouse.test_xxx")
```

## Steps Executed
```scala
// Paste your actual Spark SQL commands here
// Use comments to organize your steps

// Step 1: Setup
val timestamp = System.currentTimeMillis()
spark.sql(s"CREATE TABLE openhouse.u_openhouse.test_sql1_${timestamp} (name string)")
spark.sql(s"ALTER TABLE openhouse.u_openhouse.test_sql1_${timestamp} SET TBLPROPERTIES ('write.wap.enabled'='true')")

// Step 2: Execute test scenario

// Step 3: Verification
```

## Expected vs Actual Results
| Step | Expected | Actual | Status |
|------|----------|--------|--------|
| 1. Create table | Table created | | |
| 2. ... | ... | | |

## Verification Queries & Results
```scala
// Snapshots query
spark.sql(s"SELECT snapshot_id, operation, summary FROM openhouse.u_openhouse.test_sql1_${timestamp}.snapshots").show(false)
// Result: [paste output]

// Refs query
spark.sql(s"SELECT name, snapshot_id FROM openhouse.u_openhouse.test_sql1_${timestamp}.refs").show(false)
// Result: [paste output]

// Data verification
spark.sql(s"SELECT * FROM openhouse.u_openhouse.test_sql1_${timestamp}").show(false)
// Result: [paste output]
```

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe]
- [ ] Enhancement needed: [describe]

## Additional Notes
[Any observations or challenges]

## Cleanup
- [ ] Test tables dropped
- [ ] spark.wap.id unset
- [ ] spark.wap.branch unset

## Sign-off
- [ ] All assertions verified
- [ ] Results reproducible

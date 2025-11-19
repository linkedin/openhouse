# Test: SQL-3 - Multi-WAP Publishing to Different Branches
**Assignee:** vikram  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, enable WAP (write.wap.enabled=true), insert base on main, create branch A and branch B, stage WAP ID wap1 targeting main, stage WAP ID wap2 targeting main, stage WAP ID wap3 targeting main, cherry-pick wap1 to branch A, cherry-pick wap2 to branch B, cherry-pick wap3 to main, verify each branch has exactly one WAP-derived snapshot and correct data isolation.

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
spark.sql(s"CREATE TABLE openhouse.u_openhouse.test_sql3_${timestamp} (name string)")
spark.sql(s"ALTER TABLE openhouse.u_openhouse.test_sql3_${timestamp} SET TBLPROPERTIES ('write.wap.enabled'='true')")

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
spark.sql(s"SELECT snapshot_id, operation, summary FROM openhouse.u_openhouse.test_sql3_${timestamp}.snapshots").show(false)
// Result: [paste output]

// Refs query
spark.sql(s"SELECT name, snapshot_id FROM openhouse.u_openhouse.test_sql3_${timestamp}.refs").show(false)
// Result: [paste output]

// Data verification
spark.sql(s"SELECT * FROM openhouse.u_openhouse.test_sql3_${timestamp}").show(false)
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

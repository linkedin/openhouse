# Test: SQL-09 - Multi-Branch Fast-Forward with Stale Refs Cleanup
**Assignee:** selena  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, insert on main, create branches A B C D from main, insert on branch D, fast-forward C to D, fast-forward B to C, fast-forward A to B, verify all branches A-D now point to same snapshot as D, expire intermediate snapshots that became unreferenced, verify only current snapshot remains, all four branches still work.

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
spark.sql(s"CREATE TABLE openhouse.u_openhouse.test_sql09_${timestamp} (name string)")
spark.sql(s"ALTER TABLE openhouse.u_openhouse.test_sql09_${timestamp} SET TBLPROPERTIES ('write.wap.enabled'='true')")

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
spark.sql(s"SELECT snapshot_id, operation, summary FROM openhouse.u_openhouse.test_sql09_${timestamp}.snapshots").show(false)
// Result: [paste output]

// Refs query
spark.sql(s"SELECT name, snapshot_id FROM openhouse.u_openhouse.test_sql09_${timestamp}.refs").show(false)
// Result: [paste output]

// Data verification
spark.sql(s"SELECT * FROM openhouse.u_openhouse.test_sql09_${timestamp}").show(false)
// Result: [paste output]
```

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe]

## Additional Notes
[Any observations]

## Cleanup
- [ ] Test tables dropped

## Sign-off
- [ ] All assertions verified
- [ ] Results reproducible


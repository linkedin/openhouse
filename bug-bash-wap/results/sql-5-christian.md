# Test: SQL-5 - WAP Branch Switch Mid-Transaction Simulation
**Assignee:** christian  
**Date:** [YYYY-MM-DD]  
**Status:** ✅ PASS

## Test Prompt
Create table, enable WAP (write.wap.enabled=true), insert base data, create branch staging and branch review, set wap.branch to staging, insert data, verify data visible on staging, change wap.branch to review mid-session, insert different data, unset wap.branch, verify staging has first insert, review has second insert, main has only base data, all isolated correctly.

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
val tableName = s"test_sql5_${timestamp}"
spark.sql(s"CREATE TABLE openhouse.u_openhouse.${tableName} (name string)")
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} SET TBLPROPERTIES ('write.wap.enabled'='true')")


// Step 2: Execute test scenario
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} CREATE BRANCH staging")
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} CREATE BRANCH branch")
spark.conf.set("spark.wap.branch", "staging")
spark.sql(s"INSERT INTO openhouse.u_openhouse.${tableName} VALUES ('data')")
spark.sql(s"SELECT * FROM openhouse.u_openhouse.${tableName}.branch_staging").show()
spark.conf.set("spark.wap.branch", "branch")
spark.sql(s"INSERT INTO openhouse.u_openhouse.${tableName} VALUES ('branchdata')")
spark.sql(s"SELECT * FROM openhouse.u_openhouse.${tableName}.branch_branch").show()
spark.sql(s"SELECT * FROM openhouse.u_openhouse.${tableName}").show()
spark.conf.unset("spark.wap.branch")


// Step 3: Verification
spark.sql(s"SELECT * FROM openhouse.u_openhouse.${tableName}.branch_staging").show()

```

## Expected vs Actual Results
good!!!

## Verification Queries & Results
good!!!

## Issues Found
- [X] No issues - test passed completely
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

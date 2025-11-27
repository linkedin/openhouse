# Test: SQL-15 - Snapshot Expiration with Cross-Branch Dependencies
**Assignee:** kevin  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, insert S1 on main, create branches A B C from S1, insert S2 on A, insert S3 on B (parent S1), insert S4 on C (parent S1), cherry-pick S2 to C (S5 created), attempt expire S1 (should fail - referenced by B and original A/B/C ancestry), expire S3 (should succeed - only B head), verify all branches functional, S1 S2 S4 S5 remain.

## Quick Reference
```scala
// Setup
val timestamp = System.currentTimeMillis()
val tableName = s"test_xxx_${timestamp}"

// Create table
spark.sql(s"CREATE TABLE openhouse.u_openhouse.${tableName} (id INT, data STRING)")

// Enable WAP
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} SET TBLPROPERTIES ('write.wap.enabled'='true')")

// Insert data
spark.sql(s"INSERT INTO openhouse.u_openhouse.${tableName} VALUES (1, 'data')")

// Create branch
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} CREATE BRANCH myBranch")

// Set WAP config
spark.conf.set("spark.wap.id", "wap1")
spark.conf.set("spark.wap.branch", "myBranch")

// Cherry-pick: CALL openhouse.system.cherrypick_snapshot('openhouse.u_openhouse.${tableName}', 'main', snapshotId)
// Fast-forward: CALL openhouse.system.fast_forward('openhouse.u_openhouse.${tableName}', 'branch1', 'branch2')

// View snapshots
spark.sql(s"SELECT snapshot_id, operation, summary FROM openhouse.u_openhouse.${tableName}.snapshots").show(false)

// View refs
spark.sql(s"SELECT name, snapshot_id FROM openhouse.u_openhouse.${tableName}.refs").show(false)

// Query branch data
spark.sql(s"SELECT * FROM openhouse.u_openhouse.${tableName}.branch_myBranch").show()

// Drop table
spark.sql(s"DROP TABLE openhouse.u_openhouse.${tableName}")
```

## Input
```scala
// Copy-paste all commands you ran here

val timestamp = System.currentTimeMillis()
val tableName = s"test_sql15_${timestamp}"
spark.sql(s"CREATE TABLE openhouse.u_openhouse.${tableName} (name string)")
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} SET TBLPROPERTIES ('write.wap.enabled'='true')")

// ... your test commands ...

```

## Output
```
[Copy-paste all output here - terminal output, query results, errors, etc.]

```

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe the bug, error messages, unexpected behavior]


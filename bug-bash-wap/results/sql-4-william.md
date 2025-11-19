# Test: SQL-4 - Circular Fast-Forward Prevention
**Assignee:** william  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, insert on main, create branch A from main, insert on branch A, create branch B from branch A, insert on branch B, attempt fast-forward A to B (should succeed), attempt fast-forward B to A (should fail - would be backwards), verify branch B is ahead of A and operation preserves data integrity.

## Quick Reference
```scala
// Create table
val timestamp = System.currentTimeMillis()
val tableName = s"test_xxx_${timestamp}"

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
val tableName = s"test_sql4_${timestamp}"
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

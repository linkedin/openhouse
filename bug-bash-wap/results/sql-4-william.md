# Test: SQL-4 - Circular Fast-Forward Prevention
**Assignee:** william  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, insert on main, create branch A from main, insert on branch A, create branch B from branch A, insert on branch B, attempt fast-forward A to B (should succeed), attempt fast-forward B to A (should fail - would be backwards), verify branch B is ahead of A and operation preserves data integrity.

## Steps Executed
```scala
// Paste your actual Spark SQL commands here
// Use comments to organize your steps

// Step 1: Setup
val timestamp = System.currentTimeMillis()
spark.sql(s"CREATE TABLE openhouse.u_openhouse.test_sql4_${timestamp} (name string) USING iceberg")
spark.sql(s"ALTER TABLE openhouse.u_openhouse.test_sql4_${timestamp} SET TBLPROPERTIES ('write.wap.enabled'='true')")

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
spark.sql(s"SELECT snapshot_id, operation, summary FROM openhouse.u_openhouse.test_sql4_${timestamp}.snapshots").show(false)
// Result: [paste output]

// Refs query
spark.sql(s"SELECT name, snapshot_id FROM openhouse.u_openhouse.test_sql4_${timestamp}.refs").show(false)
// Result: [paste output]

// Data verification
spark.sql(s"SELECT * FROM openhouse.u_openhouse.test_sql4_${timestamp}").show(false)
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

# Test: SQL-3 - Multi-WAP Publishing to Different Branches
**Assignee:** vikram  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, enable WAP (write.wap.enabled=true), insert base on main, create branch A and branch B, stage WAP ID wap1 targeting main, stage WAP ID wap2 targeting main, stage WAP ID wap3 targeting main, cherry-pick wap1 to branch A, cherry-pick wap2 to branch B, cherry-pick wap3 to main, verify each branch has exactly one WAP-derived snapshot and correct data isolation.

## Steps Executed
```sql
-- Paste your actual Spark SQL commands here
-- Use comments to organize your steps

-- Step 1: Setup
val timestamp = System.currentTimeMillis()
CREATE TABLE openhouse.u_openhouse.test_sql3_${timestamp} (name string);
ALTER TABLE openhouse.u_openhouse.test_sql3_${timestamp} SET TBLPROPERTIES ('write.wap.enabled'='true');

-- Step 2: Execute test scenario

-- Step 3: Verification
```

## Expected vs Actual Results
| Step | Expected | Actual | Status |
|------|----------|--------|--------|
| 1. Create table | Table created | | |
| 2. ... | ... | | |

## Verification Queries & Results
```sql
-- Snapshots query
SELECT snapshot_id, operation, summary FROM openhouse.u_openhouse.test_sql3_xxx.snapshots;
-- Result: [paste output]

-- Refs query
SELECT name, snapshot_id FROM openhouse.u_openhouse.test_sql3_xxx.refs;
-- Result: [paste output]

-- Data verification
SELECT * FROM openhouse.u_openhouse.test_sql3_xxx;
-- Result: [paste output]
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

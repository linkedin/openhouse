# Test: SQL-1 - Diamond Branch Merge with WAP Publishing
**Assignee:** abhishek  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, enable WAP (write.wap.enabled=true), insert base data on main, create branch A from main, create branch B from main, insert data on branch A, insert different data on branch B, stage WAP snapshot on main, fast-forward main to branch A, cherry-pick WAP snapshot to branch B, verify: main has branch A data, branch B has its own data plus WAP data, all three branches point to different snapshots.

## Steps Executed
```sql
-- Paste your actual Spark SQL commands here
-- Use comments to organize your steps

-- Step 1: Setup
CREATE TABLE openhouse.d1.test_sql1_${timestamp} (name string);
ALTER TABLE openhouse.d1.test_sql1_${timestamp} SET TBLPROPERTIES ('write.wap.enabled'='true');

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
SELECT snapshot_id, operation, summary FROM openhouse.d1.test_sql1_xxx.snapshots;
-- Result: [paste output]

-- Refs query
SELECT name, snapshot_id FROM openhouse.d1.test_sql1_xxx.refs;
-- Result: [paste output]

-- Data verification
SELECT * FROM openhouse.d1.test_sql1_xxx;
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

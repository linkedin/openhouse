# Test: SQL-7 - Cherry-Pick Chain with Parent Rewriting
**Assignee:** levi  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, enable WAP (write.wap.enabled=true), insert base on main, stage wap1 (parent: base), stage wap2 (parent: wap1), stage wap3 (parent: wap2), insert regular commit on main, cherry-pick wap3 to main (skipping wap1 and wap2), verify main has base + regular + wap3 data, verify wap3 parent was rewritten to point to regular commit, verify wap1 and wap2 remain unpublished.

## Steps Executed
```sql
-- Paste your actual Spark SQL commands here
-- Use comments to organize your steps

-- Step 1: Setup
CREATE TABLE openhouse.u_openhouse.test_sql7_${timestamp} (name string);
ALTER TABLE openhouse.u_openhouse.test_sql7_${timestamp} SET TBLPROPERTIES ('write.wap.enabled'='true');

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
SELECT snapshot_id, operation, summary FROM openhouse.u_openhouse.test_sql7_xxx.snapshots;
-- Result: [paste output]

-- Refs query
SELECT name, snapshot_id FROM openhouse.u_openhouse.test_sql7_xxx.refs;
-- Result: [paste output]

-- Data verification
SELECT * FROM openhouse.u_openhouse.test_sql7_xxx;
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

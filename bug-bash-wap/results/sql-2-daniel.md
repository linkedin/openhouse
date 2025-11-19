# Test: SQL-2 - Orphaned Snapshot After Fast-Forward Race
**Assignee:** daniel  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, insert on main, create branch feature, insert twice on feature, insert once on main (diverge), create branch temp from feature, fast-forward temp to main (should fail due to divergence), verify temp still points to feature's snapshot, insert on temp, verify temp advanced independently and all snapshots are preserved.

## Steps Executed
```sql
-- Paste your actual Spark SQL commands here
-- Use comments to organize your steps

-- Step 1: Setup
CREATE TABLE openhouse.u_openhouse.test_sql2_${timestamp} (name string);
ALTER TABLE openhouse.u_openhouse.test_sql2_${timestamp} SET TBLPROPERTIES ('write.wap.enabled'='true');

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
SELECT snapshot_id, operation, summary FROM openhouse.u_openhouse.test_sql2_xxx.snapshots;
-- Result: [paste output]

-- Refs query
SELECT name, snapshot_id FROM openhouse.u_openhouse.test_sql2_xxx.refs;
-- Result: [paste output]

-- Data verification
SELECT * FROM openhouse.u_openhouse.test_sql2_xxx;
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

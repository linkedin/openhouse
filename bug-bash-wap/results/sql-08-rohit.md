# Test: SQL-08 - Branch Creation Race on Empty Table
**Assignee:** rohit  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create empty table (no snapshots), create branch A (should create empty snapshot), create branch B from branch A, verify both branches exist and point to same empty snapshot, set wap.branch to A, insert data, verify branch A advanced, branch B still at empty snapshot, create branch C from empty table root, verify C also points to original empty snapshot.

## Steps Executed
```sql
-- Paste your actual Spark SQL commands here

-- Step 1: Setup
CREATE TABLE openhouse.d1.test_sql08_${timestamp} (name string);

-- Step 2: Execute test scenario
ALTER TABLE openhouse.d1.test_sql08_xxx CREATE BRANCH branchA;
ALTER TABLE openhouse.d1.test_sql08_xxx CREATE BRANCH branchB;
-- etc...

-- Step 3: Verification
```

## Expected vs Actual Results
| Step | Expected | Actual | Status |
|------|----------|--------|--------|
| 1. Create empty table | Table created, no snapshots | | |
| 2. Create branch A | Empty snapshot created | | |
| 3. Create branch B from A | Points to same empty snapshot | | |

## Verification Queries & Results
```sql
-- Snapshots query
SELECT snapshot_id, operation, summary FROM openhouse.d1.test_sql08_xxx.snapshots;
-- Result: [paste output]

-- Refs query
SELECT name, snapshot_id FROM openhouse.d1.test_sql08_xxx.refs;
-- Result: [paste output]
```

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe]

## Additional Notes
[Any observations]

## Cleanup
- [ ] Test tables dropped
- [ ] spark.wap.branch unset

## Sign-off
- [ ] All assertions verified
- [ ] Results reproducible


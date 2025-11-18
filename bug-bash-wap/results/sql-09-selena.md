# Test: SQL-09 - Multi-Branch Fast-Forward with Stale Refs Cleanup
**Assignee:** selena  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, insert on main, create branches A B C D from main, insert on branch D, fast-forward C to D, fast-forward B to C, fast-forward A to B, verify all branches A-D now point to same snapshot as D, expire intermediate snapshots that became unreferenced, verify only current snapshot remains, all four branches still work.

## Steps Executed
```sql
-- Paste your actual Spark SQL commands here

-- Step 1: Setup
CREATE TABLE openhouse.d1.test_sql09_${timestamp} (name string);
INSERT INTO openhouse.d1.test_sql09_xxx VALUES ('base_data');

-- Step 2: Create branches
ALTER TABLE openhouse.d1.test_sql09_xxx CREATE BRANCH branchA;
-- etc...

-- Step 3: Fast-forward operations
CALL openhouse.system.fast_forward('openhouse.d1.test_sql09_xxx', 'branchC', 'branchD');
-- etc...

-- Step 4: Verification
```

## Expected vs Actual Results
| Step | Expected | Actual | Status |
|------|----------|--------|--------|
| 1. Create 4 branches | All point to same initial snapshot | | |
| 2. Advance branch D | D has new snapshot | | |
| 3. Cascade fast-forwards | All branches point to D's snapshot | | |

## Verification Queries & Results
```sql
-- Refs query
SELECT name, snapshot_id FROM openhouse.d1.test_sql09_xxx.refs;
-- Result: [paste output]

-- Snapshots query
SELECT snapshot_id FROM openhouse.d1.test_sql09_xxx.snapshots;
-- Result: [paste output]
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


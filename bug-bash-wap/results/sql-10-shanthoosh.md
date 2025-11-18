# Test: SQL-10 - WAP ID Publishing to Non-Main with Branch Deletion
**Assignee:** shanthoosh  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, enable WAP (write.wap.enabled=true), insert on main, create branches feature and temp, stage wap1 with data, cherry-pick wap1 to feature branch, verify feature has WAP data, drop temp branch via Iceberg API, stage wap2 with different data, cherry-pick wap2 to main, verify main has wap2 data, feature unchanged, temp branch refs properly cleaned up.

## Steps Executed
```sql
-- Paste your actual Spark SQL commands here

-- Step 1: Setup
CREATE TABLE openhouse.d1.test_sql10_${timestamp} (name string);
ALTER TABLE openhouse.d1.test_sql10_xxx SET TBLPROPERTIES ('write.wap.enabled'='true');

-- Step 2: Create branches and WAP snapshots
ALTER TABLE openhouse.d1.test_sql10_xxx CREATE BRANCH feature;
ALTER TABLE openhouse.d1.test_sql10_xxx CREATE BRANCH temp;
-- spark.conf().set("spark.wap.id", "wap1")
INSERT INTO openhouse.d1.test_sql10_xxx VALUES ('wap1_data');

-- Step 3: Cherry-pick and branch deletion
-- etc...

-- Step 4: Verification
```

## Expected vs Actual Results
| Step | Expected | Actual | Status |
|------|----------|--------|--------|
| 1. Enable WAP | Property set | | |
| 2. Cherry-pick wap1 to feature | Feature has WAP data | | |
| 3. Drop temp branch | Branch removed from refs | | |

## Verification Queries & Results
```sql
-- Refs query (should not show temp branch after deletion)
SELECT name, snapshot_id FROM openhouse.d1.test_sql10_xxx.refs;
-- Result: [paste output]

-- Snapshots with WAP IDs
SELECT snapshot_id, summary FROM openhouse.d1.test_sql10_xxx.snapshots WHERE summary['wap.id'] IS NOT NULL;
-- Result: [paste output]
```

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe]

## Additional Notes
[Any observations]

## Cleanup
- [ ] Test tables dropped
- [ ] spark.wap.id unset

## Sign-off
- [ ] All assertions verified
- [ ] Results reproducible


# Test: SQL-6 - Cascading Branch Deletions with Ref Validation
**Assignee:** stas  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, enable WAP (write.wap.enabled=true), insert on main, create branches A B C all from main, insert on each branch separately, get all snapshot IDs from branches, expire snapshot from branch A (non-head), verify expiration succeeded, verify branches B and C still reference their snapshots correctly, attempt to expire branch B HEAD snapshot (should fail - still referenced).

## Steps Executed
```sql
-- Paste your actual Spark SQL commands here
-- Use comments to organize your steps

-- Step 1: Setup
CREATE TABLE openhouse.d1.test_sql6_${timestamp} (name string);
ALTER TABLE openhouse.d1.test_sql6_${timestamp} SET TBLPROPERTIES ('write.wap.enabled'='true');

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
SELECT snapshot_id, operation, summary FROM openhouse.d1.test_sql6_xxx.snapshots;
-- Result: [paste output]

-- Refs query
SELECT name, snapshot_id FROM openhouse.d1.test_sql6_xxx.refs;
-- Result: [paste output]

-- Data verification
SELECT * FROM openhouse.d1.test_sql6_xxx;
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

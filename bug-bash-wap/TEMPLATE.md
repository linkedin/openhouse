# Test: [Test Name]
**Assignee:** [Your Name]  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED / 🔄 IN PROGRESS / ✅ PASS / ❌ FAIL / ⚠️ PARTIAL

## Test Prompt
[Copy the test prompt from test-details.md]

## Steps Executed
```sql
-- Paste your actual commands here
-- Use comments to organize your steps

-- Step 1: Setup

-- Step 2: Execute test scenario

-- Step 3: Verification
```

Or for Java tests:
```java
// Paste your actual code here
```

## Expected vs Actual Results
| Step | Expected | Actual | Status |
|------|----------|--------|--------|
| 1. Create table | Table created | | |
| 2. Enable WAP | Property set | | |
| 3. ... | ... | | |

## Verification Queries & Results
```sql
-- Key validation queries you ran
SELECT * FROM openhouse.d1.test_xxx.snapshots;
-- Result: [paste output or describe]

SELECT * FROM openhouse.d1.test_xxx.refs;
-- Result: [paste output or describe]
```

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe any bugs/unexpected behavior]
- [ ] Enhancement needed: [describe any improvements]
- [ ] Documentation issue: [describe any doc problems]

## Additional Notes
[Any observations, edge cases discovered, or challenges faced]

## Cleanup
- [ ] Test tables dropped
- [ ] Spark session cleaned up
- [ ] Configuration reset (spark.wap.id, spark.wap.branch)

## Screenshots/Logs (Optional)
Link to detailed logs in `logs/` directory if needed:
- `logs/[test-id]-[your-name].txt`

## Sign-off
- [ ] I verified all assertions passed
- [ ] I reviewed the test output thoroughly
- [ ] Results are reproducible


# Test: Java-3 - Concurrent Branch Creation with Overlapping Commits
**Assignee:** vikram  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, commit snapshot S1 to main, manually create two SnapshotRef objects for branches A and B both pointing to S1, set both refs in single metadata builder operation, append FILE_A to branch A (snapshot S2), append FILE_B to branch B (snapshot S3), verify refs table shows all three branches with correct snapshot IDs, no cross-contamination of data files.

## Steps Executed
```java
// Paste your actual Java test code here

@Test
void testConcurrentBranchCreationwithOverlappingCommits() throws Exception {
  try (SparkSession spark = getSparkSession()) {
    spark.sql("CREATE TABLE openhouse.d1.test_java3 (id int, data string)");
    Operations operations = Operations.withCatalog(spark, null);
    Table table = operations.getTable("d1.test_java3");
    
    // Your test implementation here
    
    spark.sql("DROP TABLE openhouse.d1.test_java3");
  }
}
```

## Expected vs Actual Results
| Step | Expected | Actual | Status |
|------|----------|--------|--------|
| 1. Create table | Table created | | |
| 2. ... | ... | | |

## Verification Queries & Results
```java
// Snapshot verification
System.out.println("Snapshots: " + table.snapshots());
// Result: [paste output]

// Refs verification
System.out.println("Refs: " + table.refs());
// Result: [paste output]

// Current snapshot
System.out.println("Current: " + table.currentSnapshot().snapshotId());
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

## Sign-off
- [ ] All assertions verified
- [ ] Results reproducible

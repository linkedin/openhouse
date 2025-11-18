# Test: Java-7 - WAP Snapshot Expiration with Lineage Preservation
**Assignee:** levi  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, set table property write.wap.enabled=true, commit S1 to main, append FILE_A with wap.id=wap1 stageOnly() to create S2, commit S3 to main, append FILE_B with wap.id=wap2 stageOnly() to create S4, call removeSnapshots() to expire S2 (unpublished wap1), verify S1 S3 S4 remain, snapshot lineage from main (S1->S3) intact, wap2 still available for publishing.

## Steps Executed
```java
// Paste your actual Java test code here

@Test
void testWAPSnapshotExpirationwithLineagePreservation() throws Exception {
  try (SparkSession spark = getSparkSession()) {
    spark.sql("CREATE TABLE openhouse.d1.test_java7 (id int, data string)");
    Operations operations = Operations.withCatalog(spark, null);
    Table table = operations.getTable("d1.test_java7");
    
    // Your test implementation here
    
    spark.sql("DROP TABLE openhouse.d1.test_java7");
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

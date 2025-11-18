# Test: Java-10 - Branch Snapshot Override with Concurrent Main Advancement
**Assignee:** shanthoosh  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, commit S1 to main, create branch feature pointing to S1, append FILE_A to feature creating S2, append FILE_B to main creating S3, call setBranchSnapshot() for feature with new snapshot S4 (child of S3) containing FILE_C, verify feature branch jumped from S1->S2 lineage to S3->S4 lineage, S2 becomes orphaned but not deleted (may be referenced elsewhere), main unaffected at S3.

## Steps Executed
```java
// Paste your actual Java test code here

@Test
void testBranchSnapshotOverrideWithConcurrentMainAdvancement() throws Exception {
  try (SparkSession spark = getSparkSession()) {
    spark.sql("CREATE TABLE openhouse.d1.test_java10 (id int, data string)");
    Operations operations = Operations.withCatalog(spark, null);
    Table table = operations.getTable("d1.test_java10");
    
    // Commit S1 to main
    table.newAppend().appendFile(FILE_A).commit();
    long s1 = table.currentSnapshot().snapshotId();
    
    // Create feature branch pointing to S1
    table.manageSnapshots()
      .setRef("feature", SnapshotRef.branchBuilder(s1).build())
      .commit();
    
    // Append to feature (creates S2)
    table.manageSnapshots()
      .setBranchSnapshot(
        table.newAppend().appendFile(FILE_B).apply(),
        "feature"
      )
      .commit();
    long s2 = table.refs().get("feature").snapshotId();
    
    // Append to main (creates S3)
    table.newAppend().appendFile(FILE_C).commit();
    long s3 = table.currentSnapshot().snapshotId();
    
    // Override feature branch to point to new S4 (child of S3)
    Snapshot s4 = table.newAppend()
      .appendFile(FILE_D)
      .apply();
    table.manageSnapshots()
      .setBranchSnapshot(s4, "feature")
      .commit();
    
    // Verify lineage change
    // ... your implementation
    
    spark.sql("DROP TABLE openhouse.d1.test_java10");
  }
}
```

## Expected vs Actual Results
| Step | Expected | Actual | Status |
|------|----------|--------|--------|
| 1. Create S1 on main | Snapshot created | | |
| 2. Feature→S1, advance to S2 | Feature at S2 (child of S1) | | |
| 3. Main advances to S3 | Main at S3 (child of S1) | | |
| 4. Override feature to S4 | Feature jumped to S4 (child of S3) | | |
| 5. Verify S2 orphaned | S2 still exists but not referenced | | |

## Verification Queries & Results
```java
// Verify feature branch points to S4
long featureSnapshotId = table.refs().get("feature").snapshotId();
System.out.println("Feature branch snapshot: " + featureSnapshotId);
// Result: [paste output]

// Verify main unchanged at S3
long mainSnapshotId = table.currentSnapshot().snapshotId();
System.out.println("Main branch snapshot: " + mainSnapshotId);
// Result: [paste output]

// Verify S2 still exists but orphaned
boolean s2Exists = false;
Iterator<Snapshot> snapshots = table.snapshots().iterator();
while (snapshots.hasNext()) {
  if (snapshots.next().snapshotId() == s2) {
    s2Exists = true;
    break;
  }
}
System.out.println("S2 still exists: " + s2Exists);
// Result: [paste output]
```

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe]

## Additional Notes
[Any observations about orphaned snapshots or lineage changes]

## Cleanup
- [ ] Test tables dropped

## Sign-off
- [ ] All assertions verified
- [ ] Results reproducible


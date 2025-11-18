# Test: Java-08 - Multi-Branch Append with Shared Parent Snapshot
**Assignee:** rohit  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, commit S1 to main with FILE_A, create branches b1 b2 b3 all pointing to S1, append FILE_B to b1 creating S2 with parent=S1, append FILE_C to b2 creating S3 with parent=S1, append FILE_D to b3 creating S4 with parent=S1, verify all three snapshots have same parent but different data files, refs table shows correct snapshot IDs for each branch.

## Steps Executed
```java
// Paste your actual Java test code here

@Test
void testMultiBranchAppendWithSharedParentSnapshot() throws Exception {
  try (SparkSession spark = getSparkSession()) {
    spark.sql("CREATE TABLE openhouse.d1.test_java08 (id int, data string)");
    Operations operations = Operations.withCatalog(spark, null);
    Table table = operations.getTable("d1.test_java08");
    
    // Commit S1 to main
    table.newAppend().appendFile(FILE_A).commit();
    long s1 = table.currentSnapshot().snapshotId();
    
    // Create branches b1, b2, b3
    SnapshotRef ref = SnapshotRef.branchBuilder(s1).build();
    table.manageSnapshots()
      .setRef("b1", ref)
      .setRef("b2", ref)
      .setRef("b3", ref)
      .commit();
    
    // Append to each branch
    // ... your implementation
    
    spark.sql("DROP TABLE openhouse.d1.test_java08");
  }
}
```

## Expected vs Actual Results
| Step | Expected | Actual | Status |
|------|----------|--------|--------|
| 1. Create S1 on main | Snapshot created | | |
| 2. Create 3 branches | All point to S1 | | |
| 3. Append to each branch | 3 new snapshots with S1 as parent | | |

## Verification Queries & Results
```java
// Refs verification
Map<String, SnapshotRef> refs = table.refs();
System.out.println("Refs: " + refs);
// Result: [paste output]

// Parent verification
Iterator<Snapshot> snapshots = table.snapshots().iterator();
while (snapshots.hasNext()) {
  Snapshot s = snapshots.next();
  System.out.println("Snapshot " + s.snapshotId() + " parent: " + s.parentId());
}
// Result: [paste output]
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


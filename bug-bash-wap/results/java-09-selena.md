# Test: Java-09 - Snapshot Ref Retention Policy Enforcement
**Assignee:** selena  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, commit 5 snapshots S1-S5 sequentially on main, create branch old pointing to S2, create branch mid pointing to S4, attempt removeSnapshots() with retention policy to expire S1 S3 (neither referenced by branches), verify S2 S4 S5 remain (referenced or current), verify old and mid branches still functional and point to correct snapshots.

## Steps Executed
```java
// Paste your actual Java test code here

@Test
void testSnapshotRefRetentionPolicyEnforcement() throws Exception {
  try (SparkSession spark = getSparkSession()) {
    spark.sql("CREATE TABLE openhouse.d1.test_java09 (id int, data string)");
    Operations operations = Operations.withCatalog(spark, null);
    Table table = operations.getTable("d1.test_java09");
    
    // Create 5 snapshots sequentially
    table.newAppend().appendFile(FILE_A).commit(); // S1
    long s1 = table.currentSnapshot().snapshotId();
    
    table.newAppend().appendFile(FILE_B).commit(); // S2
    long s2 = table.currentSnapshot().snapshotId();
    
    // ... S3, S4, S5
    
    // Create branches
    table.manageSnapshots()
      .setRef("old", SnapshotRef.branchBuilder(s2).build())
      .setRef("mid", SnapshotRef.branchBuilder(s4).build())
      .commit();
    
    // Attempt to expire S1 and S3
    table.manageSnapshots()
      .removeSnapshots(s1, s3)
      .commit();
    
    // Verify S2, S4, S5 remain
    // ... your implementation
    
    spark.sql("DROP TABLE openhouse.d1.test_java09");
  }
}
```

## Expected vs Actual Results
| Step | Expected | Actual | Status |
|------|----------|--------|--------|
| 1. Create 5 snapshots | S1-S5 exist | | |
| 2. Create branches | old→S2, mid→S4 | | |
| 3. Expire S1, S3 | Removed (not referenced) | | |
| 4. Verify S2, S4, S5 | Still exist | | |

## Verification Queries & Results
```java
// Count remaining snapshots
int count = 0;
Iterator<Snapshot> snapshots = table.snapshots().iterator();
while (snapshots.hasNext()) {
  snapshots.next();
  count++;
}
System.out.println("Remaining snapshots: " + count);
// Result: [paste output]

// Verify branches still work
Map<String, SnapshotRef> refs = table.refs();
System.out.println("Branch 'old' points to: " + refs.get("old").snapshotId());
System.out.println("Branch 'mid' points to: " + refs.get("mid").snapshotId());
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


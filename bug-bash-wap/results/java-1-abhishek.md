# Test: Java-1 - Append Chain with Manual Snapshot Ref Management
**Assignee:** abhishek  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table via Java, append FILE_A to create snapshot S1, create branch dev pointing to S1 via setRef(), append FILE_B to dev branch using setBranchSnapshot(), append FILE_C to main using setBranchSnapshot(), verify: main points to S1->S3 lineage, dev points to S1->S2 lineage, both branches isolated, total 3 snapshots exist.

## Quick Reference
```scala
// Java API imports
import liopenhouse.relocated.org.apache.iceberg._
import liopenhouse.relocated.org.apache.iceberg.catalog._
import liopenhouse.relocated.org.apache.iceberg.types.Types._

// Get catalog and table
val catalog = spark.sessionState.catalogManager.catalog("openhouse")
  .asInstanceOf[liopenhouse.relocated.org.apache.iceberg.spark.SparkCatalog]
val table: Table = catalog.loadTable(Identifier.of("u_openhouse", "table_name"))

// Get current snapshot
val snapshot: Snapshot = table.currentSnapshot()
val snapshotId = snapshot.snapshotId()
val parentId = snapshot.parentId()

// Append data
table.newAppend().appendFile(dataFile).commit()

// Set branch reference
val builder = table.manageSnapshots()
builder.setRef("branchName", SnapshotRef.branchBuilder(snapshotId).build()).commit()

// Set branch snapshot
builder.setBranchSnapshot("branchName", snapshotId).commit()

// Remove snapshots
builder.removeSnapshots(snapshotId).commit()

// View table operations
table.snapshots()  // All snapshots
table.refs()       // All references
table.currentSnapshot()  // Current snapshot
```

## Steps Executed
```java
// Paste your actual Java test code here

@Test
void testAppendChainwithManualSnapshotRefManagement() throws Exception {
  try (SparkSession spark = getSparkSession()) {
    spark.sql("CREATE TABLE openhouse.u_openhouse.test_java1 (id int, data string)");
    Operations operations = Operations.withCatalog(spark, null);
    Table table = operations.getTable("u_openhouse.test_java1");
    
    // Your test implementation here
    
    spark.sql("DROP TABLE openhouse.u_openhouse.test_java1");
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

# Test: Java-5 - Manual Snapshot Ref Update Race Condition
**Assignee:** christian  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, commit S1 to main, create branch test pointing to S1, commit S2 to main, attempt to update test branch to point to S2 using setRef() with SnapshotRef object, commit S3 to main in same transaction, verify test branch update succeeded and points to S2, main points to S3, refs properly separated in metadata.

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
void testManualSnapshotRefUpdateRaceCondition() throws Exception {
  try (SparkSession spark = getSparkSession()) {
    spark.sql("CREATE TABLE openhouse.u_openhouse.test_java5 (id int, data string)");
    Operations operations = Operations.withCatalog(spark, null);
    Table table = operations.getTable("u_openhouse.test_java5");
    
    // Your test implementation here
    
    spark.sql("DROP TABLE openhouse.u_openhouse.test_java5");
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

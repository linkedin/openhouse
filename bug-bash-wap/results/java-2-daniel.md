# Test: Java-2 - Staged WAP with Manual Cherry-Pick via replaceSnapshots
**Assignee:** daniel  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, set table property write.wap.enabled=true, append FILE_A to main and commit, append FILE_B with wap.id=test-wap using stageOnly(), manually create new snapshot via newAppend() that copies FILE_B data but with main as parent (simulating cherry-pick), call replaceSnapshots() to swap, verify main has FILE_A and FILE_B data, original WAP snapshot still exists with wap.id property.

## Quick Reference
```scala
// Java API imports (avoid wildcard so Spark's 'spark' stays unique)
import liopenhouse.relocated.org.apache.iceberg.Table
import liopenhouse.relocated.org.apache.iceberg.Snapshot
import liopenhouse.relocated.org.apache.iceberg.SnapshotRef
import liopenhouse.relocated.org.apache.iceberg.TableMetadata
import liopenhouse.relocated.org.apache.iceberg.catalog.TableIdentifier
import liopenhouse.relocated.org.apache.iceberg.types.Types

// Setup
val timestamp = System.currentTimeMillis()
val tableName = s"test_java2_${timestamp}"

// Create table via Spark SQL
spark.sql(s"CREATE TABLE openhouse.u_openhouse.${tableName} (id int, data string)")

// Get catalog and table
val catalog = spark.sessionState.catalogManager.catalog("openhouse")
  .asInstanceOf[liopenhouse.relocated.org.apache.iceberg.spark.SparkCatalog]
val table: Table = catalog.loadTable(TableIdentifier.of("u_openhouse", tableName))

// Get current snapshot
val snapshot: Snapshot = table.currentSnapshot()
val snapshotId = snapshot.snapshotId()
val parentId = snapshot.parentId()

// Append data (creates snapshot S1 on main just like WapIdJavaTest)
table.newAppend().appendFile(dataFile).commit()
val snapshotIdMain = table.currentSnapshot().snapshotId()

// Set branch reference
val builder = table.manageSnapshots()
builder.setRef("branchName", SnapshotRef.branchBuilder(snapshotId).build()).commit()

// Set branch snapshot
builder.setBranchSnapshot("branchName", snapshotId).commit()

// Remove snapshots
builder.removeSnapshots(snapshotId).commit()

// Commit S3 to main + repoint branch in one transaction (see services/tables/src/test/java/com/linkedin/openhouse/tables/e2e/h2/SnapshotsControllerTest.java)
val txn = table.newTransaction()
val appendS3 = txn.newAppend()
appendS3.appendFile(dataFileC) // replace with your FILE_C
appendS3.commit()
val snapshotIdS3 = txn.table().currentSnapshot().snapshotId()

val refsUpdate = txn.manageSnapshots()
refsUpdate.setRef("test", SnapshotRef.branchBuilder(snapshotIdS2).build()) // update branch
refsUpdate.setBranchSnapshot(SnapshotRef.MAIN_BRANCH, snapshotIdS3)        // publish S3 to main
refsUpdate.commit()
txn.commitTransaction()

// View table operations
table.snapshots()  // All snapshots
table.refs()       // All references
table.currentSnapshot()  // Current snapshot

// Cleanup
spark.sql(s"DROP TABLE openhouse.u_openhouse.${tableName}")
```

## Input
```scala
// Copy-paste all commands you ran here

val timestamp = System.currentTimeMillis()
val tableName = s"test_java2_${timestamp}"
spark.sql(s"CREATE TABLE openhouse.u_openhouse.${tableName} (id int, data string)")

val catalog = spark.sessionState.catalogManager.catalog("openhouse")
  .asInstanceOf[liopenhouse.relocated.org.apache.iceberg.spark.SparkCatalog]
val table: Table = catalog.loadTable(TableIdentifier.of("u_openhouse", tableName))

// ... your test implementation ...

```

## Output
```
[Copy-paste all output here - terminal output, query results, errors, etc.]

```

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe the bug, error messages, unexpected behavior]


# Test: Java-14 - Snapshot Ref with Custom Metadata Properties
**Assignee:** zhe  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
Create table, commit S1 to main, create branch custom from S1 using SnapshotRef.branchBuilder with custom properties (minSnapshotsToKeep=10, maxRefAgeMs=3600000), append FILE_A to custom, verify branch works correctly, query table.refs() and verify custom ref properties are persisted, verify retention policy respects custom properties.

## Quick Reference
```scala
// Java API imports (avoid wildcard so Spark's 'spark' stays unique)
import liopenhouse.relocated.org.apache.iceberg.Table
import liopenhouse.relocated.org.apache.iceberg.Snapshot
import liopenhouse.relocated.org.apache.iceberg.SnapshotRef
import liopenhouse.relocated.org.apache.iceberg.TableMetadata
import liopenhouse.relocated.org.apache.iceberg.catalog.Identifier
import liopenhouse.relocated.org.apache.iceberg.types.Types

// Setup
val timestamp = System.currentTimeMillis()
val tableName = s"test_java14_${timestamp}"

// Create table via Spark SQL
spark.sql(s"CREATE TABLE openhouse.u_openhouse.${tableName} (id int, data string)")

// Get catalog and table
val catalog = spark.sessionState.catalogManager.catalog("openhouse")
  .asInstanceOf[liopenhouse.relocated.org.apache.iceberg.spark.SparkCatalog]
val table: Table = catalog.loadTable(Identifier.of("u_openhouse", tableName))

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

// Cleanup
spark.sql(s"DROP TABLE openhouse.u_openhouse.${tableName}")
```

## Input
```scala
// Copy-paste all commands you ran here

val timestamp = System.currentTimeMillis()
val tableName = s"test_java14_${timestamp}"
spark.sql(s"CREATE TABLE openhouse.u_openhouse.${tableName} (id int, data string)")

val catalog = spark.sessionState.catalogManager.catalog("openhouse")
  .asInstanceOf[liopenhouse.relocated.org.apache.iceberg.spark.SparkCatalog]
val table: Table = catalog.loadTable(Identifier.of("u_openhouse", tableName))

// ... your test implementation ...

```

## Output
```
[Copy-paste all output here - terminal output, query results, errors, etc.]

```

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe the bug, error messages, unexpected behavior]


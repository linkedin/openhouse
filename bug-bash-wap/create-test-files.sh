#!/bin/bash

# SQL Test assignments
declare -A sql_tests
sql_tests=(
  ["01"]="abhishek:Diamond Branch Merge with WAP Publishing:Create table, enable WAP (write.wap.enabled=true), insert base data on main, create branch A from main, create branch B from main, insert data on branch A, insert different data on branch B, stage WAP snapshot on main, fast-forward main to branch A, cherry-pick WAP snapshot to branch B, verify: main has branch A data, branch B has its own data plus WAP data, all three branches point to different snapshots."
  ["02"]="daniel:Orphaned Snapshot After Fast-Forward Race:Create table, insert on main, create branch feature, insert twice on feature, insert once on main (diverge), create branch temp from feature, fast-forward temp to main (should fail due to divergence), verify temp still points to feature's snapshot, insert on temp, verify temp advanced independently and all snapshots are preserved."
  ["03"]="vikram:Multi-WAP Publishing to Different Branches:Create table, enable WAP (write.wap.enabled=true), insert base on main, create branch A and branch B, stage WAP ID wap1 targeting main, stage WAP ID wap2 targeting main, stage WAP ID wap3 targeting main, cherry-pick wap1 to branch A, cherry-pick wap2 to branch B, cherry-pick wap3 to main, verify each branch has exactly one WAP-derived snapshot and correct data isolation."
  ["04"]="william:Circular Fast-Forward Prevention:Create table, insert on main, create branch A from main, insert on branch A, create branch B from branch A, insert on branch B, attempt fast-forward A to B (should succeed), attempt fast-forward B to A (should fail - would be backwards), verify branch B is ahead of A and operation preserves data integrity."
  ["05"]="christian:WAP Branch Switch Mid-Transaction Simulation:Create table, enable WAP (write.wap.enabled=true), insert base data, create branch staging and branch review, set wap.branch to staging, insert data, verify data visible on staging, change wap.branch to review mid-session, insert different data, unset wap.branch, verify staging has first insert, review has second insert, main has only base data, all isolated correctly."
  ["06"]="stas:Cascading Branch Deletions with Ref Validation:Create table, enable WAP (write.wap.enabled=true), insert on main, create branches A B C all from main, insert on each branch separately, get all snapshot IDs from branches, expire snapshot from branch A (non-head), verify expiration succeeded, verify branches B and C still reference their snapshots correctly, attempt to expire branch B HEAD snapshot (should fail - still referenced)."
  ["07"]="levi:Cherry-Pick Chain with Parent Rewriting:Create table, enable WAP (write.wap.enabled=true), insert base on main, stage wap1 (parent: base), stage wap2 (parent: wap1), stage wap3 (parent: wap2), insert regular commit on main, cherry-pick wap3 to main (skipping wap1 and wap2), verify main has base + regular + wap3 data, verify wap3 parent was rewritten to point to regular commit, verify wap1 and wap2 remain unpublished."
  ["08"]="rohit:Branch Creation Race on Empty Table:Create empty table (no snapshots), create branch A (should create empty snapshot), create branch B from branch A, verify both branches exist and point to same empty snapshot, set wap.branch to A, insert data, verify branch A advanced, branch B still at empty snapshot, create branch C from empty table root, verify C also points to original empty snapshot."
  ["09"]="selena:Multi-Branch Fast-Forward with Stale Refs Cleanup:Create table, insert on main, create branches A B C D from main, insert on branch D, fast-forward C to D, fast-forward B to C, fast-forward A to B, verify all branches A-D now point to same snapshot as D, expire intermediate snapshots that became unreferenced, verify only current snapshot remains, all four branches still work."
  ["10"]="shanthoosh:WAP ID Publishing to Non-Main with Branch Deletion:Create table, enable WAP (write.wap.enabled=true), insert on main, create branches feature and temp, stage wap1 with data, cherry-pick wap1 to feature branch, verify feature has WAP data, drop temp branch via Iceberg API, stage wap2 with different data, cherry-pick wap2 to main, verify main has wap2 data, feature unchanged, temp branch refs properly cleaned up."
  ["11"]="simbarashe:Interleaved WAP and Direct Commits on Same Branch:Create table, enable WAP (write.wap.enabled=true), insert base on main, stage wap1 with dataA, insert dataB directly to main (non-WAP), stage wap2 with dataC, insert dataD directly to main, cherry-pick wap1 to main, verify main has base+dataB+dataD+dataA in correct snapshot lineage, verify wap2 remains unpublished, verify all parent pointers form valid chain."
  ["12"]="aastha:Branch from WAP Snapshot Before Cherry-Pick:Create table, enable WAP (write.wap.enabled=true), insert base on main, stage wap1 with data, create branch experimental from wap1 snapshot (staged but not published), insert more data on experimental, cherry-pick wap1 to main, verify main and experimental both have wap1 data, verify experimental has additional data, verify both branches share wap1 snapshot in ancestry."
  ["13"]="jiefan:Concurrent Branch Commits During Fast-Forward Window:Create table, insert on main, create branch A and B from main, insert data1 on branch A, capture A snapshot ID, insert data2 on branch B, fast-forward B to A using captured snapshot ID, insert data3 on branch A (after fast-forward initiated), verify B has data1 only, verify A has data1 and data3, verify fast-forward was point-in-time."
  ["14"]="zhe:WAP Branch Target with Non-Existent Branch:Create table, enable WAP (write.wap.enabled=true), insert base on main, set wap.branch to nonExistentBranch, attempt insert (should fail or auto-create branch per behavior), if branch created verify it exists in refs, if failed verify main unchanged, unset wap.branch, verify system state is consistent."
  ["15"]="kevin:Snapshot Expiration with Cross-Branch Dependencies:Create table, insert S1 on main, create branches A B C from S1, insert S2 on A, insert S3 on B (parent S1), insert S4 on C (parent S1), cherry-pick S2 to C (S5 created), attempt expire S1 (should fail - referenced by B and original A/B/C ancestry), expire S3 (should succeed - only B head), verify all branches functional, S1 S2 S4 S5 remain."
  ["16"]="junhao:Rename Branch via Ref Management:Create table, insert on main, create branch oldName, insert data on oldName, capture oldName snapshot ID, create new branch newName pointing to same snapshot ID, verify both branches show same data, drop oldName branch, verify newName still works with all data, verify refs table shows only newName and main."
  ["17"]="ruolin:WAP ID Collision and Override:Create table, enable WAP (write.wap.enabled=true), insert base on main, stage wap1 with dataA, stage another commit with same wap.id=wap1 but different dataB (override/collision), cherry-pick wap1 to main, verify which data landed on main (latest wap1 or error), verify snapshot metadata and wap.id properties, verify no orphaned snapshots."
)

# Java Test assignments
declare -A java_tests
java_tests=(
  ["01"]="abhishek:Append Chain with Manual Snapshot Ref Management:Create table via Java, append FILE_A to create snapshot S1, create branch dev pointing to S1 via setRef(), append FILE_B to dev branch using setBranchSnapshot(), append FILE_C to main using setBranchSnapshot(), verify: main points to S1->S3 lineage, dev points to S1->S2 lineage, both branches isolated, total 3 snapshots exist."
  ["02"]="daniel:Staged WAP with Manual Cherry-Pick via replaceSnapshots:Create table, set table property write.wap.enabled=true, append FILE_A to main and commit, append FILE_B with wap.id=test-wap using stageOnly(), manually create new snapshot via newAppend() that copies FILE_B data but with main as parent (simulating cherry-pick), call replaceSnapshots() to swap, verify main has FILE_A and FILE_B data, original WAP snapshot still exists with wap.id property."
  ["03"]="vikram:Concurrent Branch Creation with Overlapping Commits:Create table, commit snapshot S1 to main, manually create two SnapshotRef objects for branches A and B both pointing to S1, set both refs in single metadata builder operation, append FILE_A to branch A (snapshot S2), append FILE_B to branch B (snapshot S3), verify refs table shows all three branches with correct snapshot IDs, no cross-contamination of data files."
  ["04"]="william:Parent Chain Validation After Delete and Reinsert:Create table, commit snapshots S1 S2 S3 sequentially on main, get S2 snapshot ID, call removeSnapshots() to delete S2, verify S3 parent still points to S1 (parent chain intact), create branch from S3, append FILE_A to branch, verify new snapshot parent is S3, parent chain remains valid."
  ["05"]="christian:Manual Snapshot Ref Update Race Condition:Create table, commit S1 to main, create branch test pointing to S1, commit S2 to main, attempt to update test branch to point to S2 using setRef() with SnapshotRef object, commit S3 to main in same transaction, verify test branch update succeeded and points to S2, main points to S3, refs properly separated in metadata."
  ["06"]="stas:Empty Snapshot Fast-Forward Chain:Create empty table (no commits), create branch empty via setRef() with SnapshotRef pointing to null snapshot (Iceberg initial state), append FILE_A to create S1, update main to point to S1, attempt fast-forward empty branch to main S1, verify empty branch now has data, call refs() to verify branch references updated correctly."
  ["07"]="levi:WAP Snapshot Expiration with Lineage Preservation:Create table, set table property write.wap.enabled=true, commit S1 to main, append FILE_A with wap.id=wap1 stageOnly() to create S2, commit S3 to main, append FILE_B with wap.id=wap2 stageOnly() to create S4, call removeSnapshots() to expire S2 (unpublished wap1), verify S1 S3 S4 remain, snapshot lineage from main (S1->S3) intact, wap2 still available for publishing."
  ["08"]="rohit:Multi-Branch Append with Shared Parent Snapshot:Create table, commit S1 to main with FILE_A, create branches b1 b2 b3 all pointing to S1, append FILE_B to b1 creating S2 with parent=S1, append FILE_C to b2 creating S3 with parent=S1, append FILE_D to b3 creating S4 with parent=S1, verify all three snapshots have same parent but different data files, refs table shows correct snapshot IDs for each branch."
  ["09"]="selena:Snapshot Ref Retention Policy Enforcement:Create table, commit 5 snapshots S1-S5 sequentially to main, create branch old pointing to S2, create branch mid pointing to S4, attempt removeSnapshots() with retention policy to expire S1 S3 (neither referenced by branches), verify S2 S4 S5 remain (referenced or current), verify old and mid branches still functional and point to correct snapshots."
  ["10"]="shanthoosh:Branch Snapshot Override with Concurrent Main Advancement:Create table, commit S1 to main, create branch feature pointing to S1, append FILE_A to feature creating S2, append FILE_B to main creating S3, call setBranchSnapshot() for feature with new snapshot S4 (child of S3) containing FILE_C, verify feature branch jumped from S1->S2 lineage to S3->S4 lineage, S2 becomes orphaned but not deleted (may be referenced elsewhere), main unaffected at S3."
  ["11"]="simbarashe:Transactional Multi-Branch Update with Rollback:Create table, commit S1 to main, create branches A and B from S1, begin manageSnapshots() transaction, setRef A to new snapshot S2, setRef B to new snapshot S3, before commit create branch C from S1, commit transaction, verify A->S2 B->S3 C->S1, attempt rollback simulation by re-setting A and B to S1, verify transaction atomicity."
  ["12"]="aastha:Branch Creation from Detached Snapshot:Create table, commit S1 S2 S3 to main, call removeSnapshots to detach S2 from main lineage (but keep metadata), create branch orphan pointing to detached S2 snapshot ID, attempt append to orphan branch, verify behavior (should work with detached parent or fail), verify snapshot ancestry handling."
  ["13"]="jiefan:Parallel Branch Append with Metadata Conflicts:Create table, commit S1 to main, create branch A from S1, obtain two separate Table instances for same table, on instance1 append FILE_A to branch A, on instance2 append FILE_B to branch A, commit both (conflict scenario), verify conflict resolution, verify final branch A state, verify no data loss."
  ["14"]="zhe:Snapshot Ref with Custom Metadata Properties:Create table, commit S1 to main, create branch custom from S1 using SnapshotRef.branchBuilder with custom properties (minSnapshotsToKeep=10, maxRefAgeMs=3600000), append FILE_A to custom, verify branch works correctly, query table.refs() and verify custom ref properties are persisted, verify retention policy respects custom properties."
  ["15"]="kevin:Cross-Table Snapshot Reference Attempt:Create two tables tableA and tableB, commit S1 to tableA main, commit S2 to tableB main, attempt to create branch in tableA pointing to tableB's S2 snapshot ID (cross-table reference), verify operation fails with appropriate error, verify tableA metadata unchanged, verify isolation between tables."
  ["16"]="junhao:Bulk Branch Creation and Snapshot Reuse:Create table, commit S1 to main, in single manageSnapshots() transaction create 10 branches (b1 through b10) all pointing to S1, commit transaction, verify all 10 branches exist in refs(), verify all point to same snapshot S1, append FILE_A to b5 creating S2, verify other 9 branches still at S1, verify b5 at S2."
  ["17"]="ruolin:Snapshot Replace with WAP Metadata Preservation:Create table, enable WAP, commit S1 to main, append FILE_A with wap.id=wap1 stageOnly() creating S2, manually call replaceSnapshots() to swap S2 metadata (preserving wap.id property), verify S2 still marked as WAP snapshot, attempt cherry-pick wap1 after replace, verify wap.id metadata survived replacement operation."
)

# Create SQL test files
for key in "${!sql_tests[@]}"; do
  IFS=':' read -r assignee name prompt <<< "${sql_tests[$key]}"
  cat > "results/sql-${key}-${assignee}.md" << TESTEOF
# Test: SQL-${key} - ${name}
**Assignee:** ${assignee}  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
${prompt}

## Quick Reference
\`\`\`scala
// Setup
val timestamp = System.currentTimeMillis()
val tableName = s"test_xxx_\${timestamp}"

// Create table
spark.sql(s"CREATE TABLE openhouse.u_openhouse.\${tableName} (id INT, data STRING)")

// Enable WAP
spark.sql(s"ALTER TABLE openhouse.u_openhouse.\${tableName} SET TBLPROPERTIES ('write.wap.enabled'='true')")

// Insert data
spark.sql(s"INSERT INTO openhouse.u_openhouse.\${tableName} VALUES (1, 'data')")

// Create branch
spark.sql(s"ALTER TABLE openhouse.u_openhouse.\${tableName} CREATE BRANCH myBranch")

// Set WAP config
spark.conf.set("spark.wap.id", "wap1")
spark.conf.set("spark.wap.branch", "myBranch")

// Cherry-pick: CALL openhouse.system.cherrypick_snapshot('openhouse.u_openhouse.\${tableName}', 'main', snapshotId)
// Fast-forward: CALL openhouse.system.fast_forward('openhouse.u_openhouse.\${tableName}', 'branch1', 'branch2')

// View snapshots
spark.sql(s"SELECT snapshot_id, operation, summary FROM openhouse.u_openhouse.\${tableName}.snapshots").show(false)

// View refs
spark.sql(s"SELECT name, snapshot_id FROM openhouse.u_openhouse.\${tableName}.refs").show(false)

// Query branch data
spark.sql(s"SELECT * FROM openhouse.u_openhouse.\${tableName}.branch_myBranch").show()

// Drop table
spark.sql(s"DROP TABLE openhouse.u_openhouse.\${tableName}")
\`\`\`

## Input
\`\`\`scala
// Copy-paste all commands you ran here

val timestamp = System.currentTimeMillis()
val tableName = s"test_sql${key}_\${timestamp}"
spark.sql(s"CREATE TABLE openhouse.u_openhouse.\${tableName} (name string)")
spark.sql(s"ALTER TABLE openhouse.u_openhouse.\${tableName} SET TBLPROPERTIES ('write.wap.enabled'='true')")

// ... your test commands ...

\`\`\`

## Output
\`\`\`
[Copy-paste all output here - terminal output, query results, errors, etc.]

\`\`\`

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe the bug, error messages, unexpected behavior]

TESTEOF
done

# Create Java test files
for key in "${!java_tests[@]}"; do
  IFS=':' read -r assignee name prompt <<< "${java_tests[$key]}"
  cat > "results/java-${key}-${assignee}.md" << TESTEOF
# Test: Java-${key} - ${name}
**Assignee:** ${assignee}  
**Date:** [YYYY-MM-DD]  
**Status:** 🔲 NOT STARTED

## Test Prompt
${prompt}

## Quick Reference
\`\`\`scala
// Java API imports (avoid wildcard so Spark's 'spark' stays unique)
import liopenhouse.relocated.org.apache.iceberg.Table
import liopenhouse.relocated.org.apache.iceberg.Snapshot
import liopenhouse.relocated.org.apache.iceberg.SnapshotRef
import liopenhouse.relocated.org.apache.iceberg.TableMetadata
import liopenhouse.relocated.org.apache.iceberg.catalog.TableIdentifier
import liopenhouse.relocated.org.apache.iceberg.types.Types

// Setup
val timestamp = System.currentTimeMillis()
val tableName = s"test_java${key}_\${timestamp}"

// Create table via Spark SQL
spark.sql(s"CREATE TABLE openhouse.u_openhouse.\${tableName} (id int, data string)")

// Get catalog and table
val catalog = spark.sessionState.catalogManager.catalog("openhouse")
  .asInstanceOf[liopenhouse.relocated.org.apache.iceberg.spark.SparkCatalog]
val table: Table = catalog.loadTable(TableIdentifier.of("u_openhouse", tableName))

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
spark.sql(s"DROP TABLE openhouse.u_openhouse.\${tableName}")
\`\`\`

## Input
\`\`\`scala
// Copy-paste all commands you ran here

val timestamp = System.currentTimeMillis()
val tableName = s"test_java${key}_\${timestamp}"
spark.sql(s"CREATE TABLE openhouse.u_openhouse.\${tableName} (id int, data string)")

val catalog = spark.sessionState.catalogManager.catalog("openhouse")
  .asInstanceOf[liopenhouse.relocated.org.apache.iceberg.spark.SparkCatalog]
val table: Table = catalog.loadTable(TableIdentifier.of("u_openhouse", tableName))

// ... your test implementation ...

\`\`\`

## Output
\`\`\`
[Copy-paste all output here - terminal output, query results, errors, etc.]

\`\`\`

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe the bug, error messages, unexpected behavior]

TESTEOF
done

echo "Created 34 test result files!"

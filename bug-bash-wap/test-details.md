# Bug Bash Test Details

## Spark SQL Tests

### SQL-01: Diamond Branch Merge with WAP Publishing
Create table, **enable WAP** (`write.wap.enabled=true`), insert base data on main, create branch A from main, create branch B from main, insert data on branch A, insert different data on branch B, stage WAP snapshot on main, fast-forward main to branch A, cherry-pick WAP snapshot to branch B, verify: main has branch A data, branch B has its own data plus WAP data, all three branches point to different snapshots.

### SQL-02: Orphaned Snapshot After Fast-Forward Race
Create table, insert on main, create branch feature, insert twice on feature, insert once on main (diverge), create branch temp from feature, fast-forward temp to main (should fail due to divergence), verify temp still points to feature's snapshot, insert on temp, verify temp advanced independently and all snapshots are preserved.

### SQL-03: Multi-WAP Publishing to Different Branches
Create table, **enable WAP** (`write.wap.enabled=true`), insert base on main, create branch A and branch B, stage WAP ID "wap1" targeting main, stage WAP ID "wap2" targeting main, stage WAP ID "wap3" targeting main, cherry-pick wap1 to branch A, cherry-pick wap2 to branch B, cherry-pick wap3 to main, verify each branch has exactly one WAP-derived snapshot and correct data isolation.

### SQL-04: Circular Fast-Forward Prevention
Create table, insert on main, create branch A from main, insert on branch A, create branch B from branch A, insert on branch B, attempt fast-forward A to B (should succeed), attempt fast-forward B to A (should fail - would be backwards), verify branch B is ahead of A and operation preserves data integrity.

### SQL-05: WAP Branch Switch Mid-Transaction Simulation
Create table, **enable WAP** (`write.wap.enabled=true`), insert base data, create branch staging and branch review, set wap.branch to staging, insert data, verify data visible on staging, change wap.branch to review mid-session, insert different data, unset wap.branch, verify staging has first insert, review has second insert, main has only base data, all isolated correctly.

### SQL-06: Cascading Branch Deletions with Ref Validation
Create table, **enable WAP** (`write.wap.enabled=true`), insert on main, create branches A, B, C all from main, insert on each branch separately, get all snapshot IDs from branches, expire snapshot from branch A (non-head), verify expiration succeeded, verify branches B and C still reference their snapshots correctly, attempt to expire branch B's HEAD snapshot (should fail - still referenced).

### SQL-07: Cherry-Pick Chain with Parent Rewriting
Create table, **enable WAP** (`write.wap.enabled=true`), insert base on main, stage wap1 (parent: base), stage wap2 (parent: wap1), stage wap3 (parent: wap2), insert regular commit on main, cherry-pick wap3 to main (skipping wap1 and wap2), verify main has base + regular + wap3 data, verify wap3's parent was rewritten to point to regular commit, verify wap1 and wap2 remain unpublished.

### SQL-08: Branch Creation Race on Empty Table
Create empty table (no snapshots), create branch A (should create empty snapshot), create branch B from branch A, verify both branches exist and point to same empty snapshot, set wap.branch to A, insert data, verify branch A advanced, branch B still at empty snapshot, create branch C from empty table root, verify C also points to original empty snapshot.

### SQL-09: Multi-Branch Fast-Forward with Stale Refs Cleanup
Create table, insert on main, create branches A, B, C, D from main, insert on branch D, fast-forward C to D, fast-forward B to C, fast-forward A to B, verify all branches A-D now point to same snapshot as D, expire intermediate snapshots that became unreferenced, verify only current snapshot remains, all four branches still work.

### SQL-10: WAP ID Publishing to Non-Main with Branch Deletion
Create table, **enable WAP** (`write.wap.enabled=true`), insert on main, create branches feature and temp, stage wap1 with data, cherry-pick wap1 to feature branch, verify feature has WAP data, drop temp branch via Iceberg API, stage wap2 with different data, cherry-pick wap2 to main, verify main has wap2 data, feature unchanged, temp branch refs properly cleaned up.

---

## Java API Tests

### Java-01: Append Chain with Manual Snapshot Ref Management
Create table via Java, append FILE_A to create snapshot S1, create branch "dev" pointing to S1 via setRef(), append FILE_B to dev branch using setBranchSnapshot(), append FILE_C to main using setBranchSnapshot(), verify: main points to S1->S3 lineage, dev points to S1->S2 lineage, both branches isolated, total 3 snapshots exist.

### Java-02: Staged WAP with Manual Cherry-Pick via replaceSnapshots
Create table, **set table property** `write.wap.enabled=true`, append FILE_A to main and commit, append FILE_B with wap.id="test-wap" using stageOnly(), manually create new snapshot via newAppend() that copies FILE_B's data but with main as parent (simulating cherry-pick), call replaceSnapshots() to swap, verify main has FILE_A and FILE_B data, original WAP snapshot still exists with wap.id property.

### Java-03: Concurrent Branch Creation with Overlapping Commits
Create table, commit snapshot S1 to main, manually create two SnapshotRef objects for branches "A" and "B" both pointing to S1, set both refs in single metadata builder operation, append FILE_A to branch A (snapshot S2), append FILE_B to branch B (snapshot S3), verify refs table shows all three branches with correct snapshot IDs, no cross-contamination of data files.

### Java-04: Parent Chain Validation After Delete and Reinsert
Create table, commit snapshots S1, S2, S3 sequentially on main, get S2's snapshot ID, call removeSnapshots() to delete S2, verify S3's parent still points to S1 (parent chain intact), create branch from S3, append FILE_A to branch, verify new snapshot's parent is S3, parent chain remains valid.

### Java-05: Manual Snapshot Ref Update Race Condition
Create table, commit S1 to main, create branch "test" pointing to S1, commit S2 to main, attempt to update "test" branch to point to S2 using setRef() with SnapshotRef object, commit S3 to main in same transaction, verify "test" branch update succeeded and points to S2, main points to S3, refs properly separated in metadata.

### Java-06: Empty Snapshot Fast-Forward Chain
Create empty table (no commits), create branch "empty" via setRef() with SnapshotRef pointing to null snapshot (Iceberg's initial state), append FILE_A to create S1, update main to point to S1, attempt fast-forward "empty" branch to main's S1, verify empty branch now has data, call refs() to verify branch references updated correctly.

### Java-07: WAP Snapshot Expiration with Lineage Preservation
Create table, **set table property** `write.wap.enabled=true`, commit S1 to main, append FILE_A with wap.id="wap1" stageOnly() to create S2, commit S3 to main, append FILE_B with wap.id="wap2" stageOnly() to create S4, call removeSnapshots() to expire S2 (unpublished wap1), verify S1, S3, S4 remain, snapshot lineage from main (S1->S3) intact, wap2 still available for publishing.

### Java-08: Multi-Branch Append with Shared Parent Snapshot
Create table, commit S1 to main with FILE_A, create branches "b1", "b2", "b3" all pointing to S1, append FILE_B to b1 creating S2 with parent=S1, append FILE_C to b2 creating S3 with parent=S1, append FILE_D to b3 creating S4 with parent=S1, verify all three snapshots have same parent but different data files, refs table shows correct snapshot IDs for each branch.

### Java-09: Snapshot Ref Retention Policy Enforcement
Create table, commit 5 snapshots S1-S5 sequentially to main, create branch "old" pointing to S2, create branch "mid" pointing to S4, attempt removeSnapshots() with retention policy to expire S1, S3 (neither referenced by branches), verify S2, S4, S5 remain (referenced or current), verify "old" and "mid" branches still functional and point to correct snapshots.

### Java-10: Branch Snapshot Override with Concurrent Main Advancement
Create table, commit S1 to main, create branch "feature" pointing to S1, append FILE_A to feature creating S2, append FILE_B to main creating S3, call setBranchSnapshot() for feature with new snapshot S4 (child of S3) containing FILE_C, verify feature branch jumped from S1->S2 lineage to S3->S4 lineage, S2 becomes orphaned but not deleted (may be referenced elsewhere), main unaffected at S3.

---

## Quick Reference

### Spark SQL Commands
```sql
-- Create & setup
CREATE TABLE openhouse.d1.test_xxx (name string);
ALTER TABLE openhouse.d1.test_xxx SET TBLPROPERTIES ('write.wap.enabled'='true');

-- Insert data
INSERT INTO openhouse.d1.test_xxx VALUES ('data');
INSERT INTO openhouse.d1.test_xxx.branch_myBranch VALUES ('branch_data');

-- Branch operations
ALTER TABLE openhouse.d1.test_xxx CREATE BRANCH myBranch;
CALL openhouse.system.fast_forward('openhouse.d1.test_xxx', 'target_branch', 'source_branch');
CALL openhouse.system.cherrypick_snapshot('d1.test_xxx', snapshot_id);
CALL openhouse.system.expire_snapshots(table => 'd1.test_xxx', snapshot_ids => Array(snapshot_id));

-- WAP operations
-- Set before INSERT: spark.conf().set("spark.wap.id", "wap-name")
-- Set before INSERT: spark.conf().set("spark.wap.branch", "branch-name")

-- Query metadata
SELECT * FROM openhouse.d1.test_xxx.snapshots;
SELECT * FROM openhouse.d1.test_xxx.refs;
SELECT * FROM openhouse.d1.test_xxx VERSION AS OF 'branch_name';
```

### Java API Commands
```java
// Get table
Operations operations = Operations.withCatalog(spark, null);
Table table = operations.getTable("d1.test_xxx");

// Append data
table.newAppend()
  .appendFile(FILE_A)
  .commit();

// WAP staging
table.newAppend()
  .appendFile(FILE_B)
  .set("wap.id", "wap1")
  .stageOnly()
  .commit();

// Branch operations
SnapshotRef ref = SnapshotRef.branchBuilder(snapshotId).build();
table.manageSnapshots()
  .setRef("branch_name", ref)
  .commit();

table.manageSnapshots()
  .setBranchSnapshot("branch_name", snapshot)
  .commit();

// Remove snapshots
table.manageSnapshots()
  .removeSnapshots(snapshotId)
  .commit();

// Query
table.currentSnapshot();
table.refs();
table.snapshots();
snapshot.parentId();
```

### Data Files for Java Tests
```java
private static final Schema SCHEMA =
    new Schema(
        required(1, "id", Types.IntegerType.get()),
        required(2, "data", Types.StringType.get()));

private static final PartitionSpec SPEC =
    PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

private static final DataFile FILE_A =
    DataFiles.builder(SPEC)
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0")
        .withRecordCount(1)
        .build();

private static final DataFile FILE_B =
    DataFiles.builder(SPEC)
        .withPath("/path/to/data-b.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=1")
        .withRecordCount(1)
        .build();

// Add FILE_C, FILE_D as needed with different paths
```


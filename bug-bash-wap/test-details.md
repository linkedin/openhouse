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


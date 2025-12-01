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
  ["09"]="selena:Multi-Branch Fast-Forward with Stale Refs Cleanup:Create table, insert on main, create branches A B C D from main, insert on branch D, fast-forward C to D, fast-forward B to C, fast-forward A to B, verify all branches A-D now point to the same snapshot as D, expire intermediate snapshots that became unreferenced, verify only current snapshot remains, all four branches still work."
  ["10"]="shanthoosh:WAP ID Publishing to Non-Main with Branch Deletion:Create table, enable WAP (write.wap.enabled=true), insert on main, create branches feature and temp, stage wap1 with data, cherry-pick wap1 to feature branch, verify feature has WAP data, drop temp branch via Iceberg API, stage wap2 with different data, cherry-pick wap2 to main, verify main has wap2 data, feature unchanged, temp branch refs properly cleaned up."
  ["11"]="simbarashe:Interleaved WAP and Direct Commits on Same Branch:Create table, enable WAP (write.wap.enabled=true), insert base on main, stage wap1 with dataA, insert dataB directly to main (non-WAP), stage wap2 with dataC, insert dataD directly to main, cherry-pick wap1 to main, verify main has base+dataB+dataD+dataA in correct snapshot lineage, verify wap2 remains unpublished, verify all parent pointers form valid chain."
  ["12"]="aastha:Branch from WAP Snapshot Before Cherry-Pick:Create table, enable WAP (write.wap.enabled=true), insert base on main, stage wap1 with data, create branch experimental from wap1 snapshot (staged but not published), insert more data on experimental, cherry-pick wap1 to main, verify main and experimental both have wap1 data, verify experimental has additional data, verify both branches share wap1 snapshot in ancestry."
  ["13"]="jiefan:Concurrent Branch Commits During Fast-Forward Window:Create table, insert on main, create branch A and B from main, insert data1 on branch A, capture A snapshot ID, insert data2 on branch B, fast-forward B to A using captured snapshot ID, insert data3 on branch A (after fast-forward initiated), verify B has data1 only, verify A has data1 and data3, verify fast-forward was point-in-time."
  ["14"]="zhe:WAP Branch Target with Non-Existent Branch:Create table, enable WAP (write.wap.enabled=true), insert base on main, set wap.branch to nonExistentBranch, attempt insert (should fail or auto-create branch per behavior), if branch created verify it exists in refs, if failed verify main unchanged, unset wap.branch, verify system state is consistent."
  ["15"]="kevin:Snapshot Expiration with Cross-Branch Dependencies:Create table, insert S1 on main, create branches A B C from S1, insert S2 on A, insert S3 on B (parent S1), insert S4 on C (parent S1), cherry-pick S2 to C (S5 created), attempt expire S1 (should fail - referenced by B and original A/B/C ancestry), expire S3 (should succeed - only B head), verify all branches functional, S1 S2 S4 S5 remain."
  ["16"]="junhao:Rename Branch via Ref Management:Create table, insert on main, create branch oldName, insert data on oldName, capture oldName snapshot ID, create new branch newName pointing to the same snapshot ID, verify both branches show the same data, drop oldName branch, verify newName still works with all data, verify refs table shows only newName and main."
  ["17"]="ruolin:WAP ID Collision and Override:Create table, enable WAP (write.wap.enabled=true), insert base on main, stage wap1 with dataA, stage another commit with the same wap.id=wap1 but different dataB (override/collision), cherry-pick wap1 to main, verify which data landed on main (latest wap1 or error), verify snapshot metadata and wap.id properties, verify no orphaned snapshots."
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
```scala
// Setup
val timestamp = System.currentTimeMillis()
val tableName = s"test_xxx_${timestamp}"

// Create table
spark.sql(s"CREATE TABLE openhouse.u_openhouse.${tableName} (id INT, data STRING)")

// Enable WAP
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} SET TBLPROPERTIES ('write.wap.enabled'='true')")

// Insert data
spark.sql(s"INSERT INTO openhouse.u_openhouse.${tableName} VALUES (1, 'data')")

// Create branch
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} CREATE BRANCH myBranch")

// Set WAP config
spark.conf.set("spark.wap.id", "wap1")
spark.conf.set("spark.wap.branch", "myBranch")

// Cherry-pick: CALL openhouse.system.cherrypick_snapshot('openhouse.u_openhouse.${tableName}', 'main', snapshotId)
// Fast-forward: CALL openhouse.system.fast_forward('openhouse.u_openhouse.${tableName}', 'branch1', 'branch2')

// View snapshots
spark.sql(s"SELECT snapshot_id, operation, summary FROM openhouse.u_openhouse.${tableName}.snapshots").show(false)

// View refs
spark.sql(s"SELECT name, snapshot_id FROM openhouse.u_openhouse.${tableName}.refs").show(false)

// Query branch data
spark.sql(s"SELECT * FROM openhouse.u_openhouse.${tableName}.branch_myBranch").show()

// Drop table
spark.sql(s"DROP TABLE openhouse.u_openhouse.${tableName}")
```

## Input
```scala
// Copy-paste all commands you ran here

val timestamp = System.currentTimeMillis()
val tableName = s"test_sql${key}_${timestamp}"
spark.sql(s"CREATE TABLE openhouse.u_openhouse.${tableName} (name string)")
spark.sql(s"ALTER TABLE openhouse.u_openhouse.${tableName} SET TBLPROPERTIES ('write.wap.enabled'='true')")

// ... your test commands ...

```

## Output
```
[Copy-paste all output here - terminal output, query results, errors, etc.]

```

## Issues Found
- [ ] No issues - test passed completely
- [ ] Bug found: [describe the bug, error messages, unexpected behavior]

TESTEOF
done

echo "Created ${#sql_tests[@]} test result files!"

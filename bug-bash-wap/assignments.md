# Bug Bash: SnapshotDiffApplier Multi-Branch Testing

**Branch:** `bug-bash-wap-2024-11`  
**Goal:** Test complex multi-branch scenarios in SnapshotDiffApplier  
**Duration:** [Set your deadline]  
**Total Tests:** 20 (10 Spark SQL + 10 Java API)

## Instructions

1. **Claim your tests:** Rename your assigned test files from `unassigned` to your name
   - Example: `sql-01-unassigned.md` → `sql-01-abhishek.md`
2. **Execute tests:** Follow the test prompt and fill in the markdown template
3. **Submit results:** Commit your completed markdown files to this branch
4. **Optional:** Save detailed logs to the `logs/` directory

## Test Assignments

| Test# | Category | Test Name | Assignee | Status | File |
|-------|----------|-----------|----------|--------|------|
| 1 | SQL | Diamond Branch Merge with WAP Publishing | abhishek | 🔲 | `results/sql-01-abhishek.md` |
| 2 | SQL | Orphaned Snapshot After Fast-Forward Race | daniel | 🔲 | `results/sql-02-daniel.md` |
| 3 | SQL | Multi-WAP Publishing to Different Branches | vikram | 🔲 | `results/sql-03-vikram.md` |
| 4 | SQL | Circular Fast-Forward Prevention | william | 🔲 | `results/sql-04-william.md` |
| 5 | SQL | WAP Branch Switch Mid-Transaction Simulation | christian | 🔲 | `results/sql-05-christian.md` |
| 6 | SQL | Cascading Branch Deletions with Ref Validation | stas | 🔲 | `results/sql-06-stas.md` |
| 7 | SQL | Cherry-Pick Chain with Parent Rewriting | levi | 🔲 | `results/sql-07-levi.md` |
| 8 | SQL | Branch Creation Race on Empty Table | rohit | 🔲 | `results/sql-08-rohit.md` |
| 9 | SQL | Multi-Branch Fast-Forward with Stale Refs Cleanup | selena | 🔲 | `results/sql-09-selena.md` |
| 10 | SQL | WAP ID Publishing to Non-Main with Branch Deletion | shanthoosh | 🔲 | `results/sql-10-shanthoosh.md` |
| 11 | Java | Append Chain with Manual Snapshot Ref Management | abhishek | 🔲 | `results/java-01-abhishek.md` |
| 12 | Java | Staged WAP with Manual Cherry-Pick via replaceSnapshots | daniel | 🔲 | `results/java-02-daniel.md` |
| 13 | Java | Concurrent Branch Creation with Overlapping Commits | vikram | 🔲 | `results/java-03-vikram.md` |
| 14 | Java | Parent Chain Validation After Delete and Reinsert | william | 🔲 | `results/java-04-william.md` |
| 15 | Java | Manual Snapshot Ref Update Race Condition | christian | 🔲 | `results/java-05-christian.md` |
| 16 | Java | Empty Snapshot Fast-Forward Chain | stas | 🔲 | `results/java-06-stas.md` |
| 17 | Java | WAP Snapshot Expiration with Lineage Preservation | levi | 🔲 | `results/java-07-levi.md` |
| 18 | Java | Multi-Branch Append with Shared Parent Snapshot | rohit | 🔲 | `results/java-08-rohit.md` |
| 19 | Java | Snapshot Ref Retention Policy Enforcement | selena | 🔲 | `results/java-09-selena.md` |
| 20 | Java | Branch Snapshot Override with Concurrent Main Advancement | shanthoosh | 🔲 | `results/java-10-shanthoosh.md` |

**Backup/Reviewer:** sumedh

## Status Legend
- 🔲 Not Started
- 🔄 In Progress
- ✅ Pass
- ❌ Fail
- ⚠️ Partial

## Communication
- Questions: [Your Slack channel]
- Issues found: Create GitHub issues and link them in your test results
- Blockers: Tag @[your-name] in Slack

## Cleanup Reminder
Please drop your test tables after completion:
```sql
DROP TABLE IF EXISTS openhouse.d1.[your_test_table];
```


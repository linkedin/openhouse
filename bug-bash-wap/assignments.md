# Bug Bash: SnapshotDiffApplier Multi-Branch Testing

**Branch:** `bug-bash-wap-2024-11`  
**Goal:** Test complex multi-branch scenarios in SnapshotDiffApplier  
**Duration:** [Set your deadline]  
**Total Tests:** 17 (Spark SQL)

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
| 11 | SQL | Interleaved WAP and Direct Commits on Same Branch | simbarashe | 🔲 | `results/sql-11-simbarashe.md` |
| 12 | SQL | Branch from WAP Snapshot Before Cherry-Pick | aastha | 🔲 | `results/sql-12-aastha.md` |
| 13 | SQL | Concurrent Branch Commits During Fast-Forward Window | jiefan | 🔲 | `results/sql-13-jiefan.md` |
| 14 | SQL | WAP Branch Target with Non-Existent Branch | zhe | 🔲 | `results/sql-14-zhe.md` |
| 15 | SQL | Snapshot Expiration with Cross-Branch Dependencies | kevin | 🔲 | `results/sql-15-kevin.md` |
| 16 | SQL | Rename Branch via Ref Management | junhao | 🔲 | `results/sql-16-junhao.md` |
| 17 | SQL | WAP ID Collision and Override | ruolin | 🔲 | `results/sql-17-ruolin.md` |

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
DROP TABLE IF EXISTS openhouse.u_openhouse.[your_test_table];
```


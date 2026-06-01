# Replication PoC — checklist

Branch `mkuchenb/replication-poc`. Pick up at first unchecked item. Never delete; update in place.

## Setup (one-time, keep warm)
- [x] Create isolated worktree `~/code/openhouse-replica`
- [x] Write PLAN.md + this checklist
- [ ] Scaffold harness in `infra/recipes/docker-compose/oh-hadoop-spark/`: `run_replicate.sh`,
      `replicate.scala` (spike), `seed_sources.scala`; add `/var/config` mount to spark-master
- [ ] Commit plan + harness scaffold
- [ ] Verify Docker images exist (or build once); `docker compose up` the `oh-hadoop-spark` stack
- [ ] Generate `openhouse.token`; confirm spark-shell reaches `openhouse-tables:8080` catalog

## Spike (gate)
- [ ] Does OH accept a committed snapshot referencing files WE placed (relocated DataFile, carried
      metrics)? Minimal append+commit through OH catalog, then SELECT. Record the answer in NOTES.md.

## P0 — append-only, rename, full history
- [ ] `seed_sources.scala`: append-only source table, multiple snapshots, (un)partitioned
- [ ] `replicateTable` floor (full materialization) + append deltas; stamp `source-snapshot-id` in summary
- [ ] Oracle passes: `dest AS OF mapped_i == src AS OF i` for all in-window i
- [ ] Idempotent re-run = no-op (0 files copied, 0 new snapshots)

## P1 — anti-join / incremental / resume
- [ ] 2nd run copies 0 files (manifest anti-join)
- [ ] Add a source snapshot → only it replays; dest extends
- [ ] Partial copy then re-run → converges (stateless)

## P2 — MOR + mutations
- [ ] Positional deletes (internal file_path remapped) — oracle passes
- [ ] Equality deletes — oracle passes
- [ ] Overwrite + compaction snapshot (no double-count) — oracle passes
- [ ] Schema/partition-spec evolution step — oracle passes

## Deferred (out of PoC)
- P3 cross-cluster; P4 schema-edge hardening + Spark-SQL `CALL` wrapper; `promote` op; intent/config layer.

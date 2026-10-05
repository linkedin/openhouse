# Replication PoC — checklist

Branch `mkuchenb/replication-poc`. Pick up at first unchecked item. Never delete; update in place.

## Setup (one-time, keep warm)
- [x] Create isolated worktree `~/code/openhouse-replica`
- [x] Write PLAN.md + this checklist
- [x] Scaffold harness in `infra/recipes/docker-compose/oh-hadoop-spark/`: `run_replicate.sh`,
      `replicate.scala` (spike), `seed_sources.scala`; add `/var/config` mount to spark-master
- [x] Commit plan + harness scaffold
- [x] Docker images exist + stack already warm (containers up 3-5 days; no build/up needed)
- [x] Token + runtime jar already in live container; spark-shell reaches `openhouse-tables:8080` catalog
      (used `docker cp` into live spark-master since running container predates the `/var/config` mount)

## Spike (gate)
- [x] **PASS** — OH accepts a committed snapshot referencing files WE placed (relocated DataFile, carried
      metrics). See NOTES.md 2026-06-01. Files are ORC; summary `source-snapshot-id` stamp round-trips.

## P0 — append-only, rename, full history  [GREEN 2026-06-01]
- [x] append-only source table, multiple snapshots (scenario self-seeds in replicate.scala)
- [x] `replicateTable` floor (full materialization) + append deltas; stamp `source-snapshot-id` in summary
- [x] Oracle passes: `dest AS OF mapped_i == src AS OF i` for all in-window i
- [x] Idempotent re-run = no-op (toReplay=0)

## P1 — anti-join / incremental / resume
- [x] 2nd run copies 0 files (summary reconcile)
- [x] Add a source snapshot → only it replays; dest extends
- [ ] Partial copy then re-run → converges (stateless) — not yet tested

## P2 — mutations
CoW [GREEN 2026-06-01] — OpenHouse default delete/overwrite is copy-on-write (no MOR delete files emitted).
- [x] CoW delete (DELETE WHERE) — oracle passes (overwrite replay +0 -1)
- [x] Overwrite (INSERT OVERWRITE, replace whole) — oracle passes (+2 -5)
- [x] Compaction (rewrite_data_files) — no-op on tiny table; add+remove path proven by overwrite
- [ ] MOR positional deletes — force `write.delete.mode=merge-on-read`, copy delete files, rewrite their
      internal `file_path` column to remapped paths — oracle passes
- [ ] Equality deletes — oracle passes
- [ ] Schema/partition-spec evolution step — oracle passes

## Deferred (out of PoC)
- P3 cross-cluster; P4 schema-edge hardening + Spark-SQL `CALL` wrapper; `promote` op; intent/config layer.

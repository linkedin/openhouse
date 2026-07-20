# Replication PoC — plan

Isolated worktree (`~/code/openhouse-replica`, branch `mkuchenb/replication-poc`) so `~/code/openhouse`
stays free for other work. Background + decision trail: `~/code/docs/gaas-analysis/` (`README.md`,
`options.md`).

## Goal
Prove, against **real local OpenHouse** (Dockerized), that we can replace GaaS replication with a plain
**Spark Scala function** that does an **eager, full-history (retention-bounded), physical** copy of an
OpenHouse table to a **renamed** destination, with **snapshot-based recovery**.

## Chosen design (decided; see options.md)
- **Eager physical copy**, self-contained dest. Rename allowed (dest table id ≠ source).
- **Full history, retention-bounded**: copy source snapshots within the source retention window. Oldest
  in-window snapshot = full materialization (floor); newer ones = deltas (append/delete/overwrite replayed).
- **No path/UUID/snapshot-ID identity**: copy files to new dest paths (deterministic prefix remap), rewrite
  manifests to relocate, **carry file metrics from the source manifest** (no re-stat).
- **Snapshot-based recovery**: stamp `source-snapshot-id` (+ source table ref) in **each dest snapshot's
  summary** (per-snapshot → recover to any point; a table property would only hold latest). Same stamp
  doubles as the reconcile watermark.
- **Commit through the OpenHouse catalog** (normal Iceberg commit path) — no direct metadata.json writes.

## Scope
- **In:** P0 append-only; P1 anti-join/idempotency/resume; P2 MOR (positional + equality deletes),
  overwrite, compaction snapshot, schema/spec evolution. Single cluster (same OH instance, rename).
- **Out (later):** P3 cross-cluster; P4 schema-edge hardening + Spark-SQL `CALL` wrapper; the `promote`
  recovery operation; intent/config/DB-pair layer; logical + lazy-branch options.

## Function shape (`replicate.scala`, loaded into spark-shell)
```
def replicateTable(source: String, dest: String, opts: Opts): Result
```
Pure-ish: load source via `Spark3Util.loadIcebergTable(spark, source)`; ensure dest table exists (create
with source's current schema/spec if absent); compute in-window source snapshots; reconcile against dest
(via `source-snapshot-id` summaries already present); for each snapshot to replay, in order:
1. enumerate its files (floor = full `useSnapshot(id).planFiles()`; delta = added/removed),
2. anti-join vs files already on dest (manifest-vs-manifest), copy the missing bytes to remapped paths
   (executors; carry metrics from the source `DataFile`),
3. author the matching dest snapshot via Iceberg API (`newAppend`/`newRowDelta`/`newOverwrite` with
   relocated `DataFiles.builder(spec).copy(src).withPath(new).build()`), `.set("source-snapshot-id", id)`
   in the snapshot summary, commit through the OH catalog.

## Spike FIRST (de-risk the one unknown)
Before building the loop: **does the OpenHouse catalog accept a committed snapshot whose DataFiles point at
paths we placed (relocated, carried metrics)?** Minimal `replicate.scala` path: create a tiny source table,
copy one data file to a dest table's location, `newAppend().appendFile(relocated).commit()` through the OH
catalog, `SELECT`. If OH rejects/re-validates/re-stats → learn the constraint before investing. This is the
gate; everything else is mechanical.

## Iteration loop (no teardown; iterate the Scala)
Stack: `infra/recipes/docker-compose/oh-hadoop-spark` (real `openhouse-tables` + housetables + MySQL/HTS +
HDFS + Spark). One-time `docker compose up`, kept warm.
- Harness lives in the recipe dir (mounted into `spark-master` at `/var/config` — added to this branch's
  compose). Files: `run_replicate.sh`, `replicate.scala`, `seed_sources.scala`, `openhouse.token`.
- **Per cycle:** edit `replicate.scala` on host → `docker exec local.spark-master /var/config/run_replicate.sh`
  → read oracle output. Cost = `spark-shell -i` spinup (~15–20s). **No `mint build`, no jar rebuild, no
  teardown** — the function is plain Scala over the catalog APIs. (Tighter loop later: persistent Livy
  session via `local.spark-livy`, `:load` the file.)
- **Reset per cycle:** `replicate.scala` drops+recreates the dest (or uses a fresh dest name). Sources seeded
  once via `seed_sources.scala`; HDFS/HTS persist.

## Oracle
Real OH time-travel, in the same shell: for every in-window snapshot `i`,
`SELECT * FROM openhouse.db.dest VERSION AS OF <mapped_i>` ≡ `... src VERSION AS OF i` as a **multiset**
(`exceptAll` both ways → 0). Plus a PITR check: resolve a historical dest snapshot via its
`source-snapshot-id` summary, time-travel, compare. Plus metadata checks (record counts, file set).

## Phases & acceptance
- **P0 — append-only, rename, full history.** Floor + appends; summary stamp. Oracle passes for all in-window
  snapshots; re-run is a no-op (idempotent).
- **P1 — anti-join / incremental / resume.** 2nd run copies 0 files; adding a source snapshot replays only it;
  partial-copy-then-rerun converges.
- **P2 — MOR + mutations.** Positional deletes (internal `file_path` remapped) + equality deletes + overwrite
  + compaction snapshot (no double-count) + a schema/spec evolution step. Oracle passes for all.

## Setup prerequisites (one-time)
- Docker images for OH services + Spark (built from this repo). Verify before first run.
- `openhouse-spark-runtime_2.12-*-all.jar` present in the spark image (catalog client + extensions).
- An auth token at `/var/config/openhouse.token` (`common/oauth/fetch_token.sh`).

## Open question
- Recovery granularity: **snapshot-summary** (per-snapshot → true PITR; default) vs table property
  (recover-to-latest only). Defaulting to summary unless decided otherwise.

## Working notes / queries
Committed alongside in `NOTES.md` (running log of what was tried, OH responses, dead ends) per request.

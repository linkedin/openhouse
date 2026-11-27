# Replication PoC — working notes / queries log

Running log of what was tried, what OpenHouse did, dead ends, and decisions made during iteration.
Newest entries at top.

---

## Setup
- Worktree `~/code/openhouse-replica` @ `mkuchenb/replication-poc` off `openhouse` main (702a043d).
- Harness: `infra/recipes/docker-compose/oh-hadoop-spark/{run_replicate.sh, replicate.scala, seed_sources.scala}`.
  Added `./:/var/config/` mount to `spark-master` so the scala is live in-container (edit on host → re-run).
- Iteration: `docker compose up` once; then `docker exec local.spark-master /var/config/run_replicate.sh`.

## Open / to-verify on first run
- Does the OH spark image already contain `openhouse-spark-runtime_2.12-latest-all.jar`? (referenced by
  run_replicate.sh; may need a one-time build/stage.)
- Auth token: generate `/var/config/openhouse.token` (common/oauth/fetch_token.sh) before first shell.
- Does `openhouse.<db>` need to pre-exist, or is CREATE TABLE enough?
- **GATE:** does OH accept the relocated-DataFile append+commit (spike RESULT line)?

## Log

### P2 CoW — GREEN + functional refactor (2026-06-01)
Replay now handles removed files (CoW). `p2_delete`, `p2_overwrite`, `p2_compaction` all PASS, history
still intact at every snapshot.
- **Finding:** OpenHouse here writes deletes/overwrites **copy-on-write** — no `MOR delete file` NOTE
  fired on default `DELETE`/`INSERT OVERWRITE`. So MOR positional deletes only arise if a table opts into
  `write.delete.mode=merge-on-read` (must force it to exercise the internal-file_path rewrite). Compaction
  no-ops on the tiny table (toReplay=0) but the overwrite case already exercises add+remove.
- **Refactor (per review):** cases modeled explicitly — `Replay(add, remove)` from `planReplay`
  (floor vs delta), op chosen by `dels match { Nil => append; ds => overwrite }` (no nested if/else).
  Functional: `Option(parentId).map` chain walk, `Option(..).filter(_.nonEmpty).foreach` for the MOR note,
  `Try(loadTbl).getOrElse` for ensureDest (no existence check), `results.forall(identity)`, `zipWithIndex`
  instead of a mutable `bootstrapped` var.
Remaining: P1 resume-after-partial-copy; P2 MOR (force merge-on-read → positional-delete path rewrite) +
schema/partition-spec evolution.

### P0 + P1(2/3) — GREEN (2026-06-01)
Real `replicateTable` (full history, floor + append-deltas, summary reconcile) vs `db.p0_append`→`p0_replica`.
- **P0 full-history bootstrap:** chain of 3 snaps replayed (floor full-state=2 files, then +1, +2). Oracle
  PASS at every snapshot (2/3/5 rows accumulating). Each dest snapshot stamped with its `source-snapshot-id`.
- **P1 idempotent:** re-run → `toReplay=0` (reconcile via dest summaries), oracle PASS. True no-op.
- **P1 incremental:** added snap 4 → only it replayed (+1), oracle PASS across all 4 (6 rows).
Mechanics confirmed: floor = `newScan().useSnapshot(id).planFiles()`; delta = `snapshot.addedDataFiles(io)`;
relocate carries metrics; time-travel oracle via `spark.read.format("iceberg").option("snapshot-id",id)`
(Spark 3.1 → no `VERSION AS OF` SQL, DataFrame reader option works). Reconcile/recovery key = the same
`source-snapshot-id` summary. Driver-side file copy (executor distribution is a perf TODO, not correctness).
Remaining P1: resume-after-partial-copy. Next: P2 (removed-files/overwrite, MOR positional-delete rewrite,
compaction, schema evolution) — needs the replay to handle removedDataFiles + delete files, not just appends.

### Spike — GATE PASSED (2026-06-01)
OH accepts a committed snapshot referencing files WE placed. Spike: created `db.src_spike` (3 rows, 2
data files), copied both ORC files to `db.dst_spike`'s location (`<destLoc>/data/<sameName>`), rebuilt
each via `DataFiles.builder(dst.spec()).copy(df).withPath(new).build()` (metrics carried — 336B/1rec,
341B/2rec), `dst.newAppend().appendFile(...).set("source-snapshot-id", srcSnap).commit()` through the OH
catalog. Oracle: `dst == src` (`[1,a],[2,b],[3,c]`). Summary stamp present on dest snapshot.
Implications confirmed:
- Relocation + carried-metrics + commit-through-catalog is accepted by real OH (no re-stat/rejection).
- Files are **ORC** here (not parquet); paths `/data/openhouse/db/<table>-<uuid>/data/<file>`.
- `source-snapshot-id` summary stamp round-trips → recovery index mechanism viable.
Harness reality: running stack predates the `/var/config` mount, so used `docker cp` of run_replicate.sh
+ replicate.scala into live `local.spark-master`; token + `openhouse-spark-runtime_*.jar` already present.
`docker exec ... run_replicate.sh </dev/null` runs spark-shell `-i` then exits cleanly.

(append newer entries above this line)

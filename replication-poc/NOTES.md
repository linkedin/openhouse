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
(append entries here as the spike/phases run)

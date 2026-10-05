# OpenHouse multi-cluster replication recipe

This recipe provides LocalHadoopClusterA and LocalHadoopClusterB with separate
Tables services, House Tables services, MySQL databases, and HDFS namespaces.
The Spark worker can reach both clusters and the `openhouse_a` and `openhouse_b`
catalogs are configured in the jobs service.

Start the recipe from this directory when its default host ports are available:

```sh
docker compose up -d --build
```

The replica write authorization regression starts only the cluster-B and Spark
services it needs in an isolated Compose project, with host ports remapped away
from the common `oh-hadoop-spark` recipe. It does not stop or modify another
Compose project and does not use the jobs scheduler:

```sh
./test-replica-write-auth.sh
```

The script creates an empty `REPLICA_TABLE` in cluster B using the local
`openhouse` admin identity, then attempts an ordinary Spark insert as
`u_tableowner`. It expects the insert to be denied and verifies that the table
remains empty. It removes only the test table when finished and stops only its
own Compose project if the script started it. Set the
`OH_MULTICLUSTER_*_PORT` variables to override any published host ports.

## Reference replication end-to-end test

The reference Spark job is a one-shot application, not a production scheduler
integration. The end-to-end test builds and starts an isolated Compose project,
creates a source table and an empty `REPLICA_TABLE`, registers their immutable
identities through the Tables API, then submits the Spark job after each of
three source commits. It renames the source in the OpenHouse catalog, writes a
fourth commit, and submits the job again. The script verifies the destination
data, preserved replica identity under the renamed locator, absence of the old
destination locator, and matching source/destination snapshots and table
versions in the final replication checkpoint:

```sh
./test-reference-replication.sh
```

This script always uses a unique Compose project name and only tears down that
project and its volumes. Its default published ports use the `28000` range;
set `OH_MULTICLUSTER_TEST_PORT_BASE` to move the range, or set individual
`OH_MULTICLUSTER_*_PORT` variables to override published ports.

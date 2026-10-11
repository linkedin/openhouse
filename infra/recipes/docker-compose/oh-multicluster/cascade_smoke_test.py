#!/usr/bin/env python3
"""Exercise service-owned multi-cluster rename/drop over real Dockerized Tables APIs."""

import json
import os
import sys
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_URLS = {
    "a": os.environ.get("CASCADE_SMOKE_URL_A", "http://localhost:18000"),
    "b": os.environ.get("CASCADE_SMOKE_URL_B", "http://localhost:18010"),
}
TOKEN_FILES = {
    user: os.environ["CASCADE_SMOKE_TOKEN_" + user.upper()]
    for user in ("openhouse", "u_tableowner")
}
TOKENS = {
    user: open(path, encoding="utf-8").read().strip()
    for user, path in TOKEN_FILES.items()
}
DATABASE_ID = "cascade_smoke"
SUFFIX = uuid.uuid4().hex[:12]
SOURCE_CLUSTER_ID = "LocalHadoopClusterA"
DESTINATION_CLUSTER_ID = "LocalHadoopClusterB"
created_tables = {"a": set(), "b": set()}


def call(cluster, method, path, user="openhouse", body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {"Authorization": "Bearer " + TOKENS[user]}
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = Request(
        BASE_URLS[cluster] + path, data=data, headers=headers, method=method
    )
    try:
        with urlopen(request, timeout=20) as response:
            return response.status, response.read().decode("utf-8")
    except HTTPError as error:
        return error.code, error.read().decode("utf-8")
    except URLError as error:
        raise AssertionError(f"{method} {path} on cluster {cluster} failed: {error}")


def expect(label, cluster, method, path, status, user="openhouse", body=None):
    actual, content = call(cluster, method, path, user=user, body=body)
    if actual != status:
        raise AssertionError(
            f"{label}: expected HTTP {status}, got {actual}: {content}"
        )
    print(f"PASS {label}: HTTP {actual}")
    return json.loads(content) if content else None


def table_path(table_id):
    return f"/v1/databases/{DATABASE_ID}/tables/{table_id}"


def grant_database_role(cluster, role, principal="u_tableowner"):
    expect(
        f"grant {role} on cluster {cluster}",
        cluster,
        "PATCH",
        f"/v1/databases/{DATABASE_ID}/aclPolicies",
        204,
        body={"role": role, "principal": principal, "operation": "GRANT"},
    )


def create_primary(table_id):
    body = {
        "tableId": table_id,
        "databaseId": DATABASE_ID,
        "clusterId": SOURCE_CLUSTER_ID,
        "schema": (
            '{"type":"struct","fields":'
            '[{"id":1,"required":true,"name":"id","type":"long"}]}'
        ),
        "tableProperties": {},
        "baseTableVersion": "INITIAL_VERSION",
        "tableType": "PRIMARY_TABLE",
        "policies": {
            "replication": {
                "config": [
                    {"destination": DESTINATION_CLUSTER_ID, "interval": "12H"}
                ]
            }
        },
    }
    result = expect(
        f"create source {table_id}",
        "a",
        "POST",
        f"/v1/databases/{DATABASE_ID}/tables",
        201,
        body=body,
    )
    created_tables["a"].add(table_id)
    return result


def create_replica(table_id, table_uuid):
    body = {
        "tableId": table_id,
        "databaseId": DATABASE_ID,
        "clusterId": DESTINATION_CLUSTER_ID,
        "schema": (
            '{"type":"struct","fields":'
            '[{"id":1,"required":true,"name":"id","type":"long"}]}'
        ),
        "tableProperties": {"openhouse.tableUUID": table_uuid},
        "baseTableVersion": "INITIAL_VERSION",
        "tableType": "REPLICA_TABLE",
    }
    result = expect(
        f"create destination replica {table_id}",
        "b",
        "POST",
        f"/v1/databases/{DATABASE_ID}/tables",
        201,
        body=body,
    )
    created_tables["b"].add(table_id)
    return result


def register_replication_association(source, replica, source_table_id, replica_table_id):
    expect(
        f"register source-to-replica generation for {source_table_id}",
        "b",
        "PUT",
        "/v1/replication/destinations",
        200,
        body={
            "sourceClusterId": SOURCE_CLUSTER_ID,
            "sourceTableUUID": source["tableUUID"],
            "sourceCreationTime": source["creationTime"],
            "sourceDatabaseId": DATABASE_ID,
            "sourceTableId": source_table_id,
            "destinationClusterId": DESTINATION_CLUSTER_ID,
            "destinationTableUUID": replica["tableUUID"],
            "destinationCreationTime": replica["creationTime"],
            "destinationDatabaseId": DATABASE_ID,
            "destinationTableId": replica_table_id,
            "expectedVersion": 0,
        },
    )


def verify_missing(cluster, table_id, label):
    expect(label, cluster, "GET", table_path(table_id), 404)


def test_authorization_and_fanout():
    source_id = "cascade_" + SUFFIX
    renamed_id = source_id + "_renamed"
    source = create_primary(source_id)
    replica = create_replica(source_id, uuid.uuid4().hex)
    register_replication_association(source, replica, source_id, source_id)

    for role in ("TABLE_CREATOR", "TABLE_ADMIN"):
        grant_database_role("a", role)

    expect(
        "remote permission denial blocks source rename",
        "a",
        "PATCH",
        table_path(source_id)
        + f"/rename?toDatabaseId={DATABASE_ID}&toTableId={renamed_id}",
        403,
        user="u_tableowner",
    )
    for cluster, label in (("a", "source"), ("b", "replica")):
        expect(f"{label} is unchanged after denied rename", cluster, "GET", table_path(source_id), 200)
        verify_missing(cluster, renamed_id, f"{label} denied target does not exist")

    for role in ("TABLE_CREATOR", "TABLE_ADMIN"):
        grant_database_role("b", role)

    expect(
        "authorized source rename cascades",
        "a",
        "PATCH",
        table_path(source_id)
        + f"/rename?toDatabaseId={DATABASE_ID}&toTableId={renamed_id}",
        204,
        user="u_tableowner",
    )
    created_tables["a"].discard(source_id)
    created_tables["a"].add(renamed_id)
    created_tables["b"].discard(source_id)
    created_tables["b"].add(renamed_id)

    for cluster, label in (("a", "source"), ("b", "replica")):
        verify_missing(cluster, source_id, f"{label} old name is gone")
        expect(f"{label} new name exists", cluster, "GET", table_path(renamed_id), 200)

    direct_renamed_id = renamed_id + "_direct"
    expect(
        "direct replica rename is allowed without cascading",
        "b",
        "PATCH",
        table_path(renamed_id)
        + f"/rename?toDatabaseId={DATABASE_ID}&toTableId={direct_renamed_id}",
        204,
    )
    created_tables["b"].discard(renamed_id)
    created_tables["b"].add(direct_renamed_id)
    verify_missing("b", renamed_id, "replica old name is gone after direct rename")
    expect(
        "direct replica drop is allowed without cascading",
        "b",
        "DELETE",
        table_path(direct_renamed_id),
        204,
    )
    created_tables["b"].discard(direct_renamed_id)
    verify_missing("b", direct_renamed_id, "replica is gone after direct drop")

    expect(
        "authorized source drop cascades",
        "a",
        "DELETE",
        table_path(renamed_id),
        204,
        user="u_tableowner",
    )
    created_tables["a"].discard(renamed_id)
    created_tables["b"].discard(renamed_id)
    for cluster, label in (("a", "source"), ("b", "replica")):
        verify_missing(cluster, renamed_id, f"{label} is gone after source drop")


def test_missing_replicas_are_retry_safe():
    rename_source_id = "cascade_missing_rename_" + SUFFIX
    rename_target_id = rename_source_id + "_renamed"
    source = create_primary(rename_source_id)
    verify_missing("b", rename_source_id, "rename destination replica is absent")
    expect(
        "rename fails closed without a source-to-replica association",
        "a",
        "PATCH",
        table_path(rename_source_id)
        + f"/rename?toDatabaseId={DATABASE_ID}&toTableId={rename_target_id}",
        403,
    )
    expect("source is unchanged without association", "a", "GET", table_path(rename_source_id), 200)
    verify_missing("a", rename_target_id, "source target absent without association")
    replica = create_replica(rename_source_id, uuid.uuid4().hex)
    register_replication_association(source, replica, rename_source_id, rename_source_id)
    expect(
        "delete destination replica but retain its association",
        "b",
        "DELETE",
        table_path(rename_source_id),
        204,
    )
    created_tables["b"].discard(rename_source_id)
    expect(
        "rename succeeds when the associated destination replica is absent",
        "a",
        "PATCH",
        table_path(rename_source_id)
        + f"/rename?toDatabaseId={DATABASE_ID}&toTableId={rename_target_id}",
        204,
    )
    created_tables["a"].discard(rename_source_id)
    created_tables["a"].add(rename_target_id)
    expect("source rename committed", "a", "GET", table_path(rename_target_id), 200)
    verify_missing("b", rename_target_id, "associated missing replica remains absent after rename")
    expect(
        "drop succeeds when the associated destination replica is absent",
        "a",
        "DELETE",
        table_path(rename_target_id),
        204,
    )
    created_tables["a"].discard(rename_target_id)
    verify_missing("a", rename_target_id, "source gone after missing-replica drop")
    verify_missing("b", rename_target_id, "destination still absent after drop")

    drop_source_id = "cascade_missing_drop_" + SUFFIX
    drop_source = create_primary(drop_source_id)
    verify_missing("b", drop_source_id, "drop destination replica is absent")
    expect(
        "drop fails closed without a source-to-replica association",
        "a",
        "DELETE",
        table_path(drop_source_id),
        403,
    )
    expect("source is unchanged without association", "a", "GET", table_path(drop_source_id), 200)
    drop_replica = create_replica(drop_source_id, uuid.uuid4().hex)
    register_replication_association(drop_source, drop_replica, drop_source_id, drop_source_id)
    expect(
        "delete destination replica but retain its association",
        "b",
        "DELETE",
        table_path(drop_source_id),
        204,
    )
    created_tables["b"].discard(drop_source_id)
    expect(
        "drop succeeds when the associated destination replica is absent",
        "a",
        "DELETE",
        table_path(drop_source_id),
        204,
    )
    created_tables["a"].discard(drop_source_id)
    verify_missing("a", drop_source_id, "source gone after missing-replica drop")
    verify_missing("b", drop_source_id, "destination still absent after drop")


def cleanup():
    for cluster in ("a", "b"):
        for table_id in created_tables[cluster]:
            status, content = call(cluster, "DELETE", table_path(table_id))
            if status not in (204, 404):
                print(
                    f"WARNING unable to clean {cluster}/{table_id}: "
                    f"HTTP {status}: {content}",
                    file=sys.stderr,
                )


def main():
    try:
        test_authorization_and_fanout()
        test_missing_replicas_are_retry_safe()
        print("Docker multi-cluster cascade smoke test passed.")
    finally:
        cleanup()


if __name__ == "__main__":
    main()

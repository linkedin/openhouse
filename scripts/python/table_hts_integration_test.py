"""Cross-service checks against the Docker Tables Service and real HTS."""

import json
import os
import uuid

import requests

TABLES_HOST = os.environ.get('TABLES_HOST', 'http://localhost:8000').rstrip('/')
HTS_HOST = os.environ.get('HTS_HOST', 'http://localhost:8001').rstrip('/')
TIMEOUT = 60
INITIAL_VERSION = 'INITIAL_VERSION'


def assert_status(response, expected):
    assert response.status_code == expected, (
        f"Expected HTTP {expected}, got {response.status_code}: {response.text}")


def table_payload(database, table, version=INITIAL_VERSION, properties=None):
    return {
        'databaseId': database,
        'tableId': table,
        'clusterId': 'LocalFSCluster',
        'tableType': 'PRIMARY_TABLE',
        'baseTableVersion': version,
        'schema': json.dumps({
            'type': 'struct',
            'fields': [{'id': 1, 'required': True, 'name': 'id', 'type': 'string'}],
        }),
        'tableProperties': properties if properties is not None else {'test': 'created'},
    }


def read_hts(kind, database, table):
    return requests.get(
        f'{HTS_HOST}/hts/{kind}',
        params={'databaseId': database, 'tableId': table}, timeout=TIMEOUT)


def assert_table_pointer(database, table, table_response):
    neutral = read_hts('entities', database, table)
    typed = read_hts('tables', database, table)
    assert_status(neutral, 200)
    assert_status(typed, 200)
    entity = neutral.json()['entity']
    assert entity == typed.json()['entity'], (entity, typed.json())
    assert entity['entityType'] == 'TABLE', entity
    assert entity['metadataLocation'] == table_response['tableLocation'].removeprefix('file:'), (
        entity, table_response)
    assert_status(read_hts('views', database, table), 404)
    return entity


def cleanup_table(url, headers):
    response = requests.delete(url, headers=headers, params={'purge': True}, timeout=TIMEOUT)
    assert response.status_code in (204, 404), (
        f"Table cleanup failed: {response.status_code} {response.text}")


def test_table_lifecycle(headers):
    database = f'tables_hts_{uuid.uuid4().hex}'
    table = 'lifecycle'
    collection = f'{TABLES_HOST}/v1/databases/{database}/tables'
    url = f'{collection}/{table}'
    try:
        created = requests.post(
            collection, json=table_payload(database, table), headers=headers, timeout=TIMEOUT)
        assert_status(created, 201)
        before = assert_table_pointer(database, table, created.json())

        updated = requests.put(
            url,
            json=table_payload(database, table, created.json()['tableLocation'],
                               {**created.json()['tableProperties'], 'test': 'updated'}),
            headers=headers, timeout=TIMEOUT)
        assert_status(updated, 200)
        after = assert_table_pointer(database, table, updated.json())
        assert after['metadataLocation'] != before['metadataLocation'], (before, after)

        stale = requests.put(
            url,
            json=table_payload(database, table, created.json()['tableLocation'],
                               {**updated.json()['tableProperties'], 'test': 'stale'}),
            headers=headers, timeout=TIMEOUT)
        assert_status(stale, 409)
        assert_table_pointer(database, table, updated.json())

        fetched = requests.get(url, headers=headers, timeout=TIMEOUT)
        assert_status(fetched, 200)
        assert fetched.json()['tableProperties']['test'] == 'updated', fetched.json()

        deleted = requests.delete(
            url, headers=headers, params={'purge': True}, timeout=TIMEOUT)
        assert_status(deleted, 204)
        assert_status(read_hts('entities', database, table), 404)
        assert_status(requests.get(url, headers=headers, timeout=TIMEOUT), 404)
        print('Tables Service create/update/stale-write/delete persists correctly in real HTS')
    finally:
        cleanup_table(url, headers)


def test_view_collision_and_name_reuse(headers):
    database = f'tables_hts_{uuid.uuid4().hex}'
    table = 'occupied'
    collection = f'{TABLES_HOST}/v1/databases/{database}/tables'
    url = f'{collection}/{table}'
    key = {'databaseId': database, 'tableId': table}
    view_location = f'/tmp/{database}/view.metadata.json'
    view_created = False
    try:
        seeded = requests.put(
            f'{HTS_HOST}/hts/views',
            json={'entity': {
                **key, 'tableVersion': INITIAL_VERSION,
                'metadataLocation': view_location, 'storageType': 'local',
            }}, timeout=TIMEOUT)
        assert_status(seeded, 201)
        view_created = True
        before = seeded.json()['entity']

        for method, target in ((requests.post, collection), (requests.put, url)):
            conflict = method(
                target, json=table_payload(database, table), headers=headers, timeout=TIMEOUT)
            assert_status(conflict, 409)
            assert conflict.json()['message'] == f'VIEW {database}.{table} already exists', (
                conflict.json())
            occupant = read_hts('entities', database, table)
            assert_status(occupant, 200)
            assert occupant.json()['entity'] == before, occupant.json()
            assert_status(read_hts('tables', database, table), 404)

        assert_status(requests.get(url, headers=headers, timeout=TIMEOUT), 404)
        removed = requests.delete(f'{HTS_HOST}/hts/views', params=key, timeout=TIMEOUT)
        assert_status(removed, 204)
        view_created = False

        created = requests.post(
            collection, json=table_payload(database, table), headers=headers, timeout=TIMEOUT)
        assert_status(created, 201)
        assert_table_pointer(database, table, created.json())
        print('Real HTS view blocks table create/upsert without mutation; deleted name is reusable')
    finally:
        if view_created:
            removed = requests.delete(f'{HTS_HOST}/hts/views', params=key, timeout=TIMEOUT)
            assert_status(removed, 204)
        cleanup_table(url, headers)


def run_tests(token):
    headers = {'Authorization': f'Bearer {token.strip()}', 'Content-Type': 'application/json'}
    test_table_lifecycle(headers)
    test_view_collision_and_name_reuse(headers)
